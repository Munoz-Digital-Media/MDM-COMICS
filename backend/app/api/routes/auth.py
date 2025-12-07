"""
Authentication routes

P1-3: Rate limited to prevent brute force attacks
P1-5: HttpOnly cookies + CSRF protection
P2-8: Token revocation support (logout, logout all devices)

User Management System v1.0.0:
- Account lockout protection
- Audit logging for security events
- Session tracking
- Password policy enforcement
- Password reset flow
- Email verification flow
"""
import os
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel, EmailStr

from app.core.database import get_db
from app.core.security import (
    verify_password,
    get_password_hash,
    create_access_token,
    create_refresh_token,
    decode_token,
)
from app.core.rate_limit import limiter
from app.core.config import settings
from app.core.cookies import (
    set_auth_cookies,
    clear_auth_cookies,
    get_refresh_token_from_cookie,
)
from app.core.token_blacklist import token_blacklist
from app.core.password_policy import PasswordPolicy
from app.core.account_lockout import AccountLockoutPolicy
from app.models.user import User
from app.models.user_audit_log import AuditAction
from app.schemas.user import UserCreate, UserResponse, UserLogin
from app.schemas.auth import Token, RefreshToken
from app.api.deps import get_current_user
from app.services.audit_service import AuditService
from app.services.session_service import SessionService
from app.services.user_service import UserService
from app.services.role_service import RoleService

router = APIRouter()

# Admin setup secret - set this in Railway env vars
ADMIN_SETUP_SECRET = os.getenv("ADMIN_SETUP_SECRET")


def get_client_ip(request: Request) -> Optional[str]:
    """Extract client IP from request."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else None


def get_user_agent(request: Request) -> Optional[str]:
    """Extract user agent from request."""
    return request.headers.get("user-agent")


class AdminSetupRequest(BaseModel):
    email: str
    secret: str


class PasswordResetRequest(BaseModel):
    """Request password reset."""
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    """Complete password reset with token."""
    token: str
    new_password: str


class PasswordChange(BaseModel):
    """Change password (requires current password)."""
    current_password: str
    new_password: str


class EmailVerificationRequest(BaseModel):
    """Request email verification resend."""
    pass  # Uses current user's email


class EmailVerificationConfirm(BaseModel):
    """Complete email verification."""
    token: str


class PasswordStrengthResponse(BaseModel):
    """Password strength check response."""
    score: int
    label: str
    requirements: str


class AuthResponse(BaseModel):
    """
    P1-5: Auth response with optional tokens.

    For cookie-based auth, tokens are set as HttpOnly cookies.
    access_token/refresh_token are still returned for backwards compatibility
    with mobile apps and API clients that prefer header-based auth.
    """
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    csrf_token: Optional[str] = None  # For cookie-based auth


@router.post("/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED)
@limiter.limit(settings.RATE_LIMIT_AUTH)
async def register(
    request: Request,
    response: Response,
    user_data: UserCreate,
    db: AsyncSession = Depends(get_db)
):
    """
    Register a new user and return tokens.

    P1-5: Sets HttpOnly cookies for web clients.
    Also returns tokens in body for API/mobile compatibility.

    User Management System v1.0.0:
    - Password policy enforcement
    - Audit logging
    - Auto-assign customer role
    """
    # Validate password against policy
    is_valid, errors = PasswordPolicy.validate(
        user_data.password,
        email=user_data.email,
        name=user_data.name,
    )
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": "Password does not meet requirements", "errors": errors}
        )

    # Check if email exists
    result = await db.execute(select(User).where(User.email == user_data.email))
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Create user
    user = User(
        email=user_data.email,
        name=user_data.name,
        hashed_password=get_password_hash(user_data.password)
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    # Assign default customer role
    role_service = RoleService(db)
    try:
        await role_service.ensure_user_has_role(user.id, "customer")
    except ValueError:
        pass  # Role may not exist yet, safe to continue

    # Log registration
    audit_service = AuditService(db)
    await audit_service.log(
        action=AuditAction.USER_CREATED,
        user_id=user.id,
        target_user_id=user.id,
        resource_type="user",
        resource_id=str(user.id),
        details={"email": user.email, "source": "self_registration"},
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request),
    )

    # Generate tokens
    access_token = create_access_token({"sub": str(user.id)})
    refresh_token = create_refresh_token({"sub": str(user.id)})

    # P1-5: Set HttpOnly cookies
    csrf_token = set_auth_cookies(response, request, access_token, refresh_token)

    await db.commit()

    # Return tokens in body for API compatibility
    return AuthResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        csrf_token=csrf_token,
    )


@router.post("/login", response_model=AuthResponse)
@limiter.limit(settings.RATE_LIMIT_AUTH)
async def login(
    request: Request,
    response: Response,
    credentials: UserLogin,
    db: AsyncSession = Depends(get_db)
):
    """
    Login and get access token.

    P1-5: Sets HttpOnly cookies for web clients.
    Also returns tokens in body for API/mobile compatibility.

    User Management System v1.0.0:
    - Account lockout protection
    - Audit logging for login attempts
    - Session tracking
    """
    client_ip = get_client_ip(request)
    user_agent = get_user_agent(request)
    audit_service = AuditService(db)

    result = await db.execute(select(User).where(User.email == credentials.email))
    user = result.scalar_one_or_none()

    # Check if account is locked
    if user and AccountLockoutPolicy.is_locked(user):
        lockout_msg = AccountLockoutPolicy.get_lockout_message(user)
        await audit_service.log(
            action=AuditAction.LOGIN_FAILED,
            target_user_id=user.id,
            details={"reason": "account_locked", "email": credentials.email},
            ip_address=client_ip,
            user_agent=user_agent,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=lockout_msg or "Account temporarily locked"
        )

    # Validate credentials
    if not user or not verify_password(credentials.password, user.hashed_password):
        # Log failed attempt
        if user:
            lockout_result = await AccountLockoutPolicy.record_failed_attempt(db, user, client_ip)
            await audit_service.log(
                action=AuditAction.LOGIN_FAILED,
                target_user_id=user.id,
                details={
                    "reason": "invalid_password",
                    "attempts": lockout_result["attempts"],
                    "locked": lockout_result["locked"],
                },
                ip_address=client_ip,
                user_agent=user_agent,
            )
            if lockout_result["locked"]:
                await audit_service.log(
                    action=AuditAction.ACCOUNT_LOCKED,
                    target_user_id=user.id,
                    details={"locked_until": str(lockout_result["locked_until"])},
                    ip_address=client_ip,
                    user_agent=user_agent,
                )
        else:
            await audit_service.log(
                action=AuditAction.LOGIN_FAILED,
                details={"reason": "user_not_found", "email": credentials.email},
                ip_address=client_ip,
                user_agent=user_agent,
            )

        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    # Check if account is active
    if not user.is_active:
        await audit_service.log(
            action=AuditAction.LOGIN_FAILED,
            target_user_id=user.id,
            details={"reason": "account_disabled"},
            ip_address=client_ip,
            user_agent=user_agent,
        )
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is disabled"
        )

    # Check if soft-deleted
    if user.is_deleted:
        await audit_service.log(
            action=AuditAction.LOGIN_FAILED,
            target_user_id=user.id,
            details={"reason": "account_deleted"},
            ip_address=client_ip,
            user_agent=user_agent,
        )
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account not found"
        )

    # Successful login - reset lockout
    await AccountLockoutPolicy.record_successful_login(db, user)

    # Generate tokens
    access_token = create_access_token({"sub": str(user.id)})
    refresh_token = create_refresh_token({"sub": str(user.id)})

    # Create session
    session_service = SessionService(db)
    session, _ = await session_service.create_session(
        user_id=user.id,
        ip_address=client_ip,
        user_agent=user_agent,
        device_info=user_agent[:100] if user_agent else None,
    )

    # Log successful login
    await audit_service.log(
        action=AuditAction.LOGIN_SUCCESS,
        user_id=user.id,
        target_user_id=user.id,
        details={"session_id": session.id},
        ip_address=client_ip,
        user_agent=user_agent,
        session_id=session.id,
    )

    # P1-5: Set HttpOnly cookies
    csrf_token = set_auth_cookies(response, request, access_token, refresh_token)

    await db.commit()

    return AuthResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        csrf_token=csrf_token,
    )


@router.post("/logout")
async def logout(request: Request, response: Response):
    """
    P1-5: Logout and clear auth cookies.

    For cookie-based auth, this clears all auth cookies.
    For header-based auth, the client should discard the token.
    """
    clear_auth_cookies(response, request)
    return {"message": "Logged out successfully"}


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    """Get current authenticated user"""
    return current_user


@router.post("/logout-all")
async def logout_all_devices(
    request: Request,
    response: Response,
    current_user: User = Depends(get_current_user)
):
    """
    P2-8: Logout from all devices.

    Revokes all tokens issued before now for this user.
    Clears current session cookies.
    """
    # Revoke all tokens for this user
    token_blacklist.revoke_all_user_tokens(current_user.id)

    # Clear current session cookies
    clear_auth_cookies(response, request)

    return {
        "message": "Logged out from all devices successfully",
        "user_id": current_user.id
    }


@router.post("/refresh", response_model=AuthResponse)
async def refresh_token_endpoint(
    request: Request,
    response: Response,
    token_data: Optional[RefreshToken] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Get new access token using refresh token.

    P1-5: Supports both cookie-based and body-based refresh tokens.
    - For web clients: reads refresh token from HttpOnly cookie
    - For API/mobile: reads from request body

    Always returns new tokens and sets new cookies.
    """
    # Try to get refresh token from cookie first, then body
    refresh_token_value = get_refresh_token_from_cookie(request)
    if not refresh_token_value and token_data:
        refresh_token_value = token_data.refresh_token

    if not refresh_token_value:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token required"
        )

    payload = decode_token(refresh_token_value)

    if not payload or payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token"
        )

    user_id = payload.get("sub")
    result = await db.execute(select(User).where(User.id == int(user_id)))
    user = result.scalar_one_or_none()

    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive"
        )

    # Generate new tokens
    access_token = create_access_token({"sub": str(user.id)})
    new_refresh_token = create_refresh_token({"sub": str(user.id)})

    # P1-5: Set new HttpOnly cookies
    csrf_token = set_auth_cookies(response, request, access_token, new_refresh_token)

    return AuthResponse(
        access_token=access_token,
        refresh_token=new_refresh_token,
        csrf_token=csrf_token,
    )


@router.post("/admin-setup")
async def setup_admin(admin_request: AdminSetupRequest, db: AsyncSession = Depends(get_db)):
    """One-time admin setup - requires secret key"""
    if not ADMIN_SETUP_SECRET:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin setup not configured"
        )
    if admin_request.secret != ADMIN_SETUP_SECRET:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid setup secret"
        )

    result = await db.execute(select(User).where(User.email == admin_request.email))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found. Please register first."
        )

    user.is_admin = True
    await db.commit()

    return {"message": f"User {admin_request.email} is now an admin"}


# ============================================================
# Password Management Endpoints
# ============================================================

@router.post("/password-reset/request")
@limiter.limit(settings.RATE_LIMIT_AUTH)
async def request_password_reset(
    request: Request,
    data: PasswordResetRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Request a password reset token.

    Always returns success to prevent email enumeration.
    Token is sent via email (not returned in response).
    """
    user_service = UserService(db)
    audit_service = AuditService(db)

    token = await user_service.create_password_reset_token(
        email=data.email,
        ip_address=get_client_ip(request),
    )

    if token:
        # Log the request
        user = await user_service.get_user_by_email(data.email)
        if user:
            await audit_service.log(
                action=AuditAction.PASSWORD_RESET_REQUESTED,
                target_user_id=user.id,
                details={"email": data.email},
                ip_address=get_client_ip(request),
                user_agent=get_user_agent(request),
            )

        # TODO: Send email with token
        # For now, token would be sent via email service
        # email_service.send_password_reset(data.email, token)

    await db.commit()

    # Always return success to prevent email enumeration
    return {"message": "If an account with that email exists, a reset link has been sent"}


@router.post("/password-reset/confirm")
@limiter.limit(settings.RATE_LIMIT_AUTH)
async def confirm_password_reset(
    request: Request,
    data: PasswordResetConfirm,
    db: AsyncSession = Depends(get_db)
):
    """
    Complete password reset with token.
    """
    user_service = UserService(db)
    audit_service = AuditService(db)

    try:
        success = await user_service.reset_password_with_token(
            token=data.token,
            new_password=data.new_password,
            ip_address=get_client_ip(request),
        )

        # Note: We can't easily log the user ID here since we only have the token
        await db.commit()

        return {"message": "Password has been reset successfully"}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/password/change")
async def change_password(
    request: Request,
    data: PasswordChange,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Change password (requires current password).
    """
    user_service = UserService(db)
    audit_service = AuditService(db)

    try:
        success = await user_service.change_password(
            user_id=current_user.id,
            current_password=data.current_password,
            new_password=data.new_password,
        )

        await audit_service.log(
            action=AuditAction.PASSWORD_CHANGED,
            user_id=current_user.id,
            target_user_id=current_user.id,
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return {"message": "Password changed successfully"}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/password/requirements", response_model=PasswordStrengthResponse)
async def get_password_requirements():
    """Get password policy requirements."""
    return PasswordStrengthResponse(
        score=0,
        label="Unknown",
        requirements=PasswordPolicy.generate_requirements_message(),
    )


@router.post("/password/check-strength", response_model=PasswordStrengthResponse)
async def check_password_strength(password: str):
    """Check password strength without storing."""
    score, label = PasswordPolicy.get_strength_score(password)
    return PasswordStrengthResponse(
        score=score,
        label=label,
        requirements=PasswordPolicy.generate_requirements_message(),
    )


# ============================================================
# Email Verification Endpoints
# ============================================================

@router.post("/email/request-verification")
async def request_email_verification(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Request email verification (resend verification email).
    """
    if current_user.is_email_verified:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email is already verified"
        )

    user_service = UserService(db)
    audit_service = AuditService(db)

    token = await user_service.create_email_verification_token(
        user_id=current_user.id,
    )

    await audit_service.log(
        action=AuditAction.EMAIL_VERIFICATION_SENT,
        user_id=current_user.id,
        target_user_id=current_user.id,
        details={"email": current_user.email},
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request),
    )

    # TODO: Send verification email
    # email_service.send_verification(current_user.email, token)

    await db.commit()

    return {"message": "Verification email sent"}


@router.post("/email/verify")
@limiter.limit(settings.RATE_LIMIT_AUTH)
async def verify_email(
    request: Request,
    data: EmailVerificationConfirm,
    db: AsyncSession = Depends(get_db)
):
    """
    Verify email with token.
    """
    user_service = UserService(db)
    audit_service = AuditService(db)

    try:
        success = await user_service.verify_email_with_token(data.token)

        # Note: Token lookup happens in service, user is verified there
        await db.commit()

        return {"message": "Email verified successfully"}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


# ============================================================
# Session Management Endpoints
# ============================================================

@router.get("/sessions")
async def list_sessions(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    List active sessions for current user.
    """
    session_service = SessionService(db)
    sessions = await session_service.get_user_sessions(
        user_id=current_user.id,
        active_only=True,
    )

    return {
        "sessions": [
            session_service.format_session_for_display(s)
            for s in sessions
        ]
    }


@router.delete("/sessions/{session_id}")
async def revoke_session(
    session_id: int,
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Revoke a specific session.
    """
    session_service = SessionService(db)
    audit_service = AuditService(db)

    # Verify session belongs to user
    session = await session_service.get_session_by_id(session_id)
    if not session or session.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session not found"
        )

    success = await session_service.revoke_session(
        session_id=session_id,
        revoked_by_id=current_user.id,
        reason="user_revoked",
    )

    await audit_service.log(
        action=AuditAction.SESSION_REVOKED,
        user_id=current_user.id,
        target_user_id=current_user.id,
        details={"session_id": session_id},
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request),
    )

    await db.commit()

    return {"message": "Session revoked"}
