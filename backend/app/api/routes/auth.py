"""
Authentication routes

P1-3: Rate limited to prevent brute force attacks
P1-5: HttpOnly cookies + CSRF protection
"""
import os
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel

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
from app.models.user import User
from app.schemas.user import UserCreate, UserResponse, UserLogin
from app.schemas.auth import Token, RefreshToken
from app.api.deps import get_current_user

router = APIRouter()

# Admin setup secret - set this in Railway env vars
ADMIN_SETUP_SECRET = os.getenv("ADMIN_SETUP_SECRET")


class AdminSetupRequest(BaseModel):
    email: str
    secret: str


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
    """
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

    # Generate tokens
    access_token = create_access_token({"sub": str(user.id)})
    refresh_token = create_refresh_token({"sub": str(user.id)})

    # P1-5: Set HttpOnly cookies
    csrf_token = set_auth_cookies(response, request, access_token, refresh_token)

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
    """
    result = await db.execute(select(User).where(User.email == credentials.email))
    user = result.scalar_one_or_none()

    if not user or not verify_password(credentials.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is disabled"
        )

    # Generate tokens
    access_token = create_access_token({"sub": str(user.id)})
    refresh_token = create_refresh_token({"sub": str(user.id)})

    # P1-5: Set HttpOnly cookies
    csrf_token = set_auth_cookies(response, request, access_token, refresh_token)

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
