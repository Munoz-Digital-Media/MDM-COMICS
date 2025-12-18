"""
Admin User Management Routes

User Management System v1.0.0
Per constitution_cyberSec.json §3: Admin-only user management
"""
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, EmailStr

from app.core.database import get_db
from app.core.permissions import require_permission, Permission
from app.models.user import User
from app.models.user_audit_log import AuditAction
from app.services.user_service import UserService
from app.services.role_service import RoleService
from app.services.session_service import SessionService
from app.services.audit_service import AuditService
from app.api.deps import get_current_user

router = APIRouter()


def get_client_ip(request: Request) -> Optional[str]:
    """Extract client IP from request."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else None


def get_user_agent(request: Request) -> Optional[str]:
    """Extract user agent from request."""
    return request.headers.get("user-agent")


# ============================================================
# Request/Response Models
# ============================================================

class UserListResponse(BaseModel):
    """Paginated user list."""
    users: List[dict]
    total: int
    page: int
    per_page: int


class UserDetailResponse(BaseModel):
    """Detailed user info for admin."""
    id: int
    email: str
    name: str
    is_active: bool
    is_admin: bool
    is_email_verified: bool
    is_locked: bool
    roles: List[str]
    permissions: List[str]
    created_at: str
    last_login_at: Optional[str]
    lockout_count: int
    failed_login_attempts: int


class UserCreateRequest(BaseModel):
    """Admin create user request."""
    email: EmailStr
    name: str
    password: str
    is_admin: bool = False
    roles: Optional[List[str]] = None


class UserUpdateRequest(BaseModel):
    """Admin update user request."""
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    is_active: Optional[bool] = None


class RoleAssignRequest(BaseModel):
    """Assign role to user."""
    role_id: int
    expires_at: Optional[datetime] = None
    notes: Optional[str] = None


class UnlockRequest(BaseModel):
    """Unlock account request."""
    reset_lockout_count: bool = True


# ============================================================
# User CRUD Endpoints
# ============================================================

@router.get("/", response_model=UserListResponse)
async def list_users(
    request: Request,
    search: Optional[str] = Query(None, description="Search by email or name"),
    is_active: Optional[bool] = Query(None),
    is_admin: Optional[bool] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    current_user: User = Depends(require_permission(Permission.USERS_READ)),
    db: AsyncSession = Depends(get_db)
):
    """
    List all users with filtering and pagination.

    Requires: users:read permission
    """
    user_service = UserService(db)

    offset = (page - 1) * per_page
    users, total = await user_service.list_users(
        search=search,
        is_active=is_active,
        is_admin=is_admin,
        limit=per_page,
        offset=offset,
    )

    return UserListResponse(
        users=[
            user_service.format_user_for_display(u, include_pii=True)
            for u in users
        ],
        total=total,
        page=page,
        per_page=per_page,
    )


@router.get("/{user_id}", response_model=UserDetailResponse)
async def get_user(
    user_id: int,
    request: Request,
    current_user: User = Depends(require_permission(Permission.USERS_READ)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get detailed user info.

    Requires: users:read permission
    """
    user_service = UserService(db)
    role_service = RoleService(db)

    user = await user_service.get_user_by_id(user_id, include_deleted=True)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    roles = await role_service.get_effective_roles(user_id)
    permissions = await role_service.get_effective_permissions(user_id)

    return UserDetailResponse(
        id=user.id,
        email=user.email,
        name=user.name,
        is_active=user.is_active,
        is_admin=user.is_admin,
        is_email_verified=user.is_email_verified,
        is_locked=user.is_locked,
        roles=roles,
        permissions=permissions,
        created_at=user.created_at.isoformat() if user.created_at else "",
        last_login_at=user.last_login_at.isoformat() if user.last_login_at else None,
        lockout_count=user.lockout_count or 0,
        failed_login_attempts=user.failed_login_attempts or 0,
    )


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_user(
    request: Request,
    data: UserCreateRequest,
    current_user: User = Depends(require_permission(Permission.USERS_CREATE)),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new user (admin).

    Requires: users:create permission
    """
    user_service = UserService(db)
    role_service = RoleService(db)
    audit_service = AuditService(db)

    try:
        user = await user_service.create_user(
            email=data.email,
            password=data.password,
            name=data.name,
            is_admin=data.is_admin,
        )

        # Assign roles
        if data.roles:
            for role_name in data.roles:
                try:
                    await role_service.ensure_user_has_role(
                        user_id=user.id,
                        role_name=role_name,
                        granted_by_id=current_user.id,
                    )
                except ValueError:
                    pass  # Skip unknown roles
        else:
            # Default to customer role
            try:
                await role_service.ensure_user_has_role(user.id, "customer", current_user.id)
            except ValueError:
                pass

        # Audit log
        await audit_service.log(
            action=AuditAction.USER_CREATED,
            user_id=current_user.id,
            target_user_id=user.id,
            resource_type="user",
            resource_id=str(user.id),
            details={"email": user.email, "created_by_admin": True},
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return {"id": user.id, "email": user.email, "message": "User created"}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.patch("/{user_id}")
async def update_user(
    user_id: int,
    request: Request,
    data: UserUpdateRequest,
    current_user: User = Depends(require_permission(Permission.USERS_UPDATE)),
    db: AsyncSession = Depends(get_db)
):
    """
    Update user profile.

    Requires: users:update permission
    """
    user_service = UserService(db)
    audit_service = AuditService(db)

    try:
        user = await user_service.update_user(
            user_id=user_id,
            name=data.name,
            email=data.email,
            is_active=data.is_active,
        )

        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        await audit_service.log(
            action=AuditAction.USER_UPDATED,
            user_id=current_user.id,
            target_user_id=user_id,
            resource_type="user",
            resource_id=str(user_id),
            details=data.model_dump(exclude_none=True),
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return {"message": "User updated"}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/{user_id}")
async def delete_user(
    user_id: int,
    request: Request,
    hard_delete: bool = Query(False, description="Permanently delete (GDPR)"),
    current_user: User = Depends(require_permission(Permission.USERS_DELETE)),
    db: AsyncSession = Depends(get_db)
):
    """
    Soft-delete or hard-delete a user.

    Requires: users:delete permission
    """
    user_service = UserService(db)
    audit_service = AuditService(db)

    # Prevent self-delete
    if user_id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )

    if hard_delete:
        success = await user_service.hard_delete_user(user_id)
        action = AuditAction.USER_HARD_DELETED
    else:
        success = await user_service.soft_delete_user(user_id)
        action = AuditAction.USER_DELETED

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    await audit_service.log(
        action=action,
        user_id=current_user.id,
        target_user_id=user_id,
        resource_type="user",
        resource_id=str(user_id),
        details={"hard_delete": hard_delete},
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request),
    )

    await db.commit()

    return {"message": "User deleted"}


@router.post("/{user_id}/restore")
async def restore_user(
    user_id: int,
    request: Request,
    current_user: User = Depends(require_permission(Permission.USERS_UPDATE)),
    db: AsyncSession = Depends(get_db)
):
    """
    Restore a soft-deleted user.

    Requires: users:update permission
    """
    user_service = UserService(db)
    audit_service = AuditService(db)

    success = await user_service.restore_user(user_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found or not deleted"
        )

    await audit_service.log(
        action=AuditAction.USER_RESTORED,
        user_id=current_user.id,
        target_user_id=user_id,
        resource_type="user",
        resource_id=str(user_id),
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request),
    )

    await db.commit()

    return {"message": "User restored"}


# ============================================================
# Account Management Endpoints
# ============================================================

@router.post("/{user_id}/unlock")
async def unlock_user_account(
    user_id: int,
    request: Request,
    data: UnlockRequest = None,
    current_user: User = Depends(require_permission(Permission.USERS_UNLOCK)),
    db: AsyncSession = Depends(get_db)
):
    """
    Unlock a locked user account.

    Requires: users:unlock permission
    """
    user_service = UserService(db)
    audit_service = AuditService(db)

    reset_count = data.reset_lockout_count if data else True

    success = await user_service.unlock_account(
        user_id=user_id,
        by_admin=reset_count,
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    await audit_service.log(
        action=AuditAction.ACCOUNT_UNLOCKED,
        user_id=current_user.id,
        target_user_id=user_id,
        resource_type="user",
        resource_id=str(user_id),
        details={"reset_lockout_count": reset_count, "by_admin": True},
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request),
    )

    await db.commit()

    return {"message": "Account unlocked"}


@router.post("/{user_id}/reset-password")
async def admin_reset_password(
    user_id: int,
    request: Request,
    new_password: str,
    current_user: User = Depends(require_permission(Permission.USERS_UPDATE)),
    db: AsyncSession = Depends(get_db)
):
    """
    Admin set user password directly.

    Requires: users:update permission
    """
    user_service = UserService(db)
    audit_service = AuditService(db)

    try:
        success = await user_service.set_password(
            user_id=user_id,
            new_password=new_password,
        )

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        await audit_service.log(
            action=AuditAction.PASSWORD_CHANGED,
            user_id=current_user.id,
            target_user_id=user_id,
            resource_type="user",
            resource_id=str(user_id),
            details={"by_admin": True},
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return {"message": "Password reset"}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


# ============================================================
# Role Management Endpoints
# ============================================================

@router.get("/{user_id}/roles")
async def get_user_roles(
    user_id: int,
    current_user: User = Depends(require_permission(Permission.USERS_READ)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get user's role assignments.

    Requires: users:read permission
    """
    role_service = RoleService(db)

    assignments = await role_service.get_user_role_assignments(user_id)

    return {"roles": assignments}


@router.post("/{user_id}/roles")
async def assign_role(
    user_id: int,
    request: Request,
    data: RoleAssignRequest,
    current_user: User = Depends(require_permission(Permission.USERS_MANAGE_ROLES)),
    db: AsyncSession = Depends(get_db)
):
    """
    Assign a role to user.

    Requires: users:manage_roles permission
    """
    role_service = RoleService(db)
    audit_service = AuditService(db)

    try:
        assignment = await role_service.assign_role(
            user_id=user_id,
            role_id=data.role_id,
            granted_by_id=current_user.id,
            expires_at=data.expires_at,
            notes=data.notes,
        )

        await audit_service.log(
            action=AuditAction.ROLE_ASSIGNED,
            user_id=current_user.id,
            target_user_id=user_id,
            resource_type="role",
            resource_id=str(data.role_id),
            details={"expires_at": str(data.expires_at) if data.expires_at else None},
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return {"message": "Role assigned", "assignment_id": assignment.id}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/{user_id}/roles/{role_id}")
async def revoke_role(
    user_id: int,
    role_id: int,
    request: Request,
    current_user: User = Depends(require_permission(Permission.USERS_MANAGE_ROLES)),
    db: AsyncSession = Depends(get_db)
):
    """
    Revoke a role from user.

    Requires: users:manage_roles permission
    """
    role_service = RoleService(db)
    audit_service = AuditService(db)

    success = await role_service.revoke_role(user_id, role_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role assignment not found"
        )

    await audit_service.log(
        action=AuditAction.ROLE_REVOKED,
        user_id=current_user.id,
        target_user_id=user_id,
        resource_type="role",
        resource_id=str(role_id),
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request),
    )

    await db.commit()

    return {"message": "Role revoked"}


# ============================================================
# Session Management Endpoints
# ============================================================

@router.get("/{user_id}/sessions")
async def get_user_sessions(
    user_id: int,
    current_user: User = Depends(require_permission(Permission.USERS_MANAGE_SESSIONS)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get user's active sessions.

    Requires: users:manage_sessions permission
    """
    session_service = SessionService(db)

    sessions = await session_service.get_user_sessions(
        user_id=user_id,
        active_only=True,
    )

    return {
        "sessions": [
            session_service.format_session_for_display(s)
            for s in sessions
        ]
    }


@router.delete("/{user_id}/sessions")
async def revoke_all_user_sessions(
    user_id: int,
    request: Request,
    current_user: User = Depends(require_permission(Permission.USERS_MANAGE_SESSIONS)),
    db: AsyncSession = Depends(get_db)
):
    """
    Revoke all sessions for a user.

    Requires: users:manage_sessions permission
    """
    session_service = SessionService(db)
    audit_service = AuditService(db)

    count = await session_service.revoke_all_sessions(
        user_id=user_id,
        reason="admin_revoked",
    )

    await audit_service.log(
        action=AuditAction.SESSION_REVOKED,
        user_id=current_user.id,
        target_user_id=user_id,
        resource_type="session",
        resource_id="all",
        details={"sessions_revoked": count},
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request),
    )

    await db.commit()

    return {"message": f"Revoked {count} sessions"}


# ============================================================
# Audit Log Endpoints
# ============================================================

@router.get("/{user_id}/audit")
async def get_user_audit_log(
    user_id: int,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(require_permission(Permission.USERS_VIEW_AUDIT)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get audit trail for a user.

    Requires: users:view_audit permission
    """
    audit_service = AuditService(db)

    entries = await audit_service.get_user_audit_trail(
        user_id=user_id,
        include_as_target=True,
        limit=limit,
        offset=offset,
    )

    return {
        "entries": [
            {
                "id": e.id,
                "action": e.action,
                "resource_type": e.resource_type,
                "resource_id": e.resource_id,
                "details": e.details,
                "created_at": e.created_at.isoformat(),
            }
            for e in entries
        ]
    }


# ============================================================
# Statistics Endpoints
# ============================================================

@router.get("/stats/overview")
async def get_user_stats(
    current_user: User = Depends(require_permission(Permission.USERS_READ)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get user statistics overview.

    Requires: users:read permission
    """
    user_service = UserService(db)

    stats = await user_service.get_user_stats()

    return stats
