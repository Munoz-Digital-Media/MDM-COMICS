"""
Permission system for RBAC

Per constitution_cyberSec.json §3: Role-based access control
"""
from typing import Set, List, Optional
from functools import wraps
from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.database import get_db
from app.models.user import User
from app.models.user_role import UserRole
from app.models.role import Role


class Permission:
    """
    Permission string format: "resource:action" or "resource:*" for all actions.

    Resources: users, orders, products, inventory, roles, settings, sessions
    Actions: create, read, update, delete, manage, view_audit, unlock, export
    """

    # User permissions
    USERS_READ = "users:read"
    USERS_CREATE = "users:create"
    USERS_UPDATE = "users:update"
    USERS_DELETE = "users:delete"
    USERS_MANAGE_ROLES = "users:manage_roles"
    USERS_VIEW_AUDIT = "users:view_audit"
    USERS_UNLOCK = "users:unlock"
    USERS_MANAGE_SESSIONS = "users:manage_sessions"
    USERS_EXPORT = "users:export"
    USERS_ALL = "users:*"

    # Order permissions
    ORDERS_READ = "orders:read"
    ORDERS_CREATE = "orders:create"
    ORDERS_UPDATE = "orders:update"
    ORDERS_REFUND = "orders:refund"
    ORDERS_ALL = "orders:*"

    # Product permissions
    PRODUCTS_READ = "products:read"
    PRODUCTS_CREATE = "products:create"
    PRODUCTS_UPDATE = "products:update"
    PRODUCTS_DELETE = "products:delete"
    PRODUCTS_ALL = "products:*"

    # Inventory permissions
    INVENTORY_READ = "inventory:read"
    INVENTORY_UPDATE = "inventory:update"
    INVENTORY_ADJUST = "inventory:adjust"
    INVENTORY_ALL = "inventory:*"

    # Role permissions
    ROLES_READ = "roles:read"
    ROLES_CREATE = "roles:create"
    ROLES_UPDATE = "roles:update"
    ROLES_DELETE = "roles:delete"
    ROLES_ALL = "roles:*"

    # Settings permissions
    SETTINGS_READ = "settings:read"
    SETTINGS_UPDATE = "settings:update"
    SETTINGS_ALL = "settings:*"

    # Profile permissions (self-service)
    PROFILE_READ = "profile:read"
    PROFILE_UPDATE = "profile:update"
    PROFILE_ALL = "profile:*"

    # Session permissions
    SESSIONS_READ = "sessions:read"
    SESSIONS_REVOKE = "sessions:revoke"
    SESSIONS_ALL = "sessions:*"

    # DSAR (Data Subject Access Request) permissions
    DSAR_ADMIN = "dsar:admin"
    DSAR_READ = "dsar:read"
    DSAR_PROCESS = "dsar:process"
    DSAR_ALL = "dsar:*"

    # Self-service permissions (for users managing their own data)
    USERS_SELF = "users:self"

    # Admin superuser
    ADMIN_ALL = "*"


def has_permission(user_permissions: Set[str], required: str) -> bool:
    """
    Check if user has required permission.

    Supports wildcards:
    - "*" grants all permissions
    - "resource:*" grants all actions on resource

    Args:
        user_permissions: Set of user's permission strings
        required: Required permission string

    Returns:
        True if user has permission
    """
    # Superuser has all permissions
    if Permission.ADMIN_ALL in user_permissions:
        return True

    # Direct match
    if required in user_permissions:
        return True

    # Check resource wildcard
    if ":" in required:
        resource = required.split(":")[0]
        if f"{resource}:*" in user_permissions:
            return True

    return False


def has_any_permission(user_permissions: Set[str], required: List[str]) -> bool:
    """Check if user has any of the required permissions."""
    return any(has_permission(user_permissions, perm) for perm in required)


def has_all_permissions(user_permissions: Set[str], required: List[str]) -> bool:
    """Check if user has all required permissions."""
    return all(has_permission(user_permissions, perm) for perm in required)


async def get_user_permissions(db: AsyncSession, user_id: int) -> Set[str]:
    """
    Get all permissions for a user from their roles.

    Args:
        db: Database session
        user_id: User ID

    Returns:
        Set of permission strings
    """
    from datetime import datetime, timezone

    # Get user's active roles (not expired)
    result = await db.execute(
        select(Role.permissions)
        .join(UserRole, Role.id == UserRole.role_id)
        .where(UserRole.user_id == user_id)
        .where(
            (UserRole.expires_at.is_(None)) |
            (UserRole.expires_at > datetime.now(timezone.utc))
        )
    )

    permissions = set()
    for row in result.fetchall():
        role_permissions = row[0]
        if role_permissions:
            permissions.update(role_permissions)

    return permissions


async def get_user_roles(db: AsyncSession, user_id: int) -> List[str]:
    """
    Get list of role names for a user.

    Args:
        db: Database session
        user_id: User ID

    Returns:
        List of role names
    """
    from datetime import datetime, timezone

    result = await db.execute(
        select(Role.name)
        .join(UserRole, Role.id == UserRole.role_id)
        .where(UserRole.user_id == user_id)
        .where(
            (UserRole.expires_at.is_(None)) |
            (UserRole.expires_at > datetime.now(timezone.utc))
        )
    )

    return [row[0] for row in result.fetchall()]


def require_permission(*permissions: str):
    """
    Dependency that requires specific permissions.

    Usage:
        @router.get("/users")
        async def list_users(
            user: User = Depends(require_permission(Permission.USERS_READ))
        ):
            ...

    Args:
        permissions: One or more required permission strings

    Returns:
        Dependency function that validates permissions
    """
    from app.api.deps import get_current_user

    async def permission_checker(
        current_user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db)
    ) -> User:
        # Legacy admin check - admins have all permissions
        if current_user.is_admin:
            return current_user

        # Get user's permissions from roles
        user_permissions = await get_user_permissions(db, current_user.id)

        # Check all required permissions
        for required in permissions:
            if not has_permission(user_permissions, required):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Permission denied: {required}"
                )

        return current_user

    return permission_checker


def require_any_permission(*permissions: str):
    """
    Dependency that requires any of the specified permissions.

    Usage:
        @router.get("/data")
        async def get_data(
            user: User = Depends(require_any_permission("data:read", "data:admin"))
        ):
            ...
    """
    from app.api.deps import get_current_user

    async def permission_checker(
        current_user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db)
    ) -> User:
        # Legacy admin check
        if current_user.is_admin:
            return current_user

        user_permissions = await get_user_permissions(db, current_user.id)

        if not has_any_permission(user_permissions, list(permissions)):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied: requires one of {', '.join(permissions)}"
            )

        return current_user

    return permission_checker


class PermissionChecker:
    """
    Class-based permission checker for more complex scenarios.

    Usage:
        checker = PermissionChecker(db, user_id)
        if await checker.can("users:read"):
            ...
    """

    def __init__(self, db: AsyncSession, user_id: int):
        self.db = db
        self.user_id = user_id
        self._permissions: Optional[Set[str]] = None

    async def load(self) -> None:
        """Load permissions from database."""
        self._permissions = await get_user_permissions(self.db, self.user_id)

    async def can(self, permission: str) -> bool:
        """Check if user can perform action."""
        if self._permissions is None:
            await self.load()
        return has_permission(self._permissions, permission)

    async def can_any(self, permissions: List[str]) -> bool:
        """Check if user can perform any of the actions."""
        if self._permissions is None:
            await self.load()
        return has_any_permission(self._permissions, permissions)

    async def can_all(self, permissions: List[str]) -> bool:
        """Check if user can perform all actions."""
        if self._permissions is None:
            await self.load()
        return has_all_permissions(self._permissions, permissions)

    async def get_permissions(self) -> Set[str]:
        """Get all user permissions."""
        if self._permissions is None:
            await self.load()
        return self._permissions

    async def get_roles(self) -> List[str]:
        """Get all user role names."""
        return await get_user_roles(self.db, self.user_id)
