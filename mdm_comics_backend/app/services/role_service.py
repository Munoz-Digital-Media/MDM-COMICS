"""
Role Service

Per constitution_cyberSec.json §3: Role-based access control management.
"""
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.role import Role, SYSTEM_ROLES
from app.models.user_role import UserRole
from app.models.user import User
from app.core.permissions import get_user_permissions, get_user_roles


class RoleService:
    """
    Service for managing roles and role assignments.

    Features:
    - System role management
    - Custom role creation
    - Role assignment with expiration
    - Permission aggregation
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_all_roles(self, include_system: bool = True) -> List[Role]:
        """
        Get all roles.

        Args:
            include_system: Include system roles

        Returns:
            List of roles
        """
        query = select(Role).order_by(Role.name)

        if not include_system:
            query = query.where(Role.is_system == False)

        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_role_by_id(self, role_id: int) -> Optional[Role]:
        """Get a role by ID."""
        result = await self.db.execute(
            select(Role).where(Role.id == role_id)
        )
        return result.scalar_one_or_none()

    async def get_role_by_name(self, name: str) -> Optional[Role]:
        """Get a role by name."""
        result = await self.db.execute(
            select(Role).where(Role.name == name)
        )
        return result.scalar_one_or_none()

    async def create_role(
        self,
        name: str,
        description: Optional[str] = None,
        permissions: Optional[List[str]] = None,
    ) -> Role:
        """
        Create a new custom role.

        Args:
            name: Role name (unique)
            description: Role description
            permissions: List of permission strings

        Returns:
            Created role

        Raises:
            ValueError: If role name already exists
        """
        # Check for existing role
        existing = await self.get_role_by_name(name)
        if existing:
            raise ValueError(f"Role '{name}' already exists")

        role = Role(
            name=name,
            description=description,
            permissions=permissions or [],
            is_system=False,
        )

        self.db.add(role)
        await self.db.flush()
        return role

    async def update_role(
        self,
        role_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        permissions: Optional[List[str]] = None,
    ) -> Optional[Role]:
        """
        Update a role.

        System roles cannot be renamed but can have permissions updated.

        Args:
            role_id: Role ID
            name: New name (ignored for system roles)
            description: New description
            permissions: New permissions list

        Returns:
            Updated role or None if not found
        """
        role = await self.get_role_by_id(role_id)
        if not role:
            return None

        # Can't rename system roles
        if name and not role.is_system:
            # Check for name conflict
            existing = await self.get_role_by_name(name)
            if existing and existing.id != role_id:
                raise ValueError(f"Role '{name}' already exists")
            role.name = name

        if description is not None:
            role.description = description

        if permissions is not None:
            role.permissions = permissions

        role.updated_at = datetime.now(timezone.utc)
        await self.db.flush()
        return role

    async def delete_role(self, role_id: int) -> bool:
        """
        Delete a role.

        System roles cannot be deleted.

        Args:
            role_id: Role ID

        Returns:
            True if deleted

        Raises:
            ValueError: If trying to delete system role
        """
        role = await self.get_role_by_id(role_id)
        if not role:
            return False

        if role.is_system:
            raise ValueError("Cannot delete system role")

        await self.db.delete(role)
        await self.db.flush()
        return True

    async def assign_role(
        self,
        user_id: int,
        role_id: int,
        granted_by_id: Optional[int] = None,
        expires_at: Optional[datetime] = None,
        notes: Optional[str] = None,
    ) -> UserRole:
        """
        Assign a role to a user.

        Args:
            user_id: User to assign role to
            role_id: Role to assign
            granted_by_id: Admin who granted the role
            expires_at: Optional expiration time
            notes: Optional notes

        Returns:
            UserRole assignment

        Raises:
            ValueError: If role doesn't exist or already assigned
        """
        # Verify role exists
        role = await self.get_role_by_id(role_id)
        if not role:
            raise ValueError(f"Role {role_id} not found")

        # Check if already assigned
        result = await self.db.execute(
            select(UserRole)
            .where(
                and_(
                    UserRole.user_id == user_id,
                    UserRole.role_id == role_id,
                )
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            # Update existing assignment
            existing.expires_at = expires_at
            existing.notes = notes
            if granted_by_id:
                existing.granted_by_id = granted_by_id
                existing.granted_at = datetime.now(timezone.utc)
            await self.db.flush()
            return existing

        # Create new assignment
        user_role = UserRole(
            user_id=user_id,
            role_id=role_id,
            granted_by_id=granted_by_id,
            expires_at=expires_at,
            notes=notes,
        )

        self.db.add(user_role)
        await self.db.flush()
        return user_role

    async def revoke_role(
        self,
        user_id: int,
        role_id: int,
    ) -> bool:
        """
        Revoke a role from a user.

        Args:
            user_id: User to revoke from
            role_id: Role to revoke

        Returns:
            True if revoked
        """
        result = await self.db.execute(
            select(UserRole)
            .where(
                and_(
                    UserRole.user_id == user_id,
                    UserRole.role_id == role_id,
                )
            )
        )
        user_role = result.scalar_one_or_none()

        if not user_role:
            return False

        await self.db.delete(user_role)
        await self.db.flush()
        return True

    async def get_user_role_assignments(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Get all role assignments for a user.

        Args:
            user_id: User ID

        Returns:
            List of role assignment details
        """
        result = await self.db.execute(
            select(UserRole)
            .options(selectinload(UserRole.role))
            .where(UserRole.user_id == user_id)
        )
        assignments = result.scalars().all()

        return [
            {
                "id": a.id,
                "role_id": a.role_id,
                "role_name": a.role.name if a.role else None,
                "role_description": a.role.description if a.role else None,
                "permissions": a.role.permissions if a.role else [],
                "granted_at": a.granted_at.isoformat() if a.granted_at else None,
                "granted_by_id": a.granted_by_id,
                "expires_at": a.expires_at.isoformat() if a.expires_at else None,
                "is_expired": a.expires_at < datetime.now(timezone.utc) if a.expires_at else False,
                "notes": a.notes,
            }
            for a in assignments
        ]

    async def get_users_with_role(
        self,
        role_id: int,
        active_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Get all users with a specific role.

        Args:
            role_id: Role ID
            active_only: Only include non-expired assignments

        Returns:
            List of users with the role
        """
        conditions = [UserRole.role_id == role_id]

        if active_only:
            conditions.append(
                or_(
                    UserRole.expires_at.is_(None),
                    UserRole.expires_at > datetime.now(timezone.utc),
                )
            )

        result = await self.db.execute(
            select(UserRole)
            .options(selectinload(UserRole.user))
            .where(and_(*conditions))
        )
        assignments = result.scalars().all()

        return [
            {
                "user_id": a.user_id,
                "email": a.user.email if a.user else None,
                "name": a.user.name if a.user else None,
                "granted_at": a.granted_at.isoformat() if a.granted_at else None,
                "expires_at": a.expires_at.isoformat() if a.expires_at else None,
            }
            for a in assignments
        ]

    async def get_effective_permissions(self, user_id: int) -> List[str]:
        """
        Get aggregated permissions from all user's roles.

        Args:
            user_id: User ID

        Returns:
            List of permission strings (deduplicated)
        """
        permissions = await get_user_permissions(self.db, user_id)
        return list(permissions)

    async def get_effective_roles(self, user_id: int) -> List[str]:
        """
        Get list of active role names for a user.

        Args:
            user_id: User ID

        Returns:
            List of role names
        """
        return await get_user_roles(self.db, user_id)

    async def seed_system_roles(self) -> int:
        """
        Seed or update system roles from SYSTEM_ROLES constant.

        Returns:
            Number of roles created/updated
        """
        count = 0
        for role_def in SYSTEM_ROLES:
            existing = await self.get_role_by_name(role_def["name"])

            if existing:
                existing.description = role_def.get("description", "")
                existing.permissions = role_def["permissions"]
                existing.is_system = True
                existing.updated_at = datetime.now(timezone.utc)
            else:
                role = Role(
                    name=role_def["name"],
                    description=role_def.get("description", ""),
                    permissions=role_def["permissions"],
                    is_system=True,
                )
                self.db.add(role)

            count += 1

        await self.db.flush()
        return count

    async def ensure_user_has_role(
        self,
        user_id: int,
        role_name: str,
        granted_by_id: Optional[int] = None,
    ) -> UserRole:
        """
        Ensure user has a specific role (idempotent).

        Args:
            user_id: User ID
            role_name: Role name to ensure
            granted_by_id: Who is granting

        Returns:
            UserRole assignment
        """
        role = await self.get_role_by_name(role_name)
        if not role:
            raise ValueError(f"Role '{role_name}' not found")

        return await self.assign_role(
            user_id=user_id,
            role_id=role.id,
            granted_by_id=granted_by_id,
            notes=f"Auto-assigned {role_name} role",
        )

    async def promote_to_admin(
        self,
        user_id: int,
        granted_by_id: Optional[int] = None,
    ) -> UserRole:
        """
        Promote user to admin role.

        Also sets legacy is_admin flag for compatibility.

        Args:
            user_id: User ID
            granted_by_id: Admin doing the promotion

        Returns:
            UserRole assignment
        """
        # Assign admin role
        user_role = await self.ensure_user_has_role(
            user_id=user_id,
            role_name="admin",
            granted_by_id=granted_by_id,
        )

        # Set legacy flag
        result = await self.db.execute(
            select(User).where(User.id == user_id)
        )
        user = result.scalar_one_or_none()
        if user:
            user.is_admin = True

        await self.db.flush()
        return user_role

    async def demote_from_admin(
        self,
        user_id: int,
    ) -> bool:
        """
        Remove admin role from user.

        Also clears legacy is_admin flag.

        Args:
            user_id: User ID

        Returns:
            True if demoted
        """
        # Get admin role
        admin_role = await self.get_role_by_name("admin")
        if not admin_role:
            return False

        # Revoke admin role
        revoked = await self.revoke_role(user_id, admin_role.id)

        # Clear legacy flag
        result = await self.db.execute(
            select(User).where(User.id == user_id)
        )
        user = result.scalar_one_or_none()
        if user:
            user.is_admin = False

        await self.db.flush()
        return revoked
