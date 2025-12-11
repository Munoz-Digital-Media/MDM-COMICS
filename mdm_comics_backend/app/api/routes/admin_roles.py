"""
Admin Role Management Routes

User Management System v1.0.0
Per constitution_cyberSec.json §3: Role-based access control management
"""
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from app.core.database import get_db
from app.core.permissions import require_permission, Permission
from app.models.user import User
from app.models.user_audit_log import AuditAction
from app.services.role_service import RoleService
from app.services.audit_service import AuditService

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

class RoleResponse(BaseModel):
    """Role details."""
    id: int
    name: str
    description: Optional[str]
    permissions: List[str]
    is_system: bool
    created_at: str
    updated_at: str


class RoleCreateRequest(BaseModel):
    """Create role request."""
    name: str
    description: Optional[str] = None
    permissions: List[str] = []


class RoleUpdateRequest(BaseModel):
    """Update role request."""
    name: Optional[str] = None
    description: Optional[str] = None
    permissions: Optional[List[str]] = None


class PermissionListResponse(BaseModel):
    """List of available permissions."""
    permissions: List[dict]


# ============================================================
# Role CRUD Endpoints
# ============================================================

@router.get("", response_model=List[RoleResponse])
async def list_roles(
    include_system: bool = Query(True, description="Include system roles"),
    current_user: User = Depends(require_permission(Permission.ROLES_READ)),
    db: AsyncSession = Depends(get_db)
):
    """
    List all roles.

    Requires: roles:read permission
    """
    role_service = RoleService(db)

    roles = await role_service.get_all_roles(include_system=include_system)

    return [
        RoleResponse(
            id=r.id,
            name=r.name,
            description=r.description,
            permissions=r.permissions or [],
            is_system=r.is_system,
            created_at=r.created_at.isoformat() if r.created_at else "",
            updated_at=r.updated_at.isoformat() if r.updated_at else "",
        )
        for r in roles
    ]


@router.get("/permissions", response_model=PermissionListResponse)
async def list_available_permissions(
    current_user: User = Depends(require_permission(Permission.ROLES_READ)),
):
    """
    List all available permissions.

    Requires: roles:read permission
    """
    # Get all permissions from Permission class
    permissions = []
    for attr in dir(Permission):
        if not attr.startswith("_") and isinstance(getattr(Permission, attr), str):
            perm = getattr(Permission, attr)
            if ":" in perm or perm == "*":
                permissions.append({
                    "permission": perm,
                    "category": perm.split(":")[0] if ":" in perm else "admin",
                    "action": perm.split(":")[1] if ":" in perm else "*",
                })

    return PermissionListResponse(permissions=permissions)


@router.get("/{role_id}", response_model=RoleResponse)
async def get_role(
    role_id: int,
    current_user: User = Depends(require_permission(Permission.ROLES_READ)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get role details.

    Requires: roles:read permission
    """
    role_service = RoleService(db)

    role = await role_service.get_role_by_id(role_id)
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )

    return RoleResponse(
        id=role.id,
        name=role.name,
        description=role.description,
        permissions=role.permissions or [],
        is_system=role.is_system,
        created_at=role.created_at.isoformat() if role.created_at else "",
        updated_at=role.updated_at.isoformat() if role.updated_at else "",
    )


@router.post("", status_code=status.HTTP_201_CREATED, response_model=RoleResponse)
async def create_role(
    request: Request,
    data: RoleCreateRequest,
    current_user: User = Depends(require_permission(Permission.ROLES_CREATE)),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new custom role.

    Requires: roles:create permission
    """
    role_service = RoleService(db)
    audit_service = AuditService(db)

    try:
        role = await role_service.create_role(
            name=data.name,
            description=data.description,
            permissions=data.permissions,
        )

        await audit_service.log(
            action=AuditAction.ROLE_CREATED,
            user_id=current_user.id,
            resource_type="role",
            resource_id=str(role.id),
            details={"name": role.name, "permissions": role.permissions},
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return RoleResponse(
            id=role.id,
            name=role.name,
            description=role.description,
            permissions=role.permissions or [],
            is_system=role.is_system,
            created_at=role.created_at.isoformat() if role.created_at else "",
            updated_at=role.updated_at.isoformat() if role.updated_at else "",
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.patch("/{role_id}", response_model=RoleResponse)
async def update_role(
    role_id: int,
    request: Request,
    data: RoleUpdateRequest,
    current_user: User = Depends(require_permission(Permission.ROLES_UPDATE)),
    db: AsyncSession = Depends(get_db)
):
    """
    Update a role.

    System roles can have permissions updated but not renamed.

    Requires: roles:update permission
    """
    role_service = RoleService(db)
    audit_service = AuditService(db)

    try:
        role = await role_service.update_role(
            role_id=role_id,
            name=data.name,
            description=data.description,
            permissions=data.permissions,
        )

        if not role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Role not found"
            )

        await audit_service.log(
            action=AuditAction.ROLE_UPDATED,
            user_id=current_user.id,
            resource_type="role",
            resource_id=str(role.id),
            details=data.model_dump(exclude_none=True),
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return RoleResponse(
            id=role.id,
            name=role.name,
            description=role.description,
            permissions=role.permissions or [],
            is_system=role.is_system,
            created_at=role.created_at.isoformat() if role.created_at else "",
            updated_at=role.updated_at.isoformat() if role.updated_at else "",
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/{role_id}")
async def delete_role(
    role_id: int,
    request: Request,
    current_user: User = Depends(require_permission(Permission.ROLES_DELETE)),
    db: AsyncSession = Depends(get_db)
):
    """
    Delete a custom role.

    System roles cannot be deleted.

    Requires: roles:delete permission
    """
    role_service = RoleService(db)
    audit_service = AuditService(db)

    try:
        # Get role info before deletion
        role = await role_service.get_role_by_id(role_id)
        if not role:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Role not found"
            )

        role_name = role.name

        success = await role_service.delete_role(role_id)

        await audit_service.log(
            action=AuditAction.ROLE_DELETED,
            user_id=current_user.id,
            resource_type="role",
            resource_id=str(role_id),
            details={"name": role_name},
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return {"message": "Role deleted"}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/{role_id}/users")
async def get_role_users(
    role_id: int,
    current_user: User = Depends(require_permission(Permission.ROLES_READ)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all users with a specific role.

    Requires: roles:read permission
    """
    role_service = RoleService(db)

    # Verify role exists
    role = await role_service.get_role_by_id(role_id)
    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )

    users = await role_service.get_users_with_role(role_id)

    return {"users": users}


@router.post("/seed-system-roles")
async def seed_system_roles(
    request: Request,
    current_user: User = Depends(require_permission(Permission.ROLES_CREATE)),
    db: AsyncSession = Depends(get_db)
):
    """
    Seed or update system roles from defaults.

    Requires: roles:create permission
    """
    role_service = RoleService(db)
    audit_service = AuditService(db)

    count = await role_service.seed_system_roles()

    await audit_service.log(
        action=AuditAction.ROLE_CREATED,
        user_id=current_user.id,
        resource_type="role",
        resource_id="system",
        details={"action": "seed_system_roles", "count": count},
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request),
    )

    await db.commit()

    return {"message": f"Seeded {count} system roles"}
