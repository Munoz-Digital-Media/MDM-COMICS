"""
Role model for RBAC

Per constitution_cyberSec.json §3: Role-based permissions
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, JSON
from sqlalchemy.orm import relationship

from app.core.database import Base


class Role(Base):
    """
    System and custom roles for permission management.

    System roles (is_system=True) cannot be deleted.
    Permissions are stored as JSON array of permission strings.
    """
    __tablename__ = "roles"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    permissions = Column(JSON, nullable=False, default=list)
    is_system = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    user_roles = relationship("UserRole", back_populates="role", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Role(id={self.id}, name='{self.name}')>"


# Default system roles - seeded on startup
SYSTEM_ROLES = [
    {
        "name": "customer",
        "description": "Default customer role",
        "permissions": ["orders:read", "orders:create", "profile:read", "profile:update"],
        "is_system": True
    },
    {
        "name": "admin",
        "description": "Full administrative access",
        "permissions": ["*"],
        "is_system": True
    },
    {
        "name": "support",
        "description": "Customer support role",
        "permissions": ["users:read", "orders:read", "orders:update"],
        "is_system": True
    },
    {
        "name": "inventory",
        "description": "Inventory management",
        "permissions": ["products:*", "inventory:*"],
        "is_system": True
    }
]
