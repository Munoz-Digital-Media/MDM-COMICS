"""
User model

Enhanced for User Management System v1.0.0
Per constitution_db.json, constitution_pii.json, constitution_cyberSec.json
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Index
from sqlalchemy.orm import relationship

from app.core.database import Base


class User(Base):
    """
    User account model.

    PII fields (email, name) are tagged per constitution_pii.json.
    Supports soft-delete via deleted_at per constitution.json A5.
    Account lockout fields for brute force protection per constitution_cyberSec.json §8.
    """
    __tablename__ = "users"

    # Primary key
    id = Column(Integer, primary_key=True, index=True)

    # Core fields
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    name = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    is_admin = Column(Boolean, default=False)

    # Email verification (OWASP ASVS L2)
    email_verified_at = Column(DateTime(timezone=True), nullable=True)

    # Account lockout (constitution_cyberSec.json §8)
    failed_login_attempts = Column(Integer, default=0)
    locked_until = Column(DateTime(timezone=True), nullable=True)
    lockout_count = Column(Integer, default=0)  # For progressive lockout

    # Password management
    password_changed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Login tracking
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    last_login_ip_hash = Column(String(64), nullable=True)

    # Soft delete (constitution.json A5)
    deleted_at = Column(DateTime(timezone=True), nullable=True)

    # Timestamps (UTC-aware per NASTY-008)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    orders = relationship("Order", back_populates="user", foreign_keys="Order.user_id")
    cart_items = relationship("CartItem", back_populates="user")
    grade_requests = relationship("GradeRequest", back_populates="user")
    addresses = relationship("Address", back_populates="user")

    # New relationships for User Management System
    user_roles = relationship("UserRole", foreign_keys="UserRole.user_id", back_populates="user", cascade="all, delete-orphan")
    sessions = relationship("UserSession", back_populates="user", cascade="all, delete-orphan")

    # Indexes for common queries
    __table_args__ = (
        Index('ix_users_active', 'id', postgresql_where=deleted_at.is_(None)),
        Index('ix_users_email_active', 'email', postgresql_where=deleted_at.is_(None)),
    )

    def __repr__(self):
        return f"<User(id={self.id}, email='{self.email}')>"

    @property
    def is_deleted(self) -> bool:
        """Check if user is soft-deleted."""
        return self.deleted_at is not None

    @property
    def is_email_verified(self) -> bool:
        """Check if email is verified."""
        return self.email_verified_at is not None

    @property
    def is_locked(self) -> bool:
        """Check if account is currently locked."""
        if not self.locked_until:
            return False
        return datetime.now(timezone.utc) < self.locked_until

    def soft_delete(self) -> None:
        """Mark user as soft-deleted."""
        self.deleted_at = datetime.now(timezone.utc)
        self.is_active = False

    def restore(self) -> None:
        """Restore soft-deleted user."""
        self.deleted_at = None
        self.is_active = True

    def verify_email(self) -> None:
        """Mark email as verified."""
        self.email_verified_at = datetime.now(timezone.utc)

    def record_login(self, ip_hash: str = None) -> None:
        """Record successful login."""
        self.last_login_at = datetime.now(timezone.utc)
        if ip_hash:
            self.last_login_ip_hash = ip_hash
        self.failed_login_attempts = 0
        self.locked_until = None

    def record_password_change(self) -> None:
        """Record password change."""
        self.password_changed_at = datetime.now(timezone.utc)
