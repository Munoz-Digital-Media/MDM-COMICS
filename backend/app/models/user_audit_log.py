"""
User Audit Log model

Per constitution_logging.json §3: BFF Audit Stream specification
Per constitution_logging.json §7: Hash chain for immutability
"""
from datetime import datetime, timezone
from sqlalchemy import Column, BigInteger, String, DateTime, JSON, Index, Text

from app.core.database import Base


class UserAuditLog(Base):
    """
    Immutable audit log for user-related actions.

    Features:
    - No PII stored (all IDs are hashed)
    - Hash chain for tamper detection
    - JSON metadata for flexibility

    Retention: 730 days per constitution_logging.json §3
    """
    __tablename__ = "user_audit_log"

    id = Column(BigInteger, primary_key=True, index=True)
    ts = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    # Actor info (hashed, no PII)
    actor_type = Column(String(20), nullable=False)  # 'user', 'admin', 'system', 'api'
    actor_id_hash = Column(String(64), nullable=False)

    # Action details
    action = Column(String(100), nullable=False)  # e.g., 'user.login', 'user.password_change'
    resource_type = Column(String(50), nullable=False)  # e.g., 'user', 'role', 'session'
    resource_id_hash = Column(String(64), nullable=True)

    # State change tracking
    before_hash = Column(String(128), nullable=True)
    after_hash = Column(String(128), nullable=True)

    # Outcome
    outcome = Column(String(20), nullable=False)  # 'success', 'failure', 'denied'

    # Context (hashed)
    ip_hash = Column(String(64), nullable=True)

    # Flexible metadata (no PII)
    metadata = Column(JSON, default=dict)

    # Hash chain for immutability verification
    prev_hash = Column(String(128), nullable=True)
    entry_hash = Column(String(128), nullable=False)

    __table_args__ = (
        Index('ix_audit_ts', 'ts'),
        Index('ix_audit_actor', 'actor_id_hash', 'ts'),
        Index('ix_audit_resource', 'resource_type', 'resource_id_hash'),
        Index('ix_audit_action', 'action'),
    )

    def __repr__(self):
        return f"<UserAuditLog(id={self.id}, action='{self.action}', outcome='{self.outcome}')>"


# Audit action constants
class AuditAction:
    """Standard audit action names."""
    # Authentication
    USER_LOGIN = "user.login"
    USER_LOGIN_FAILED = "user.login_failed"
    USER_LOGOUT = "user.logout"
    USER_LOGOUT_ALL = "user.logout_all"

    # Account management
    USER_REGISTER = "user.register"
    USER_UPDATE = "user.update"
    USER_PASSWORD_CHANGE = "user.password_change"
    USER_PASSWORD_RESET_REQUEST = "user.password_reset_request"
    USER_PASSWORD_RESET = "user.password_reset"
    USER_EMAIL_VERIFY = "user.email_verify"
    USER_LOCKED = "user.locked"
    USER_UNLOCKED = "user.unlocked"
    USER_DEACTIVATED = "user.deactivated"
    USER_REACTIVATED = "user.reactivated"
    USER_DELETED = "user.deleted"

    # Role management
    ROLE_ASSIGNED = "role.assigned"
    ROLE_REVOKED = "role.revoked"
    ROLE_CREATED = "role.created"
    ROLE_UPDATED = "role.updated"
    ROLE_DELETED = "role.deleted"

    # Session management
    SESSION_CREATED = "session.created"
    SESSION_REVOKED = "session.revoked"
    SESSION_EXPIRED = "session.expired"

    # Admin actions
    ADMIN_USER_CREATE = "admin.user_create"
    ADMIN_USER_UPDATE = "admin.user_update"
    ADMIN_USER_DELETE = "admin.user_delete"
    ADMIN_FORCE_LOGOUT = "admin.force_logout"

    # DSAR (Data Subject Access Request)
    DSAR_EXPORT_REQUEST = "dsar.export_request"
    DSAR_EXPORT_COMPLETE = "dsar.export_complete"
    DSAR_DELETE_REQUEST = "dsar.delete_request"
    DSAR_DELETE_COMPLETE = "dsar.delete_complete"
    DSAR_REQUESTED = "dsar.requested"
    DSAR_CANCELLED = "dsar.cancelled"
    DSAR_COMPLETED = "dsar.completed"

    # Retention
    RETENTION_CLEANUP = "retention.cleanup"
