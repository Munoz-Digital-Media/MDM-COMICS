"""
User Session model for session tracking

Per constitution_logging.json §3: Session tracking with hashed PII
Per constitution_pii.json: No raw PII stored
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Index
from sqlalchemy.orm import relationship

from app.core.database import Base


class UserSession(Base):
    """
    Tracks active user sessions for:
    - Session management (list/revoke sessions)
    - Security monitoring (suspicious activity)
    - Concurrent session limits

    All PII (IP, user agent) is hashed per constitution_pii.json.
    """
    __tablename__ = "user_sessions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    # Token identifiers (for revocation)
    token_jti = Column(String(36), unique=True, nullable=False, index=True)
    refresh_jti = Column(String(36), nullable=True)

    # Device/location info (hashed for privacy)
    device_fingerprint_hash = Column(String(64), nullable=True)
    user_agent_hash = Column(String(64), nullable=True)
    ip_address_hash = Column(String(64), nullable=True)

    # Human-readable device info (derived from user agent, not PII)
    device_type = Column(String(50), nullable=True)  # "Chrome on Windows", "Safari on iPhone"

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_activity_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=False)

    # Revocation
    revoked_at = Column(DateTime(timezone=True), nullable=True)
    revoke_reason = Column(String(50), nullable=True)  # "logout", "password_change", "admin_action", "max_concurrent"

    # Relationships
    user = relationship("User", back_populates="sessions")

    __table_args__ = (
        Index('ix_sessions_user_id', 'user_id'),
        Index('ix_sessions_active', 'user_id', 'revoked_at', postgresql_where=revoked_at.is_(None)),
    )

    def __repr__(self):
        return f"<UserSession(id={self.id}, user_id={self.user_id}, jti='{self.token_jti[:8]}...')>"

    @property
    def is_active(self) -> bool:
        """Check if session is still active (not revoked, not expired)."""
        if self.revoked_at is not None:
            return False
        return datetime.now(timezone.utc) < self.expires_at

    def revoke(self, reason: str = "logout") -> None:
        """Mark session as revoked."""
        self.revoked_at = datetime.now(timezone.utc)
        self.revoke_reason = reason

    def update_activity(self) -> None:
        """Update last activity timestamp."""
        self.last_activity_at = datetime.now(timezone.utc)
