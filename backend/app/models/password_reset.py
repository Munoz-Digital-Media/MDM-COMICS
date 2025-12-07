"""
Password Reset Token model

Per constitution_cyberSec.json §3: Secure password reset flow
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Index

from app.core.database import Base


class PasswordResetToken(Base):
    """
    Stores password reset tokens.

    Tokens are:
    - One-time use (used_at tracks usage)
    - Time-limited (expires_at)
    - Hashed (token_hash, not plaintext)
    """
    __tablename__ = "password_reset_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    # Token is hashed before storage
    token_hash = Column(String(128), unique=True, nullable=False, index=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)

    # Security tracking (hashed IP)
    ip_hash = Column(String(64), nullable=True)

    __table_args__ = (
        Index('ix_reset_token_user', 'user_id'),
        Index('ix_reset_token_expires', 'expires_at'),
    )

    def __repr__(self):
        return f"<PasswordResetToken(id={self.id}, user_id={self.user_id})>"

    @property
    def is_valid(self) -> bool:
        """Check if token is valid (not used, not expired)."""
        if self.used_at is not None:
            return False
        return datetime.now(timezone.utc) < self.expires_at

    def mark_used(self) -> None:
        """Mark token as used."""
        self.used_at = datetime.now(timezone.utc)
