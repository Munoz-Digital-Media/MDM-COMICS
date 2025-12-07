"""
Email Verification Token model

Per OWASP ASVS L2: Email verification for new accounts
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Index

from app.core.database import Base


class EmailVerificationToken(Base):
    """
    Stores email verification tokens.

    Used for:
    - New account verification
    - Email change verification

    Tokens are hashed before storage.
    """
    __tablename__ = "email_verification_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    # Token is hashed before storage
    token_hash = Column(String(128), unique=True, nullable=False, index=True)

    # Email being verified (hashed for throttle detection)
    email_hash = Column(String(64), nullable=False)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=False)
    verified_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index('ix_verify_token_user', 'user_id'),
        Index('ix_verify_token_email', 'email_hash'),
    )

    def __repr__(self):
        return f"<EmailVerificationToken(id={self.id}, user_id={self.user_id})>"

    @property
    def is_valid(self) -> bool:
        """Check if token is valid (not verified, not expired)."""
        if self.verified_at is not None:
            return False
        return datetime.now(timezone.utc) < self.expires_at

    def mark_verified(self) -> None:
        """Mark token as verified."""
        self.verified_at = datetime.now(timezone.utc)
