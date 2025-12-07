"""
Newsletter Models

v1.5.0: Outreach System - Newsletter subscription and email event tracking
"""
import secrets
from datetime import datetime, timezone, timedelta
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index, Text
from sqlalchemy.dialects.postgresql import ARRAY

from app.core.database import Base
from app.core.utils import utcnow


def generate_secure_token():
    """Generate cryptographically secure token (256 bits)."""
    return secrets.token_urlsafe(32)


def default_content_types():
    """Callable factory for ARRAY default."""
    return ["all"]


class NewsletterSubscriber(Base):
    """Newsletter subscription management."""
    __tablename__ = "newsletter_subscribers"

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False, index=True)

    # SECURITY: Use HMAC-SHA256 with pepper for GDPR lookup
    email_hash = Column(String(64), nullable=False)
    name = Column(String(255), nullable=True)

    # Preferences
    frequency = Column(String(20), default="weekly")
    content_types = Column(ARRAY(String), default=default_content_types)

    # Status
    status = Column(String(20), default="pending")
    confirmed_at = Column(DateTime(timezone=True), nullable=True)
    unsubscribed_at = Column(DateTime(timezone=True), nullable=True)
    unsubscribe_reason = Column(String(255), nullable=True)

    # SEPARATE TOKENS for different flows
    confirmation_token = Column(String(64), nullable=True, default=generate_secure_token)
    confirmation_expires = Column(DateTime(timezone=True), nullable=True)
    unsubscribe_token = Column(String(64), nullable=True, default=generate_secure_token)

    # Tracking
    source = Column(String(50), default="website")
    created_at = Column(DateTime(timezone=True), default=utcnow)

    # Deliverability
    hard_bounce_count = Column(Integer, default=0)
    soft_bounce_count = Column(Integer, default=0)
    last_bounce_at = Column(DateTime(timezone=True), nullable=True)
    last_bounce_type = Column(String(20), nullable=True)
    complaint_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index('ix_newsletter_status', 'status'),
        Index('ix_newsletter_email_hash', 'email_hash'),
        Index('ix_newsletter_status_bounce', 'status', 'hard_bounce_count'),
        Index('ix_newsletter_unsubscribe_token', 'unsubscribe_token'),
    )

    def is_sendable(self) -> bool:
        """Check if subscriber can receive emails."""
        return (
            self.status == "confirmed" and
            self.hard_bounce_count < 3 and
            self.complaint_at is None
        )

    def regenerate_unsubscribe_token(self):
        """Generate new unsubscribe token (for security rotation)."""
        self.unsubscribe_token = generate_secure_token()


class EmailEvent(Base):
    """
    Email event tracking.

    Stores open/click events from SendGrid webhooks.
    """
    __tablename__ = "email_events"

    id = Column(Integer, primary_key=True)
    subscriber_id = Column(Integer, nullable=True, index=True)
    email = Column(String(255), nullable=False, index=True)

    event_type = Column(String(30), nullable=False, index=True)
    event_data = Column(Text, nullable=True)  # JSON for click URLs, etc.

    campaign_id = Column(String(100), nullable=True, index=True)
    message_id = Column(String(255), nullable=True)

    occurred_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    __table_args__ = (
        Index('ix_email_events_campaign', 'campaign_id', 'event_type'),
    )
