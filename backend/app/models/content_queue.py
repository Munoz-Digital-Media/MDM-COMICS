"""
Content Queue Model

v1.5.0: Outreach System - Content generation and approval workflow
v1.5.1: Phase 1 - Added Facebook platform support
"""
import enum
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, Index
from sqlalchemy.sql import func

from app.core.database import Base
from app.core.utils import utcnow


class ContentStatus(str, enum.Enum):
    """Content queue status values."""
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    SCHEDULED = "scheduled"
    POSTING = "posting"
    POSTED = "posted"
    FAILED = "failed"


class ContentQueueItem(Base):
    """
    Content queue for social media and newsletter posts.

    Workflow:
    1. Content generated (original_content)
    2. AI enhancement (ai_enhanced_content) - optional
    3. Admin review (status: pending_review -> approved/rejected)
    4. Scheduled posting (scheduled_for)
    5. Posted (posted_at, external_post_id)
    """
    __tablename__ = "content_queue"

    id = Column(Integer, primary_key=True)

    # Content type and platform
    content_type = Column(String(50), nullable=False)  # price_winner, price_loser, new_arrival, weekly_recap
    platform = Column(String(50), nullable=False)  # bluesky, facebook, instagram, newsletter, tiktok

    # Content versions
    original_content = Column(Text, nullable=False)
    ai_enhanced_content = Column(Text, nullable=True)
    final_content = Column(Text, nullable=True)  # What gets posted (after admin edits)

    # AI metadata
    ai_model = Column(String(50), nullable=True)
    ai_fallback_used = Column(Boolean, default=False)

    # Status workflow
    status = Column(String(20), default="pending_review")
    # pending_review, approved, rejected, scheduled, posting, posted, failed
    status_changed_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    # Scheduling
    scheduled_for = Column(DateTime(timezone=True), nullable=True)
    posted_at = Column(DateTime(timezone=True), nullable=True)

    # Audit trail
    created_by = Column(Integer, nullable=True)  # User ID who created/triggered
    reviewed_by = Column(Integer, nullable=True)  # Admin who approved/rejected
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    rejection_reason = Column(Text, nullable=True)

    # Idempotency and posting
    idempotency_key = Column(String(32), unique=True, nullable=True)
    posting_started_at = Column(DateTime(timezone=True), nullable=True)
    external_post_id = Column(String(255), nullable=True)  # Bluesky post URI, etc.
    post_url = Column(String(500), nullable=True)

    # Error handling
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), onupdate=utcnow)

    __table_args__ = (
        Index('ix_content_queue_status', 'status'),
        Index('ix_content_queue_scheduled', 'scheduled_for'),
        Index('ix_content_queue_platform_status', 'platform', 'status'),
        Index('ix_content_queue_idempotency', 'idempotency_key'),
    )

    def approve(self, reviewer_id: int, final_content: str = None):
        """Approve content for posting."""
        self.status = "approved"
        self.reviewed_by = reviewer_id
        self.reviewed_at = utcnow()
        if final_content:
            self.final_content = final_content
        elif not self.final_content:
            self.final_content = self.ai_enhanced_content or self.original_content

    def reject(self, reviewer_id: int, reason: str):
        """Reject content."""
        self.status = "rejected"
        self.reviewed_by = reviewer_id
        self.reviewed_at = utcnow()
        self.rejection_reason = reason

    def mark_posted(self, external_id: str, post_url: str = None):
        """Mark as successfully posted."""
        self.status = "posted"
        self.posted_at = utcnow()
        self.external_post_id = external_id
        if post_url:
            self.post_url = post_url

    def mark_failed(self, error: str):
        """Mark as failed with error."""
        self.status = "failed"
        self.error_message = error
        self.retry_count += 1
