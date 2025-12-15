"""
Match Review Queue Models

Per constitution_db.json:
- snake_case naming
- FK constraints
- Timestamps
"""

from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime,
    ForeignKey, Numeric, CheckConstraint, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class MatchReviewQueue(Base):
    """Pending matches for human review."""

    __tablename__ = "match_review_queue"

    id = Column(Integer, primary_key=True, index=True)

    # Source record
    entity_type = Column(String(20), nullable=False)  # 'comic' or 'funko'
    entity_id = Column(Integer, nullable=False)

    # Match candidate
    candidate_source = Column(String(50), nullable=False)  # 'pricecharting', 'metron', etc.
    candidate_id = Column(String(100), nullable=False)
    candidate_name = Column(String(500))
    candidate_data = Column(JSONB)  # Full API response for display

    # Matching metadata
    match_method = Column(String(50), nullable=False)  # 'isbn', 'upc', 'fuzzy_score_6'
    match_score = Column(Integer)
    match_details = Column(JSONB)  # Scoring breakdown

    # Queue status
    status = Column(String(20), nullable=False, default='pending')
    is_escalated = Column(Boolean, default=False)  # True after 30 days

    # Resolution
    reviewed_by = Column(Integer, ForeignKey('users.id'))
    reviewed_at = Column(DateTime(timezone=True))
    resolution_notes = Column(Text)

    # Optimistic locking
    locked_by = Column(Integer, ForeignKey('users.id'))
    locked_at = Column(DateTime(timezone=True))

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    expires_at = Column(DateTime(timezone=True))

    # Relationships
    reviewer = relationship("User", foreign_keys=[reviewed_by])
    locker = relationship("User", foreign_keys=[locked_by])

    __table_args__ = (
        CheckConstraint("entity_type IN ('comic', 'funko', 'cover_ingestion', 'cover_upload')", name='ck_entity_type'),
        CheckConstraint("status IN ('pending', 'approved', 'rejected', 'skipped', 'expired')", name='ck_status'),
        UniqueConstraint('entity_type', 'entity_id', 'candidate_source', 'candidate_id', name='uq_match_candidate'),
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.expires_at:
            self.expires_at = datetime.utcnow() + timedelta(days=30)

    @property
    def is_locked(self) -> bool:
        """Check if match is currently locked by another user."""
        if not self.locked_at:
            return False
        # Lock expires after 5 minutes
        lock_expiry = self.locked_at + timedelta(minutes=5)
        return datetime.utcnow() < lock_expiry

    @property
    def can_bulk_approve(self) -> bool:
        """Score >= 8 allows bulk approval."""
        return self.match_score is not None and self.match_score >= 8


class MatchAuditLog(Base):
    """Immutable audit trail with hash chains per constitution_logging.json."""

    __tablename__ = "match_audit_log"

    id = Column(Integer, primary_key=True, index=True)

    # Action details
    action = Column(String(50), nullable=False)  # 'auto_link', 'manual_approve', etc.
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(Integer, nullable=False)

    # Before/after state (hashed)
    before_state_hash = Column(String(128))
    after_state_hash = Column(String(128))

    # Actor (pseudonymized - never store raw user_id)
    actor_type = Column(String(20), nullable=False)  # 'system', 'user'
    actor_id_hash = Column(String(128))  # SHA-512 of user_id

    # Match details
    match_source = Column(String(50))
    match_id = Column(String(100))
    match_method = Column(String(50))
    match_score = Column(Integer)

    # Immutability - hash chain
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    log_hash = Column(String(128))
    previous_hash = Column(String(128))


class IsbnSource(Base):
    """Track ISBN provenance from multiple enrichment sources."""

    __tablename__ = "isbn_sources"

    id = Column(Integer, primary_key=True, index=True)
    comic_issue_id = Column(Integer, ForeignKey('comic_issues.id', ondelete='CASCADE'), nullable=False)

    source_name = Column(String(50), nullable=False)  # 'gcd', 'comic_vine', 'metron'
    source_id = Column(String(100))  # ID in source system

    isbn_raw = Column(String(50))  # Original format
    isbn_normalized = Column(String(13))  # ISBN-13 normalized

    confidence = Column(Numeric(3, 2), default=1.00)
    fetched_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship
    comic_issue = relationship("ComicIssue", back_populates="isbn_sources")

    __table_args__ = (
        UniqueConstraint('comic_issue_id', 'source_name', name='uq_isbn_source'),
    )
