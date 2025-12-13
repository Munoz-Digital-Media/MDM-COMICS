"""
Source Quota Models v1.10.0

Multi-source enrichment quota tracking with database persistence.

Per constitution_telemetry.json: Defensive telemetry for rate limiting.
Per constitution_db.json: Atomic transactions, audit columns.

Review Notes Applied:
- Database-backed for multi-worker safety
- Atomic increment operations
- Automatic daily reset via rolling window
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, Numeric, DateTime,
    Index, text
)
from sqlalchemy.dialects.postgresql import JSONB

from app.core.database import Base
from app.core.utils import utcnow


class SourceQuota(Base):
    """
    Tracks rate limit quotas for each data source.

    Designed for multi-worker deployments with atomic operations.
    """
    __tablename__ = "source_quotas"

    id = Column(Integer, primary_key=True, index=True)
    source_name = Column(String(50), nullable=False, unique=True, index=True)

    # Rate limits
    requests_today = Column(Integer, default=0, nullable=False)
    daily_limit = Column(Integer, nullable=False)
    requests_per_second = Column(Numeric(4, 2), nullable=False)

    # Timing
    last_request_at = Column(DateTime(timezone=True))
    last_reset_at = Column(DateTime(timezone=True), server_default=text("NOW()"))

    # Health tracking
    is_healthy = Column(Boolean, default=True, nullable=False)
    consecutive_failures = Column(Integer, default=0, nullable=False)
    last_failure_at = Column(DateTime(timezone=True))
    last_success_at = Column(DateTime(timezone=True))

    # Circuit breaker state: closed, open, half_open
    circuit_state = Column(String(20), default="closed", nullable=False)
    circuit_opened_at = Column(DateTime(timezone=True))

    # Metadata
    extra_config = Column(JSONB, default=dict)
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    def __repr__(self):
        return f"<SourceQuota {self.source_name}: {self.requests_today}/{self.daily_limit}>"


class EnrichmentAttempt(Base):
    """
    Tracks individual enrichment attempts per entity per source.

    Used for debugging, metrics, and avoiding duplicate work.
    """
    __tablename__ = "enrichment_attempts"

    id = Column(Integer, primary_key=True, index=True)

    # Entity reference
    entity_type = Column(String(50), nullable=False)  # "comic_issue", "series", etc.
    entity_id = Column(Integer, nullable=False)

    # Source info
    source_name = Column(String(50), nullable=False)

    # Attempt details
    attempt_at = Column(DateTime(timezone=True), default=utcnow, nullable=False)
    success = Column(Boolean, nullable=False)
    error_message = Column(String(500))
    error_type = Column(String(100))

    # Results
    data_fields_returned = Column(JSONB)  # List of field names populated
    response_time_ms = Column(Integer)

    # Metadata
    created_at = Column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (
        # Index for finding attempts by entity
        Index('idx_enrichment_attempts_entity', 'entity_type', 'entity_id'),
        # Index for finding attempts by source
        Index('idx_enrichment_attempts_source', 'source_name'),
        # Index for finding failed attempts (for retry logic)
        Index('idx_enrichment_attempts_failed', 'source_name', 'success',
              postgresql_where=text('success = false')),
        # Index for recent attempts (idempotency window)
        Index('idx_enrichment_attempts_recent', 'entity_type', 'entity_id',
              'source_name', 'attempt_at'),
    )

    def __repr__(self):
        status = "OK" if self.success else "FAIL"
        return f"<EnrichmentAttempt {self.source_name}:{self.entity_type}:{self.entity_id} [{status}]>"


class GradingTrainingExample(Base):
    """
    Training data for CGC grade prediction model.

    Per constitution_ai_behavior.json: Tollgates, human review for <80% confidence.
    """
    __tablename__ = "grading_training_examples"

    id = Column(Integer, primary_key=True, index=True)

    # Source provenance
    source = Column(String(50), nullable=False)  # "grading_tool", "cbrealm", "cgc_sales"
    source_url = Column(String(500))

    # Input features (JSONB for flexibility)
    defects = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    defect_severity = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    cover_condition = Column(String(500))
    page_quality = Column(String(50))
    spine_condition = Column(String(500))
    raw_description = Column(String(2000))

    # Labels
    grade_numeric = Column(Numeric(3, 1), nullable=False)  # 0.5 - 10.0
    grade_label = Column(String(10), nullable=False)  # "NM+", "VF", etc.
    confidence = Column(Numeric(3, 2), nullable=False, default=0.5)

    # Validation workflow
    is_validated = Column(Boolean, default=False, nullable=False)
    validated_by = Column(String(100))
    validated_at = Column(DateTime(timezone=True))
    validation_notes = Column(String(1000))

    # Metadata
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    __table_args__ = (
        Index('idx_grading_examples_grade', 'grade_numeric'),
        Index('idx_grading_examples_validated', 'is_validated'),
        Index('idx_grading_examples_confidence', 'confidence'),
        # GIN index for defect containment queries
        Index('idx_grading_examples_defects', 'defects', postgresql_using='gin'),
    )

    def __repr__(self):
        validated = "V" if self.is_validated else "U"
        return f"<GradingExample {self.grade_label} ({self.grade_numeric}) [{validated}]>"
