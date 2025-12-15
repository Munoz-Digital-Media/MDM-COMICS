"""
Data Acquisition Pipeline Models v1.0.0

Per 20251207_MDM_COMICS_DATA_ACQUISITION_PIPELINE.json:
- FieldChangelog: Generalized field change tracking for any entity
- DeadLetterQueue: Failed job/record storage for retry
- PipelineCheckpoint: Job resume state persistence
- DataQuarantine: Low-confidence/conflict records for review
- FieldProvenance: Source tracking per field

These models support the ETL pipeline for comic/collectible data.
"""
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean,
    ForeignKey, JSON, Numeric, Index, text, Enum as SQLAEnum
)
from sqlalchemy.dialects.postgresql import UUID
from uuid import uuid4

from app.core.database import Base
from app.core.utils import utcnow


class ChangeReason(str, Enum):
    """Reasons for data changes."""
    SYNC = "sync"              # Regular sync job
    MANUAL = "manual"          # Admin manual edit
    IMPORT = "import"          # Bulk import
    MERGE = "merge"            # Deduplication merge
    CORRECTION = "correction"  # Data correction
    API_UPDATE = "api_update"  # External API update


class FieldChangelog(Base):
    """
    Generalized field-level change tracking for any entity.

    Replaces price-specific PriceChangelog with a generic solution
    that can track any field on any entity type.

    Per pipeline spec:
    - Field-level history for all major records
    - Tracks what changed, when, why, and source
    """
    __tablename__ = "field_changelog"

    id = Column(Integer, primary_key=True)

    # Entity reference (polymorphic)
    entity_type = Column(String(50), nullable=False)  # 'comic_issue', 'funko', 'series', etc.
    entity_id = Column(Integer, nullable=False)
    entity_name = Column(String(500), nullable=True)  # Human-readable for logs

    # Field change details
    field_name = Column(String(100), nullable=False)
    old_value = Column(Text, nullable=True)  # JSON-serialized for any type
    new_value = Column(Text, nullable=True)
    value_type = Column(String(50), default='string')  # string, numeric, json, date

    # For numeric fields - change percentage
    change_pct = Column(Numeric(8, 2), nullable=True)

    # Provenance
    data_source = Column(String(50), nullable=False)  # 'pricecharting', 'metron', 'gcd', 'manual'
    reason = Column(SQLAEnum(ChangeReason), nullable=False, default=ChangeReason.SYNC)
    changed_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    # Batch tracking for sync runs
    sync_batch_id = Column(UUID(as_uuid=True), nullable=True)

    # User who made manual change (null for automated)
    changed_by_user_id = Column(Integer, ForeignKey('users.id'), nullable=True)

    __table_args__ = (
        Index('ix_field_changelog_entity', 'entity_type', 'entity_id'),
        Index('ix_field_changelog_changed_at', 'changed_at'),
        Index('ix_field_changelog_batch', 'sync_batch_id'),
        Index('ix_field_changelog_source', 'data_source', 'changed_at'),
        # Idempotency index
        Index(
            'ix_field_changelog_idempotent',
            'entity_type', 'entity_id', 'field_name', 'sync_batch_id',
            unique=True,
            postgresql_where=text('sync_batch_id IS NOT NULL')
        ),
    )


class DLQStatus(str, Enum):
    """Dead Letter Queue record status."""
    PENDING = "pending"      # Awaiting retry
    RETRYING = "retrying"    # Currently being retried
    RESOLVED = "resolved"    # Successfully processed on retry
    ABANDONED = "abandoned"  # Manual intervention required


class DeadLetterQueue(Base):
    """
    Dead Letter Queue for failed pipeline jobs.

    Per pipeline spec:
    - Failed jobs/data parked for review/retry
    - Stores failed record details for debugging
    - Supports automatic retry with backoff
    """
    __tablename__ = "dead_letter_queue"

    id = Column(Integer, primary_key=True)

    # Job identification
    job_type = Column(String(100), nullable=False)  # 'price_sync', 'metron_import', etc.
    batch_id = Column(UUID(as_uuid=True), nullable=True)

    # Entity that failed (if applicable)
    entity_type = Column(String(50), nullable=True)
    entity_id = Column(Integer, nullable=True)
    external_id = Column(String(100), nullable=True)  # e.g., pricecharting_id

    # Error details
    error_message = Column(Text, nullable=False)
    error_type = Column(String(100), nullable=True)  # Exception class name
    error_trace = Column(Text, nullable=True)        # Full traceback

    # Request/response data for debugging
    request_data = Column(JSON, nullable=True)
    response_data = Column(JSON, nullable=True)

    # Retry tracking
    status = Column(SQLAEnum(DLQStatus), nullable=False, default=DLQStatus.PENDING)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    next_retry_at = Column(DateTime(timezone=True), nullable=True)
    last_retry_at = Column(DateTime(timezone=True), nullable=True)

    # Resolution
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolved_by_user_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    resolution_notes = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    __table_args__ = (
        Index('ix_dlq_status', 'status'),
        Index('ix_dlq_job', 'job_type', 'status'),
        Index('ix_dlq_retry', 'status', 'next_retry_at'),
        Index('ix_dlq_entity', 'entity_type', 'entity_id'),
    )


class ControlSignal(str, Enum):
    """Job control signals for admin management."""
    RUN = "run"                    # Normal operation
    PAUSE_REQUESTED = "pause"      # Pause requested, job will stop at next checkpoint
    STOP_REQUESTED = "stop"        # Stop requested, job will stop and release lock


class PipelineCheckpoint(Base):
    """
    Job resume checkpoints for long-running pipeline tasks.

    Per pipeline spec:
    - All fetchers persist state to survive crashes
    - Stores last-id, last-page, cursor for resumption

    v1.20.0: Added control_signal for admin Start/Pause/Stop control
    """
    __tablename__ = "pipeline_checkpoints"

    id = Column(Integer, primary_key=True)

    # Job identification
    job_name = Column(String(100), nullable=False, unique=True)  # 'price_sync_funkos', 'metron_full_import'
    job_type = Column(String(50), nullable=False)  # 'sync', 'import', 'enrich'

    # Checkpoint state
    last_processed_id = Column(Integer, nullable=True)
    last_page = Column(Integer, nullable=True)
    cursor = Column(String(500), nullable=True)  # API cursor if applicable

    # Progress tracking
    total_processed = Column(Integer, default=0)
    total_updated = Column(Integer, default=0)
    total_errors = Column(Integer, default=0)

    # State data (flexible JSON for job-specific data)
    state_data = Column(JSON, nullable=True)

    # Batch tracking
    current_batch_id = Column(UUID(as_uuid=True), nullable=True)

    # Status
    is_running = Column(Boolean, default=False)
    last_run_started = Column(DateTime(timezone=True), nullable=True)
    last_run_completed = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)

    # v1.20.0: Admin control signal
    # - 'run': Normal operation (default)
    # - 'pause': Job should pause at next checkpoint, preserve state
    # - 'stop': Job should stop, release lock (cron will auto-resume)
    control_signal = Column(String(20), default='run', nullable=False)
    paused_at = Column(DateTime(timezone=True), nullable=True)
    paused_by_user_id = Column(Integer, ForeignKey('users.id'), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        Index('ix_checkpoint_running', 'is_running'),
        Index('ix_checkpoint_control', 'control_signal'),
    )


class QuarantineReason(str, Enum):
    """Reasons for data quarantine."""
    LOW_CONFIDENCE = "low_confidence"  # Confidence score below threshold
    FUZZY_MATCH = "fuzzy_match"        # Potential duplicate, needs review
    CONFLICT = "conflict"              # Multiple sources disagree
    VALIDATION_FAIL = "validation"     # Failed data validation
    MANUAL_FLAG = "manual_flag"        # Flagged by admin
    OUTLIER = "outlier"                # Statistical outlier (e.g., price)


class DataQuarantine(Base):
    """
    Quarantine for low-confidence or conflicting data.

    Per pipeline spec:
    - Dirty/fuzzy/low-confidence merges routed to 'Needs Review'
    - Supports human or ML curation
    """
    __tablename__ = "data_quarantine"

    id = Column(Integer, primary_key=True)

    # Entity reference
    entity_type = Column(String(50), nullable=False)
    entity_id = Column(Integer, nullable=True)  # Null if new record

    # Quarantine reason
    reason = Column(SQLAEnum(QuarantineReason), nullable=False)
    confidence_score = Column(Numeric(5, 4), nullable=True)  # 0.0000 to 1.0000

    # The data that's quarantined (full record or specific fields)
    quarantined_data = Column(JSON, nullable=False)

    # For merge conflicts - the competing values
    conflict_data = Column(JSON, nullable=True)  # {field: [{source: 'metron', value: X}, {source: 'gcd', value: Y}]}

    # For fuzzy matches - potential duplicate entities
    potential_match_ids = Column(JSON, nullable=True)  # [123, 456, 789]
    match_scores = Column(JSON, nullable=True)  # [0.92, 0.87, 0.85]

    # Source that triggered quarantine
    data_source = Column(String(50), nullable=False)
    batch_id = Column(UUID(as_uuid=True), nullable=True)

    # Status
    is_resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolved_by_user_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    resolution_action = Column(String(50), nullable=True)  # 'accept', 'reject', 'merge', 'manual_edit'
    resolution_notes = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    __table_args__ = (
        Index('ix_quarantine_status', 'is_resolved'),
        Index('ix_quarantine_entity', 'entity_type', 'entity_id'),
        Index('ix_quarantine_reason', 'reason', 'is_resolved'),
    )


class FieldProvenance(Base):
    """
    Track the source of each field value for any entity.

    Per pipeline spec:
    - Every data element knows its source(s)
    - Tracks fetch date and confidence score
    - Enables quick compliance or takedown by source
    """
    __tablename__ = "field_provenance"

    id = Column(Integer, primary_key=True)

    # Entity reference
    entity_type = Column(String(50), nullable=False)
    entity_id = Column(Integer, nullable=False)
    field_name = Column(String(100), nullable=False)

    # Source information
    data_source = Column(String(50), nullable=False)  # 'pricecharting', 'metron', 'gcd', 'manual'
    source_id = Column(String(100), nullable=True)    # External ID in that source
    source_url = Column(Text, nullable=True)          # Direct link to source

    # Confidence and trust
    confidence_score = Column(Numeric(5, 4), default=1.0)  # 0.0000 to 1.0000
    trust_weight = Column(Numeric(3, 2), default=1.0)      # Source trust multiplier

    # Licensing (for compliance/takedown)
    license_type = Column(String(100), nullable=True)  # 'CC-BY-SA-4.0', 'proprietary', 'fair_use'
    requires_attribution = Column(Boolean, default=False)
    attribution_text = Column(Text, nullable=True)

    # Field locking (for manual overrides)
    is_locked = Column(Boolean, default=False)  # Prevents automated updates
    locked_by_user_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    lock_reason = Column(Text, nullable=True)

    # Timestamps
    fetched_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        # Unique constraint - one provenance per field per entity
        Index(
            'ix_provenance_unique',
            'entity_type', 'entity_id', 'field_name',
            unique=True
        ),
        Index('ix_provenance_source', 'data_source'),
        Index('ix_provenance_locked', 'is_locked'),
        Index('ix_provenance_license', 'license_type'),
    )
