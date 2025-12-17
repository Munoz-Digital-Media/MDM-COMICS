"""
Feature Flag model for runtime feature toggles v1.0.0

Per 20251216_shipping_compartmentalization_proposal.json:
- Carrier-level runtime toggles via database
- Shipping module always on; individual carriers toggleable
- No deployment required for toggling

Per constitution_binder.json:
- snake_case for all database columns
- Audit trail for state changes (disabled_by, disabled_at, disabled_reason)
"""
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    Text, JSON, Index, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import UUID
import uuid

from app.core.database import Base


class FeatureFlag(Base):
    """
    Runtime feature flags for module/carrier toggles.

    Enables runtime control without deployment:
    - Carrier toggles: (module='shipping', feature='ups')
    - Future module toggles: (module='enrichment', feature='comicvine')

    Audit trail captures who disabled what and why.
    """
    __tablename__ = "feature_flags"
    __table_args__ = (
        UniqueConstraint("module", "feature", name="uq_feature_flags_module_feature"),
        Index("idx_feature_flags_lookup", "module", "feature"),
        Index("idx_feature_flags_enabled", "module", "is_enabled"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Flag identification
    module = Column(String(50), nullable=False, index=True)  # e.g., 'shipping', 'enrichment'
    feature = Column(String(50), nullable=False)  # e.g., 'ups', 'usps', 'comicvine'

    # Toggle state
    is_enabled = Column(Boolean, default=True, nullable=False)

    # Feature-specific configuration (JSON)
    # e.g., {"sandbox_mode": false, "rate_limit": 100}
    config_json = Column(JSON, default=dict, nullable=False)

    # Audit trail for disabled state
    disabled_reason = Column(Text, nullable=True)  # Why feature is disabled
    disabled_at = Column(DateTime(timezone=True), nullable=True)  # When disabled
    disabled_by = Column(String(100), nullable=True)  # Who disabled it (user email or system)

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    def __repr__(self):
        return f"<FeatureFlag(module={self.module}, feature={self.feature}, enabled={self.is_enabled})>"

    @property
    def flag_key(self) -> str:
        """Returns the cache key for this flag: 'module:feature'"""
        return f"{self.module}:{self.feature}"

    def disable(self, reason: str, disabled_by: str) -> None:
        """Disable the feature with audit trail."""
        self.is_enabled = False
        self.disabled_reason = reason
        self.disabled_by = disabled_by
        self.disabled_at = datetime.now(timezone.utc)

    def enable(self) -> None:
        """Enable the feature and clear audit trail."""
        self.is_enabled = True
        self.disabled_reason = None
        self.disabled_by = None
        self.disabled_at = None
