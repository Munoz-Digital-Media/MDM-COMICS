"""
Price Changelog Model

Tracks all price changes for analytics and reporting.
Originally created via raw SQL in price_sync_daily.py.
v1.5.0: Formalized as SQLAlchemy model for outreach analytics.
"""
from sqlalchemy import Column, Integer, String, Numeric, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID
from uuid import uuid4

from app.core.database import Base
from app.core.utils import utcnow


class PriceChangelog(Base):
    """Price change history for funkos and comics."""
    __tablename__ = "price_changelog"

    id = Column(Integer, primary_key=True)

    # Entity reference
    entity_type = Column(String(50), nullable=False)  # 'funko', 'comic'
    entity_id = Column(Integer, nullable=False)
    entity_name = Column(String(500), nullable=True)

    # Change details
    field_name = Column(String(100), nullable=False)  # 'price_loose', 'price_cib', etc.
    old_value = Column(Numeric(12, 2), nullable=True)
    new_value = Column(Numeric(12, 2), nullable=True)
    change_pct = Column(Numeric(8, 2), nullable=True)

    # Provenance
    data_source = Column(String(50), nullable=False, default='pricecharting')
    reason = Column(String(100), nullable=False, default='daily_sync')
    changed_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    sync_batch_id = Column(UUID(as_uuid=True), nullable=True)

    __table_args__ = (
        Index('ix_price_changelog_entity', 'entity_type', 'entity_id'),
        Index('ix_price_changelog_changed_at', 'changed_at'),
        Index('ix_price_changelog_batch', 'sync_batch_id'),
        Index('ix_price_changelog_weekly_movers', 'entity_type', 'changed_at', 'change_pct'),
    )
