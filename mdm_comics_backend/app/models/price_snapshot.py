"""
Price Snapshot Model

v1.7.0: Price Snapshots for AI Intelligence
Daily point-in-time capture of ALL prices for AI/ML model development.
Unlike price_changelog (deltas only), this captures complete state daily.

Design Principles:
- One row per entity per day (regardless of change)
- Includes derived ML features (volatility, trend, momentum)
- Tracks data quality/confidence per snapshot
- Supports time-series AI models with regular interval data

Governance Compliance:
- constitution_db.json: snake_case naming, PK, indexes, partitioning
- constitution_cyberSec.json: No PII (public market data)
- constitution_data_hygiene.json: Indefinite retention for AI training
"""
from sqlalchemy import (
    Column,
    BigInteger,
    Integer,
    String,
    Date,
    Numeric,
    Boolean,
    DateTime,
    Index,
    CheckConstraint,
    text,
)
from sqlalchemy.sql import func

from app.core.database import Base


class PriceSnapshot(Base):
    """Daily price snapshot for AI/ML training data."""
    __tablename__ = "price_snapshots"

    # Primary key - BIGSERIAL for high volume
    id = Column(BigInteger, primary_key=True)

    # Snapshot identity
    snapshot_date = Column(Date, nullable=False, index=True)
    entity_type = Column(String(50), nullable=False)  # 'funko' or 'comic'
    entity_id = Column(Integer, nullable=False)

    # External correlation
    pricecharting_id = Column(Integer, nullable=True, index=True)

    # Price fields (all nullable - entity may not have all price types)
    price_loose = Column(Numeric(12, 2), nullable=True)  # Out-of-box/ungraded
    price_cib = Column(Numeric(12, 2), nullable=True)    # Complete in box
    price_new = Column(Numeric(12, 2), nullable=True)    # Sealed/new
    price_graded = Column(Numeric(12, 2), nullable=True)  # Generic graded (comics)
    price_bgs_10 = Column(Numeric(12, 2), nullable=True)  # BGS 10 grade (comics)
    price_cgc_98 = Column(Numeric(12, 2), nullable=True)  # CGC 9.8 grade (comics)
    price_cgc_96 = Column(Numeric(12, 2), nullable=True)  # CGC 9.6 grade (comics)

    # Sales volume (from PriceCharting) - valuable for demand correlation
    sales_volume = Column(Integer, nullable=True)

    # Change tracking
    price_changed = Column(Boolean, nullable=False, default=False)  # Any price change since yesterday?
    days_since_change = Column(Integer, nullable=True)  # Days since last price movement

    # ML Features (pre-computed for training efficiency)
    volatility_7d = Column(Numeric(8, 4), nullable=True)   # 7-day price volatility (std dev)
    volatility_30d = Column(Numeric(8, 4), nullable=True)  # 30-day price volatility
    trend_7d = Column(Numeric(8, 4), nullable=True)        # 7-day trend direction (-1 to +1)
    trend_30d = Column(Numeric(8, 4), nullable=True)       # 30-day trend direction
    momentum = Column(Numeric(8, 4), nullable=True)        # Price momentum indicator

    # Data quality
    data_source = Column(String(50), nullable=False, default='pricecharting')
    confidence_score = Column(Numeric(3, 2), nullable=True)  # Data quality score 0.00-1.00
    is_stale = Column(Boolean, nullable=False, default=False)  # Flag if data > 7 days old

    # Timestamps
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        # Primary lookup + uniqueness (one snapshot per entity per day)
        Index(
            'ix_price_snapshots_lookup',
            'entity_type', 'entity_id', 'snapshot_date',
            unique=True
        ),
        # Entity history queries
        Index('ix_price_snapshots_entity', 'entity_type', 'entity_id'),
        # ML feature queries (find volatile items)
        Index('ix_price_snapshots_volatility', 'entity_type', 'snapshot_date', 'volatility_30d'),
        # Find changed items by date
        Index('ix_price_snapshots_changed', 'snapshot_date', 'price_changed'),
        # Constraints
        CheckConstraint(
            "entity_type IN ('funko', 'comic')",
            name='ck_price_snapshots_entity_type'
        ),
        CheckConstraint(
            "confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 1)",
            name='ck_price_snapshots_confidence_range'
        ),
    )

    def __repr__(self):
        return f"<PriceSnapshot {self.entity_type}:{self.entity_id} @ {self.snapshot_date}>"

    @property
    def primary_price(self) -> float | None:
        """Return the most commonly used price (loose for funkos, cib for comics)."""
        if self.entity_type == 'funko':
            return float(self.price_loose) if self.price_loose else None
        return float(self.price_cib) if self.price_cib else None

    def to_feature_dict(self) -> dict:
        """Export snapshot as feature dictionary for ML models."""
        return {
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'snapshot_date': self.snapshot_date.isoformat() if self.snapshot_date else None,
            'price_loose': float(self.price_loose) if self.price_loose else None,
            'price_cib': float(self.price_cib) if self.price_cib else None,
            'price_new': float(self.price_new) if self.price_new else None,
            'sales_volume': self.sales_volume,
            'price_changed': self.price_changed,
            'days_since_change': self.days_since_change,
            'volatility_7d': float(self.volatility_7d) if self.volatility_7d else None,
            'volatility_30d': float(self.volatility_30d) if self.volatility_30d else None,
            'trend_7d': float(self.trend_7d) if self.trend_7d else None,
            'trend_30d': float(self.trend_30d) if self.trend_30d else None,
            'momentum': float(self.momentum) if self.momentum else None,
            'confidence_score': float(self.confidence_score) if self.confidence_score else None,
            'is_stale': self.is_stale,
        }
