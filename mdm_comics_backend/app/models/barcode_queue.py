"""
Barcode Queue model for scan-to-inventory workflow

Per constitution_db.json:
- Primary key, not null for required columns
- snake_case naming enforced
- Track change provenance (user_id, scanned_at)
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Text, DateTime, ForeignKey,
    CheckConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class BarcodeQueue(Base):
    """Queued barcode scans awaiting processing"""
    __tablename__ = "barcode_queue"

    id = Column(Integer, primary_key=True, index=True)

    # Barcode data
    barcode = Column(String(50), nullable=False, index=True)
    barcode_type = Column(String(20), default="UPC")  # UPC, ISBN, EAN

    # User who scanned
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Timestamps
    scanned_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc)
    )

    # Status workflow
    status = Column(String(20), nullable=False, default="pending", index=True)
    # pending, matched, processing, processed, failed, skipped

    # Match results
    matched_product_id = Column(Integer, ForeignKey("products.id"), nullable=True)
    matched_comic_id = Column(Integer, nullable=True)  # References comic_issues
    matched_funko_id = Column(Integer, ForeignKey("funkos.id"), nullable=True)
    match_source = Column(String(50), nullable=True)
    # existing_product, comic_issue, pricecharting, manual
    match_confidence = Column(Integer, nullable=True)  # 0-100

    # Processing
    processed_at = Column(DateTime(timezone=True), nullable=True)
    processed_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    notes = Column(Text, nullable=True)

    # Audit
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )

    # Relationships
    user = relationship("User", foreign_keys=[user_id])
    processed_by_user = relationship("User", foreign_keys=[processed_by])
    matched_product = relationship("Product", foreign_keys=[matched_product_id])
    matched_funko = relationship("Funko", foreign_keys=[matched_funko_id])

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'matched', 'processing', 'processed', 'failed', 'skipped')",
            name="chk_barcode_status"
        ),
        # PERF-001: Prevent duplicate pending scans
        Index(
            "ix_barcode_queue_pending_barcode",
            barcode,
            unique=True,
            postgresql_where=(status == "pending")
        ),
        Index("ix_barcode_queue_user_status", user_id, status),
        Index("ix_barcode_queue_scanned_at", scanned_at.desc()),
    )

    def __repr__(self):
        return f"<BarcodeQueue {self.id}: {self.barcode} ({self.status})>"
