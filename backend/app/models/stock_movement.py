"""
Stock Movement model for inventory tracking and audit

Per constitution_db.json Section 5:
- Track change provenance (who, when, reason)
- Primary key, not null for required columns
- snake_case naming enforced
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Text,
    CheckConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class StockMovement(Base):
    """Audit trail for inventory stock changes"""
    __tablename__ = "stock_movements"

    id = Column(Integer, primary_key=True, index=True)

    # What changed
    product_id = Column(
        Integer,
        ForeignKey("products.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    # Movement details
    movement_type = Column(String(30), nullable=False, index=True)
    # received, sold, adjustment, damaged, returned, transfer

    quantity = Column(Integer, nullable=False)  # positive for in, negative for out
    previous_stock = Column(Integer, nullable=False)
    new_stock = Column(Integer, nullable=False)

    # Why
    reason = Column(String(255), nullable=True)

    # Reference to source
    reference_type = Column(String(50), nullable=True)
    # order, adjustment, scan_queue, manual, checkout, return
    reference_id = Column(Integer, nullable=True)

    # Who
    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=False
    )

    # When
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True
    )

    # Relationships
    product = relationship("Product", back_populates="stock_movements")
    user = relationship("User")

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "movement_type IN ('received', 'sold', 'adjustment', 'damaged', 'returned', 'transfer')",
            name="chk_movement_type"
        ),
        Index("ix_stock_movements_created_desc", created_at.desc()),
        Index("ix_stock_movements_product_created", product_id, created_at.desc()),
    )

    def __repr__(self):
        return f"<StockMovement {self.id}: {self.movement_type} {self.quantity:+d} on product {self.product_id}>"
