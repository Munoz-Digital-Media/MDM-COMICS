"""
Inventory Alert model for low stock and price change notifications

Per constitution_db.json:
- Primary key, not null for required columns
- snake_case naming enforced
"""
from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Boolean, Numeric,
    CheckConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class InventoryAlert(Base):
    """Alerts for inventory conditions requiring attention"""
    __tablename__ = "inventory_alerts"

    id = Column(Integer, primary_key=True, index=True)

    # What product
    product_id = Column(
        Integer,
        ForeignKey("products.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    # Alert type
    alert_type = Column(String(30), nullable=False, index=True)
    # low_stock, out_of_stock, overstock, price_drop, price_spike

    # Threshold that triggered alert
    threshold_value = Column(Numeric(12, 2), nullable=True)
    current_value = Column(Numeric(12, 2), nullable=True)

    # Status
    is_active = Column(Boolean, nullable=False, default=True)

    # Acknowledgment
    acknowledged_at = Column(DateTime(timezone=True), nullable=True)
    acknowledged_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )

    # Relationships
    product = relationship("Product")
    acknowledged_by_user = relationship("User", foreign_keys=[acknowledged_by])

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "alert_type IN ('low_stock', 'out_of_stock', 'overstock', 'price_drop', 'price_spike')",
            name="chk_alert_type"
        ),
        Index("ix_inventory_alerts_active", is_active, alert_type),
        Index("ix_inventory_alerts_product_active", product_id, is_active),
    )

    def __repr__(self):
        return f"<InventoryAlert {self.id}: {self.alert_type} for product {self.product_id}>"
