"""
Order models

Updated for UPS Shipping Integration v1.28.0:
- Added normalized_address_id FK for normalized addresses
- Added shipments relationship
- Added shipment_rates relationship

DB-004/DB-005/MED-001: Added indexes and audit columns per constitution_db.json
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, Text, JSON, Numeric, Index
from sqlalchemy.orm import relationship

from app.core.database import Base


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    # DB-003: FK with SET NULL to preserve order history per constitution_db.json
    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    # Order details
    order_number = Column(String, unique=True, index=True, nullable=False)
    status = Column(String, default="pending", index=True)  # MED-001: indexed for filtering

    # Pricing - DB-001: Numeric(12,2) per constitution_db.json Section 5
    subtotal = Column(Numeric(12, 2), nullable=False)
    shipping_cost = Column(Numeric(12, 2), default=0.0)
    tax = Column(Numeric(12, 2), default=0.0)
    total = Column(Numeric(12, 2), nullable=False)

    # Shipping (legacy JSON field retained for backward compatibility)
    shipping_address = Column(JSON)
    shipping_method = Column(String)
    tracking_number = Column(String)

    # UPS Shipping Integration v1.28.0: Normalized address reference
    normalized_address_id = Column(Integer, ForeignKey("addresses.id"), nullable=True)

    # Payment
    payment_method = Column(String)
    payment_id = Column(String)  # Stripe/PayPal transaction ID

    # Notes
    notes = Column(Text)

    # Timestamps
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)  # MED-001: indexed
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    paid_at = Column(DateTime)
    shipped_at = Column(DateTime)
    delivered_at = Column(DateTime)

    # DB-005: Audit trail columns per constitution_db.json Section 5
    updated_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    update_reason = Column(String(255), nullable=True)

    # Relationships
    user = relationship("User", back_populates="orders", foreign_keys=[user_id])
    items = relationship("OrderItem", back_populates="order")
    # UPS Shipping Integration v1.28.0
    normalized_address = relationship("Address", back_populates="orders", foreign_keys=[normalized_address_id])
    shipments = relationship("Shipment", back_populates="order")
    shipment_rates = relationship("ShipmentRate", back_populates="order")
    updater = relationship("User", foreign_keys=[updated_by])
    # BCW Dropship Integration v1.0.0
    bcw_order = relationship("BCWOrder", back_populates="order", uselist=False)

    # BCW Refund Request Module v1.0.0
    refund_requests = relationship("BCWRefundRequest", back_populates="order")

    # DB-004/MED-001: Additional indexes
    __table_args__ = (
        Index('ix_orders_user_id', 'user_id'),
        Index('ix_orders_paid_at', 'paid_at', postgresql_where=paid_at.isnot(None)),
    )


class OrderItem(Base):
    __tablename__ = "order_items"

    id = Column(Integer, primary_key=True, index=True)
    # DB-003: FK cascades per constitution_db.json Section 5
    order_id = Column(Integer, ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)  # DB-004
    product_id = Column(Integer, ForeignKey("products.id", ondelete="SET NULL"), nullable=True, index=True)  # DB-004

    # Snapshot of product at time of order
    product_name = Column(String(500), nullable=False)  # MED-002: bounded length
    product_sku = Column(String(50))  # MED-002: bounded length
    price = Column(Numeric(12, 2), nullable=False)  # DB-001: Numeric for monetary
    quantity = Column(Integer, nullable=False)

    # BCW Refund Request Module v1.0.0: Category/source for refund eligibility
    category = Column(String(100), nullable=True)  # Product category at time of order
    source = Column(String(50), nullable=True)  # Product source (bcw, inventory, etc.)

    # Relationships
    order = relationship("Order", back_populates="items")
    product = relationship("Product", back_populates="order_items")
