"""
Order models

Updated for UPS Shipping Integration v1.28.0:
- Added normalized_address_id FK for normalized addresses
- Added shipments relationship
- Added shipment_rates relationship
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, Text, JSON
from sqlalchemy.orm import relationship

from app.core.database import Base


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Order details
    order_number = Column(String, unique=True, index=True, nullable=False)
    status = Column(String, default="pending")  # pending, paid, shipped, delivered, cancelled

    # Pricing
    subtotal = Column(Float, nullable=False)
    shipping_cost = Column(Float, default=0.0)
    tax = Column(Float, default=0.0)
    total = Column(Float, nullable=False)

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
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    paid_at = Column(DateTime)
    shipped_at = Column(DateTime)
    delivered_at = Column(DateTime)

    # Relationships
    user = relationship("User", back_populates="orders")
    items = relationship("OrderItem", back_populates="order")
    # UPS Shipping Integration v1.28.0
    normalized_address = relationship("Address", back_populates="orders", foreign_keys=[normalized_address_id])
    shipments = relationship("Shipment", back_populates="order")
    shipment_rates = relationship("ShipmentRate", back_populates="order")


class OrderItem(Base):
    __tablename__ = "order_items"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    
    # Snapshot of product at time of order
    product_name = Column(String, nullable=False)
    product_sku = Column(String)
    price = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False)
    
    # Relationships
    order = relationship("Order", back_populates="items")
    product = relationship("Product", back_populates="order_items")
