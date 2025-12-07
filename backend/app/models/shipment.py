"""
Shipment and ShipmentRate models for UPS Shipping Integration v1.28.0

Tracks shipments from rate quote through delivery.
Stores rate quotes with TTL for shopping cart display.
"""
from datetime import datetime, timezone, timedelta
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    Float, Text, JSON, ForeignKey, Index, Enum as SQLEnum
)
from sqlalchemy.orm import relationship
import enum

from app.core.database import Base


class ShipmentStatus(str, enum.Enum):
    """Shipment lifecycle status"""
    DRAFT = "draft"  # Rate quoted, not yet purchased
    LABEL_PENDING = "label_pending"  # Payment complete, generating label
    LABEL_CREATED = "label_created"  # Label ready to print
    PICKED_UP = "picked_up"  # Carrier has package
    IN_TRANSIT = "in_transit"
    OUT_FOR_DELIVERY = "out_for_delivery"
    DELIVERED = "delivered"
    EXCEPTION = "exception"  # Delivery issue
    RETURNED = "returned"
    CANCELLED = "cancelled"
    VOIDED = "voided"  # Label voided before pickup


class ShipmentRate(Base):
    """
    Cached shipping rate quotes.

    Rates expire after TTL (default 30 min) and must be re-quoted.
    Stores all rate options for a given origin/destination/package.
    """
    __tablename__ = "shipment_rates"
    __table_args__ = (
        Index("ix_shipment_rates_order_id", "order_id"),
        Index("ix_shipment_rates_quote_id", "quote_id"),
        Index("ix_shipment_rates_expires_at", "expires_at"),
    )

    id = Column(Integer, primary_key=True, index=True)

    # Quote identification
    quote_id = Column(String(50), unique=True, nullable=False, index=True)  # UUID for client reference
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=True)  # If associated with order

    # Carrier
    carrier_id = Column(Integer, ForeignKey("carriers.id"), nullable=False)
    service_code = Column(String(10), nullable=False)  # UPS service code (01, 02, 03, etc.)
    service_name = Column(String(100), nullable=False)  # Human readable name

    # Addresses (references for audit, not FK to prevent cascade issues)
    origin_postal_code = Column(String(20), nullable=False)
    origin_country_code = Column(String(2), nullable=False)
    destination_postal_code = Column(String(20), nullable=False)
    destination_country_code = Column(String(2), nullable=False)
    destination_residential = Column(Boolean, default=True)

    # Package details
    weight = Column(Float, nullable=False)  # In LBS or KG
    weight_unit = Column(String(10), default="LBS")
    length = Column(Float, nullable=True)
    width = Column(Float, nullable=True)
    height = Column(Float, nullable=True)
    dimension_unit = Column(String(10), default="IN")
    package_type = Column(String(10), default="02")  # UPS package type code

    # Rate breakdown
    base_rate = Column(Float, nullable=False)  # Carrier base rate
    fuel_surcharge = Column(Float, default=0.0)
    residential_surcharge = Column(Float, default=0.0)
    delivery_area_surcharge = Column(Float, default=0.0)
    other_surcharges = Column(Float, default=0.0)
    total_carrier_rate = Column(Float, nullable=False)  # Sum of above

    # Our pricing
    markup = Column(Float, default=0.0)  # Our markup
    total_rate = Column(Float, nullable=False)  # What customer pays

    # Delivery estimate
    guaranteed_delivery = Column(Boolean, default=False)
    estimated_delivery_date = Column(DateTime(timezone=True), nullable=True)
    estimated_transit_days = Column(Integer, nullable=True)

    # Carrier response data
    carrier_quote_id = Column(String(100), nullable=True)  # UPS transaction reference
    carrier_response = Column(JSON, nullable=True)  # Full API response

    # Validity
    quoted_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=False)
    selected = Column(Boolean, default=False)  # User selected this rate
    selected_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    carrier = relationship("Carrier", back_populates="shipment_rates")
    order = relationship("Order", back_populates="shipment_rates")

    @property
    def is_expired(self) -> bool:
        """Check if rate quote has expired."""
        return datetime.now(timezone.utc) > self.expires_at

    @classmethod
    def create_expiry(cls, ttl_minutes: int = 30) -> datetime:
        """Calculate expiry timestamp from now."""
        return datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)

    def __repr__(self):
        return f"<ShipmentRate(id={self.id}, service={self.service_code}, rate={self.total_rate}, expires={self.expires_at})>"


class Shipment(Base):
    """
    Tracks a shipment from label creation through delivery.

    Links orders to tracking numbers and delivery status.
    """
    __tablename__ = "shipments"
    __table_args__ = (
        Index("ix_shipments_order_id", "order_id"),
        Index("ix_shipments_tracking_number", "tracking_number"),
        Index("ix_shipments_status", "status"),
        Index("ix_shipments_carrier_id", "carrier_id"),
    )

    id = Column(Integer, primary_key=True, index=True)

    # Order association
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)

    # Carrier
    carrier_id = Column(Integer, ForeignKey("carriers.id"), nullable=False)
    service_code = Column(String(10), nullable=False)
    service_name = Column(String(100), nullable=False)

    # Tracking
    tracking_number = Column(String(100), unique=True, nullable=True, index=True)
    tracking_url = Column(String(500), nullable=True)

    # Addresses
    origin_address_id = Column(Integer, ForeignKey("addresses.id"), nullable=True)
    destination_address_id = Column(Integer, ForeignKey("addresses.id"), nullable=False)

    # Status
    status = Column(
        SQLEnum(ShipmentStatus),
        default=ShipmentStatus.DRAFT,
        nullable=False
    )
    status_detail = Column(String(255), nullable=True)  # Additional status info

    # Package details
    weight = Column(Float, nullable=False)
    weight_unit = Column(String(10), default="LBS")
    length = Column(Float, nullable=True)
    width = Column(Float, nullable=True)
    height = Column(Float, nullable=True)
    dimension_unit = Column(String(10), default="IN")
    package_type = Column(String(10), default="02")
    package_count = Column(Integer, default=1)

    # Declared value (for insurance)
    declared_value = Column(Float, default=0.0)
    declared_value_currency = Column(String(3), default="USD")

    # Costs
    shipping_cost = Column(Float, nullable=False)  # What we charged customer
    carrier_cost = Column(Float, nullable=True)  # What carrier charged us
    insurance_cost = Column(Float, default=0.0)

    # Label data
    label_format = Column(String(10), default="ZPL")  # ZPL, GIF, PNG, EPL
    label_data = Column(Text, nullable=True)  # Base64 encoded or URL
    label_url = Column(String(500), nullable=True)  # If stored externally
    label_created_at = Column(DateTime(timezone=True), nullable=True)

    # Carrier response data
    shipment_id_number = Column(String(100), nullable=True)  # UPS shipment ID
    carrier_response = Column(JSON, nullable=True)

    # Delivery details
    signature_required = Column(Boolean, default=False)
    adult_signature_required = Column(Boolean, default=False)
    saturday_delivery = Column(Boolean, default=False)
    hold_for_pickup = Column(Boolean, default=False)

    # Delivery estimates
    estimated_delivery_date = Column(DateTime(timezone=True), nullable=True)
    actual_delivery_date = Column(DateTime(timezone=True), nullable=True)
    delivery_confirmation = Column(String(255), nullable=True)

    # Tracking history (JSON array of events)
    tracking_events = Column(JSON, default=list)
    last_tracking_update = Column(DateTime(timezone=True), nullable=True)

    # Return shipment (if this is a return)
    is_return = Column(Boolean, default=False)
    original_shipment_id = Column(Integer, ForeignKey("shipments.id"), nullable=True)
    return_reason = Column(String(255), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )
    shipped_at = Column(DateTime(timezone=True), nullable=True)
    voided_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    order = relationship("Order", back_populates="shipments")
    carrier = relationship("Carrier", back_populates="shipments")
    origin_address = relationship("Address", foreign_keys=[origin_address_id])
    destination_address = relationship("Address", foreign_keys=[destination_address_id], back_populates="shipments")
    original_shipment = relationship("Shipment", remote_side=[id], foreign_keys=[original_shipment_id])

    @property
    def is_shipped(self) -> bool:
        """Check if package has left origin."""
        return self.status in [
            ShipmentStatus.PICKED_UP,
            ShipmentStatus.IN_TRANSIT,
            ShipmentStatus.OUT_FOR_DELIVERY,
            ShipmentStatus.DELIVERED,
        ]

    @property
    def is_delivered(self) -> bool:
        """Check if package was delivered."""
        return self.status == ShipmentStatus.DELIVERED

    @property
    def is_active(self) -> bool:
        """Check if shipment is still active (not terminal)."""
        terminal = [
            ShipmentStatus.DELIVERED,
            ShipmentStatus.RETURNED,
            ShipmentStatus.CANCELLED,
            ShipmentStatus.VOIDED,
        ]
        return self.status not in terminal

    def __repr__(self):
        return f"<Shipment(id={self.id}, tracking={self.tracking_number}, status={self.status})>"


class TrackingEvent(Base):
    """
    Individual tracking events for a shipment.

    Separate table for efficient querying and history.
    """
    __tablename__ = "tracking_events"
    __table_args__ = (
        Index("ix_tracking_events_shipment_id", "shipment_id"),
        Index("ix_tracking_events_event_time", "event_time"),
    )

    id = Column(Integer, primary_key=True, index=True)
    shipment_id = Column(Integer, ForeignKey("shipments.id"), nullable=False)

    # Event details
    event_code = Column(String(20), nullable=True)  # Carrier event code
    event_type = Column(String(50), nullable=False)  # Normalized type
    description = Column(String(500), nullable=False)

    # Location
    city = Column(String(100), nullable=True)
    state_province = Column(String(100), nullable=True)
    postal_code = Column(String(20), nullable=True)
    country_code = Column(String(2), nullable=True)

    # Timing
    event_time = Column(DateTime(timezone=True), nullable=False)
    received_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Carrier raw data
    carrier_data = Column(JSON, nullable=True)

    # Relationships
    shipment = relationship("Shipment")

    def __repr__(self):
        return f"<TrackingEvent(id={self.id}, shipment={self.shipment_id}, type={self.event_type})>"
