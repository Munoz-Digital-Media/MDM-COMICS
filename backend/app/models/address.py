"""
Address model for UPS Shipping Integration v1.28.0

Stores normalized shipping addresses with encrypted PII fields.
Supports address validation status and geocoding data.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    ForeignKey, Text, Index, Enum as SQLEnum
)
from sqlalchemy.orm import relationship
import enum

from app.core.database import Base


class AddressType(str, enum.Enum):
    """Type of address"""
    SHIPPING = "shipping"
    BILLING = "billing"
    ORIGIN = "origin"  # Store/warehouse origin


class AddressValidationStatus(str, enum.Enum):
    """Address validation status from UPS API"""
    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"
    AMBIGUOUS = "ambiguous"  # Multiple matches, needs clarification
    CORRECTED = "corrected"  # Valid after auto-correction


class Address(Base):
    """
    Normalized address storage with encryption for PII fields.

    Per constitution_binder.json: PII fields are encrypted at rest.
    Address validation status tracks UPS API verification.
    """
    __tablename__ = "addresses"
    __table_args__ = (
        Index("ix_addresses_user_id", "user_id"),
        Index("ix_addresses_country_code", "country_code"),
        Index("ix_addresses_postal_code", "postal_code"),
        Index("ix_addresses_validation_status", "validation_status"),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)  # Null for guest checkout

    # Address type
    address_type = Column(
        SQLEnum(AddressType),
        default=AddressType.SHIPPING,
        nullable=False
    )

    # Core address fields (recipient info encrypted)
    recipient_name_encrypted = Column(Text, nullable=False)  # AES-256 encrypted
    company_name_encrypted = Column(Text, nullable=True)  # AES-256 encrypted
    phone_hash = Column(String(255), nullable=True)  # SHA-256 hash for lookup
    phone_last4 = Column(String(4), nullable=True)  # Last 4 for display
    phone_encrypted = Column(Text, nullable=True)  # Full phone AES-256 encrypted
    email_encrypted = Column(Text, nullable=True)  # AES-256 encrypted

    # Address lines (street info encrypted)
    address_line1_encrypted = Column(Text, nullable=False)  # AES-256 encrypted
    address_line2_encrypted = Column(Text, nullable=True)  # AES-256 encrypted
    address_line3_encrypted = Column(Text, nullable=True)  # AES-256 encrypted

    # Location fields (not encrypted - needed for shipping calculations)
    city = Column(String(100), nullable=False)
    state_province = Column(String(100), nullable=True)  # State/Province/Region
    postal_code = Column(String(20), nullable=False)
    country_code = Column(String(2), nullable=False)  # ISO 3166-1 alpha-2

    # UPS-specific fields
    residential = Column(Boolean, default=True)  # Residential vs Commercial
    urbanization_code = Column(String(50), nullable=True)  # Puerto Rico specific

    # Validation tracking
    validation_status = Column(
        SQLEnum(AddressValidationStatus),
        default=AddressValidationStatus.PENDING,
        nullable=False
    )
    validation_messages = Column(Text, nullable=True)  # JSON array of validation notes
    validated_at = Column(DateTime(timezone=True), nullable=True)

    # UPS Address Classification
    ups_classification = Column(String(50), nullable=True)  # "COMMERCIAL", "RESIDENTIAL", "UNKNOWN"

    # Geocoding (optional, for future use)
    latitude = Column(String(20), nullable=True)
    longitude = Column(String(20), nullable=True)

    # Address normalization tracking
    original_input = Column(Text, nullable=True)  # Original user input (encrypted)
    normalized = Column(Boolean, default=False)  # Has been normalized

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    # Relationships
    user = relationship("User", back_populates="addresses")
    shipments = relationship("Shipment", back_populates="destination_address", foreign_keys="Shipment.destination_address_id")
    orders = relationship("Order", back_populates="normalized_address", foreign_keys="Order.normalized_address_id")

    @property
    def is_valid(self) -> bool:
        """Check if address is valid for shipping."""
        return self.validation_status in [
            AddressValidationStatus.VALID,
            AddressValidationStatus.CORRECTED
        ]

    @property
    def is_deleted(self) -> bool:
        """Check if address is soft deleted."""
        return self.deleted_at is not None

    def __repr__(self):
        return f"<Address(id={self.id}, city={self.city}, country={self.country_code}, status={self.validation_status})>"
