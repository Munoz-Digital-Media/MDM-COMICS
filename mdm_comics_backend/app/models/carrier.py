"""
Carrier model for UPS Shipping Integration v1.28.0

Stores carrier configuration (UPS, future carriers).
Manages API credentials, service levels, and carrier-specific settings.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    Float, Text, JSON, Index, Enum as SQLEnum
)
from sqlalchemy.orm import relationship
import enum

from app.core.database import Base


class CarrierCode(str, enum.Enum):
    """
    Supported shipping carriers.

    Per 20251216_shipping_compartmentalization_proposal.json:
    - Each carrier independently toggleable via feature_flags table
    - Carrier availability controlled at runtime, not compile time
    """
    UPS = "UPS"
    USPS = "USPS"
    # Future carriers
    # FEDEX = "FEDEX"
    # DHL = "DHL"


class Carrier(Base):
    """
    Carrier configuration and settings.

    Stores API credentials (encrypted), service levels, and carrier-specific config.
    Per constitution_binder.json: API credentials encrypted at rest.
    """
    __tablename__ = "carriers"
    __table_args__ = (
        Index("ix_carriers_code", "code"),
        Index("ix_carriers_active", "is_active"),
    )

    id = Column(Integer, primary_key=True, index=True)

    # Carrier identification
    code = Column(SQLEnum(CarrierCode), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    display_name = Column(String(100), nullable=False)  # User-facing name

    # Status
    is_active = Column(Boolean, default=True, nullable=False)
    is_default = Column(Boolean, default=False, nullable=False)

    # API Configuration (encrypted)
    api_key_encrypted = Column(Text, nullable=True)
    api_secret_encrypted = Column(Text, nullable=True)
    account_number_encrypted = Column(Text, nullable=True)

    # UPS-specific OAuth credentials
    client_id_encrypted = Column(Text, nullable=True)
    client_secret_encrypted = Column(Text, nullable=True)
    oauth_token_encrypted = Column(Text, nullable=True)
    oauth_token_expires_at = Column(DateTime(timezone=True), nullable=True)

    # Carrier-agnostic credentials storage (Fernet-encrypted JSON)
    # Used for new carriers; existing carriers use dedicated columns above
    credentials_json = Column(Text, nullable=True)

    # API endpoints
    api_base_url = Column(String(255), nullable=True)
    sandbox_url = Column(String(255), nullable=True)
    use_sandbox = Column(Boolean, default=False)

    # Default settings
    default_package_type = Column(String(50), default="02")  # UPS Customer Supplied Package
    default_weight_unit = Column(String(10), default="LBS")
    default_dimension_unit = Column(String(10), default="IN")

    # Rate markup/margin
    rate_markup_percentage = Column(Float, default=0.0)  # Add X% to carrier rates
    rate_markup_flat = Column(Float, default=0.0)  # Add flat amount

    # Free shipping threshold
    free_shipping_threshold = Column(Float, nullable=True)  # Order total for free shipping
    free_shipping_service_code = Column(String(10), nullable=True)  # Which service to use

    # Service level configuration (JSON)
    # {
    #   "01": {"name": "UPS Next Day Air", "enabled": true, "markup": 0},
    #   "02": {"name": "UPS 2nd Day Air", "enabled": true, "markup": 0},
    #   "03": {"name": "UPS Ground", "enabled": true, "markup": 0},
    # }
    service_levels = Column(JSON, default=dict)

    # Origin address (store location)
    origin_address_line1 = Column(String(255), nullable=True)
    origin_address_line2 = Column(String(255), nullable=True)
    origin_city = Column(String(100), nullable=True)
    origin_state = Column(String(100), nullable=True)
    origin_postal_code = Column(String(20), nullable=True)
    origin_country_code = Column(String(2), default="US")
    origin_phone = Column(String(20), nullable=True)
    origin_company_name = Column(String(100), nullable=True)
    origin_attention_name = Column(String(100), nullable=True)

    # Shipping policy settings
    saturday_delivery_enabled = Column(Boolean, default=False)
    signature_required_threshold = Column(Float, default=100.0)  # Require signature above this
    insurance_required_threshold = Column(Float, default=200.0)  # Require insurance above this

    # Label settings
    label_format = Column(String(10), default="ZPL")  # ZPL, GIF, PNG, EPL
    label_size = Column(String(20), default="4x6")

    # Webhook configuration
    webhook_url = Column(String(500), nullable=True)
    webhook_secret = Column(String(255), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    # Relationships
    shipments = relationship("Shipment", back_populates="carrier")
    shipment_rates = relationship("ShipmentRate", back_populates="carrier")

    def __repr__(self):
        return f"<Carrier(id={self.id}, code={self.code}, name={self.name}, active={self.is_active})>"


# UPS Service Codes Reference
UPS_SERVICE_CODES = {
    # Domestic US
    "01": "UPS Next Day Air",
    "02": "UPS 2nd Day Air",
    "03": "UPS Ground",
    "12": "UPS 3 Day Select",
    "13": "UPS Next Day Air Saver",
    "14": "UPS Next Day Air Early",
    "59": "UPS 2nd Day Air A.M.",
    # International
    "07": "UPS Worldwide Express",
    "08": "UPS Worldwide Expedited",
    "11": "UPS Standard",
    "54": "UPS Worldwide Express Plus",
    "65": "UPS Saver",
    # Ground
    "92": "UPS SurePost Less Than 1 lb",
    "93": "UPS SurePost 1 lb or Greater",
    "94": "UPS SurePost BPM",
    "95": "UPS SurePost Media Mail",
}

# UPS Package Types
UPS_PACKAGE_TYPES = {
    "00": "UNKNOWN",
    "01": "UPS Letter",
    "02": "Customer Supplied Package",
    "03": "Tube",
    "04": "PAK",
    "21": "UPS Express Box",
    "24": "UPS 25KG Box",
    "25": "UPS 10KG Box",
    "30": "Pallet",
    "2a": "Small Express Box",
    "2b": "Medium Express Box",
    "2c": "Large Express Box",
    "56": "Flats",
    "57": "Parcels",
    "58": "BPM",
    "59": "First Class",
    "60": "Priority",
    "61": "Machinables",
    "62": "Irregulars",
    "63": "Parcel Post",
    "64": "BPM Parcel",
    "65": "Media Mail",
    "66": "BPM Flat",
    "67": "Standard Flat",
}
