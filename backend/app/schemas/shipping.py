"""
Shipping Schemas for UPS Shipping Integration v1.28.0

Pydantic models for shipping API requests and responses.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
import re


# ==================== Address Schemas ====================


class AddressCreate(BaseModel):
    """Create a new address."""
    recipient_name: str = Field(..., min_length=1, max_length=100)
    company_name: Optional[str] = Field(None, max_length=100)
    address_line1: str = Field(..., min_length=1, max_length=100)
    address_line2: Optional[str] = Field(None, max_length=100)
    address_line3: Optional[str] = Field(None, max_length=100)
    city: str = Field(..., min_length=1, max_length=100)
    state_province: str = Field(..., min_length=2, max_length=50)
    postal_code: str = Field(..., min_length=3, max_length=20)
    country_code: str = Field("US", min_length=2, max_length=2)
    phone: Optional[str] = Field(None, max_length=20)
    email: Optional[str] = Field(None, max_length=100)
    residential: bool = True
    validate_address: bool = True
    address_type: str = "shipping"

    @field_validator("country_code")
    @classmethod
    def validate_country_code(cls, v):
        return v.upper()

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        if v:
            # Remove non-digits for normalization
            digits = re.sub(r'\D', '', v)
            if len(digits) < 10 or len(digits) > 15:
                raise ValueError("Phone number must be 10-15 digits")
        return v


class AddressResponse(BaseModel):
    """Address response (PII masked)."""
    id: int
    address_type: str
    recipient_name_masked: str = Field(..., description="First initial + ***")
    company_name: Optional[str] = None
    address_line1_masked: str = Field(..., description="Street number + ***")
    city: str
    state_province: str
    postal_code: str
    country_code: str
    residential: bool
    validation_status: str
    validated_at: Optional[datetime] = None
    is_default: bool = False
    created_at: datetime

    class Config:
        from_attributes = True


class AddressValidationResponse(BaseModel):
    """Address validation result."""
    is_valid: bool
    original_address: Dict[str, Any]
    corrected_address: Optional[Dict[str, Any]] = None
    messages: List[str] = []
    validation_status: str


# ==================== Package Schemas ====================


class PackageInfo(BaseModel):
    """Package details for rate quoting and shipping."""
    weight: float = Field(..., gt=0, le=150, description="Weight in LBS")
    weight_unit: str = "LBS"
    length: Optional[float] = Field(None, gt=0, le=108, description="Length in inches")
    width: Optional[float] = Field(None, gt=0, le=108, description="Width in inches")
    height: Optional[float] = Field(None, gt=0, le=108, description="Height in inches")
    dimension_unit: str = "IN"
    package_type: str = Field("02", description="UPS package type code")
    declared_value: Optional[float] = Field(None, ge=0, description="Declared value for insurance")


# ==================== Rate Schemas ====================


class RateRequest(BaseModel):
    """Request shipping rates."""
    destination_address_id: int
    packages: Optional[List[PackageInfo]] = None
    order_id: Optional[int] = None
    service_code: Optional[str] = Field(None, description="Specific UPS service code (optional)")


class RateResponse(BaseModel):
    """A single shipping rate option."""
    quote_id: str
    service_code: str
    service_name: str
    total_rate: float
    estimated_delivery_date: Optional[datetime] = None
    estimated_transit_days: Optional[int] = None
    guaranteed_delivery: bool = False
    expires_at: datetime

    class Config:
        from_attributes = True


class RateListResponse(BaseModel):
    """List of available shipping rates."""
    rates: List[RateResponse]
    destination_postal_code: str
    destination_country: str


class SelectRateRequest(BaseModel):
    """Select a rate quote."""
    quote_id: str


# ==================== Shipment Schemas ====================


class ShipmentCreate(BaseModel):
    """Create a shipment (generate label)."""
    order_id: int
    destination_address_id: int
    service_code: str
    packages: Optional[List[PackageInfo]] = None
    signature_required: bool = False
    declared_value: Optional[float] = None
    label_format: str = Field("ZPL", pattern="^(ZPL|GIF|PNG|EPL)$")


class TrackingEventResponse(BaseModel):
    """A single tracking event."""
    event_type: str
    description: str
    event_time: datetime
    city: Optional[str] = None
    state_province: Optional[str] = None
    postal_code: Optional[str] = None
    country_code: Optional[str] = None

    class Config:
        from_attributes = True


class ShipmentResponse(BaseModel):
    """Shipment details."""
    id: int
    order_id: int
    tracking_number: Optional[str] = None
    tracking_url: Optional[str] = None
    service_code: str
    service_name: str
    status: str
    status_detail: Optional[str] = None
    weight: float
    package_count: int
    shipping_cost: Optional[float] = None
    carrier_cost: Optional[float] = None
    label_format: Optional[str] = None
    has_label: bool = False
    signature_required: bool = False
    actual_delivery_date: Optional[datetime] = None
    estimated_delivery_date: Optional[datetime] = None
    created_at: datetime
    shipped_at: Optional[datetime] = None
    last_tracking_update: Optional[datetime] = None
    tracking_events: Optional[List[TrackingEventResponse]] = None

    class Config:
        from_attributes = True


class ShipmentListResponse(BaseModel):
    """List of shipments."""
    shipments: List[ShipmentResponse]
    total: int


class LabelResponse(BaseModel):
    """Shipping label data."""
    shipment_id: int
    tracking_number: str
    label_format: str
    label_data: str  # Base64 encoded
    created_at: datetime


class TrackingResponse(BaseModel):
    """Complete tracking information."""
    shipment_id: int
    tracking_number: str
    status: str
    status_detail: Optional[str] = None
    delivered: bool = False
    delivery_date: Optional[datetime] = None
    estimated_delivery: Optional[datetime] = None
    signature: Optional[str] = None
    events: List[TrackingEventResponse] = []


class VoidShipmentRequest(BaseModel):
    """Void a shipment."""
    shipment_id: int
    reason: Optional[str] = None


class VoidShipmentResponse(BaseModel):
    """Void shipment result."""
    success: bool
    shipment_id: int
    message: str
