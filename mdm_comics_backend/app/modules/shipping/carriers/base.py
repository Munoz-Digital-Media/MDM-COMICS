"""
Base Carrier Interface v1.0.0

Per 20251216_shipping_compartmentalization_proposal.json:
- All carriers implement this interface
- Can share code internally (codependent, not isolated)
- Each carrier provides its own:
  - Address validation
  - Rate calculation
  - Shipment creation
  - Tracking
  - Label generation
  - Status mapping
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Any
from enum import Enum

from app.models.carrier import CarrierCode, Carrier
from app.models.shipment import ShipmentStatus


# =============================================================================
# Carrier-Agnostic Data Classes
# =============================================================================

@dataclass
class AddressInput:
    """Input for address validation."""
    address_line1: str
    city: str
    state_province: str
    postal_code: str
    country_code: str = "US"
    address_line2: Optional[str] = None
    address_line3: Optional[str] = None
    recipient_name: Optional[str] = None
    company_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    residential: bool = True


@dataclass
class AddressValidationResult:
    """Result of address validation."""
    is_valid: bool
    original_address: AddressInput
    corrected_address: Optional[AddressInput] = None
    validation_messages: Optional[List[str]] = None
    status: str = "PENDING"  # VALID, INVALID, AMBIGUOUS, CORRECTED


@dataclass
class Package:
    """Package dimensions and weight."""
    weight: float  # pounds
    length: float = 0.0  # inches
    width: float = 0.0  # inches
    height: float = 0.0  # inches
    package_type: str = "02"  # carrier-specific code
    declared_value: float = 0.0
    description: Optional[str] = None


@dataclass
class Rate:
    """Shipping rate quote."""
    carrier_code: CarrierCode
    service_code: str
    service_name: str
    rate: float  # base carrier rate
    currency: str = "USD"
    delivery_date: Optional[datetime] = None
    delivery_days: Optional[int] = None
    guaranteed: bool = False
    ttl_seconds: int = 1800  # 30 minutes default


@dataclass
class ShipmentRequest:
    """Request to create a shipment."""
    origin: AddressInput
    destination: AddressInput
    packages: List[Package]
    service_code: str
    signature_required: bool = False
    adult_signature_required: bool = False
    declared_value: float = 0.0
    reference1: Optional[str] = None
    reference2: Optional[str] = None
    return_shipment: bool = False
    label_format: str = "ZPL"  # ZPL, GIF, PNG, PDF


@dataclass
class ShipmentResult:
    """Result of shipment creation."""
    success: bool
    tracking_number: Optional[str] = None
    tracking_url: Optional[str] = None
    label_data: Optional[str] = None  # Base64 encoded
    label_format: str = "ZPL"
    carrier_cost: float = 0.0
    error_message: Optional[str] = None
    carrier_response: Optional[Any] = None


@dataclass
class TrackingEvent:
    """A single tracking event."""
    timestamp: datetime
    status: str  # carrier-specific status
    description: str
    location: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    signed_by: Optional[str] = None


@dataclass
class TrackingInfo:
    """Full tracking information."""
    tracking_number: str
    carrier_code: CarrierCode
    status: ShipmentStatus
    carrier_status: str
    estimated_delivery: Optional[datetime] = None
    actual_delivery: Optional[datetime] = None
    events: Optional[List[TrackingEvent]] = None
    last_updated: Optional[datetime] = None


@dataclass
class VoidResult:
    """Result of voiding a shipment."""
    success: bool
    tracking_number: str
    error_message: Optional[str] = None


# =============================================================================
# Base Carrier Interface
# =============================================================================

class BaseCarrier(ABC):
    """
    Abstract base class for all shipping carriers.

    All carriers must implement these methods.
    Carriers can share utility code but must provide their own API integration.
    """

    def __init__(self, carrier_config: Optional[Carrier] = None):
        """
        Initialize the carrier.

        Args:
            carrier_config: Optional Carrier model with credentials and settings
        """
        self._config = carrier_config

    @property
    @abstractmethod
    def carrier_code(self) -> CarrierCode:
        """Return the carrier code enum value."""
        pass

    @property
    @abstractmethod
    def carrier_name(self) -> str:
        """Return the human-readable carrier name."""
        pass

    @abstractmethod
    def get_rate_ttl(self) -> int:
        """
        Return the rate quote TTL in seconds.

        Different carriers have different quote validity periods:
        - UPS: 1800 (30 minutes)
        - USPS: 86400 (24 hours)
        """
        pass

    @abstractmethod
    async def validate_address(self, address: AddressInput) -> AddressValidationResult:
        """
        Validate an address with the carrier's API.

        Args:
            address: The address to validate

        Returns:
            AddressValidationResult with validation status and corrected address
        """
        pass

    @abstractmethod
    async def get_rates(
        self,
        origin: AddressInput,
        destination: AddressInput,
        packages: List[Package]
    ) -> List[Rate]:
        """
        Get shipping rates from the carrier.

        Args:
            origin: Origin address
            destination: Destination address
            packages: List of packages to ship

        Returns:
            List of Rate objects for available services
        """
        pass

    @abstractmethod
    async def create_shipment(self, request: ShipmentRequest) -> ShipmentResult:
        """
        Create a shipment and generate label.

        Args:
            request: ShipmentRequest with all shipment details

        Returns:
            ShipmentResult with tracking number, label, and cost
        """
        pass

    @abstractmethod
    async def get_tracking(self, tracking_number: str) -> TrackingInfo:
        """
        Get tracking information for a shipment.

        Args:
            tracking_number: The tracking number to look up

        Returns:
            TrackingInfo with status and events
        """
        pass

    @abstractmethod
    async def void_shipment(self, tracking_number: str) -> VoidResult:
        """
        Void/cancel a shipment.

        Args:
            tracking_number: The tracking number to void

        Returns:
            VoidResult indicating success or failure
        """
        pass

    @abstractmethod
    def get_tracking_url(self, tracking_number: str) -> str:
        """
        Get the public tracking URL for a shipment.

        Args:
            tracking_number: The tracking number

        Returns:
            URL string for public tracking page
        """
        pass

    @abstractmethod
    def map_status(self, carrier_status: str) -> ShipmentStatus:
        """
        Map carrier-specific status to normalized ShipmentStatus enum.

        Args:
            carrier_status: The carrier's status string

        Returns:
            Normalized ShipmentStatus enum value
        """
        pass

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value from the carrier config.

        Args:
            key: Configuration key
            default: Default value if not found

        Returns:
            Configuration value or default
        """
        if not self._config:
            return default

        # Check service_levels JSON first
        if self._config.service_levels and key in self._config.service_levels:
            return self._config.service_levels[key]

        # Check model attributes
        return getattr(self._config, key, default)
