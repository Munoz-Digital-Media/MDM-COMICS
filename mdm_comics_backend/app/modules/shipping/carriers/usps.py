"""
USPS Carrier Implementation v1.0.0 (Stub)

Per 20251216_shipping_compartmentalization_proposal.json:
- Implements BaseCarrier interface
- Stub implementation pending USPS Ship API access
- Feature flag (shipping:usps) is disabled by default

Dependencies (blocked):
- USPS Ship API access
- EPS MID configuration
- OAuth token (EPS token) implementation

When USPS API access is granted, implement:
- OAuth 2.0 token management (EPS token)
- Address validation (USPS Web Tools)
- Rate calculation (eVS)
- Label generation (4x6 thermal, PDF)
- Tracking (USPS Tracking API)
"""
import logging
from datetime import datetime, timezone
from typing import List, Optional

from app.models.carrier import CarrierCode, Carrier
from app.models.shipment import ShipmentStatus
from app.modules.shipping.carriers.base import (
    BaseCarrier,
    AddressInput,
    AddressValidationResult,
    Package,
    Rate,
    ShipmentRequest,
    ShipmentResult,
    TrackingEvent,
    TrackingInfo,
    VoidResult,
)
from app.modules.shipping.carriers import register_carrier

logger = logging.getLogger(__name__)

# USPS Rate Quote TTL: 24 hours (USPS quotes are valid longer than UPS)
USPS_RATE_TTL_SECONDS = 86400

# USPS Status to ShipmentStatus mapping (to be expanded when implementing)
USPS_STATUS_MAP = {
    "DELIVERED": ShipmentStatus.DELIVERED,
    "IN_TRANSIT": ShipmentStatus.IN_TRANSIT,
    "OUT_FOR_DELIVERY": ShipmentStatus.OUT_FOR_DELIVERY,
    "ACCEPTED": ShipmentStatus.PICKED_UP,
    "PRE-SHIPMENT": ShipmentStatus.LABEL_CREATED,
    "ALERT": ShipmentStatus.EXCEPTION,
    "RETURN_TO_SENDER": ShipmentStatus.RETURNED,
}

# USPS Service Codes (to be expanded when implementing)
USPS_SERVICE_CODES = {
    "PRIORITY_MAIL_EXPRESS": "Priority Mail Express",
    "PRIORITY_MAIL": "Priority Mail",
    "FIRST_CLASS_MAIL": "First-Class Mail",
    "GROUND_ADVANTAGE": "USPS Ground Advantage",
    "MEDIA_MAIL": "Media Mail",
    "LIBRARY_MAIL": "Library Mail",
}


class USPSNotConfiguredError(Exception):
    """USPS carrier is not yet configured."""
    pass


@register_carrier(CarrierCode.USPS)
class USPSCarrier(BaseCarrier):
    """
    USPS shipping carrier implementation.

    STUB IMPLEMENTATION - Pending USPS Ship API access.

    This carrier is registered but disabled by default via feature flags.
    All methods return appropriate stub responses until USPS integration is complete.
    """

    def __init__(self, carrier_config: Optional[Carrier] = None):
        super().__init__(carrier_config)
        self._configured = False
        # TODO: Initialize USPS API client when credentials are available

    @property
    def carrier_code(self) -> CarrierCode:
        return CarrierCode.USPS

    @property
    def carrier_name(self) -> str:
        return "USPS"

    def get_rate_ttl(self) -> int:
        return USPS_RATE_TTL_SECONDS

    def _check_configured(self) -> None:
        """Check if USPS is configured and raise if not."""
        if not self._configured:
            raise USPSNotConfiguredError(
                "USPS carrier is not yet configured. Pending USPS Ship API enrollment."
            )

    async def validate_address(self, address: AddressInput) -> AddressValidationResult:
        """
        Validate address using USPS Address Validation API.

        TODO: Implement when USPS Web Tools access is granted.
        """
        logger.warning("USPS address validation not yet implemented")

        return AddressValidationResult(
            is_valid=False,
            original_address=address,
            validation_messages=["USPS address validation not yet available. Pending API enrollment."],
            status="PENDING",
        )

    async def get_rates(
        self,
        origin: AddressInput,
        destination: AddressInput,
        packages: List[Package]
    ) -> List[Rate]:
        """
        Get shipping rates from USPS.

        TODO: Implement when USPS eVS access is granted.
        """
        logger.warning("USPS rate calculation not yet implemented")

        # Return empty list - USPS rates not available yet
        return []

    async def create_shipment(self, request: ShipmentRequest) -> ShipmentResult:
        """
        Create a shipment and generate label via USPS.

        TODO: Implement when USPS Ship API access is granted.
        """
        logger.warning("USPS shipment creation not yet implemented")

        return ShipmentResult(
            success=False,
            error_message="USPS shipment creation not yet available. Pending API enrollment.",
        )

    async def get_tracking(self, tracking_number: str) -> TrackingInfo:
        """
        Get tracking information from USPS.

        TODO: Implement when USPS Tracking API access is granted.
        """
        logger.warning(f"USPS tracking not yet implemented for {tracking_number}")

        return TrackingInfo(
            tracking_number=tracking_number,
            carrier_code=CarrierCode.USPS,
            status=ShipmentStatus.EXCEPTION,
            carrier_status="NOT_IMPLEMENTED",
            events=[TrackingEvent(
                timestamp=datetime.now(timezone.utc),
                status="NOT_IMPLEMENTED",
                description="USPS tracking not yet available. Pending API enrollment.",
            )],
        )

    async def void_shipment(self, tracking_number: str) -> VoidResult:
        """
        Void/cancel a USPS shipment.

        TODO: Implement when USPS refund API access is granted.
        """
        logger.warning(f"USPS void not yet implemented for {tracking_number}")

        return VoidResult(
            success=False,
            tracking_number=tracking_number,
            error_message="USPS void/refund not yet available. Pending API enrollment.",
        )

    def get_tracking_url(self, tracking_number: str) -> str:
        """Get public USPS tracking URL."""
        return f"https://tools.usps.com/go/TrackConfirmAction?tLabels={tracking_number}"

    def map_status(self, carrier_status: str) -> ShipmentStatus:
        """Map USPS status to normalized ShipmentStatus."""
        status_upper = carrier_status.upper().strip()

        if status_upper in USPS_STATUS_MAP:
            return USPS_STATUS_MAP[status_upper]

        # Check partial matches
        for key, value in USPS_STATUS_MAP.items():
            if key in status_upper:
                return value

        # Default to in_transit for unknown statuses
        logger.warning(f"Unknown USPS status: {carrier_status}, defaulting to IN_TRANSIT")
        return ShipmentStatus.IN_TRANSIT
