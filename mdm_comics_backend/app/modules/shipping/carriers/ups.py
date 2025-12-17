"""
UPS Carrier Implementation v1.0.0

Per 20251216_shipping_compartmentalization_proposal.json:
- Implements BaseCarrier interface
- Wraps existing UPSClient for backward compatibility
- Registered via @register_carrier decorator
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
from app.services.ups_client import (
    UPSClient,
    UPSCredentials,
    UPSAddress,
    UPSPackage,
    UPSAPIError,
)
from app.services.encryption import decrypt_pii

logger = logging.getLogger(__name__)

# UPS Rate Quote TTL: 30 minutes
UPS_RATE_TTL_SECONDS = 1800

# UPS Status to ShipmentStatus mapping
UPS_STATUS_MAP = {
    # Delivered
    "D": ShipmentStatus.DELIVERED,
    "DELIVERED": ShipmentStatus.DELIVERED,
    # In Transit
    "I": ShipmentStatus.IN_TRANSIT,
    "IN TRANSIT": ShipmentStatus.IN_TRANSIT,
    "IN_TRANSIT": ShipmentStatus.IN_TRANSIT,
    # Out for Delivery
    "O": ShipmentStatus.OUT_FOR_DELIVERY,
    "OUT FOR DELIVERY": ShipmentStatus.OUT_FOR_DELIVERY,
    # Picked Up
    "P": ShipmentStatus.PICKED_UP,
    "PICKED UP": ShipmentStatus.PICKED_UP,
    "PICKUP": ShipmentStatus.PICKED_UP,
    # Exception
    "X": ShipmentStatus.EXCEPTION,
    "EXCEPTION": ShipmentStatus.EXCEPTION,
    # Returned
    "RS": ShipmentStatus.RETURNED,
    "RETURNED": ShipmentStatus.RETURNED,
    # Label Created (Manifest)
    "M": ShipmentStatus.LABEL_CREATED,
    "MV": ShipmentStatus.LABEL_CREATED,
    "LABEL CREATED": ShipmentStatus.LABEL_CREATED,
    "MANIFEST": ShipmentStatus.LABEL_CREATED,
    "BILLING INFORMATION RECEIVED": ShipmentStatus.LABEL_CREATED,
}


@register_carrier(CarrierCode.UPS)
class UPSCarrier(BaseCarrier):
    """
    UPS shipping carrier implementation.

    Wraps the existing UPSClient to implement BaseCarrier interface.
    Provides backward compatibility while enabling multi-carrier architecture.
    """

    def __init__(self, carrier_config: Optional[Carrier] = None):
        super().__init__(carrier_config)
        self._ups_client: Optional[UPSClient] = None

    @property
    def carrier_code(self) -> CarrierCode:
        return CarrierCode.UPS

    @property
    def carrier_name(self) -> str:
        return "UPS"

    def get_rate_ttl(self) -> int:
        return UPS_RATE_TTL_SECONDS

    def _get_ups_client(self) -> UPSClient:
        """
        Get or create the UPS client instance.

        Uses carrier config if available, otherwise falls back to env vars.
        """
        if self._ups_client:
            return self._ups_client

        if self._config:
            # Use credentials from Carrier model
            credentials = UPSCredentials(
                client_id=decrypt_pii(self._config.client_id_encrypted) if self._config.client_id_encrypted else "",
                client_secret=decrypt_pii(self._config.client_secret_encrypted) if self._config.client_secret_encrypted else "",
                account_number=decrypt_pii(self._config.account_number_encrypted) if self._config.account_number_encrypted else "",
                use_sandbox=self._config.use_sandbox or False,
            )
        else:
            # Fall back to environment variables
            from app.core.config import settings
            credentials = UPSCredentials(
                client_id=settings.UPS_CLIENT_ID or "",
                client_secret=settings.UPS_CLIENT_SECRET or "",
                account_number=settings.UPS_ACCOUNT_NUMBER or "",
                use_sandbox=getattr(settings, "UPS_SANDBOX_MODE", False),
            )

        self._ups_client = UPSClient(credentials)
        return self._ups_client

    def _convert_to_ups_address(self, address: AddressInput) -> UPSAddress:
        """Convert AddressInput to UPSAddress."""
        return UPSAddress(
            name=address.recipient_name or "Recipient",
            address_line1=address.address_line1,
            address_line2=address.address_line2,
            address_line3=address.address_line3,
            city=address.city,
            state_province=address.state_province,
            postal_code=address.postal_code,
            country_code=address.country_code,
            company_name=address.company_name,
            phone=address.phone,
            email=address.email,
            residential=address.residential,
        )

    def _convert_to_ups_package(self, package: Package) -> UPSPackage:
        """Convert Package to UPSPackage."""
        return UPSPackage(
            weight=package.weight,
            length=package.length if package.length > 0 else None,
            width=package.width if package.width > 0 else None,
            height=package.height if package.height > 0 else None,
            package_type=package.package_type,
            declared_value=package.declared_value if package.declared_value > 0 else None,
        )

    async def validate_address(self, address: AddressInput) -> AddressValidationResult:
        """Validate address using UPS Address Validation API."""
        try:
            client = self._get_ups_client()
            ups_address = self._convert_to_ups_address(address)

            is_valid, corrected, messages = await client.validate_address(ups_address)

            corrected_address = None
            if corrected:
                corrected_address = AddressInput(
                    address_line1=corrected.address_line1,
                    address_line2=corrected.address_line2,
                    address_line3=corrected.address_line3,
                    city=corrected.city,
                    state_province=corrected.state_province,
                    postal_code=corrected.postal_code,
                    country_code=corrected.country_code,
                    recipient_name=corrected.name,
                    company_name=corrected.company_name,
                    phone=corrected.phone,
                    email=corrected.email,
                    residential=corrected.residential,
                )

            status = "VALID" if is_valid else ("CORRECTED" if corrected_address else "INVALID")

            return AddressValidationResult(
                is_valid=is_valid,
                original_address=address,
                corrected_address=corrected_address,
                validation_messages=messages,
                status=status,
            )

        except UPSAPIError as e:
            logger.error(f"UPS address validation error: {e.message}")
            return AddressValidationResult(
                is_valid=False,
                original_address=address,
                validation_messages=[f"Validation error: {e.message}"],
                status="INVALID",
            )

    async def get_rates(
        self,
        origin: AddressInput,
        destination: AddressInput,
        packages: List[Package]
    ) -> List[Rate]:
        """Get shipping rates from UPS."""
        try:
            client = self._get_ups_client()
            ups_origin = self._convert_to_ups_address(origin)
            ups_destination = self._convert_to_ups_address(destination)
            ups_packages = [self._convert_to_ups_package(p) for p in packages]

            ups_rates = await client.get_rates(ups_origin, ups_destination, ups_packages)

            rates = []
            for ups_rate in ups_rates:
                rates.append(Rate(
                    carrier_code=CarrierCode.UPS,
                    service_code=ups_rate.service_code,
                    service_name=ups_rate.service_name,
                    rate=ups_rate.total_charges,
                    currency=ups_rate.currency,
                    delivery_date=ups_rate.estimated_delivery,
                    delivery_days=ups_rate.estimated_days,
                    guaranteed=ups_rate.guaranteed_delivery,
                    ttl_seconds=self.get_rate_ttl(),
                ))

            return rates

        except UPSAPIError as e:
            logger.error(f"UPS get rates error: {e.message}")
            return []

    async def create_shipment(self, request: ShipmentRequest) -> ShipmentResult:
        """Create a shipment and generate label via UPS."""
        try:
            client = self._get_ups_client()
            ups_origin = self._convert_to_ups_address(request.origin)
            ups_destination = self._convert_to_ups_address(request.destination)
            ups_packages = [self._convert_to_ups_package(p) for p in request.packages]

            # Combine references if both provided
            reference = request.reference1
            if request.reference1 and request.reference2:
                reference = f"{request.reference1} | {request.reference2}"
            elif request.reference2:
                reference = request.reference2

            ups_result = await client.create_shipment(
                origin=ups_origin,
                destination=ups_destination,
                packages=ups_packages,
                service_code=request.service_code,
                label_format=request.label_format,
                signature_required=request.signature_required,
                reference=reference,
            )

            return ShipmentResult(
                success=True,
                tracking_number=ups_result.tracking_number,
                tracking_url=self.get_tracking_url(ups_result.tracking_number),
                label_data=ups_result.label_data,
                label_format=ups_result.label_format,
                carrier_cost=ups_result.total_charges,
            )

        except UPSAPIError as e:
            logger.error(f"UPS create shipment error: {e.message}")
            return ShipmentResult(
                success=False,
                error_message=e.message,
            )

    async def get_tracking(self, tracking_number: str) -> TrackingInfo:
        """Get tracking information from UPS."""
        try:
            client = self._get_ups_client()
            ups_result = await client.track_shipment(tracking_number)

            events = []
            for ups_event in ups_result.events:
                events.append(TrackingEvent(
                    timestamp=ups_event.event_time,
                    status=ups_event.event_type,
                    description=ups_event.description,
                    city=ups_event.city,
                    state=ups_event.state,
                    country=ups_event.country,
                    location=f"{ups_event.city}, {ups_event.state}" if ups_event.city else None,
                    signed_by=None,
                ))

            return TrackingInfo(
                tracking_number=tracking_number,
                carrier_code=CarrierCode.UPS,
                status=self.map_status(ups_result.status),
                carrier_status=ups_result.status,
                estimated_delivery=ups_result.estimated_delivery,
                actual_delivery=ups_result.delivery_date if ups_result.delivered else None,
                events=events,
                last_updated=datetime.now(timezone.utc),
            )

        except UPSAPIError as e:
            logger.error(f"UPS tracking error: {e.message}")
            return TrackingInfo(
                tracking_number=tracking_number,
                carrier_code=CarrierCode.UPS,
                status=ShipmentStatus.EXCEPTION,
                carrier_status="ERROR",
                events=[TrackingEvent(
                    timestamp=datetime.now(timezone.utc),
                    status="ERROR",
                    description=f"Tracking error: {e.message}",
                )],
            )

    async def void_shipment(self, tracking_number: str) -> VoidResult:
        """
        Void/cancel a UPS shipment.

        Note: UPS void API takes shipment_id which is typically the tracking number
        for single-package shipments.
        """
        try:
            client = self._get_ups_client()
            # For UPS, tracking number is often used as shipment ID for single-package shipments
            success = await client.void_shipment(shipment_id=tracking_number)

            return VoidResult(
                success=success,
                tracking_number=tracking_number,
            )

        except UPSAPIError as e:
            logger.error(f"UPS void shipment error: {e.message}")
            return VoidResult(
                success=False,
                tracking_number=tracking_number,
                error_message=e.message,
            )

    def get_tracking_url(self, tracking_number: str) -> str:
        """Get public UPS tracking URL."""
        return f"https://www.ups.com/track?tracknum={tracking_number}"

    def map_status(self, carrier_status: str) -> ShipmentStatus:
        """Map UPS status to normalized ShipmentStatus."""
        status_upper = carrier_status.upper().strip()

        # Check direct mapping
        if status_upper in UPS_STATUS_MAP:
            return UPS_STATUS_MAP[status_upper]

        # Check partial matches
        for key, value in UPS_STATUS_MAP.items():
            if key in status_upper:
                return value

        # Default to in_transit for unknown statuses
        logger.warning(f"Unknown UPS status: {carrier_status}, defaulting to IN_TRANSIT")
        return ShipmentStatus.IN_TRANSIT
