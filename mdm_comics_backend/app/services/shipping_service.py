"""
Shipping Service for UPS Shipping Integration v1.28.0

High-level service that coordinates:
- Address creation and validation
- Rate quoting
- Label generation
- Tracking updates
- Order fulfillment

Per constitution_binder.json: All business logic centralized in services layer.
"""
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.address import Address, AddressType, AddressValidationStatus
from app.models.carrier import Carrier, CarrierCode
from app.models.shipment import Shipment, ShipmentRate, ShipmentStatus, TrackingEvent
from app.models.order import Order
from app.services.encryption import encrypt_pii, decrypt_pii, hash_phone, get_phone_last4
from app.services.ups_client import (
    UPSClient,
    UPSAddress,
    UPSPackage,
    UPSAPIError,
    create_ups_client_from_carrier,
)

logger = logging.getLogger(__name__)

# Default package weight for comics (in lbs)
DEFAULT_COMIC_WEIGHT = 0.5
MIN_PACKAGE_WEIGHT = 0.1


class ShippingError(Exception):
    """Shipping service error."""

    def __init__(self, message: str, code: str = "SHIPPING_ERROR", details: Optional[Dict] = None):
        self.message = message
        self.code = code
        self.details = details or {}
        super().__init__(message)


class ShippingService:
    """
    Central service for all shipping operations.
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self._ups_client: Optional[UPSClient] = None
        self._carrier: Optional[Carrier] = None

    async def _get_carrier(self, carrier_code: CarrierCode = CarrierCode.UPS) -> Carrier:
        """Get carrier configuration from database."""
        if self._carrier and self._carrier.code == carrier_code:
            return self._carrier

        result = await self.db.execute(
            select(Carrier).where(
                and_(
                    Carrier.code == carrier_code,
                    Carrier.is_active == True,
                )
            )
        )
        carrier = result.scalar_one_or_none()

        if not carrier:
            raise ShippingError(
                message=f"Carrier {carrier_code.value} not configured or inactive",
                code="CARRIER_NOT_FOUND",
            )

        self._carrier = carrier
        return carrier

    async def _get_ups_client(self) -> UPSClient:
        """Get or create UPS API client."""
        if self._ups_client:
            return self._ups_client

        carrier = await self._get_carrier(CarrierCode.UPS)
        self._ups_client = await create_ups_client_from_carrier(carrier)
        return self._ups_client

    async def close(self):
        """Clean up resources."""
        if self._ups_client:
            await self._ups_client.close()
            self._ups_client = None

    # ==================== Address Management ====================

    async def create_address(
        self,
        user_id: Optional[int],
        recipient_name: str,
        address_line1: str,
        city: str,
        state_province: str,
        postal_code: str,
        country_code: str,
        address_line2: Optional[str] = None,
        address_line3: Optional[str] = None,
        company_name: Optional[str] = None,
        phone: Optional[str] = None,
        email: Optional[str] = None,
        address_type: AddressType = AddressType.SHIPPING,
        residential: bool = True,
        validate: bool = True,
    ) -> Address:
        """
        Create a new address with optional validation.

        All PII fields are encrypted before storage.
        """
        # Create address model with encrypted fields
        address = Address(
            user_id=user_id,
            address_type=address_type,
            recipient_name_encrypted=encrypt_pii(recipient_name),
            company_name_encrypted=encrypt_pii(company_name) if company_name else None,
            phone_hash=hash_phone(phone) if phone else None,
            phone_last4=get_phone_last4(phone) if phone else None,
            phone_encrypted=encrypt_pii(phone) if phone else None,
            email_encrypted=encrypt_pii(email) if email else None,
            address_line1_encrypted=encrypt_pii(address_line1),
            address_line2_encrypted=encrypt_pii(address_line2) if address_line2 else None,
            address_line3_encrypted=encrypt_pii(address_line3) if address_line3 else None,
            city=city,
            state_province=state_province,
            postal_code=postal_code,
            country_code=country_code.upper(),
            residential=residential,
            validation_status=AddressValidationStatus.PENDING,
        )

        # Store original input for reference
        original = {
            "recipient_name": recipient_name,
            "address_line1": address_line1,
            "address_line2": address_line2,
            "city": city,
            "state_province": state_province,
            "postal_code": postal_code,
            "country_code": country_code,
        }
        address.original_input = encrypt_pii(json.dumps(original))

        self.db.add(address)

        # Validate if requested
        if validate and country_code.upper() in ["US", "PR", "VI", "GU", "AS"]:
            try:
                validated, corrected, messages = await self.validate_address(address)
                if validated:
                    address.validation_status = AddressValidationStatus.VALID
                    if corrected:
                        address.validation_status = AddressValidationStatus.CORRECTED
                else:
                    address.validation_status = AddressValidationStatus.INVALID

                address.validation_messages = json.dumps(messages)
                address.validated_at = datetime.now(timezone.utc)

            except Exception as e:
                logger.warning(f"Address validation failed: {e}")
                address.validation_messages = json.dumps([str(e)])

        await self.db.flush()
        return address

    async def validate_address(self, address: Address) -> Tuple[bool, Optional[Address], List[str]]:
        """
        Validate an address using UPS API.

        Returns:
            Tuple of (is_valid, corrected_address_model, messages)
        """
        ups_client = await self._get_ups_client()

        # Build UPS address from model
        ups_address = UPSAddress(
            name=decrypt_pii(address.recipient_name_encrypted),
            address_line1=decrypt_pii(address.address_line1_encrypted),
            address_line2=decrypt_pii(address.address_line2_encrypted) if address.address_line2_encrypted else None,
            city=address.city,
            state_province=address.state_province,
            postal_code=address.postal_code,
            country_code=address.country_code,
            company_name=decrypt_pii(address.company_name_encrypted) if address.company_name_encrypted else None,
            residential=address.residential,
        )

        is_valid, corrected_ups, messages = await ups_client.validate_address(ups_address)

        # If corrected, update the address
        corrected_address = None
        if corrected_ups and is_valid:
            address.address_line1_encrypted = encrypt_pii(corrected_ups.address_line1)
            if corrected_ups.address_line2:
                address.address_line2_encrypted = encrypt_pii(corrected_ups.address_line2)
            address.city = corrected_ups.city
            address.state_province = corrected_ups.state_province
            address.postal_code = corrected_ups.postal_code
            address.residential = corrected_ups.residential
            address.normalized = True
            corrected_address = address

        return is_valid, corrected_address, messages

    async def get_address(self, address_id: int, user_id: Optional[int] = None) -> Optional[Address]:
        """Get an address by ID, optionally filtered by user."""
        query = select(Address).where(
            and_(
                Address.id == address_id,
                Address.deleted_at == None,
            )
        )
        if user_id:
            query = query.where(Address.user_id == user_id)

        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def get_user_addresses(self, user_id: int, address_type: Optional[AddressType] = None) -> List[Address]:
        """Get all addresses for a user."""
        query = select(Address).where(
            and_(
                Address.user_id == user_id,
                Address.deleted_at == None,
            )
        )
        if address_type:
            query = query.where(Address.address_type == address_type)

        result = await self.db.execute(query)
        return list(result.scalars().all())

    # ==================== Rate Quoting ====================

    async def get_shipping_rates(
        self,
        destination_address_id: int,
        packages: Optional[List[Dict]] = None,
        order_id: Optional[int] = None,
        service_code: Optional[str] = None,
    ) -> List[ShipmentRate]:
        """
        Get shipping rates for a destination.

        Args:
            destination_address_id: Destination address ID
            packages: Optional list of package details (weight, dimensions)
            order_id: Optional order ID to associate rates with
            service_code: Optional specific service code to quote

        Returns:
            List of ShipmentRate models (saved to DB)
        """
        # Get destination address
        dest_address = await self.get_address(destination_address_id)
        if not dest_address:
            raise ShippingError(message="Destination address not found", code="ADDRESS_NOT_FOUND")

        # Get carrier
        carrier = await self._get_carrier(CarrierCode.UPS)

        # Build origin from carrier config
        origin = UPSAddress(
            name=carrier.origin_attention_name or "MDM Comics",
            company_name=carrier.origin_company_name or "MDM Comics",
            address_line1=carrier.origin_address_line1 or "",
            address_line2=carrier.origin_address_line2,
            city=carrier.origin_city or "",
            state_province=carrier.origin_state or "",
            postal_code=carrier.origin_postal_code or "",
            country_code=carrier.origin_country_code or "US",
            phone=carrier.origin_phone,
            residential=False,
        )

        # Build destination
        destination = UPSAddress(
            name=decrypt_pii(dest_address.recipient_name_encrypted),
            address_line1=decrypt_pii(dest_address.address_line1_encrypted),
            address_line2=decrypt_pii(dest_address.address_line2_encrypted) if dest_address.address_line2_encrypted else None,
            city=dest_address.city,
            state_province=dest_address.state_province,
            postal_code=dest_address.postal_code,
            country_code=dest_address.country_code,
            company_name=decrypt_pii(dest_address.company_name_encrypted) if dest_address.company_name_encrypted else None,
            residential=dest_address.residential,
        )

        # Build packages
        ups_packages = []
        if packages:
            for pkg in packages:
                ups_packages.append(UPSPackage(
                    weight=max(pkg.get("weight", DEFAULT_COMIC_WEIGHT), MIN_PACKAGE_WEIGHT),
                    weight_unit=pkg.get("weight_unit", "LBS"),
                    length=pkg.get("length"),
                    width=pkg.get("width"),
                    height=pkg.get("height"),
                    dimension_unit=pkg.get("dimension_unit", "IN"),
                    package_type=pkg.get("package_type", "02"),
                    declared_value=pkg.get("declared_value"),
                ))
        else:
            # Default package
            ups_packages.append(UPSPackage(weight=DEFAULT_COMIC_WEIGHT))

        # Get rates from UPS
        ups_client = await self._get_ups_client()

        try:
            ups_rates = await ups_client.get_rates(origin, destination, ups_packages, service_code)
        except UPSAPIError as e:
            raise ShippingError(
                message=f"Failed to get rates: {e.message}",
                code=e.code,
                details=e.details,
            )

        # Convert to ShipmentRate models and save
        rate_models = []
        for ups_rate in ups_rates:
            # Check if service is enabled in carrier config
            service_config = carrier.service_levels.get(ups_rate.service_code, {})
            if not service_config.get("enabled", True):
                continue

            # Apply markup
            markup = service_config.get("markup", 0) + carrier.rate_markup_percentage
            flat_markup = carrier.rate_markup_flat
            total_with_markup = ups_rate.total_charges * (1 + markup / 100) + flat_markup

            # Generate quote ID
            quote_id = f"QT-{uuid.uuid4().hex[:12].upper()}"

            rate = ShipmentRate(
                quote_id=quote_id,
                order_id=order_id,
                carrier_id=carrier.id,
                service_code=ups_rate.service_code,
                service_name=ups_rate.service_name,
                origin_postal_code=origin.postal_code,
                origin_country_code=origin.country_code,
                destination_postal_code=destination.postal_code,
                destination_country_code=destination.country_code,
                destination_residential=destination.residential,
                weight=ups_packages[0].weight,
                weight_unit=ups_packages[0].weight_unit,
                length=ups_packages[0].length,
                width=ups_packages[0].width,
                height=ups_packages[0].height,
                dimension_unit=ups_packages[0].dimension_unit,
                package_type=ups_packages[0].package_type,
                base_rate=ups_rate.base_charge,
                fuel_surcharge=ups_rate.fuel_surcharge,
                residential_surcharge=ups_rate.residential_surcharge,
                delivery_area_surcharge=ups_rate.delivery_area_surcharge,
                other_surcharges=ups_rate.other_surcharges,
                total_carrier_rate=ups_rate.total_charges,
                markup=total_with_markup - ups_rate.total_charges,
                total_rate=round(total_with_markup, 2),
                guaranteed_delivery=ups_rate.guaranteed_delivery,
                estimated_delivery_date=ups_rate.estimated_delivery,
                estimated_transit_days=ups_rate.estimated_days,
                carrier_response=ups_rate.raw_response,
                expires_at=ShipmentRate.create_expiry(),
            )

            self.db.add(rate)
            rate_models.append(rate)

        await self.db.flush()
        return rate_models

    async def select_rate(self, quote_id: str) -> ShipmentRate:
        """Mark a rate quote as selected."""
        result = await self.db.execute(
            select(ShipmentRate).where(ShipmentRate.quote_id == quote_id)
        )
        rate = result.scalar_one_or_none()

        if not rate:
            raise ShippingError(message="Rate quote not found", code="QUOTE_NOT_FOUND")

        if rate.is_expired:
            raise ShippingError(message="Rate quote has expired", code="QUOTE_EXPIRED")

        rate.selected = True
        rate.selected_at = datetime.now(timezone.utc)
        await self.db.flush()

        return rate

    # ==================== Shipment Creation ====================

    async def create_shipment(
        self,
        order_id: int,
        destination_address_id: int,
        service_code: str,
        packages: Optional[List[Dict]] = None,
        signature_required: bool = False,
        declared_value: Optional[float] = None,
        label_format: str = "ZPL",
    ) -> Shipment:
        """
        Create a shipment and generate label.

        Args:
            order_id: Order to ship
            destination_address_id: Destination address ID
            service_code: UPS service code
            packages: Package details
            signature_required: Require delivery signature
            declared_value: Declared value for insurance
            label_format: Label format (ZPL, GIF, PNG, EPL)

        Returns:
            Shipment model with tracking number and label
        """
        # Get order
        result = await self.db.execute(select(Order).where(Order.id == order_id))
        order = result.scalar_one_or_none()
        if not order:
            raise ShippingError(message="Order not found", code="ORDER_NOT_FOUND")

        # Get destination address
        dest_address = await self.get_address(destination_address_id)
        if not dest_address:
            raise ShippingError(message="Destination address not found", code="ADDRESS_NOT_FOUND")

        # Get carrier
        carrier = await self._get_carrier(CarrierCode.UPS)

        # Build origin from carrier config
        origin = UPSAddress(
            name=carrier.origin_attention_name or "MDM Comics",
            company_name=carrier.origin_company_name or "MDM Comics",
            address_line1=carrier.origin_address_line1 or "",
            address_line2=carrier.origin_address_line2,
            city=carrier.origin_city or "",
            state_province=carrier.origin_state or "",
            postal_code=carrier.origin_postal_code or "",
            country_code=carrier.origin_country_code or "US",
            phone=carrier.origin_phone,
            residential=False,
        )

        # Build destination
        destination = UPSAddress(
            name=decrypt_pii(dest_address.recipient_name_encrypted),
            address_line1=decrypt_pii(dest_address.address_line1_encrypted),
            address_line2=decrypt_pii(dest_address.address_line2_encrypted) if dest_address.address_line2_encrypted else None,
            city=dest_address.city,
            state_province=dest_address.state_province,
            postal_code=dest_address.postal_code,
            country_code=dest_address.country_code,
            company_name=decrypt_pii(dest_address.company_name_encrypted) if dest_address.company_name_encrypted else None,
            phone=decrypt_pii(dest_address.phone_encrypted) if dest_address.phone_encrypted else None,
            email=decrypt_pii(dest_address.email_encrypted) if dest_address.email_encrypted else None,
            residential=dest_address.residential,
        )

        # Build packages
        ups_packages = []
        total_weight = 0.0

        if packages:
            for pkg in packages:
                weight = max(pkg.get("weight", DEFAULT_COMIC_WEIGHT), MIN_PACKAGE_WEIGHT)
                total_weight += weight
                ups_packages.append(UPSPackage(
                    weight=weight,
                    weight_unit=pkg.get("weight_unit", "LBS"),
                    length=pkg.get("length"),
                    width=pkg.get("width"),
                    height=pkg.get("height"),
                    dimension_unit=pkg.get("dimension_unit", "IN"),
                    package_type=pkg.get("package_type", "02"),
                    declared_value=pkg.get("declared_value") or declared_value,
                ))
        else:
            ups_packages.append(UPSPackage(
                weight=DEFAULT_COMIC_WEIGHT,
                declared_value=declared_value,
            ))
            total_weight = DEFAULT_COMIC_WEIGHT

        # Check if signature required based on carrier threshold
        if not signature_required and carrier.signature_required_threshold:
            if order.total >= carrier.signature_required_threshold:
                signature_required = True

        # Create shipment via UPS
        ups_client = await self._get_ups_client()

        try:
            result = await ups_client.create_shipment(
                origin=origin,
                destination=destination,
                packages=ups_packages,
                service_code=service_code,
                label_format=label_format,
                reference=order.order_number,
                signature_required=signature_required,
            )
        except UPSAPIError as e:
            raise ShippingError(
                message=f"Failed to create shipment: {e.message}",
                code=e.code,
                details=e.details,
            )

        # Create shipment model
        from app.models.carrier import UPS_SERVICE_CODES

        shipment = Shipment(
            order_id=order_id,
            carrier_id=carrier.id,
            service_code=service_code,
            service_name=UPS_SERVICE_CODES.get(service_code, f"UPS {service_code}"),
            tracking_number=result.tracking_number,
            tracking_url=f"https://www.ups.com/track?tracknum={result.tracking_number}",
            destination_address_id=destination_address_id,
            status=ShipmentStatus.LABEL_CREATED,
            weight=total_weight,
            weight_unit="LBS",
            package_count=len(ups_packages),
            declared_value=declared_value or 0.0,
            shipping_cost=order.shipping_cost,
            carrier_cost=result.total_charges,
            label_format=label_format,
            label_data=result.label_data,
            label_created_at=datetime.now(timezone.utc),
            shipment_id_number=result.shipment_id,
            carrier_response=result.raw_response,
            signature_required=signature_required,
        )

        self.db.add(shipment)

        # Update order
        order.tracking_number = result.tracking_number
        order.status = "shipped"
        order.shipped_at = datetime.now(timezone.utc)

        await self.db.flush()

        logger.info(f"Shipment created: {shipment.id} tracking: {result.tracking_number}")
        return shipment

    # ==================== Tracking ====================

    async def update_tracking(self, shipment_id: int) -> Shipment:
        """
        Update tracking information for a shipment.

        Args:
            shipment_id: Shipment ID

        Returns:
            Updated shipment with new tracking events
        """
        result = await self.db.execute(
            select(Shipment).where(Shipment.id == shipment_id)
        )
        shipment = result.scalar_one_or_none()

        if not shipment:
            raise ShippingError(message="Shipment not found", code="SHIPMENT_NOT_FOUND")

        if not shipment.tracking_number:
            raise ShippingError(message="No tracking number", code="NO_TRACKING")

        ups_client = await self._get_ups_client()

        try:
            tracking = await ups_client.track_shipment(shipment.tracking_number)
        except UPSAPIError as e:
            raise ShippingError(
                message=f"Failed to get tracking: {e.message}",
                code=e.code,
                details=e.details,
            )

        # Update shipment status
        status_map = {
            "M": ShipmentStatus.LABEL_CREATED,
            "I": ShipmentStatus.IN_TRANSIT,
            "X": ShipmentStatus.EXCEPTION,
            "D": ShipmentStatus.DELIVERED,
            "P": ShipmentStatus.PICKED_UP,
            "O": ShipmentStatus.OUT_FOR_DELIVERY,
        }
        shipment.status = status_map.get(tracking.status.upper(), shipment.status)
        shipment.status_detail = tracking.status_description

        if tracking.delivered:
            shipment.actual_delivery_date = tracking.delivery_date
            if tracking.signature:
                shipment.delivery_confirmation = tracking.signature

        # Add tracking events
        for event in tracking.events:
            tracking_event = TrackingEvent(
                shipment_id=shipment.id,
                event_code=event.event_code,
                event_type=event.event_type,
                description=event.description,
                city=event.city,
                state_province=event.state,
                postal_code=event.postal_code,
                country_code=event.country,
                event_time=event.event_time,
                carrier_data={"raw": str(event)},
            )
            self.db.add(tracking_event)

        shipment.last_tracking_update = datetime.now(timezone.utc)
        shipment.tracking_events = [
            {
                "type": e.event_type,
                "description": e.description,
                "time": e.event_time.isoformat() if e.event_time else None,
                "location": f"{e.city}, {e.state}" if e.city else None,
            }
            for e in tracking.events
        ]

        await self.db.flush()
        return shipment

    async def void_shipment(self, shipment_id: int) -> bool:
        """
        Void a shipment (before pickup).

        Args:
            shipment_id: Shipment ID

        Returns:
            True if voided successfully
        """
        result = await self.db.execute(
            select(Shipment).where(Shipment.id == shipment_id)
        )
        shipment = result.scalar_one_or_none()

        if not shipment:
            raise ShippingError(message="Shipment not found", code="SHIPMENT_NOT_FOUND")

        if shipment.status not in [ShipmentStatus.LABEL_CREATED, ShipmentStatus.DRAFT]:
            raise ShippingError(
                message="Cannot void - shipment already picked up",
                code="CANNOT_VOID",
            )

        ups_client = await self._get_ups_client()

        try:
            success = await ups_client.void_shipment(shipment.shipment_id_number)
        except UPSAPIError as e:
            raise ShippingError(
                message=f"Failed to void shipment: {e.message}",
                code=e.code,
                details=e.details,
            )

        if success:
            shipment.status = ShipmentStatus.VOIDED
            shipment.voided_at = datetime.now(timezone.utc)
            await self.db.flush()

        return success


# Factory function for dependency injection
async def get_shipping_service(db: AsyncSession) -> ShippingService:
    """Create shipping service instance."""
    return ShippingService(db)
