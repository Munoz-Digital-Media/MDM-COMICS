"""
Shipping API Routes for UPS Shipping Integration v1.28.0

Provides endpoints for:
- Address management (create, validate, list)
- Rate quoting (get shipping options)
- Shipment creation (generate labels)
- Tracking (get shipment status)

Per constitution_binder.json: All PII must be encrypted at rest.
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.core.database import get_db
from app.api.deps import get_current_user, get_current_admin
from app.models.user import User
from app.models.address import Address, AddressType
from app.models.shipment import Shipment, ShipmentStatus
from app.models.order import Order
from app.services.shipping_service import ShippingService, ShippingError, get_shipping_service
from app.services.encryption import mask_address_line, decrypt_pii
from app.schemas.shipping import (
    AddressCreate,
    AddressResponse,
    AddressValidationResponse,
    RateRequest,
    RateResponse,
    RateListResponse,
    SelectRateRequest,
    ShipmentCreate,
    ShipmentResponse,
    ShipmentListResponse,
    LabelResponse,
    TrackingResponse,
    TrackingEventResponse,
    VoidShipmentRequest,
    VoidShipmentResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/shipping", tags=["shipping"])


# ==================== Helper Functions ====================


def mask_name(name: str) -> str:
    """Mask a name for display."""
    if not name or len(name) < 2:
        return "***"
    return f"{name[0]}***"


def address_to_response(address: Address) -> AddressResponse:
    """Convert address model to response with masked PII."""
    # Decrypt and mask PII
    recipient_name = decrypt_pii(address.recipient_name_encrypted) if address.recipient_name_encrypted else ""
    address_line1 = decrypt_pii(address.address_line1_encrypted) if address.address_line1_encrypted else ""

    return AddressResponse(
        id=address.id,
        address_type=address.address_type.value if address.address_type else "shipping",
        recipient_name_masked=mask_name(recipient_name),
        company_name=decrypt_pii(address.company_name_encrypted) if address.company_name_encrypted else None,
        address_line1_masked=mask_address_line(address_line1),
        city=address.city,
        state_province=address.state_province,
        postal_code=address.postal_code,
        country_code=address.country_code,
        residential=address.residential,
        validation_status=address.validation_status.value if address.validation_status else "pending",
        validated_at=address.validated_at,
        is_default=address.is_default,
        created_at=address.created_at,
    )


# ==================== Address Endpoints ====================


@router.post("/addresses", response_model=AddressResponse, status_code=status.HTTP_201_CREATED)
async def create_address(
    address_data: AddressCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new shipping address.

    PII fields are encrypted before storage.
    Address is validated with UPS if validate_address is true.
    """
    try:
        shipping_service = ShippingService(db)

        address_type = AddressType.SHIPPING
        if address_data.address_type == "billing":
            address_type = AddressType.BILLING

        address = await shipping_service.create_address(
            user_id=current_user.id,
            recipient_name=address_data.recipient_name,
            address_line1=address_data.address_line1,
            address_line2=address_data.address_line2,
            address_line3=address_data.address_line3,
            city=address_data.city,
            state_province=address_data.state_province,
            postal_code=address_data.postal_code,
            country_code=address_data.country_code,
            company_name=address_data.company_name,
            phone=address_data.phone,
            email=address_data.email,
            address_type=address_type,
            residential=address_data.residential,
            validate=address_data.validate_address,
        )

        await shipping_service.close()
        await db.commit()

        return address_to_response(address)

    except ShippingError as e:
        raise HTTPException(status_code=400, detail=e.message)
    except Exception as e:
        logger.error(f"Failed to create address: {e}")
        raise HTTPException(status_code=500, detail="Failed to create address")


@router.get("/addresses", response_model=List[AddressResponse])
async def list_addresses(
    address_type: Optional[str] = Query(None, description="Filter by type: shipping, billing"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get all addresses for the current user."""
    shipping_service = ShippingService(db)

    filter_type = None
    if address_type:
        filter_type = AddressType.SHIPPING if address_type == "shipping" else AddressType.BILLING

    addresses = await shipping_service.get_user_addresses(current_user.id, filter_type)
    await shipping_service.close()

    return [address_to_response(addr) for addr in addresses]


@router.get("/addresses/{address_id}", response_model=AddressResponse)
async def get_address(
    address_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a specific address."""
    shipping_service = ShippingService(db)
    address = await shipping_service.get_address(address_id, current_user.id)
    await shipping_service.close()

    if not address:
        raise HTTPException(status_code=404, detail="Address not found")

    return address_to_response(address)


@router.post("/addresses/{address_id}/validate", response_model=AddressValidationResponse)
async def validate_address(
    address_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Validate an address with UPS."""
    shipping_service = ShippingService(db)
    address = await shipping_service.get_address(address_id, current_user.id)

    if not address:
        await shipping_service.close()
        raise HTTPException(status_code=404, detail="Address not found")

    try:
        is_valid, corrected, messages = await shipping_service.validate_address(address)
        await db.commit()
        await shipping_service.close()

        # Build response
        original = {
            "address_line1": mask_address_line(decrypt_pii(address.address_line1_encrypted)),
            "city": address.city,
            "state_province": address.state_province,
            "postal_code": address.postal_code,
            "country_code": address.country_code,
        }

        corrected_dict = None
        if corrected:
            corrected_dict = {
                "address_line1": mask_address_line(decrypt_pii(corrected.address_line1_encrypted)),
                "city": corrected.city,
                "state_province": corrected.state_province,
                "postal_code": corrected.postal_code,
                "country_code": corrected.country_code,
            }

        return AddressValidationResponse(
            is_valid=is_valid,
            original_address=original,
            corrected_address=corrected_dict,
            messages=messages,
            validation_status=address.validation_status.value if address.validation_status else "pending",
        )

    except ShippingError as e:
        await shipping_service.close()
        raise HTTPException(status_code=400, detail=e.message)


@router.delete("/addresses/{address_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_address(
    address_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Soft delete an address."""
    shipping_service = ShippingService(db)
    address = await shipping_service.get_address(address_id, current_user.id)
    await shipping_service.close()

    if not address:
        raise HTTPException(status_code=404, detail="Address not found")

    from datetime import datetime, timezone
    address.deleted_at = datetime.now(timezone.utc)
    await db.commit()


# ==================== Rate Endpoints ====================


@router.post("/rates", response_model=RateListResponse)
async def get_shipping_rates(
    rate_request: RateRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get available shipping rates for a destination.

    Returns a list of rate options with pricing and delivery estimates.
    Rate quotes expire after 30 minutes.
    """
    shipping_service = ShippingService(db)

    # Verify user owns the address
    address = await shipping_service.get_address(rate_request.destination_address_id, current_user.id)
    if not address:
        await shipping_service.close()
        raise HTTPException(status_code=404, detail="Destination address not found")

    try:
        # Convert package info if provided
        packages = None
        if rate_request.packages:
            packages = [p.model_dump() for p in rate_request.packages]

        rates = await shipping_service.get_shipping_rates(
            destination_address_id=rate_request.destination_address_id,
            packages=packages,
            order_id=rate_request.order_id,
            service_code=rate_request.service_code,
        )

        await db.commit()
        await shipping_service.close()

        return RateListResponse(
            rates=[
                RateResponse(
                    quote_id=rate.quote_id,
                    service_code=rate.service_code,
                    service_name=rate.service_name,
                    total_rate=rate.total_rate,
                    estimated_delivery_date=rate.estimated_delivery_date,
                    estimated_transit_days=rate.estimated_transit_days,
                    guaranteed_delivery=rate.guaranteed_delivery,
                    expires_at=rate.expires_at,
                )
                for rate in rates
            ],
            destination_postal_code=address.postal_code,
            destination_country=address.country_code,
        )

    except ShippingError as e:
        await shipping_service.close()
        raise HTTPException(status_code=400, detail=e.message)


@router.post("/rates/select")
async def select_shipping_rate(
    request: SelectRateRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Select a rate quote for checkout."""
    shipping_service = ShippingService(db)

    try:
        rate = await shipping_service.select_rate(request.quote_id)
        await db.commit()
        await shipping_service.close()

        return {
            "success": True,
            "quote_id": rate.quote_id,
            "service_name": rate.service_name,
            "total_rate": rate.total_rate,
        }

    except ShippingError as e:
        await shipping_service.close()
        raise HTTPException(status_code=400, detail=e.message)


# ==================== Shipment Endpoints ====================


@router.post("/shipments", response_model=ShipmentResponse, status_code=status.HTTP_201_CREATED)
async def create_shipment(
    shipment_data: ShipmentCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a shipment and generate shipping label.

    This is typically called by admin or after order is ready to ship.
    Returns tracking number and label data.
    """
    shipping_service = ShippingService(db)

    # Verify order belongs to user (or user is admin)
    result = await db.execute(select(Order).where(Order.id == shipment_data.order_id))
    order = result.scalar_one_or_none()

    if not order:
        await shipping_service.close()
        raise HTTPException(status_code=404, detail="Order not found")

    if order.user_id != current_user.id and not current_user.is_admin:
        await shipping_service.close()
        raise HTTPException(status_code=403, detail="Not authorized to ship this order")

    # Verify address
    address = await shipping_service.get_address(shipment_data.destination_address_id)
    if not address:
        await shipping_service.close()
        raise HTTPException(status_code=404, detail="Destination address not found")

    try:
        packages = None
        if shipment_data.packages:
            packages = [p.model_dump() for p in shipment_data.packages]

        shipment = await shipping_service.create_shipment(
            order_id=shipment_data.order_id,
            destination_address_id=shipment_data.destination_address_id,
            service_code=shipment_data.service_code,
            packages=packages,
            signature_required=shipment_data.signature_required,
            declared_value=shipment_data.declared_value,
            label_format=shipment_data.label_format,
        )

        await db.commit()
        await shipping_service.close()

        return ShipmentResponse(
            id=shipment.id,
            order_id=shipment.order_id,
            tracking_number=shipment.tracking_number,
            tracking_url=shipment.tracking_url,
            service_code=shipment.service_code,
            service_name=shipment.service_name,
            status=shipment.status.value if shipment.status else "unknown",
            status_detail=shipment.status_detail,
            weight=shipment.weight,
            package_count=shipment.package_count,
            shipping_cost=shipment.shipping_cost,
            carrier_cost=shipment.carrier_cost,
            label_format=shipment.label_format,
            has_label=bool(shipment.label_data),
            signature_required=shipment.signature_required,
            actual_delivery_date=shipment.actual_delivery_date,
            estimated_delivery_date=shipment.estimated_delivery_date,
            created_at=shipment.created_at,
            shipped_at=shipment.label_created_at,
            last_tracking_update=shipment.last_tracking_update,
        )

    except ShippingError as e:
        await shipping_service.close()
        raise HTTPException(status_code=400, detail=e.message)


@router.get("/shipments", response_model=ShipmentListResponse)
async def list_shipments(
    order_id: Optional[int] = None,
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List shipments for the current user."""
    # Build query
    query = (
        select(Shipment)
        .join(Order)
        .where(Order.user_id == current_user.id)
    )

    if order_id:
        query = query.where(Shipment.order_id == order_id)
    if status:
        try:
            status_enum = ShipmentStatus(status)
            query = query.where(Shipment.status == status_enum)
        except ValueError:
            pass

    # Get total
    count_result = await db.execute(
        select(func.count(Shipment.id))
        .join(Order)
        .where(Order.user_id == current_user.id)
    )
    total = count_result.scalar() or 0

    # Get paginated results
    offset = (page - 1) * page_size
    result = await db.execute(
        query.order_by(Shipment.created_at.desc())
        .offset(offset)
        .limit(page_size)
    )
    shipments = result.scalars().all()

    return ShipmentListResponse(
        shipments=[
            ShipmentResponse(
                id=s.id,
                order_id=s.order_id,
                tracking_number=s.tracking_number,
                tracking_url=s.tracking_url,
                service_code=s.service_code,
                service_name=s.service_name,
                status=s.status.value if s.status else "unknown",
                status_detail=s.status_detail,
                weight=s.weight,
                package_count=s.package_count,
                shipping_cost=s.shipping_cost,
                carrier_cost=s.carrier_cost,
                label_format=s.label_format,
                has_label=bool(s.label_data),
                signature_required=s.signature_required,
                actual_delivery_date=s.actual_delivery_date,
                estimated_delivery_date=s.estimated_delivery_date,
                created_at=s.created_at,
                shipped_at=s.label_created_at,
                last_tracking_update=s.last_tracking_update,
            )
            for s in shipments
        ],
        total=total,
    )


@router.get("/shipments/{shipment_id}", response_model=ShipmentResponse)
async def get_shipment(
    shipment_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a specific shipment."""
    result = await db.execute(
        select(Shipment)
        .join(Order)
        .where(Shipment.id == shipment_id)
        .where(Order.user_id == current_user.id)
    )
    shipment = result.scalar_one_or_none()

    if not shipment:
        raise HTTPException(status_code=404, detail="Shipment not found")

    return ShipmentResponse(
        id=shipment.id,
        order_id=shipment.order_id,
        tracking_number=shipment.tracking_number,
        tracking_url=shipment.tracking_url,
        service_code=shipment.service_code,
        service_name=shipment.service_name,
        status=shipment.status.value if shipment.status else "unknown",
        status_detail=shipment.status_detail,
        weight=shipment.weight,
        package_count=shipment.package_count,
        shipping_cost=shipment.shipping_cost,
        carrier_cost=shipment.carrier_cost,
        label_format=shipment.label_format,
        has_label=bool(shipment.label_data),
        signature_required=shipment.signature_required,
        actual_delivery_date=shipment.actual_delivery_date,
        estimated_delivery_date=shipment.estimated_delivery_date,
        created_at=shipment.created_at,
        shipped_at=shipment.label_created_at,
        last_tracking_update=shipment.last_tracking_update,
    )


@router.get("/shipments/{shipment_id}/label", response_model=LabelResponse)
async def get_shipping_label(
    shipment_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get the shipping label for a shipment."""
    result = await db.execute(
        select(Shipment)
        .join(Order)
        .where(Shipment.id == shipment_id)
        .where(Order.user_id == current_user.id)
    )
    shipment = result.scalar_one_or_none()

    if not shipment:
        raise HTTPException(status_code=404, detail="Shipment not found")

    if not shipment.label_data:
        raise HTTPException(status_code=404, detail="No label available for this shipment")

    return LabelResponse(
        shipment_id=shipment.id,
        tracking_number=shipment.tracking_number,
        label_format=shipment.label_format or "ZPL",
        label_data=shipment.label_data,
        created_at=shipment.label_created_at or shipment.created_at,
    )


@router.get("/shipments/{shipment_id}/tracking", response_model=TrackingResponse)
async def get_tracking(
    shipment_id: int,
    refresh: bool = Query(False, description="Force refresh from carrier"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get tracking information for a shipment."""
    result = await db.execute(
        select(Shipment)
        .join(Order)
        .where(Shipment.id == shipment_id)
        .where(Order.user_id == current_user.id)
    )
    shipment = result.scalar_one_or_none()

    if not shipment:
        raise HTTPException(status_code=404, detail="Shipment not found")

    if not shipment.tracking_number:
        raise HTTPException(status_code=400, detail="No tracking number for this shipment")

    # Refresh from carrier if requested
    if refresh:
        shipping_service = ShippingService(db)
        try:
            await shipping_service.update_tracking(shipment_id)
            await db.commit()
        except ShippingError as e:
            logger.warning(f"Failed to refresh tracking: {e}")
        finally:
            await shipping_service.close()

        # Reload shipment
        await db.refresh(shipment)

    # Build tracking events
    events = []
    if shipment.tracking_events:
        for event in shipment.tracking_events:
            events.append(TrackingEventResponse(
                event_type=event.get("type", ""),
                description=event.get("description", ""),
                event_time=event.get("time"),
                city=event.get("location", "").split(",")[0] if event.get("location") else None,
            ))

    return TrackingResponse(
        shipment_id=shipment.id,
        tracking_number=shipment.tracking_number,
        status=shipment.status.value if shipment.status else "unknown",
        status_detail=shipment.status_detail,
        delivered=shipment.status == ShipmentStatus.DELIVERED,
        delivery_date=shipment.actual_delivery_date,
        estimated_delivery=shipment.estimated_delivery_date,
        signature=shipment.delivery_confirmation,
        events=events,
    )


@router.post("/shipments/{shipment_id}/void", response_model=VoidShipmentResponse)
async def void_shipment(
    shipment_id: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Void a shipment (admin only).

    Can only void shipments that haven't been picked up yet.
    """
    shipping_service = ShippingService(db)

    try:
        success = await shipping_service.void_shipment(shipment_id)
        await db.commit()
        await shipping_service.close()

        return VoidShipmentResponse(
            success=success,
            shipment_id=shipment_id,
            message="Shipment voided successfully" if success else "Failed to void shipment",
        )

    except ShippingError as e:
        await shipping_service.close()
        raise HTTPException(status_code=400, detail=e.message)


# ==================== Public Tracking Endpoint ====================


@router.get("/track/{tracking_number}")
async def public_tracking(
    tracking_number: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Public tracking lookup by tracking number.

    Does not require authentication.
    Returns limited information for privacy.
    """
    # Find shipment by tracking number
    result = await db.execute(
        select(Shipment).where(Shipment.tracking_number == tracking_number)
    )
    shipment = result.scalar_one_or_none()

    if not shipment:
        raise HTTPException(status_code=404, detail="Tracking number not found")

    # Return limited public info
    events = []
    if shipment.tracking_events:
        for event in shipment.tracking_events[-5:]:  # Last 5 events only
            events.append({
                "status": event.get("type", ""),
                "description": event.get("description", ""),
                "time": event.get("time"),
                "location": event.get("location"),
            })

    return {
        "tracking_number": shipment.tracking_number,
        "status": shipment.status.value if shipment.status else "unknown",
        "carrier": "UPS",
        "delivered": shipment.status == ShipmentStatus.DELIVERED,
        "delivery_date": shipment.actual_delivery_date,
        "events": events,
    }
