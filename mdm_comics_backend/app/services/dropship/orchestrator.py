"""
Dropship Orchestrator

Main workflow coordinator for dropship fulfillment.
Per bcw_submission_flow and order_state_machine in proposal doc.

Order Flow:
1. Validate address (block PO/APO)
2. Check inventory (precheck)
3. Get shipping quote
4. Create internal order (PENDING_VENDOR_SUBMISSION)
5. Submit to BCW with idempotency key
6. Store BCW order ID
7. Transition to VENDOR_SUBMITTED
8. Notify customer
"""
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.bcw import BCWOrder, BCWOrderState, BCWOrderEvent
from app.models.order import Order
from app.services.bcw.browser_client import BCWBrowserClient, ShippingOption
from app.services.bcw.session_manager import BCWSessionManager
from app.services.bcw.cart_builder import CartItem
from app.services.bcw.order_submitter import (
    BCWOrderSubmitter,
    OrderSubmissionRequest,
)
from app.services.dropship.inventory_sync import BCWInventorySyncService
from app.services.dropship.quote_service import DropshipQuoteService
from app.core.exceptions import (
    DropshipError,
    DropshipAddressValidationError,
    DropshipInventoryError,
    DropshipIdempotencyError,
    BCWOrderError,
)

logger = logging.getLogger(__name__)


@dataclass
class DropshipOrderResult:
    """Result of dropship order submission."""
    success: bool
    bcw_order_id: Optional[str] = None
    correlation_id: Optional[str] = None
    state: Optional[str] = None
    bcw_total: Optional[float] = None
    bcw_shipping: Optional[float] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class AddressValidationResult:
    """Result of address validation."""
    valid: bool
    errors: List[str] = field(default_factory=list)
    normalized_address: Optional[Dict[str, str]] = None


class DropshipOrchestrator:
    """
    Orchestrates the complete dropship workflow.

    Usage:
        async with BCWBrowserClient() as client:
            session_mgr = BCWSessionManager(db)
            creds = await session_mgr.get_credentials()
            await client.login(creds["username"], creds["password"])

            orchestrator = DropshipOrchestrator(client, db)
            result = await orchestrator.submit_order(
                order_id=123,
                items=[CartItem(sku="ABC", quantity=2)],
                shipping_address={...},
                shipping_method_id="ground"
            )
    """

    # PO Box patterns to block
    PO_BOX_PATTERNS = [
        r"\bP\.?O\.?\s*BOX\b",
        r"\bPOST\s*OFFICE\s*BOX\b",
        r"\bPO\s+BOX\b",
    ]

    # APO/FPO patterns to block
    MILITARY_PATTERNS = [
        r"\bAPO\b",
        r"\bFPO\b",
        r"\bDPO\b",
    ]

    # US Continental states only (BCW cannot blind ship internationally)
    # Excludes: AK, HI, PR, VI, GU, AS, MP (territories and non-continental)
    ALLOWED_STATES = {
        "AL", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
        "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
        "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
        "WV", "WI", "WY", "DC",  # Include DC
    }

    # Non-continental US (blocked for dropship)
    NON_CONTINENTAL_STATES = {"AK", "HI", "PR", "VI", "GU", "AS", "MP"}

    # Countries we explicitly block (BCW must show their info on international)
    BLOCKED_COUNTRIES = {"CA", "CANADA", "MX", "MEXICO"}

    def __init__(
        self,
        browser_client: BCWBrowserClient,
        db: AsyncSession,
    ):
        self.client = browser_client
        self.db = db
        self.quote_service = DropshipQuoteService(browser_client, db)
        self.inventory_service = BCWInventorySyncService(browser_client, db)
        self.order_submitter = BCWOrderSubmitter(browser_client, db)

    async def submit_order(
        self,
        order_id: int,
        items: List[CartItem],
        shipping_address: Dict[str, str],
        shipping_method_id: str,
        skip_inventory_check: bool = False,
    ) -> DropshipOrderResult:
        """
        Submit a dropship order to BCW.

        Full workflow with validation, inventory check, and submission.

        Args:
            order_id: Internal order ID
            items: Cart items to order
            shipping_address: Shipping address
            shipping_method_id: Selected shipping method
            skip_inventory_check: Skip pre-purchase inventory validation

        Returns:
            DropshipOrderResult with BCW order details or error
        """
        correlation_id = str(uuid.uuid4())
        logger.info(
            f"Starting dropship order: order_id={order_id}, "
            f"correlation_id={correlation_id}"
        )

        try:
            # Step 1: Validate address
            address_result = self.validate_address(shipping_address)
            if not address_result.valid:
                return DropshipOrderResult(
                    success=False,
                    correlation_id=correlation_id,
                    error_code="INVALID_ADDRESS",
                    error_message="; ".join(address_result.errors),
                )

            # Step 2: Check inventory (unless skipped)
            if not skip_inventory_check:
                inventory_ok = await self._check_inventory(items)
                if not inventory_ok:
                    return DropshipOrderResult(
                        success=False,
                        correlation_id=correlation_id,
                        error_code="INVENTORY_UNAVAILABLE",
                        error_message="One or more items are out of stock",
                    )

            # Step 3: Create/update BCW order record
            bcw_order = await self._get_or_create_bcw_order(
                order_id, items, shipping_address, correlation_id
            )

            # Step 4: Generate idempotency key
            idempotency_key = BCWOrderSubmitter.generate_idempotency_key(
                order_id, items, shipping_address
            )

            # Step 5: Transition to PENDING_VENDOR_SUBMISSION
            await self._transition_state(
                bcw_order,
                BCWOrderState.PENDING_VENDOR_SUBMISSION,
                "order_submission_started",
            )

            # Step 6: Submit to BCW
            submission_request = OrderSubmissionRequest(
                order_id=order_id,
                correlation_id=correlation_id,
                items=items,
                shipping_address=shipping_address,
                shipping_method_id=shipping_method_id,
                idempotency_key=idempotency_key,
            )

            result = await self.order_submitter.submit_order(submission_request)

            if not result.success:
                # Transition to exception review
                await self._transition_state(
                    bcw_order,
                    BCWOrderState.EXCEPTION_REVIEW,
                    "order_submission_failed",
                    {"error": result.error_message},
                )
                return DropshipOrderResult(
                    success=False,
                    correlation_id=correlation_id,
                    state=BCWOrderState.EXCEPTION_REVIEW.value,
                    error_code="SUBMISSION_FAILED",
                    error_message=result.error_message,
                )

            # Step 7: Update BCW order with confirmation
            bcw_order.bcw_order_id = result.bcw_order_id
            bcw_order.confirmation_number = result.confirmation_number
            bcw_order.bcw_total = result.bcw_total
            bcw_order.bcw_shipping = result.bcw_shipping
            bcw_order.submitted_at = datetime.now(timezone.utc)
            bcw_order.idempotency_key = idempotency_key

            # Step 8: Transition to VENDOR_SUBMITTED
            await self._transition_state(
                bcw_order,
                BCWOrderState.VENDOR_SUBMITTED,
                "bcw_order_placed",
                {
                    "bcw_order_id": result.bcw_order_id,
                    "bcw_total": result.bcw_total,
                },
            )

            await self.db.commit()

            logger.info(
                f"Dropship order submitted: bcw_order_id={result.bcw_order_id}, "
                f"correlation_id={correlation_id}"
            )

            return DropshipOrderResult(
                success=True,
                bcw_order_id=result.bcw_order_id,
                correlation_id=correlation_id,
                state=BCWOrderState.VENDOR_SUBMITTED.value,
                bcw_total=result.bcw_total,
                bcw_shipping=result.bcw_shipping,
            )

        except DropshipIdempotencyError as e:
            logger.warning(f"Duplicate order detected: {e.idempotency_key}")
            return DropshipOrderResult(
                success=False,
                bcw_order_id=e.existing_order_id,
                correlation_id=correlation_id,
                error_code="DUPLICATE_ORDER",
                error_message=e.message,
            )
        except Exception as e:
            logger.error(f"Dropship order failed: {e}")
            return DropshipOrderResult(
                success=False,
                correlation_id=correlation_id,
                error_code="UNEXPECTED_ERROR",
                error_message=str(e),
            )

    def validate_address(
        self,
        address: Dict[str, str],
    ) -> AddressValidationResult:
        """
        Validate shipping address for dropship eligibility.

        Blocks:
        - PO Boxes (BCW requires street address)
        - APO/FPO/DPO (military addresses)
        - International addresses (BCW cannot blind ship)
        - Non-continental US (AK, HI, territories)

        Args:
            address: Address dictionary

        Returns:
            AddressValidationResult with validation status
        """
        errors = []
        address_text = " ".join([
            address.get("address1", ""),
            address.get("address2", ""),
            address.get("city", ""),
            address.get("state", ""),
        ]).upper()

        # Check for blocked countries first
        country = address.get("country", "").upper().strip()
        if country and country not in ("US", "USA", "UNITED STATES", ""):
            if country in self.BLOCKED_COUNTRIES:
                errors.append(
                    f"We cannot ship to {country} for these items. "
                    "International orders require customs documentation that would reveal our supplier."
                )
            else:
                errors.append(
                    "These items can only be shipped within the continental United States"
                )
            # Return early - no point checking other validations
            return AddressValidationResult(valid=False, errors=errors)

        # Check for PO Box
        for pattern in self.PO_BOX_PATTERNS:
            if re.search(pattern, address_text, re.IGNORECASE):
                errors.append("PO Box addresses are not supported for dropship orders")
                break

        # Check for military addresses
        for pattern in self.MILITARY_PATTERNS:
            if re.search(pattern, address_text, re.IGNORECASE):
                errors.append("Military addresses (APO/FPO/DPO) are not supported")
                break

        # Check required fields
        required = ["name", "address1", "city", "state", "zip"]
        for field in required:
            if not address.get(field):
                errors.append(f"Missing required field: {field}")

        # Validate ZIP format (US)
        zip_code = address.get("zip", "")
        if zip_code and not re.match(r"^\d{5}(-\d{4})?$", zip_code):
            errors.append("Invalid ZIP code format")

        # Validate state (2-letter code)
        state = address.get("state", "").upper().strip()
        if state:
            if len(state) != 2:
                errors.append("State must be 2-letter code")
            elif state in self.NON_CONTINENTAL_STATES:
                # Friendly message for AK/HI customers
                if state in ("AK", "HI"):
                    errors.append(
                        f"We're sorry, but we cannot ship these items to {state}. "
                        "Only continental US addresses are supported for this product line."
                    )
                else:
                    errors.append(
                        "Shipping to US territories is not available for these items"
                    )
            elif state not in self.ALLOWED_STATES:
                errors.append(f"Invalid or unsupported state code: {state}")

        return AddressValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            normalized_address=address if not errors else None,
        )

    async def get_shipping_options(
        self,
        items: List[CartItem],
        address: Dict[str, str],
    ) -> List[ShippingOption]:
        """
        Get available shipping options for items.

        Wrapper around quote service with address validation.

        Args:
            items: Cart items
            address: Shipping address

        Returns:
            List of shipping options
        """
        # Validate address first
        validation = self.validate_address(address)
        if not validation.valid:
            raise DropshipAddressValidationError(
                message="; ".join(validation.errors),
                address=address,
                validation_errors=validation.errors,
            )

        result = await self.quote_service.get_shipping_quote(items, address)
        if not result.success:
            raise DropshipError(
                message=result.error_message or "Failed to get shipping options",
                code="QUOTE_FAILED",
            )

        return result.options

    async def check_order_status(
        self,
        bcw_order_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Check current status of a BCW order.

        Args:
            bcw_order_id: BCW order number

        Returns:
            Status info or None if not found
        """
        from app.services.bcw.status_poller import BCWStatusPoller

        poller = BCWStatusPoller(self.client)
        status = await poller.get_order_status(bcw_order_id)

        if not status:
            return None

        return {
            "bcw_order_id": status.bcw_order_id,
            "status": status.status,
            "tracking_number": status.tracking_number,
            "carrier": status.carrier,
            "tracking_url": status.tracking_url,
        }

    async def _check_inventory(self, items: List[CartItem]) -> bool:
        """Check inventory availability for all items."""
        skus = [item.sku for item in items]
        availability = await self.inventory_service.check_items_availability(skus)

        for item in items:
            product = availability.get(item.sku)
            if not product or not product.in_stock:
                logger.warning(f"Item {item.sku} is out of stock")
                return False

            # Check quantity if available
            if product.available_qty and product.available_qty < item.quantity:
                logger.warning(
                    f"Insufficient quantity for {item.sku}: "
                    f"need {item.quantity}, have {product.available_qty}"
                )
                return False

        return True

    async def _get_or_create_bcw_order(
        self,
        order_id: int,
        items: List[CartItem],
        address: Dict[str, str],
        correlation_id: str,
    ) -> BCWOrder:
        """Get existing or create new BCW order record."""
        # Check for existing
        result = await self.db.execute(
            select(BCWOrder).where(BCWOrder.order_id == order_id)
        )
        existing = result.scalar_one_or_none()

        if existing:
            existing.correlation_id = correlation_id
            return existing

        # Create new
        bcw_order = BCWOrder(
            order_id=order_id,
            correlation_id=correlation_id,
            state=BCWOrderState.DRAFT,
            shipping_address_json=address,
            cart_items_json=[
                {"sku": item.sku, "quantity": item.quantity}
                for item in items
            ],
        )
        self.db.add(bcw_order)
        await self.db.flush()

        return bcw_order

    async def _transition_state(
        self,
        bcw_order: BCWOrder,
        new_state: BCWOrderState,
        event_type: str,
        event_data: Optional[Dict] = None,
    ):
        """Transition order to new state with audit logging."""
        old_state = bcw_order.state

        # Update state
        bcw_order.state = new_state
        bcw_order.updated_at = datetime.now(timezone.utc)

        # Log state change
        event = BCWOrderEvent(
            bcw_order_id=bcw_order.id,
            from_state=old_state.value,
            to_state=new_state.value,
            event_type=event_type,
            event_data=event_data or {},
            correlation_id=bcw_order.correlation_id,
        )
        self.db.add(event)

        logger.info(
            f"BCW order {bcw_order.id} state: {old_state.value} -> {new_state.value} "
            f"(event: {event_type})"
        )

        await self.db.flush()

    async def handle_tracking_update(
        self,
        bcw_order_id: str,
        tracking_number: str,
        carrier: Optional[str] = None,
        tracking_url: Optional[str] = None,
    ) -> bool:
        """
        Handle tracking information update.

        Args:
            bcw_order_id: BCW order number
            tracking_number: Tracking number
            carrier: Carrier name
            tracking_url: Tracking URL

        Returns:
            True if order updated successfully
        """
        result = await self.db.execute(
            select(BCWOrder).where(BCWOrder.bcw_order_id == bcw_order_id)
        )
        order = result.scalar_one_or_none()

        if not order:
            logger.warning(f"BCW order not found: {bcw_order_id}")
            return False

        # Update tracking info
        order.tracking_number = tracking_number
        order.carrier = carrier
        order.tracking_url = tracking_url

        # Transition to shipped if appropriate
        if order.state in [BCWOrderState.VENDOR_SUBMITTED, BCWOrderState.BACKORDERED]:
            order.shipped_at = datetime.now(timezone.utc)
            await self._transition_state(
                order,
                BCWOrderState.SHIPPED,
                "tracking_received",
                {
                    "tracking_number": tracking_number,
                    "carrier": carrier,
                },
            )

        await self.db.commit()
        return True

    async def handle_delivery_confirmation(
        self,
        bcw_order_id: str,
        delivered_at: Optional[datetime] = None,
    ) -> bool:
        """
        Handle delivery confirmation.

        Args:
            bcw_order_id: BCW order number
            delivered_at: Delivery timestamp

        Returns:
            True if order updated successfully
        """
        result = await self.db.execute(
            select(BCWOrder).where(BCWOrder.bcw_order_id == bcw_order_id)
        )
        order = result.scalar_one_or_none()

        if not order:
            logger.warning(f"BCW order not found: {bcw_order_id}")
            return False

        order.delivered_at = delivered_at or datetime.now(timezone.utc)

        if order.state == BCWOrderState.SHIPPED:
            await self._transition_state(
                order,
                BCWOrderState.DELIVERED,
                "delivery_confirmed",
                {"delivered_at": order.delivered_at.isoformat()},
            )

        await self.db.commit()
        return True
