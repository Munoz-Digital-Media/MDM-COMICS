"""
BCW Refund Service

Centralized refund processing service for BCW Supplies only.

Enforces:
- BCW Supplies only eligibility (collectibles are FINAL SALE)
- Vendor credit recovery blocking (customer refund gated until BCW credit received)
- State machine transitions with validation
- Immutable audit trail with hash chain

Per constitution_db.json, constitution_pii.json, constitution_cyberSec.json
"""
import uuid
import hashlib
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple

import stripe
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.models import (
    Order,
    OrderItem,
    Product,
    BCWOrder,
    BCWRefundRequest,
    BCWRefundEvent,
    BCWRefundState,
    RefundEligibilityPolicy,
    VALID_REFUND_TRANSITIONS,
    can_process_customer_refund,
)

logger = logging.getLogger(__name__)

# Initialize Stripe with API key
stripe.api_key = settings.STRIPE_SECRET_KEY


class StripeRefundError(Exception):
    """Raised when Stripe refund fails."""
    def __init__(self, message: str, stripe_error_code: Optional[str] = None):
        super().__init__(message)
        self.stripe_error_code = stripe_error_code


class RefundBlockedError(Exception):
    """Raised when customer refund is blocked due to missing vendor credit."""
    pass


class InvalidStateTransitionError(Exception):
    """Raised when attempting an invalid state transition."""
    pass


class RefundNotEligibleError(Exception):
    """Raised when items are not eligible for refund."""
    pass


class BCWRefundService:
    """
    Centralized refund processing service.

    CRITICAL BUSINESS RULES:
    1. Only BCW Supplies are refundable (collectibles are FINAL SALE)
    2. Customer refund BLOCKED until vendor credit is received
    3. All state changes are audited with hash chain for tamper evidence
    """

    @staticmethod
    def generate_refund_number() -> str:
        """Generate unique refund number in format RFD-YYYYMMDD-XXXXXXXX."""
        return f"RFD-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

    @staticmethod
    def generate_idempotency_key(order_id: int, item_ids: List[int]) -> str:
        """Generate idempotency key for refund request."""
        key_data = f"{order_id}:{sorted(item_ids)}"
        return hashlib.sha256(key_data.encode()).hexdigest()

    @staticmethod
    def hash_actor_id(actor_id: int) -> str:
        """Hash actor ID using SHA-512 for PII compliance."""
        return hashlib.sha512(str(actor_id).encode()).hexdigest()

    @staticmethod
    def compute_event_hash(
        refund_request_id: int,
        from_state: Optional[str],
        to_state: str,
        trigger: str,
        created_at: datetime,
        prev_hash: Optional[str]
    ) -> str:
        """Compute SHA-512 hash for event chain integrity."""
        data = f"{refund_request_id}:{from_state}:{to_state}:{trigger}:{created_at.isoformat()}:{prev_hash or ''}"
        return hashlib.sha512(data.encode()).hexdigest()

    @staticmethod
    async def get_refund_policy(
        db: AsyncSession,
        product_type: str
    ) -> Optional[RefundEligibilityPolicy]:
        """Get refund policy for a product type."""
        result = await db.execute(
            select(RefundEligibilityPolicy)
            .where(RefundEligibilityPolicy.product_type == product_type)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def validate_refund_eligibility(
        db: AsyncSession,
        order_items: List[OrderItem]
    ) -> Tuple[bool, str, List[OrderItem]]:
        """
        Validate that ALL items in request are BCW Supplies.

        BCW Supplies = source == 'bcw' or product_type == 'bcw_supply'
        Collectibles (comics, funkos, graded) = NOT ELIGIBLE

        Returns:
            (is_eligible, message, eligible_items)
        """
        eligible_items = []
        ineligible_items = []
        ineligible_reasons = []

        for item in order_items:
            # Get the product to check its type
            product = item.product
            if not product:
                # Product was deleted - check snapshot if available
                ineligible_items.append(item)
                ineligible_reasons.append(f"Product #{item.product_id} no longer exists")
                continue

            # Check if BCW supply (only BCW supplies are refundable)
            is_bcw_supply = (
                getattr(product, 'source', None) == 'bcw' or
                getattr(product, 'product_type', None) == 'bcw_supply' or
                getattr(product, 'category', '').lower() in ['supplies', 'bcw supplies', 'bcw']
            )

            if is_bcw_supply:
                eligible_items.append(item)
            else:
                ineligible_items.append(item)
                product_type = getattr(product, 'category', 'collectible')
                ineligible_reasons.append(
                    f"{item.product_name} is a {product_type} - ALL SALES FINAL"
                )

        if ineligible_items:
            return (
                False,
                f"Refunds not available for collectibles. Ineligible items: {'; '.join(ineligible_reasons)}",
                []
            )

        return (True, "All items eligible for refund (BCW Supplies)", eligible_items)

    @staticmethod
    async def get_last_event(
        db: AsyncSession,
        refund_request_id: int
    ) -> Optional[BCWRefundEvent]:
        """Get the last event for hash chain computation."""
        result = await db.execute(
            select(BCWRefundEvent)
            .where(BCWRefundEvent.refund_request_id == refund_request_id)
            .order_by(BCWRefundEvent.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def create_refund_request(
        db: AsyncSession,
        order_id: int,
        user_id: int,
        items: List[Dict[str, Any]],
        reason_code: str,
        reason_description: Optional[str],
        correlation_id: Optional[str] = None
    ) -> BCWRefundRequest:
        """
        Create a new refund request with validation.

        Args:
            db: Database session
            order_id: Order ID
            user_id: User ID making the request
            items: List of {order_item_id, quantity} dicts
            reason_code: Reason code (damaged, wrong_item, etc.)
            reason_description: Optional description
            correlation_id: Optional correlation ID for tracing

        Returns:
            Created BCWRefundRequest

        Raises:
            RefundNotEligibleError: If any items are not eligible
        """
        correlation_id = correlation_id or str(uuid.uuid4())

        # Load order with items
        result = await db.execute(
            select(Order)
            .options(selectinload(Order.items).selectinload(OrderItem.product))
            .options(selectinload(Order.bcw_order))
            .where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()

        if not order:
            raise ValueError(f"Order {order_id} not found")

        if order.user_id != user_id:
            raise ValueError("Order does not belong to this user")

        # Get requested order items
        item_ids = [i['order_item_id'] for i in items]
        requested_items = [item for item in order.items if item.id in item_ids]

        if len(requested_items) != len(items):
            raise ValueError("Some requested items not found in order")

        # Validate eligibility
        is_eligible, message, eligible_items = await BCWRefundService.validate_refund_eligibility(
            db, requested_items
        )

        if not is_eligible:
            raise RefundNotEligibleError(message)

        # Calculate amounts
        original_amount = Decimal('0')
        refund_items = []

        for item in items:
            order_item = next(oi for oi in requested_items if oi.id == item['order_item_id'])
            qty = item.get('quantity', order_item.quantity)
            unit_price = Decimal(str(order_item.price))
            item_total = unit_price * qty
            original_amount += item_total

            refund_items.append({
                'order_item_id': order_item.id,
                'quantity': qty,
                'unit_price': float(unit_price),
                'item_total': float(item_total),
                'product_name': order_item.product_name,
            })

        # Get restocking fee from policy
        policy = await BCWRefundService.get_refund_policy(db, 'bcw_supply')
        restocking_fee_percent = Decimal(str(policy.restocking_fee_percent)) if policy else Decimal('15')
        restocking_fee = original_amount * (restocking_fee_percent / 100)
        refund_amount = original_amount - restocking_fee

        # Generate idempotency key
        idempotency_key = BCWRefundService.generate_idempotency_key(order_id, item_ids)

        # Check for existing request with same idempotency key
        existing = await db.execute(
            select(BCWRefundRequest)
            .where(BCWRefundRequest.idempotency_key == idempotency_key)
        )
        existing_request = existing.scalar_one_or_none()
        if existing_request:
            logger.info(f"Returning existing refund request {existing_request.refund_number}")
            return existing_request

        # Create refund request
        refund_request = BCWRefundRequest(
            refund_number=BCWRefundService.generate_refund_number(),
            idempotency_key=idempotency_key,
            correlation_id=correlation_id,
            order_id=order_id,
            bcw_order_id=order.bcw_order.id if order.bcw_order else None,
            user_id=user_id,
            state=BCWRefundState.REQUESTED,
            reason_code=reason_code,
            reason_description=reason_description,
            refund_items=refund_items,
            original_amount=original_amount,
            restocking_fee=restocking_fee,
            refund_amount=refund_amount,
        )
        db.add(refund_request)
        await db.flush()

        # Create initial event
        event = await BCWRefundService._create_event(
            db=db,
            refund_request=refund_request,
            from_state=None,
            to_state=BCWRefundState.REQUESTED,
            trigger="customer_requested",
            actor_type="user",
            actor_id=user_id,
            event_data={
                "reason_code": reason_code,
                "items_count": len(refund_items),
                "original_amount": float(original_amount),
                "refund_amount": float(refund_amount),
            },
            correlation_id=correlation_id,
        )

        logger.info(
            f"Created refund request {refund_request.refund_number} for order {order_id} "
            f"(amount: ${refund_amount}, items: {len(refund_items)})"
        )

        return refund_request

    @staticmethod
    async def transition_state(
        db: AsyncSession,
        refund_request: BCWRefundRequest,
        new_state: BCWRefundState,
        trigger: str,
        actor_type: str,
        actor_id: Optional[int] = None,
        event_data: Optional[Dict] = None,
        correlation_id: Optional[str] = None
    ) -> BCWRefundRequest:
        """
        Transition refund request to new state with validation.

        Creates immutable BCWRefundEvent with hash chain.

        Raises:
            InvalidStateTransitionError: If transition is not allowed
        """
        current_state = refund_request.state

        # Validate transition
        valid_next_states = VALID_REFUND_TRANSITIONS.get(current_state, [])
        if new_state not in valid_next_states:
            raise InvalidStateTransitionError(
                f"Cannot transition from {current_state} to {new_state}. "
                f"Valid transitions: {valid_next_states}"
            )

        # Update state
        refund_request.previous_state = current_state.value
        refund_request.state = new_state
        refund_request.updated_at = datetime.now(timezone.utc)

        # Create audit event
        await BCWRefundService._create_event(
            db=db,
            refund_request=refund_request,
            from_state=current_state,
            to_state=new_state,
            trigger=trigger,
            actor_type=actor_type,
            actor_id=actor_id,
            event_data=event_data,
            correlation_id=correlation_id or refund_request.correlation_id,
        )

        logger.info(
            f"Refund {refund_request.refund_number} transitioned "
            f"{current_state} -> {new_state} (trigger: {trigger})"
        )

        return refund_request

    @staticmethod
    async def _create_event(
        db: AsyncSession,
        refund_request: BCWRefundRequest,
        from_state: Optional[BCWRefundState],
        to_state: BCWRefundState,
        trigger: str,
        actor_type: str,
        actor_id: Optional[int],
        event_data: Optional[Dict],
        correlation_id: str,
    ) -> BCWRefundEvent:
        """Create an immutable audit event with hash chain."""
        created_at = datetime.now(timezone.utc)

        # Get previous event hash for chain
        last_event = await BCWRefundService.get_last_event(db, refund_request.id)
        prev_hash = last_event.event_hash if last_event else None

        # Compute event hash
        event_hash = BCWRefundService.compute_event_hash(
            refund_request_id=refund_request.id,
            from_state=from_state.value if from_state else None,
            to_state=to_state.value,
            trigger=trigger,
            created_at=created_at,
            prev_hash=prev_hash,
        )

        event = BCWRefundEvent(
            refund_request_id=refund_request.id,
            from_state=from_state.value if from_state else None,
            to_state=to_state.value,
            trigger=trigger,
            correlation_id=correlation_id,
            actor_type=actor_type,
            actor_id_hash=BCWRefundService.hash_actor_id(actor_id) if actor_id else None,
            event_data=event_data,
            prev_event_hash=prev_hash,
            event_hash=event_hash,
            created_at=created_at,
        )
        db.add(event)
        await db.flush()

        return event

    @staticmethod
    async def approve_refund(
        db: AsyncSession,
        refund_request: BCWRefundRequest,
        admin_id: int,
        notes: Optional[str] = None
    ) -> BCWRefundRequest:
        """Approve a refund request."""
        refund_request.reviewed_by = admin_id
        refund_request.reviewed_at = datetime.now(timezone.utc)

        return await BCWRefundService.transition_state(
            db=db,
            refund_request=refund_request,
            new_state=BCWRefundState.APPROVED,
            trigger="admin_approved",
            actor_type="admin",
            actor_id=admin_id,
            event_data={"notes": notes} if notes else None,
        )

    @staticmethod
    async def deny_refund(
        db: AsyncSession,
        refund_request: BCWRefundRequest,
        admin_id: int,
        denial_reason: str
    ) -> BCWRefundRequest:
        """Deny a refund request."""
        refund_request.reviewed_by = admin_id
        refund_request.reviewed_at = datetime.now(timezone.utc)
        refund_request.denial_reason = denial_reason

        return await BCWRefundService.transition_state(
            db=db,
            refund_request=refund_request,
            new_state=BCWRefundState.DENIED,
            trigger="admin_denied",
            actor_type="admin",
            actor_id=admin_id,
            event_data={"denial_reason": denial_reason},
        )

    @staticmethod
    async def record_vendor_credit(
        db: AsyncSession,
        refund_request: BCWRefundRequest,
        credit_amount: Decimal,
        credit_reference: str,
        admin_id: int
    ) -> BCWRefundRequest:
        """
        Record vendor credit received from BCW.

        This is the BLOCKING GATE - customer refund can only proceed after this.
        """
        refund_request.vendor_credit_amount = credit_amount
        refund_request.vendor_credit_reference = credit_reference
        refund_request.vendor_credit_received_at = datetime.now(timezone.utc)

        return await BCWRefundService.transition_state(
            db=db,
            refund_request=refund_request,
            new_state=BCWRefundState.VENDOR_CREDIT_RECEIVED,
            trigger="vendor_credit_confirmed",
            actor_type="admin",
            actor_id=admin_id,
            event_data={
                "credit_amount": float(credit_amount),
                "credit_reference": credit_reference,
            },
        )


    @staticmethod
    async def _execute_stripe_refund(
        order: Order,
        refund_amount: Decimal,
        refund_number: str,
    ) -> str:
        """
        Execute actual Stripe refund.

        Args:
            order: Order with payment_intent_id
            refund_amount: Amount to refund in dollars
            refund_number: Our refund reference number

        Returns:
            Stripe refund ID (e.g., 're_xxx')

        Raises:
            StripeRefundError: If Stripe refund fails
        """
        payment_intent_id = getattr(order, 'payment_intent_id', None)
        if not payment_intent_id:
            raise StripeRefundError(
                "Order has no payment_intent_id - cannot process Stripe refund",
                stripe_error_code="missing_payment_intent"
            )

        try:
            # Convert dollars to cents for Stripe
            amount_cents = int(refund_amount * 100)

            refund = stripe.Refund.create(
                payment_intent=payment_intent_id,
                amount=amount_cents,
                metadata={
                    "refund_number": refund_number,
                    "source": "bcw_refund_service",
                },
            )

            logger.info(
                f"Stripe refund created: {refund.id} for ${refund_amount} "
                f"(payment_intent: {payment_intent_id})"
            )

            return refund.id

        except stripe.error.InvalidRequestError as e:
            logger.error(f"Stripe InvalidRequestError: {e}")
            raise StripeRefundError(
                f"Invalid refund request: {str(e)}",
                stripe_error_code=e.code
            )
        except stripe.error.CardError as e:
            logger.error(f"Stripe CardError: {e}")
            raise StripeRefundError(
                f"Card error during refund: {str(e)}",
                stripe_error_code=e.code
            )
        except stripe.error.StripeError as e:
            logger.error(f"Stripe error: {e}")
            raise StripeRefundError(
                f"Stripe error: {str(e)}",
                stripe_error_code=getattr(e, 'code', None)
            )

    @staticmethod
    async def process_customer_refund(
        db: AsyncSession,
        refund_request: BCWRefundRequest,
        admin_id: int,
    ) -> BCWRefundRequest:
        """
        Process customer refund via Stripe.

        CRITICAL: This method REFUSES to execute unless
        refund_request.state == VENDOR_CREDIT_RECEIVED

        Feature Flag: ENABLE_REAL_STRIPE_REFUNDS
        - False (default): Generates fake refund ID for testing
        - True: Executes real Stripe refund

        Raises:
            RefundBlockedError: If vendor credit not yet received
            StripeRefundError: If Stripe refund fails (when real refunds enabled)
        """
        if not can_process_customer_refund(refund_request):
            raise RefundBlockedError(
                f"Customer refund BLOCKED: Vendor credit not yet received. "
                f"Current state: {refund_request.state}. "
                f"Refund can only be processed after VENDOR_CREDIT_RECEIVED state."
            )

        # Transition to processing
        await BCWRefundService.transition_state(
            db=db,
            refund_request=refund_request,
            new_state=BCWRefundState.CUSTOMER_REFUND_PROCESSING,
            trigger="customer_refund_initiated",
            actor_type="admin",
            actor_id=admin_id,
        )

        # Check feature flag for real Stripe refunds
        enable_real_refunds = getattr(settings, 'ENABLE_REAL_STRIPE_REFUNDS', False)

        if enable_real_refunds and settings.STRIPE_SECRET_KEY:
            # REAL STRIPE REFUND
            logger.info(f"Processing REAL Stripe refund for {refund_request.refund_number}")

            # Load order to get payment_intent_id
            result = await db.execute(
                select(Order).where(Order.id == refund_request.order_id)
            )
            order = result.scalar_one_or_none()

            if not order:
                raise StripeRefundError(
                    f"Order {refund_request.order_id} not found",
                    stripe_error_code="order_not_found"
                )

            try:
                stripe_refund_id = await BCWRefundService._execute_stripe_refund(
                    order=order,
                    refund_amount=refund_request.refund_amount,
                    refund_number=refund_request.refund_number,
                )
            except StripeRefundError as e:
                # Log the failure and transition to error state
                logger.error(f"Stripe refund failed for {refund_request.refund_number}: {e}")
                await BCWRefundService.transition_state(
                    db=db,
                    refund_request=refund_request,
                    new_state=BCWRefundState.CUSTOMER_REFUND_FAILED,
                    trigger="stripe_refund_failed",
                    actor_type="system",
                    event_data={
                        "error": str(e),
                        "stripe_error_code": e.stripe_error_code,
                    },
                )
                raise
        else:
            # SIMULATED REFUND (feature flag off or no Stripe key)
            logger.warning(
                f"SIMULATED refund for {refund_request.refund_number} "
                f"(ENABLE_REAL_STRIPE_REFUNDS={enable_real_refunds})"
            )
            stripe_refund_id = f"re_SIMULATED_{uuid.uuid4().hex[:16]}"

        refund_request.stripe_refund_id = stripe_refund_id
        refund_request.customer_refund_issued_at = datetime.now(timezone.utc)

        # Transition to issued
        await BCWRefundService.transition_state(
            db=db,
            refund_request=refund_request,
            new_state=BCWRefundState.CUSTOMER_REFUND_ISSUED,
            trigger="stripe_refund_completed",
            actor_type="system",
            event_data={
                "stripe_refund_id": stripe_refund_id,
                "refund_amount": float(refund_request.refund_amount),
                "real_refund": enable_real_refunds,
            },
        )

        # Complete
        await BCWRefundService.transition_state(
            db=db,
            refund_request=refund_request,
            new_state=BCWRefundState.COMPLETED,
            trigger="refund_completed",
            actor_type="system",
        )

        logger.info(
            f"Refund {refund_request.refund_number} completed. "
            f"Customer refunded ${refund_request.refund_amount} via Stripe ({stripe_refund_id})"
        )

        return refund_request

    @staticmethod
    async def get_user_refund_requests(
        db: AsyncSession,
        user_id: int,
        limit: int = 50,
        offset: int = 0
    ) -> List[BCWRefundRequest]:
        """Get refund requests for a user."""
        result = await db.execute(
            select(BCWRefundRequest)
            .where(BCWRefundRequest.user_id == user_id)
            .order_by(BCWRefundRequest.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(result.scalars().all())

    @staticmethod
    async def get_pending_refunds(
        db: AsyncSession,
        states: Optional[List[BCWRefundState]] = None
    ) -> List[BCWRefundRequest]:
        """Get pending refund requests for admin review."""
        if states is None:
            states = [
                BCWRefundState.REQUESTED,
                BCWRefundState.UNDER_REVIEW,
                BCWRefundState.APPROVED,
                BCWRefundState.VENDOR_RETURN_INITIATED,
                BCWRefundState.VENDOR_CREDIT_PENDING,
                BCWRefundState.VENDOR_CREDIT_RECEIVED,
            ]

        result = await db.execute(
            select(BCWRefundRequest)
            .where(BCWRefundRequest.state.in_(states))
            .order_by(BCWRefundRequest.created_at.asc())
        )
        return list(result.scalars().all())


# Singleton instance
refund_service = BCWRefundService()
