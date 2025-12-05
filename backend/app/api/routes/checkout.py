"""
Stripe Checkout API Routes

SAFE CHECKOUT FLOW with stock reservation per constitution.json §15:
1. create-payment-intent: FOR UPDATE lock on products, reserve stock
2. confirm-order: Verify reservation, convert to sale
3. Background job: Release expired reservations
4. P1-4: Stripe webhook for payment verification (replaces client-trust model)
5. P1-3: Rate limited to prevent abuse
6. P2-9: Observability logging for checkout flows

This prevents the race condition where two users could pay for the same item.
"""
import stripe
import logging
import uuid
import time
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Depends, Request, Header
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, update

from app.core.config import settings
from app.core.database import get_db, AsyncSessionLocal
from app.core.rate_limit import limiter
from app.api.routes.auth import get_current_user
from app.models import User, Product, Order, OrderItem, StockReservation
from app.models.stock_reservation import RESERVATION_TTL_MINUTES

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/checkout", tags=["checkout"])

# Initialize Stripe
stripe.api_key = settings.STRIPE_SECRET_KEY

# P1-4: Track processed webhook events for idempotency
_processed_webhook_events: set = set()


class CheckoutItem(BaseModel):
    product_id: int
    quantity: int


class PaymentIntentRequest(BaseModel):
    items: List[CheckoutItem]


class ConfirmOrderRequest(BaseModel):
    payment_intent_id: str
    items: List[CheckoutItem]


@router.post("/create-payment-intent")
@limiter.limit(settings.RATE_LIMIT_CHECKOUT)
async def create_payment_intent(
    request: Request,
    payload: PaymentIntentRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a Stripe PaymentIntent with stock reservation.

    SAFE CHECKOUT FLOW:
    1. Lock products with FOR UPDATE (pessimistic locking)
    2. Validate stock availability
    3. Decrement stock (reserve)
    4. Create reservation records
    5. Create Stripe PaymentIntent
    6. Return client_secret

    If payment fails/expires, background job restores stock.
    """
    # P2-9: Start timing for observability
    start_time = time.time()

    if not settings.STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured")

    if not payload.items:
        raise HTTPException(status_code=400, detail="No items in cart")

    product_ids = [item.product_id for item in payload.items]
    quantities = {item.product_id: item.quantity for item in payload.items}

    # Step 1: Single query with FOR UPDATE lock
    # This prevents other transactions from modifying these rows
    result = await db.execute(
        select(Product)
        .where(Product.id.in_(product_ids))
        .with_for_update()  # Pessimistic lock
    )
    products = {p.id: p for p in result.scalars().all()}

    # Validate all products exist
    missing = set(product_ids) - set(products.keys())
    if missing:
        raise HTTPException(
            status_code=404,
            detail=f"Products not found: {list(missing)}"
        )

    # Step 2 & 3: Validate stock and reserve
    total_cents = 0
    line_items = []
    reservations_to_create = []

    for product_id, qty in quantities.items():
        product = products[product_id]

        if product.stock < qty:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient stock for {product.name} "
                       f"(requested: {qty}, available: {product.stock})"
            )

        # Reserve stock (decrement now)
        product.stock -= qty

        item_total = int(product.price * 100) * qty
        total_cents += item_total

        line_items.append({
            "product_id": product.id,
            "name": product.name,
            "quantity": qty,
            "unit_price": int(product.price * 100)
        })

        reservations_to_create.append({
            "product_id": product.id,
            "quantity": qty
        })

    if total_cents < 50:  # Stripe minimum
        raise HTTPException(status_code=400, detail="Order total must be at least $0.50")

    # Step 5: Create Stripe PaymentIntent
    try:
        intent = stripe.PaymentIntent.create(
            amount=total_cents,
            currency="usd",
            metadata={
                "user_id": str(current_user.id),
                "item_count": str(len(payload.items))
            },
            automatic_payment_methods={"enabled": True}
        )
    except stripe.error.StripeError as e:
        # Rollback will restore stock on exception
        raise HTTPException(status_code=400, detail=str(e))

    # Step 4: Create reservation records
    expires_at = StockReservation.create_expiry(RESERVATION_TTL_MINUTES)

    for res in reservations_to_create:
        reservation = StockReservation(
            user_id=current_user.id,
            product_id=res["product_id"],
            quantity=res["quantity"],
            payment_intent_id=intent.id,
            expires_at=expires_at
        )
        db.add(reservation)

    # Commit: stock decremented + reservations created
    await db.commit()

    # P2-9: Observability - log checkout metrics
    duration_ms = (time.time() - start_time) * 1000
    logger.info(
        f"CHECKOUT_METRIC: payment_intent_created "
        f"user_id={current_user.id} "
        f"intent_id={intent.id} "
        f"amount_cents={total_cents} "
        f"item_count={len(reservations_to_create)} "
        f"duration_ms={duration_ms:.2f}"
    )

    return {
        "client_secret": intent.client_secret,
        "payment_intent_id": intent.id,
        "amount": total_cents,
        "line_items": line_items,
        "reservation_expires_at": expires_at.isoformat(),
        "reservation_ttl_minutes": RESERVATION_TTL_MINUTES
    }


@router.post("/confirm-order")
async def confirm_order(
    request: ConfirmOrderRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Called after successful payment to create the order.

    SAFE FLOW:
    1. Verify payment succeeded with Stripe
    2. Verify reservation exists and belongs to user
    3. Create order from reservation data
    4. Delete reservation (stock already decremented)
    """
    import uuid

    try:
        # Step 1: Verify payment with Stripe
        intent = stripe.PaymentIntent.retrieve(request.payment_intent_id)

        if intent.status != "succeeded":
            raise HTTPException(
                status_code=400,
                detail=f"Payment not completed. Status: {intent.status}"
            )

        # Verify this payment belongs to this user
        if intent.metadata.get("user_id") != str(current_user.id):
            raise HTTPException(status_code=403, detail="Payment does not belong to this user")

        # Step 2: Check reservation exists
        result = await db.execute(
            select(StockReservation)
            .where(StockReservation.payment_intent_id == request.payment_intent_id)
            .where(StockReservation.user_id == current_user.id)
        )
        reservations = result.scalars().all()

        if not reservations:
            # Check if order already exists (idempotency)
            existing = await db.execute(
                select(Order).where(Order.payment_id == request.payment_intent_id)
            )
            if existing.scalar_one_or_none():
                raise HTTPException(status_code=400, detail="Order already created for this payment")

            # No reservation and no order - reservation may have expired
            raise HTTPException(
                status_code=400,
                detail="Reservation expired or not found. Please try checkout again."
            )

        # Check if any reservations are expired
        expired = [r for r in reservations if r.is_expired]
        if expired:
            logger.warning(
                f"Confirming order with {len(expired)} expired reservations "
                f"for payment {request.payment_intent_id}"
            )

        # Step 3: Create order
        order_number = f"MDM-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

        # Calculate totals from reservations
        subtotal = 0
        order_items_data = []

        for reservation in reservations:
            result = await db.execute(
                select(Product).where(Product.id == reservation.product_id)
            )
            product = result.scalar_one_or_none()
            if product:
                item_total = product.price * reservation.quantity
                subtotal += item_total
                order_items_data.append({
                    "product": product,
                    "quantity": reservation.quantity
                })

        order = Order(
            user_id=current_user.id,
            order_number=order_number,
            status="paid",
            subtotal=subtotal,
            total=intent.amount / 100,
            payment_method="stripe",
            payment_id=request.payment_intent_id,
            paid_at=datetime.now(timezone.utc)
        )
        db.add(order)
        await db.flush()

        # Create order items
        for item_data in order_items_data:
            order_item = OrderItem(
                order_id=order.id,
                product_id=item_data["product"].id,
                product_name=item_data["product"].name,
                product_sku=item_data["product"].sku,
                price=item_data["product"].price,
                quantity=item_data["quantity"]
            )
            db.add(order_item)

        # Step 4: Delete reservations (stock was already decremented)
        await db.execute(
            delete(StockReservation)
            .where(StockReservation.payment_intent_id == request.payment_intent_id)
        )

        await db.commit()

        logger.info(f"Order {order_number} created for payment {request.payment_intent_id}")

        return {
            "order_id": order.id,
            "order_number": order_number,
            "status": "success",
            "total": order.total
        }

    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/cancel-reservation")
async def cancel_reservation(
    payment_intent_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Cancel a reservation and restore stock.
    Called when user cancels checkout.
    """
    # Find reservations for this payment intent
    result = await db.execute(
        select(StockReservation)
        .where(StockReservation.payment_intent_id == payment_intent_id)
        .where(StockReservation.user_id == current_user.id)
    )
    reservations = result.scalars().all()

    if not reservations:
        return {"status": "no_reservation", "message": "No active reservation found"}

    # Restore stock for each reservation
    for reservation in reservations:
        await db.execute(
            update(Product)
            .where(Product.id == reservation.product_id)
            .values(stock=Product.stock + reservation.quantity)
        )

    # Delete reservations
    await db.execute(
        delete(StockReservation)
        .where(StockReservation.payment_intent_id == payment_intent_id)
        .where(StockReservation.user_id == current_user.id)
    )

    await db.commit()

    logger.info(f"Cancelled {len(reservations)} reservations for payment {payment_intent_id}")

    return {
        "status": "cancelled",
        "reservations_released": len(reservations)
    }


@router.get("/config")
async def get_stripe_config():
    """
    Return publishable key for frontend.
    This is safe to expose - it's meant to be public.
    """
    return {
        "publishable_key": settings.STRIPE_PUBLISHABLE_KEY
    }


@router.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    """
    P1-4: Stripe webhook handler with signature verification.

    SECURITY:
    1. Verifies webhook signature using STRIPE_WEBHOOK_SECRET
    2. Idempotent - stores processed event IDs to prevent duplicates
    3. Only processes payment_intent.succeeded events

    This replaces client-trust model where frontend confirms payment.
    Now Stripe tells us directly when payment succeeds.
    """
    global _processed_webhook_events

    # P1-4: Verify webhook secret is configured
    if not settings.STRIPE_WEBHOOK_SECRET:
        logger.error("Stripe webhook received but STRIPE_WEBHOOK_SECRET not configured")
        raise HTTPException(status_code=500, detail="Webhook not configured")

    # Get the raw body and signature
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not sig_header:
        logger.warning("Stripe webhook missing signature header")
        raise HTTPException(status_code=400, detail="Missing signature")

    # P1-4: Verify signature
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, settings.STRIPE_WEBHOOK_SECRET
        )
    except ValueError as e:
        logger.warning(f"Stripe webhook invalid payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError as e:
        logger.warning(f"Stripe webhook signature verification failed: {e}")
        raise HTTPException(status_code=400, detail="Invalid signature")

    # P1-4: Idempotency check - skip if we've already processed this event
    event_id = event.get("id")
    if event_id in _processed_webhook_events:
        logger.info(f"Stripe webhook event {event_id} already processed, skipping")
        return {"status": "already_processed"}

    # Handle the event
    event_type = event.get("type")
    logger.info(f"Stripe webhook received: {event_type} (event_id={event_id})")

    if event_type == "payment_intent.succeeded":
        payment_intent = event["data"]["object"]
        await handle_payment_succeeded(payment_intent)
    elif event_type == "payment_intent.payment_failed":
        payment_intent = event["data"]["object"]
        await handle_payment_failed(payment_intent)
    else:
        logger.info(f"Unhandled webhook event type: {event_type}")

    # Mark as processed (in-memory for now, could use Redis/DB for persistence)
    _processed_webhook_events.add(event_id)

    # Cleanup old event IDs to prevent memory growth (keep last 10000)
    if len(_processed_webhook_events) > 10000:
        # Convert to list, sort, keep recent half
        _processed_webhook_events = set(list(_processed_webhook_events)[-5000:])

    return {"status": "success"}


async def handle_payment_succeeded(payment_intent: dict):
    """
    P1-4: Handle successful payment from Stripe webhook.

    Creates order if not already created by confirm-order endpoint.
    This ensures orders are created even if frontend fails to call confirm-order.
    """
    payment_intent_id = payment_intent["id"]
    user_id = payment_intent.get("metadata", {}).get("user_id")

    if not user_id:
        logger.warning(f"Payment {payment_intent_id} missing user_id in metadata")
        return

    async with AsyncSessionLocal() as db:
        # Check if order already exists (created by confirm-order endpoint)
        existing = await db.execute(
            select(Order).where(Order.payment_id == payment_intent_id)
        )
        if existing.scalar_one_or_none():
            logger.info(f"Order already exists for payment {payment_intent_id}")
            return

        # Check for reservations
        result = await db.execute(
            select(StockReservation)
            .where(StockReservation.payment_intent_id == payment_intent_id)
        )
        reservations = result.scalars().all()

        if not reservations:
            logger.warning(
                f"Payment {payment_intent_id} succeeded but no reservations found. "
                f"Order may have been created by confirm-order or reservations expired."
            )
            return

        # Create order from reservations
        order_number = f"MDM-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

        subtotal = 0
        order_items_data = []

        for reservation in reservations:
            product_result = await db.execute(
                select(Product).where(Product.id == reservation.product_id)
            )
            product = product_result.scalar_one_or_none()
            if product:
                item_total = product.price * reservation.quantity
                subtotal += item_total
                order_items_data.append({
                    "product": product,
                    "quantity": reservation.quantity
                })

        order = Order(
            user_id=int(user_id),
            order_number=order_number,
            status="paid",
            subtotal=subtotal,
            total=payment_intent["amount"] / 100,
            payment_method="stripe",
            payment_id=payment_intent_id,
            paid_at=datetime.now(timezone.utc)
        )
        db.add(order)
        await db.flush()

        for item_data in order_items_data:
            order_item = OrderItem(
                order_id=order.id,
                product_id=item_data["product"].id,
                product_name=item_data["product"].name,
                product_sku=item_data["product"].sku,
                price=item_data["product"].price,
                quantity=item_data["quantity"]
            )
            db.add(order_item)

        # Delete reservations
        await db.execute(
            delete(StockReservation)
            .where(StockReservation.payment_intent_id == payment_intent_id)
        )

        await db.commit()
        logger.info(
            f"Webhook created order {order_number} for payment {payment_intent_id}"
        )


async def handle_payment_failed(payment_intent: dict):
    """
    P1-4: Handle failed payment from Stripe webhook.

    Releases stock reservations when payment fails.
    """
    payment_intent_id = payment_intent["id"]

    async with AsyncSessionLocal() as db:
        # Find reservations for this payment
        result = await db.execute(
            select(StockReservation)
            .where(StockReservation.payment_intent_id == payment_intent_id)
        )
        reservations = result.scalars().all()

        if not reservations:
            logger.info(f"No reservations to release for failed payment {payment_intent_id}")
            return

        # Restore stock
        for reservation in reservations:
            await db.execute(
                update(Product)
                .where(Product.id == reservation.product_id)
                .values(stock=Product.stock + reservation.quantity)
            )

        # Delete reservations
        await db.execute(
            delete(StockReservation)
            .where(StockReservation.payment_intent_id == payment_intent_id)
        )

        await db.commit()
        logger.info(
            f"Released {len(reservations)} reservations for failed payment {payment_intent_id}"
        )
