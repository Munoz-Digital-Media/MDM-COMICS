"""
Stripe Checkout API Routes

SAFE CHECKOUT FLOW with stock reservation per constitution.json §15:
1. create-payment-intent: FOR UPDATE lock on products, reserve stock
2. confirm-order: Verify reservation, convert to sale
3. Background job: Release expired reservations

This prevents the race condition where two users could pay for the same item.
"""
import stripe
import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, update

from app.core.config import settings
from app.core.database import get_db
from app.api.routes.auth import get_current_user
from app.models import User, Product, Order, OrderItem, StockReservation
from app.models.stock_reservation import RESERVATION_TTL_MINUTES

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/checkout", tags=["checkout"])

# Initialize Stripe
stripe.api_key = settings.STRIPE_SECRET_KEY


class CheckoutItem(BaseModel):
    product_id: int
    quantity: int


class PaymentIntentRequest(BaseModel):
    items: List[CheckoutItem]


class ConfirmOrderRequest(BaseModel):
    payment_intent_id: str
    items: List[CheckoutItem]


@router.post("/create-payment-intent")
async def create_payment_intent(
    request: PaymentIntentRequest,
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
    if not settings.STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured")

    if not request.items:
        raise HTTPException(status_code=400, detail="No items in cart")

    product_ids = [item.product_id for item in request.items]
    quantities = {item.product_id: item.quantity for item in request.items}

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
                "item_count": str(len(request.items))
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

    logger.info(
        f"Created payment intent {intent.id} for user {current_user.id} "
        f"with {len(reservations_to_create)} reserved items"
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
        order_number = f"MDM-{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

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
            paid_at=datetime.utcnow()
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
