"""
Stripe Checkout API Routes
Minimal data to Stripe - customer info stays with us.
"""
import stripe
from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.config import settings
from app.core.database import get_db
from app.api.routes.auth import get_current_user
from app.models import User, Product, Order, OrderItem

router = APIRouter(prefix="/checkout", tags=["checkout"])

# Initialize Stripe
stripe.api_key = settings.STRIPE_SECRET_KEY


class CheckoutItem(BaseModel):
    product_id: int
    quantity: int


class CheckoutRequest(BaseModel):
    items: List[CheckoutItem]


class PaymentIntentRequest(BaseModel):
    items: List[CheckoutItem]


@router.post("/create-payment-intent")
async def create_payment_intent(
    request: PaymentIntentRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a Stripe PaymentIntent for the cart items.
    Returns client_secret for frontend to complete payment.

    We do NOT send customer email/name/address to Stripe here.
    Only the amount and our internal reference.
    """
    if not settings.STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured")

    if not request.items:
        raise HTTPException(status_code=400, detail="No items in cart")

    # Calculate total from database prices (never trust client-side prices)
    total_cents = 0
    line_items = []

    for item in request.items:
        result = await db.execute(
            select(Product).where(Product.id == item.product_id)
        )
        product = result.scalar_one_or_none()

        if not product:
            raise HTTPException(status_code=404, detail=f"Product {item.product_id} not found")

        if product.stock < item.quantity:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient stock for {product.name}"
            )

        item_total = int(product.price * 100) * item.quantity  # Convert to cents
        total_cents += item_total
        line_items.append({
            "product_id": product.id,
            "name": product.name,
            "quantity": item.quantity,
            "unit_price": int(product.price * 100)
        })

    if total_cents < 50:  # Stripe minimum is $0.50
        raise HTTPException(status_code=400, detail="Order total must be at least $0.50")

    try:
        # Create PaymentIntent - minimal data to Stripe
        # We only send: amount, currency, and our internal metadata
        # Customer info stays in OUR database
        intent = stripe.PaymentIntent.create(
            amount=total_cents,
            currency="usd",
            metadata={
                "user_id": str(current_user.id),
                "item_count": str(len(request.items))
            },
            # Automatic payment methods lets Stripe optimize for conversion
            # while still using Elements on our site
            automatic_payment_methods={"enabled": True}
        )

        return {
            "client_secret": intent.client_secret,
            "payment_intent_id": intent.id,
            "amount": total_cents,
            "line_items": line_items
        }

    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))


class ConfirmOrderRequest(BaseModel):
    payment_intent_id: str
    items: List[CheckoutItem]


@router.post("/confirm-order")
async def confirm_order(
    request: ConfirmOrderRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Called after successful payment to create the order in our database.
    Verifies payment succeeded before creating order.
    """
    import uuid
    from datetime import datetime

    try:
        # Verify payment succeeded with Stripe
        intent = stripe.PaymentIntent.retrieve(request.payment_intent_id)

        if intent.status != "succeeded":
            raise HTTPException(
                status_code=400,
                detail=f"Payment not completed. Status: {intent.status}"
            )

        # Verify this payment belongs to this user
        if intent.metadata.get("user_id") != str(current_user.id):
            raise HTTPException(status_code=403, detail="Payment does not belong to this user")

        # Check if order already exists for this payment
        existing = await db.execute(
            select(Order).where(Order.payment_id == request.payment_intent_id)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Order already created for this payment")

        # Generate order number
        order_number = f"MDM-{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

        # Calculate totals from items
        subtotal = 0
        order_items_data = []

        for item in request.items:
            result = await db.execute(
                select(Product).where(Product.id == item.product_id)
            )
            product = result.scalar_one_or_none()
            if product:
                item_total = product.price * item.quantity
                subtotal += item_total
                order_items_data.append({
                    "product": product,
                    "quantity": item.quantity
                })
                # Reduce stock
                product.stock -= item.quantity

        # Create order
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

        await db.commit()

        return {
            "order_id": order.id,
            "order_number": order_number,
            "status": "success",
            "total": order.total
        }

    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/config")
async def get_stripe_config():
    """
    Return publishable key for frontend.
    This is safe to expose - it's meant to be public.
    """
    return {
        "publishable_key": settings.STRIPE_PUBLISHABLE_KEY
    }
