"""
OrderService - Centralized order creation logic

Extracts duplicated order creation from checkout.py confirm_order() and
handle_payment_succeeded() webhook handler to prevent logic drift.

Per constitution.json §15: Single source of truth for order creation.
"""
import uuid
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete

from app.models import Order, OrderItem, Product, StockReservation

logger = logging.getLogger(__name__)


def dollars_to_cents(amount: float) -> int:
    """Convert a float dollar amount to integer cents using bankers rounding."""
    if amount is None:
        return 0
    quantized = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return int(quantized * 100)


def cents_to_dollars(amount_cents: int) -> float:
    """Convert integer cents to float dollars."""
    return float(Decimal(amount_cents) / Decimal(100))


def build_product_snapshot(product: Product) -> Dict[str, Any]:
    """Capture immutable product data for order records."""
    return {
        "id": product.id,
        "name": product.name,
        "sku": product.sku,
        "price": product.price,
        "image_url": product.image_url,
        "category": product.category,
        "subcategory": product.subcategory,
    }


class OrderCreationResult:
    """Result of order creation attempt."""

    def __init__(
        self,
        success: bool,
        order: Optional[Order] = None,
        order_number: Optional[str] = None,
        error: Optional[str] = None,
        already_exists: bool = False
    ):
        self.success = success
        self.order = order
        self.order_number = order_number
        self.error = error
        self.already_exists = already_exists


class OrderService:
    """Centralized order creation service."""

    @staticmethod
    def generate_order_number() -> str:
        """Generate unique order number in format MDM-YYYYMMDD-XXXXXXXX."""
        return f"MDM-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

    @staticmethod
    async def check_existing_order(
        db: AsyncSession,
        payment_intent_id: str
    ) -> Optional[Order]:
        """Check if an order already exists for this payment intent."""
        result = await db.execute(
            select(Order).where(Order.payment_id == payment_intent_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def get_reservations(
        db: AsyncSession,
        payment_intent_id: str,
        user_id: Optional[int] = None
    ) -> List[StockReservation]:
        """Get reservations for a payment intent."""
        query = select(StockReservation).where(
            StockReservation.payment_intent_id == payment_intent_id
        )
        if user_id is not None:
            query = query.where(StockReservation.user_id == user_id)

        result = await db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    async def create_order_from_reservations(
        db: AsyncSession,
        user_id: int,
        payment_intent_id: str,
        payment_amount_cents: int,
        reservations: List[StockReservation],
        currency: str = "usd",
        expected_currency: str = "usd",
        validate_amount: bool = True
    ) -> OrderCreationResult:
        """
        Create an order from stock reservations.

        This is the single source of truth for order creation logic.

        Args:
            db: Database session
            user_id: User ID for the order
            payment_intent_id: Stripe payment intent ID
            payment_amount_cents: Amount paid in cents (from Stripe)
            reservations: List of stock reservations
            currency: Currency from payment intent
            expected_currency: Expected currency for checkout
            validate_amount: Whether to validate amount matches

        Returns:
            OrderCreationResult with success status and order or error
        """
        if not reservations:
            return OrderCreationResult(
                success=False,
                error="No reservations found"
            )

        # Validate currency
        if currency.lower() != expected_currency.lower():
            logger.error(
                "Currency mismatch intent=%s expected=%s received=%s",
                payment_intent_id,
                expected_currency,
                currency,
            )
            return OrderCreationResult(
                success=False,
                error="Payment currency mismatch"
            )

        # Collect fallback product IDs for reservations missing data
        fallback_ids = {
            r.product_id
            for r in reservations
            if not r.unit_price_cents or not r.product_snapshot
        }
        fallback_products: Dict[int, Product] = {}
        if fallback_ids:
            result = await db.execute(
                select(Product).where(Product.id.in_(list(fallback_ids)))
            )
            fallback_products = {prod.id: prod for prod in result.scalars().all()}

        # Check for expired reservations
        expired = [r for r in reservations if r.is_expired]
        if expired:
            logger.warning(
                "Creating order with %d expired reservations for payment %s",
                len(expired),
                payment_intent_id
            )

        # Build order items data and calculate subtotal
        subtotal_cents = 0
        order_items_data = []

        for reservation in reservations:
            unit_price_cents = reservation.unit_price_cents
            snapshot = reservation.product_snapshot or {}

            # Fallback to product data if reservation missing pricing
            if not unit_price_cents or unit_price_cents <= 0:
                product = fallback_products.get(reservation.product_id)
                if not product:
                    logger.error(
                        "Reservation missing pricing data for product_id=%s intent=%s",
                        reservation.product_id,
                        payment_intent_id,
                    )
                    return OrderCreationResult(
                        success=False,
                        error="Reservation pricing data missing"
                    )
                unit_price_cents = dollars_to_cents(product.price)
                if not snapshot:
                    snapshot = build_product_snapshot(product)
            elif not snapshot:
                product = fallback_products.get(reservation.product_id)
                if product:
                    snapshot = build_product_snapshot(product)

            reservation_total = unit_price_cents * reservation.quantity
            subtotal_cents += reservation_total
            order_items_data.append({
                "product_id": reservation.product_id,
                "quantity": reservation.quantity,
                "unit_price_cents": unit_price_cents,
                "snapshot": snapshot
            })

        # Validate amount matches
        if validate_amount and subtotal_cents != payment_amount_cents:
            logger.error(
                "Amount mismatch for payment %s (expected=%s actual=%s)",
                payment_intent_id,
                subtotal_cents,
                payment_amount_cents,
            )
            return OrderCreationResult(
                success=False,
                error="Order amount mismatch detected"
            )

        # Create the order
        order_number = OrderService.generate_order_number()

        order = Order(
            user_id=user_id,
            order_number=order_number,
            status="paid",
            subtotal=cents_to_dollars(subtotal_cents),
            total=cents_to_dollars(payment_amount_cents),
            payment_method="stripe",
            payment_id=payment_intent_id,
            paid_at=datetime.now(timezone.utc)
        )
        db.add(order)
        await db.flush()

        # Create order items
        for item_data in order_items_data:
            snapshot = item_data["snapshot"] or {}
            order_item = OrderItem(
                order_id=order.id,
                product_id=item_data["product_id"],
                product_name=snapshot.get("name") or f"Product #{item_data['product_id']}",
                product_sku=snapshot.get("sku"),
                price=cents_to_dollars(item_data["unit_price_cents"]),
                quantity=item_data["quantity"]
            )
            db.add(order_item)

        # Delete reservations (stock was already decremented)
        await db.execute(
            delete(StockReservation)
            .where(StockReservation.payment_intent_id == payment_intent_id)
        )

        logger.info(
            "Order %s created for payment %s (user=%s, total=%s)",
            order_number,
            payment_intent_id,
            user_id,
            cents_to_dollars(payment_amount_cents)
        )

        return OrderCreationResult(
            success=True,
            order=order,
            order_number=order_number
        )


# Singleton instance
order_service = OrderService()
