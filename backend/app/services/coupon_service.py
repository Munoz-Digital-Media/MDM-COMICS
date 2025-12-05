"""
Coupon Service

Handles validation, application, and campaign management.
"""

import logging
import secrets
import string
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal

from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.coupon import Coupon, CouponCampaign, CouponUsage
from app.models.user import User
from app.models.order import Order

logger = logging.getLogger(__name__)


class CouponValidationError(Exception):
    """Raised when coupon validation fails."""
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class CouponService:
    """
    Full-featured coupon management.

    Features:
    - Code validation with all constraint checks
    - Discount calculation
    - Usage tracking
    - Campaign-based auto-generation
    - Abandonment recovery coupon generation
    """

    async def validate_coupon(
        self,
        db: AsyncSession,
        code: str,
        cart_items: List[Dict[str, Any]],
        cart_total: Decimal,
        user: Optional[User] = None,
        session_id: Optional[str] = None,
    ) -> Tuple[Coupon, Decimal]:
        """
        Validate a coupon code and calculate discount.

        Args:
            db: Database session
            code: Coupon code to validate
            cart_items: List of {product_id, category, price, quantity}
            cart_total: Cart subtotal before discount
            user: Current user (if authenticated)
            session_id: Session ID (for anonymous users)

        Returns:
            Tuple of (coupon, discount_amount)

        Raises:
            CouponValidationError: If validation fails
        """
        # Find coupon
        result = await db.execute(
            select(Coupon).where(
                Coupon.code == code.upper().strip(),
                Coupon.is_active == True,
            )
        )
        coupon = result.scalar_one_or_none()

        if not coupon:
            raise CouponValidationError("INVALID", "Coupon code not found")

        now = datetime.now(timezone.utc)

        # Check dates
        if coupon.starts_at and now < coupon.starts_at:
            raise CouponValidationError("NOT_STARTED", "This coupon is not yet active")

        if coupon.expires_at and now > coupon.expires_at:
            raise CouponValidationError("EXPIRED", "This coupon has expired")

        # Check total usage limit
        if coupon.usage_limit_total and coupon.usage_count >= coupon.usage_limit_total:
            raise CouponValidationError("EXHAUSTED", "This coupon has reached its usage limit")

        # Check per-user usage limit
        if user and coupon.usage_limit_per_user:
            user_usage_count = await db.scalar(
                select(func.count(CouponUsage.id)).where(
                    CouponUsage.coupon_id == coupon.id,
                    CouponUsage.user_id == user.id,
                    CouponUsage.status.in_(["applied", "converted"]),
                )
            )
            if user_usage_count >= coupon.usage_limit_per_user:
                raise CouponValidationError("ALREADY_USED", "You've already used this coupon")

        # Check user restrictions
        if coupon.registered_users_only and not user:
            raise CouponValidationError("REGISTERED_ONLY", "This coupon requires an account")

        if coupon.specific_user_ids and user:
            if user.id not in coupon.specific_user_ids:
                raise CouponValidationError("NOT_ELIGIBLE", "This coupon is not valid for your account")

        # Check first order only
        if coupon.first_order_only and user:
            order_count = await db.scalar(
                select(func.count(Order.id)).where(
                    Order.user_id == user.id,
                    Order.status != "cancelled",
                )
            )
            if order_count > 0:
                raise CouponValidationError("FIRST_ORDER_ONLY", "This coupon is for first orders only")

        # Check minimum order value
        if coupon.minimum_order_value and cart_total < coupon.minimum_order_value:
            min_val = coupon.minimum_order_value
            raise CouponValidationError(
                "MIN_ORDER",
                f"Minimum order of ${min_val:.2f} required"
            )

        # Calculate applicable cart value
        applicable_value = self._calculate_applicable_value(coupon, cart_items)

        if applicable_value <= 0:
            raise CouponValidationError("NO_APPLICABLE_ITEMS", "No items in cart qualify for this coupon")

        # Calculate discount
        discount = self._calculate_discount(coupon, applicable_value, cart_total)

        return coupon, discount

    def _calculate_applicable_value(
        self,
        coupon: Coupon,
        cart_items: List[Dict[str, Any]],
    ) -> Decimal:
        """Calculate the cart value that the coupon applies to."""
        if coupon.applies_to == "all":
            # Check exclusions only
            excluded = set(coupon.excluded_product_ids or [])
            return sum(
                Decimal(str(item["price"])) * item["quantity"]
                for item in cart_items
                if item["product_id"] not in excluded
            )

        elif coupon.applies_to == "category":
            categories = set(coupon.applicable_categories or [])
            excluded = set(coupon.excluded_product_ids or [])
            return sum(
                Decimal(str(item["price"])) * item["quantity"]
                for item in cart_items
                if item.get("category") in categories and item["product_id"] not in excluded
            )

        elif coupon.applies_to == "product":
            products = set(coupon.applicable_product_ids or [])
            return sum(
                Decimal(str(item["price"])) * item["quantity"]
                for item in cart_items
                if item["product_id"] in products
            )

        return Decimal("0")

    def _calculate_discount(
        self,
        coupon: Coupon,
        applicable_value: Decimal,
        cart_total: Decimal,
    ) -> Decimal:
        """Calculate the discount amount."""
        if coupon.discount_type == "percentage":
            discount = applicable_value * (coupon.discount_value / Decimal("100"))

            # Apply maximum cap if set
            if coupon.maximum_discount:
                discount = min(discount, coupon.maximum_discount)

        elif coupon.discount_type == "fixed_amount":
            discount = min(coupon.discount_value, applicable_value)

        elif coupon.discount_type == "free_shipping":
            # Free shipping handled differently - return shipping cost
            # For now, return a fixed shipping discount
            discount = Decimal("5.99")  # TODO: Get actual shipping cost

        else:
            discount = Decimal("0")

        # Never discount more than cart total
        return min(discount, cart_total)

    async def apply_coupon(
        self,
        db: AsyncSession,
        coupon: Coupon,
        discount: Decimal,
        cart_total: Decimal,
        user: Optional[User] = None,
        session_id: Optional[str] = None,
    ) -> CouponUsage:
        """
        Record coupon application.

        Call this when coupon is applied to cart (before order).
        """
        usage = CouponUsage(
            coupon_id=coupon.id,
            coupon_code=coupon.code,
            user_id=user.id if user else None,
            session_id=session_id,
            cart_value_before=cart_total,
            discount_applied=discount,
            cart_value_after=cart_total - discount,
            status="applied",
        )

        db.add(usage)

        # Increment usage count
        coupon.usage_count = (coupon.usage_count or 0) + 1

        await db.flush()

        logger.info(f"Coupon {coupon.code} applied: ${discount:.2f} discount")
        return usage

    async def convert_coupon_usage(
        self,
        db: AsyncSession,
        usage_id: int,
        order_id: int,
    ):
        """Mark coupon usage as converted (order completed)."""
        result = await db.execute(
            select(CouponUsage).where(CouponUsage.id == usage_id)
        )
        usage = result.scalar_one_or_none()

        if usage:
            usage.order_id = order_id
            usage.status = "converted"
            usage.converted_at = datetime.now(timezone.utc)

            # Update campaign stats if applicable
            if usage.coupon.campaign_id:
                result = await db.execute(
                    select(CouponCampaign).where(
                        CouponCampaign.id == usage.coupon.campaign_id
                    )
                )
                campaign = result.scalar_one_or_none()
                if campaign:
                    campaign.redeemed_count = (campaign.redeemed_count or 0) + 1
                    campaign.total_revenue = (
                        (campaign.total_revenue or Decimal("0")) + usage.cart_value_after
                    )

            await db.flush()

    async def generate_recovery_coupon(
        self,
        db: AsyncSession,
        campaign_id: int,
        user_email: Optional[str] = None,
        user_id: Optional[int] = None,
    ) -> Coupon:
        """
        Generate a unique recovery coupon for abandoned cart.

        Uses campaign settings for discount configuration.
        """
        result = await db.execute(
            select(CouponCampaign).where(CouponCampaign.id == campaign_id)
        )
        campaign = result.scalar_one_or_none()

        if not campaign or not campaign.auto_generate:
            raise ValueError("Campaign not found or auto-generation disabled")

        # Generate unique code
        prefix = campaign.code_prefix or "RECOVER"
        suffix = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        code = f"{prefix}{suffix}"

        # Calculate expiry
        expires_at = datetime.now(timezone.utc) + timedelta(hours=campaign.validity_hours or 72)

        coupon = Coupon(
            code=code,
            name=f"Recovery: {code}",
            description=f"Auto-generated recovery coupon from campaign: {campaign.name}",
            discount_type=campaign.discount_type or "percentage",
            discount_value=campaign.discount_value or Decimal("10"),
            usage_limit_total=1,
            usage_limit_per_user=1,
            starts_at=datetime.now(timezone.utc),
            expires_at=expires_at,
            is_active=True,
            campaign_id=campaign_id,
            source="abandonment",
            specific_user_ids=[user_id] if user_id else [],
        )

        db.add(coupon)

        # Update campaign counter
        campaign.generated_count = (campaign.generated_count or 0) + 1

        await db.flush()

        logger.info(f"Generated recovery coupon: {code}")
        return coupon

    async def create_coupon(
        self,
        db: AsyncSession,
        data: Dict[str, Any],
        created_by: int,
    ) -> Coupon:
        """Create a new coupon."""
        coupon = Coupon(
            code=data["code"].upper().strip(),
            name=data["name"],
            description=data.get("description"),
            discount_type=data["discount_type"],
            discount_value=Decimal(str(data["discount_value"])),
            minimum_order_value=Decimal(str(data["minimum_order_value"])) if data.get("minimum_order_value") else None,
            maximum_discount=Decimal(str(data["maximum_discount"])) if data.get("maximum_discount") else None,
            applies_to=data.get("applies_to", "all"),
            applicable_categories=data.get("applicable_categories", []),
            applicable_product_ids=data.get("applicable_product_ids", []),
            excluded_product_ids=data.get("excluded_product_ids", []),
            usage_limit_total=data.get("usage_limit_total"),
            usage_limit_per_user=data.get("usage_limit_per_user", 1),
            first_order_only=data.get("first_order_only", False),
            registered_users_only=data.get("registered_users_only", False),
            starts_at=data.get("starts_at", datetime.now(timezone.utc)),
            expires_at=data.get("expires_at"),
            is_active=data.get("is_active", True),
            campaign_id=data.get("campaign_id"),
            source=data.get("source", "manual"),
            created_by=created_by,
        )

        db.add(coupon)
        await db.flush()

        return coupon


# Singleton
_service: Optional[CouponService] = None


def get_coupon_service() -> CouponService:
    global _service
    if _service is None:
        _service = CouponService()
    return _service
