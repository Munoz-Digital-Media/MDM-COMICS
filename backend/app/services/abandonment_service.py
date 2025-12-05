"""
Cart Abandonment Detection & Recovery Service

Identifies abandoned carts and queues them for recovery campaigns.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from decimal import Decimal

from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.analytics import CartSnapshot, CartEvent, AnalyticsSession, CartAbandonmentQueue
from app.models.user import User
from app.models.coupon import Coupon, CouponCampaign
from app.services.coupon_service import get_coupon_service

logger = logging.getLogger(__name__)


class AbandonmentService:
    """
    Detects abandoned carts and manages recovery queue.

    Abandonment criteria:
    - Cart has items
    - No activity for X hours (configurable)
    - User has not completed checkout
    - User has email (for recovery)
    """

    # Default thresholds
    ABANDONMENT_THRESHOLD_HOURS = 2  # Consider abandoned after 2 hours
    HIGH_VALUE_THRESHOLD = Decimal("100")  # High priority if over $100
    RECOVERY_WINDOW_HOURS = 72  # Stop trying after 72 hours

    async def detect_abandoned_carts(
        self,
        db: AsyncSession,
        threshold_hours: int = None,
    ) -> List[CartAbandonmentQueue]:
        """
        Scan for abandoned carts and add to recovery queue.

        Returns list of newly queued abandonments.
        """
        threshold = threshold_hours or self.ABANDONMENT_THRESHOLD_HOURS
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=threshold)
        recovery_expiry = datetime.now(timezone.utc) + timedelta(hours=self.RECOVERY_WINDOW_HOURS)

        # Find carts that haven't been updated since cutoff
        # and haven't converted
        # and aren't already in queue

        # Get recent cart snapshots that haven't converted
        result = await db.execute(
            select(CartSnapshot)
            .where(
                CartSnapshot.snapshot_type.in_(["updated", "checkout_started"]),
                CartSnapshot.snapshot_at < cutoff_time,
                CartSnapshot.order_id == None,
                CartSnapshot.item_count > 0,
            )
            .order_by(CartSnapshot.snapshot_at.desc())
        )
        snapshots = result.scalars().all()

        # Deduplicate by cart_id (take most recent)
        seen_carts = set()
        unique_snapshots = []
        for snap in snapshots:
            if snap.cart_id not in seen_carts:
                seen_carts.add(snap.cart_id)
                unique_snapshots.append(snap)

        queued = []

        for snapshot in unique_snapshots:
            # Check if already in queue
            existing = await db.execute(
                select(CartAbandonmentQueue).where(
                    CartAbandonmentQueue.cart_id == snapshot.cart_id,
                    CartAbandonmentQueue.recovery_status.in_(["pending", "email_sent"]),
                )
            )
            if existing.scalar_one_or_none():
                continue

            # Get user info if available
            user_email = None
            user_name = None

            if snapshot.user_id:
                user_result = await db.execute(
                    select(User).where(User.id == snapshot.user_id)
                )
                user = user_result.scalar_one_or_none()
                if user:
                    user_email = user.email
                    user_name = user.name

            # Skip if no email (can't recover)
            if not user_email:
                continue

            # Calculate priority
            priority = self._calculate_priority(snapshot)

            # Create queue entry
            abandonment = CartAbandonmentQueue(
                cart_id=snapshot.cart_id,
                session_id=snapshot.session_id,
                user_id=snapshot.user_id,
                user_email=user_email,
                user_name=user_name,
                cart_snapshot=snapshot.items,
                cart_value=snapshot.subtotal,
                item_count=snapshot.item_count,
                last_activity_at=snapshot.snapshot_at,
                checkout_step_reached="checkout" if snapshot.snapshot_type == "checkout_started" else None,
                recovery_priority=priority,
                expires_at=recovery_expiry,
            )

            db.add(abandonment)
            queued.append(abandonment)

        await db.flush()

        logger.info(f"Detected {len(queued)} abandoned carts")
        return queued

    def _calculate_priority(self, snapshot: CartSnapshot) -> str:
        """Calculate recovery priority based on cart value and context."""
        if snapshot.subtotal >= self.HIGH_VALUE_THRESHOLD:
            return "high"
        elif snapshot.subtotal >= Decimal("50"):
            return "medium"
        else:
            return "low"

    async def get_pending_recoveries(
        self,
        db: AsyncSession,
        limit: int = 50,
        priority: Optional[str] = None,
    ) -> List[CartAbandonmentQueue]:
        """Get pending recoveries, optionally filtered by priority."""
        query = select(CartAbandonmentQueue).where(
            CartAbandonmentQueue.recovery_status == "pending",
            CartAbandonmentQueue.expires_at > datetime.now(timezone.utc),
        )

        if priority:
            query = query.where(CartAbandonmentQueue.recovery_priority == priority)

        query = query.order_by(
            CartAbandonmentQueue.recovery_priority.desc(),
            CartAbandonmentQueue.cart_value.desc(),
        ).limit(limit)

        result = await db.execute(query)
        return result.scalars().all()

    async def generate_recovery_coupon(
        self,
        db: AsyncSession,
        abandonment_id: int,
        campaign_id: int,
    ) -> Optional[Coupon]:
        """Generate a recovery coupon for an abandoned cart."""
        result = await db.execute(
            select(CartAbandonmentQueue).where(CartAbandonmentQueue.id == abandonment_id)
        )
        abandonment = result.scalar_one_or_none()

        if not abandonment:
            return None

        coupon_service = get_coupon_service()

        try:
            coupon = await coupon_service.generate_recovery_coupon(
                db=db,
                campaign_id=campaign_id,
                user_email=abandonment.user_email,
                user_id=abandonment.user_id,
            )

            # Link coupon to abandonment
            abandonment.recovery_coupon_id = coupon.id

            await db.flush()
            return coupon

        except Exception as e:
            logger.error(f"Failed to generate recovery coupon: {e}")
            return None

    async def mark_email_sent(
        self,
        db: AsyncSession,
        abandonment_id: int,
    ):
        """Mark recovery email as sent."""
        result = await db.execute(
            select(CartAbandonmentQueue).where(CartAbandonmentQueue.id == abandonment_id)
        )
        abandonment = result.scalar_one_or_none()

        if abandonment:
            abandonment.recovery_status = "email_sent"
            abandonment.recovery_email_sent_at = datetime.now(timezone.utc)
            await db.flush()

    async def mark_recovered(
        self,
        db: AsyncSession,
        cart_id: str,
        order_id: int,
    ):
        """Mark cart as recovered (order placed)."""
        result = await db.execute(
            select(CartAbandonmentQueue).where(
                CartAbandonmentQueue.cart_id == cart_id,
                CartAbandonmentQueue.recovery_status.in_(["pending", "email_sent"]),
            )
        )
        abandonment = result.scalar_one_or_none()

        if abandonment:
            abandonment.recovery_status = "recovered"
            abandonment.recovered_at = datetime.now(timezone.utc)
            abandonment.recovered_order_id = order_id
            await db.flush()

            logger.info(f"Cart {cart_id} recovered with order {order_id}")


# Singleton
_service: Optional[AbandonmentService] = None


def get_abandonment_service() -> AbandonmentService:
    global _service
    if _service is None:
        _service = AbandonmentService()
    return _service
