"""
Background Jobs for UPS Shipping Integration v1.28.0

Provides scheduled tasks for:
- Tracking sync (update shipment status from UPS)
- Rate quote cleanup (expire old quotes)
- Shipment reconciliation (match orders to shipments)
- Address normalization (batch validate addresses)

Per constitution_binder.json: Background jobs must be idempotent and have proper error handling.
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from sqlalchemy import select, and_, or_, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.models.shipment import Shipment, ShipmentRate, ShipmentStatus, TrackingEvent
from app.models.address import Address, AddressValidationStatus
from app.models.order import Order
from app.services.shipping_service import ShippingService, ShippingError
from app.services.alerting import (
    alert_tracking_sync_failure,
    alert_shipment_exception,
    resolve_pagerduty_alert,
)

logger = logging.getLogger(__name__)

# Job configuration
TRACKING_SYNC_BATCH_SIZE = 50
TRACKING_SYNC_INTERVAL_SECONDS = 300  # 5 minutes
RATE_CLEANUP_INTERVAL_SECONDS = 3600  # 1 hour
ADDRESS_VALIDATION_BATCH_SIZE = 20

# Tracking statuses that need updates
ACTIVE_TRACKING_STATUSES = [
    ShipmentStatus.LABEL_CREATED,
    ShipmentStatus.PICKED_UP,
    ShipmentStatus.IN_TRANSIT,
    ShipmentStatus.OUT_FOR_DELIVERY,
    ShipmentStatus.EXCEPTION,
]

# How long to keep tracking active shipments after last update
STALE_TRACKING_HOURS = 168  # 7 days


class ShippingJobRunner:
    """
    Manages and runs shipping background jobs.
    """

    def __init__(self):
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._consecutive_failures: dict = {}

    async def start(self):
        """Start all background jobs."""
        if self._running:
            logger.warning("Shipping jobs already running")
            return

        self._running = True
        logger.info("Starting shipping background jobs")

        # Create tasks
        self._tasks = [
            asyncio.create_task(self._tracking_sync_loop()),
            asyncio.create_task(self._rate_cleanup_loop()),
        ]

    async def stop(self):
        """Stop all background jobs."""
        self._running = False

        for task in self._tasks:
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []

        logger.info("Shipping background jobs stopped")

    # ==================== Tracking Sync Job ====================

    async def _tracking_sync_loop(self):
        """Main loop for tracking sync job."""
        while self._running:
            try:
                await self._run_tracking_sync()
            except Exception as e:
                logger.error(f"Tracking sync job error: {e}")

            await asyncio.sleep(TRACKING_SYNC_INTERVAL_SECONDS)

    async def _run_tracking_sync(self):
        """Run a single tracking sync cycle."""
        async with AsyncSessionLocal() as db:
            try:
                # Get shipments needing tracking update
                shipments = await self._get_shipments_for_tracking(db)

                if not shipments:
                    logger.debug("No shipments need tracking update")
                    return

                logger.info(f"Syncing tracking for {len(shipments)} shipments")

                shipping_service = ShippingService(db)
                updated = 0
                failed = 0

                try:
                    for shipment in shipments:
                        try:
                            await self._update_shipment_tracking(db, shipping_service, shipment)
                            updated += 1

                            # Clear consecutive failure counter
                            if shipment.id in self._consecutive_failures:
                                del self._consecutive_failures[shipment.id]
                                resolve_pagerduty_alert(f"tracking-sync-{shipment.id}")

                        except ShippingError as e:
                            failed += 1
                            self._handle_tracking_failure(shipment, str(e))

                        except Exception as e:
                            failed += 1
                            logger.error(f"Tracking update error for {shipment.id}: {e}")
                            self._handle_tracking_failure(shipment, str(e))

                finally:
                    await shipping_service.close()

                await db.commit()
                logger.info(f"Tracking sync complete: {updated} updated, {failed} failed")

            except Exception as e:
                await db.rollback()
                logger.error(f"Tracking sync batch error: {e}")

    async def _get_shipments_for_tracking(self, db: AsyncSession) -> List[Shipment]:
        """Get shipments that need tracking updates."""
        stale_cutoff = datetime.now(timezone.utc) - timedelta(hours=STALE_TRACKING_HOURS)

        result = await db.execute(
            select(Shipment)
            .where(
                and_(
                    Shipment.status.in_(ACTIVE_TRACKING_STATUSES),
                    Shipment.tracking_number.isnot(None),
                    or_(
                        Shipment.last_tracking_update.is_(None),
                        Shipment.last_tracking_update < datetime.now(timezone.utc) - timedelta(minutes=30),
                    ),
                    Shipment.created_at > stale_cutoff,
                )
            )
            .order_by(Shipment.last_tracking_update.asc().nullsfirst())
            .limit(TRACKING_SYNC_BATCH_SIZE)
        )

        return list(result.scalars().all())

    async def _update_shipment_tracking(
        self,
        db: AsyncSession,
        shipping_service: ShippingService,
        shipment: Shipment,
    ):
        """Update tracking for a single shipment."""
        old_status = shipment.status

        await shipping_service.update_tracking(shipment.id)

        # Check for exceptions
        if shipment.status == ShipmentStatus.EXCEPTION and old_status != ShipmentStatus.EXCEPTION:
            alert_shipment_exception(
                shipment_id=shipment.id,
                tracking_number=shipment.tracking_number,
                exception_code=shipment.status_detail or "UNKNOWN",
                exception_description=shipment.status_detail or "Delivery exception occurred",
            )

        # Check for delivery
        if shipment.status == ShipmentStatus.DELIVERED:
            logger.info(f"Shipment {shipment.id} delivered: {shipment.tracking_number}")
            # Resolve any existing exception alert
            resolve_pagerduty_alert(f"shipment-exception-{shipment.tracking_number}")

    def _handle_tracking_failure(self, shipment: Shipment, error: str):
        """Handle tracking update failure."""
        # Increment consecutive failure counter
        self._consecutive_failures[shipment.id] = self._consecutive_failures.get(shipment.id, 0) + 1
        failures = self._consecutive_failures[shipment.id]

        # Alert on repeated failures
        if failures >= 3:
            alert_tracking_sync_failure(
                shipment_id=shipment.id,
                tracking_number=shipment.tracking_number or "",
                error=error,
                consecutive_failures=failures,
            )

    # ==================== Rate Cleanup Job ====================

    async def _rate_cleanup_loop(self):
        """Main loop for rate cleanup job."""
        while self._running:
            try:
                await self._run_rate_cleanup()
            except Exception as e:
                logger.error(f"Rate cleanup job error: {e}")

            await asyncio.sleep(RATE_CLEANUP_INTERVAL_SECONDS)

    async def _run_rate_cleanup(self):
        """Run a single rate cleanup cycle."""
        async with AsyncSessionLocal() as db:
            try:
                now = datetime.now(timezone.utc)

                # Delete expired, unselected rate quotes
                result = await db.execute(
                    select(func.count(ShipmentRate.id)).where(
                        and_(
                            ShipmentRate.selected == False,
                            ShipmentRate.expires_at < now,
                        )
                    )
                )
                expired_count = result.scalar_one()

                if expired_count > 0:
                    await db.execute(
                        ShipmentRate.__table__.delete().where(
                            and_(
                                ShipmentRate.selected == False,
                                ShipmentRate.expires_at < now,
                            )
                        )
                    )
                    await db.commit()
                    logger.info(f"Cleaned up {expired_count} expired rate quotes")
                else:
                    logger.debug("No expired rate quotes to clean up")

            except Exception as e:
                await db.rollback()
                logger.error(f"Rate cleanup error: {e}")


# ==================== One-off Job Functions ====================


async def sync_all_active_tracking():
    """Manually trigger tracking sync for all active shipments."""
    runner = ShippingJobRunner()
    await runner._run_tracking_sync()


async def cleanup_expired_rates():
    """Manually trigger rate quote cleanup."""
    runner = ShippingJobRunner()
    await runner._run_rate_cleanup()


async def batch_validate_addresses(user_id: Optional[int] = None):
    """
    Batch validate pending addresses.

    Args:
        user_id: Optional user ID to filter addresses
    """
    async with AsyncSessionLocal() as db:
        # Get pending addresses
        query = select(Address).where(
            and_(
                Address.validation_status == AddressValidationStatus.PENDING,
                Address.deleted_at.is_(None),
                Address.country_code.in_(["US", "PR", "VI", "GU", "AS"]),
            )
        ).limit(ADDRESS_VALIDATION_BATCH_SIZE)

        if user_id:
            query = query.where(Address.user_id == user_id)

        result = await db.execute(query)
        addresses = list(result.scalars().all())

        if not addresses:
            logger.info("No addresses pending validation")
            return

        logger.info(f"Validating {len(addresses)} addresses")

        shipping_service = ShippingService(db)
        validated = 0
        failed = 0

        try:
            for address in addresses:
                try:
                    is_valid, _, messages = await shipping_service.validate_address(address)

                    if is_valid:
                        address.validation_status = AddressValidationStatus.VALID
                        validated += 1
                    else:
                        address.validation_status = AddressValidationStatus.INVALID
                        failed += 1

                    address.validated_at = datetime.now(timezone.utc)
                    address.validation_messages = str(messages)

                except Exception as e:
                    logger.warning(f"Address {address.id} validation error: {e}")
                    failed += 1

            await db.commit()
            logger.info(f"Address validation complete: {validated} valid, {failed} invalid/failed")

        finally:
            await shipping_service.close()


async def reconcile_shipments():
    """
    Reconcile shipments with orders.

    Finds orders marked as shipped but without shipment records.
    """
    async with AsyncSessionLocal() as db:
        # Find shipped orders without shipments
        result = await db.execute(
            select(Order)
            .outerjoin(Shipment, Order.id == Shipment.order_id)
            .where(
                and_(
                    Order.status == "shipped",
                    Order.tracking_number.isnot(None),
                    Shipment.id.is_(None),
                )
            )
            .limit(100)
        )
        orphan_orders = list(result.scalars().all())

        if not orphan_orders:
            logger.info("No orphan shipped orders found")
            return

        logger.warning(f"Found {len(orphan_orders)} shipped orders without shipment records")

        for order in orphan_orders:
            logger.warning(
                f"Orphan order {order.id}: tracking={order.tracking_number}, "
                f"shipped_at={order.shipped_at}"
            )

        # Note: Automatic repair would require more context about how these orders
        # were shipped. For now, we log for manual review.


# ==================== Job Scheduler Integration ====================


# Global job runner instance
_job_runner: Optional[ShippingJobRunner] = None


async def start_shipping_jobs():
    """Start the shipping background jobs."""
    global _job_runner

    if _job_runner is None:
        _job_runner = ShippingJobRunner()

    await _job_runner.start()


async def stop_shipping_jobs():
    """Stop the shipping background jobs."""
    global _job_runner

    if _job_runner:
        await _job_runner.stop()
        _job_runner = None
