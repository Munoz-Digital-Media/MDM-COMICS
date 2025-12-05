"""
Stock Reservation Cleanup Service

Background service to release expired stock reservations.
Should be run every 5 minutes via scheduler (APScheduler, cron, etc.).

Per constitution.json §15: "Stock reservation before capture; fail closed."
P2-6: Uses timezone-aware datetime
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import select, delete, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.models import Product, StockReservation

logger = logging.getLogger(__name__)


async def release_expired_reservations() -> dict:
    """
    Release all expired stock reservations and restore inventory.

    Returns:
        dict with count of released reservations and affected products
    """
    stats = {
        "reservations_released": 0,
        "products_restored": set(),
        "stock_restored": 0,
        "errors": 0
    }

    async with AsyncSessionLocal() as db:
        try:
            # Find all expired reservations with FOR UPDATE lock
            result = await db.execute(
                select(StockReservation)
                .where(StockReservation.expires_at < datetime.now(timezone.utc))
                .with_for_update()
            )
            expired = result.scalars().all()

            if not expired:
                logger.debug("No expired reservations to clean up")
                return stats

            # Group by product for efficient stock restoration
            product_quantities = {}
            for reservation in expired:
                product_id = reservation.product_id
                if product_id not in product_quantities:
                    product_quantities[product_id] = 0
                product_quantities[product_id] += reservation.quantity
                stats["reservations_released"] += 1

            # Restore stock for each affected product
            for product_id, quantity in product_quantities.items():
                try:
                    await db.execute(
                        update(Product)
                        .where(Product.id == product_id)
                        .values(stock=Product.stock + quantity)
                    )
                    stats["products_restored"].add(product_id)
                    stats["stock_restored"] += quantity
                except Exception as e:
                    logger.error(f"Error restoring stock for product {product_id}: {e}")
                    stats["errors"] += 1

            # Delete all expired reservations
            await db.execute(
                delete(StockReservation)
                .where(StockReservation.expires_at < datetime.now(timezone.utc))
            )

            await db.commit()

            # Convert set to count for JSON serialization
            stats["products_restored"] = len(stats["products_restored"])

            if stats["reservations_released"] > 0:
                logger.info(
                    f"Released {stats['reservations_released']} expired reservations, "
                    f"restored {stats['stock_restored']} units across {stats['products_restored']} products"
                )

        except Exception as e:
            logger.error(f"Error in stock cleanup: {e}")
            stats["errors"] += 1
            await db.rollback()

    return stats


async def get_reservation_stats() -> dict:
    """
    Get current reservation statistics for monitoring.
    """
    async with AsyncSessionLocal() as db:
        # Total active reservations
        result = await db.execute(
            select(StockReservation)
        )
        all_reservations = result.scalars().all()

        total = len(all_reservations)
        expired = sum(1 for r in all_reservations if r.is_expired)
        active = total - expired

        # Group by expiry time bucket
        now = datetime.now(timezone.utc)
        expiring_soon = sum(
            1 for r in all_reservations
            if not r.is_expired and (r.expires_at - now).seconds < 300  # < 5 min
        )

        return {
            "total_reservations": total,
            "active_reservations": active,
            "expired_reservations": expired,
            "expiring_within_5min": expiring_soon
        }


# For running as standalone script
if __name__ == "__main__":
    import asyncio

    async def main():
        print("Running stock reservation cleanup...")
        stats = await release_expired_reservations()
        print(f"Cleanup complete: {stats}")

        print("\nCurrent reservation stats:")
        reservation_stats = await get_reservation_stats()
        print(f"  Total: {reservation_stats['total_reservations']}")
        print(f"  Active: {reservation_stats['active_reservations']}")
        print(f"  Expired: {reservation_stats['expired_reservations']}")
        print(f"  Expiring soon: {reservation_stats['expiring_within_5min']}")

    asyncio.run(main())
