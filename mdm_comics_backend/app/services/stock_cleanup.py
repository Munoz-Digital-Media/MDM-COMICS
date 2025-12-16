"""
Stock Reservation Cleanup Service

Background service to release expired stock reservations.
Should be run every 5 minutes via scheduler (APScheduler, cron, etc.).

Per constitution.json §15: "Stock reservation before capture; fail closed."
P2-6: Uses timezone-aware datetime
"""
import logging
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, delete, update, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.models import Product, StockReservation

logger = logging.getLogger(__name__)


async def release_expired_reservations() -> dict:
    """
    Release all expired stock reservations and restore inventory using an
    efficient, atomic, set-based SQL query. This is a PostgreSQL-specific
    implementation that avoids loading all expired records into memory.

    Returns:
        dict with count of released reservations and affected products
    """
    stats = {
        "reservations_released": 0,
        "products_restored": 0,
        "stock_restored": 0,
        "errors": 0
    }

    # This query is PostgreSQL specific.
    # It atomically finds expired reservations, updates product stock,
    # deletes the reservations, and returns statistics in a single transaction.
    atomic_cleanup_sql = text("""
        WITH expired AS (
            SELECT id, product_id, quantity
            FROM stock_reservations
            WHERE expires_at < NOW() AT TIME ZONE 'UTC'
            FOR UPDATE SKIP LOCKED
        ),
        agg_expired AS (
            SELECT product_id, SUM(quantity) as total_quantity
            FROM expired
            GROUP BY product_id
        ),
        updated_products AS (
            UPDATE products
            SET stock = products.stock + agg_expired.total_quantity
            FROM agg_expired
            WHERE products.id = agg_expired.product_id
            RETURNING products.id
        ),
        deleted_reservations AS (
            DELETE FROM stock_reservations
            WHERE id IN (SELECT id FROM expired)
            RETURNING 1
        )
        SELECT
            (SELECT COUNT(*) FROM deleted_reservations) as reservations_released,
            (SELECT COUNT(*) FROM updated_products) as products_restored,
            (SELECT COALESCE(SUM(quantity), 0) FROM expired) as stock_restored;
    """)

    async with AsyncSessionLocal() as db:
        try:
            async with db.begin():
                result = await db.execute(atomic_cleanup_sql)
                query_stats = result.first()

                if query_stats and query_stats.reservations_released > 0:
                    stats.update({
                        "reservations_released": query_stats.reservations_released,
                        "products_restored": query_stats.products_restored,
                        "stock_restored": int(query_stats.stock_restored),
                    })
                    logger.info(
                        f"Released {stats['reservations_released']} expired reservations, "
                        f"restored {stats['stock_restored']} units across {stats['products_restored']} products"
                    )
                else:
                    logger.debug("No expired reservations to clean up")

        except Exception as e:
            logger.error(f"Error in atomic stock cleanup: {e}", exc_info=True)
            stats["errors"] += 1

    return stats


async def get_reservation_stats() -> dict:
    """
    Get current reservation statistics for monitoring.
    """
    async with AsyncSessionLocal() as db:
        now = datetime.now(timezone.utc)
        soon = now + timedelta(minutes=5)

        stmt = select(
            func.count(StockReservation.id),
            func.count(StockReservation.id).filter(StockReservation.expires_at <= now),
            func.count(StockReservation.id).filter(StockReservation.expires_at > now),
            func.count(StockReservation.id).filter(
                and_(StockReservation.expires_at > now, StockReservation.expires_at <= soon)
            ),
        )

        total, expired, active, expiring = (await db.execute(stmt)).one()

        return {
            "total_reservations": int(total or 0),
            "active_reservations": int(active or 0),
            "expired_reservations": int(expired or 0),
            "expiring_within_5min": int(expiring or 0),
        }


# For running as standalone script
if __name__ == "__main__":
    import asyncio

    async def main():
        print("Running stock reservation cleanup...")
        stats = await release_expired_reservations()
        print(f"Cleanup complete: {stats}")

        print("
Current reservation stats:")
        reservation_stats = await get_reservation_stats()
        print(f"  Total: {reservation_stats['total_reservations']}")
        print(f"  Active: {reservation_stats['active_reservations']}")
        print(f"  Expired: {reservation_stats['expired_reservations']}")
        print(f"  Expiring soon: {reservation_stats['expiring_within_5min']}")

    asyncio.run(main())
