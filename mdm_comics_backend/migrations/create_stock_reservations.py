"""
Migration: Create stock_reservations table

Creates the stock_reservations table for tracking temporary stock reservations
during checkout. This prevents overselling race conditions.
"""
import asyncio
import logging
import os
import sys

from sqlalchemy import text

# Ensure app modules are importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def table_exists(db, table_name: str) -> bool:
    result = await db.execute(
        text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :name)"),
        {"name": table_name}
    )
    return result.scalar()


async def create_stock_reservations_table() -> None:
    async with AsyncSessionLocal() as db:
        try:
            if await table_exists(db, "stock_reservations"):
                logger.info("stock_reservations table already exists, skipping creation")
                return

            logger.info("Creating stock_reservations table")
            await db.execute(text("""
                CREATE TABLE stock_reservations (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    product_id INTEGER NOT NULL REFERENCES products(id),
                    quantity INTEGER NOT NULL CHECK (quantity > 0),
                    payment_intent_id VARCHAR(255) NOT NULL,
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    unit_price_cents INTEGER NOT NULL,
                    currency_code VARCHAR(3) NOT NULL DEFAULT 'USD',
                    product_snapshot JSONB
                )
            """))

            logger.info("Creating indexes on stock_reservations")
            await db.execute(text(
                "CREATE INDEX ix_stock_reservations_user_id ON stock_reservations(user_id)"
            ))
            await db.execute(text(
                "CREATE INDEX ix_stock_reservations_product_id ON stock_reservations(product_id)"
            ))
            await db.execute(text(
                "CREATE INDEX ix_stock_reservations_payment_intent_id ON stock_reservations(payment_intent_id)"
            ))
            await db.execute(text(
                "CREATE INDEX ix_stock_reservations_expires_at ON stock_reservations(expires_at)"
            ))

            await db.commit()
            logger.info("stock_reservations table created successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to create stock_reservations table")
            raise


async def main():
    logger.info("Starting migration: create stock_reservations table")
    await create_stock_reservations_table()
    logger.info("Migration complete")


if __name__ == "__main__":
    asyncio.run(main())
