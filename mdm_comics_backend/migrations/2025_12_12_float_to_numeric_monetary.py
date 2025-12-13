"""
Migration: Float to Numeric(12,2) for monetary columns

DB-001 per constitution_db.json Section 5:
- Converts Float columns to Numeric(12,2) for financial precision
- Prevents binary representation errors (e.g., $19.99 stored as 19.989999...)

Affected tables:
- products: price, original_price, cost
- orders: subtotal, shipping_cost, tax, total
- order_items: price

Rollback SQL included per governance requirements.
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


async def migrate_products_pricing() -> None:
    """Convert products pricing columns from Float to Numeric(12,2)."""
    async with AsyncSessionLocal() as db:
        try:
            logger.info("Converting products.price to NUMERIC(12,2)")
            await db.execute(
                text("""
                    ALTER TABLE products
                    ALTER COLUMN price TYPE NUMERIC(12,2) USING price::NUMERIC(12,2)
                """)
            )

            logger.info("Converting products.original_price to NUMERIC(12,2)")
            await db.execute(
                text("""
                    ALTER TABLE products
                    ALTER COLUMN original_price TYPE NUMERIC(12,2) USING original_price::NUMERIC(12,2)
                """)
            )

            logger.info("Converting products.cost to NUMERIC(12,2)")
            await db.execute(
                text("""
                    ALTER TABLE products
                    ALTER COLUMN cost TYPE NUMERIC(12,2) USING cost::NUMERIC(12,2)
                """)
            )

            await db.commit()
            logger.info("Products pricing columns migrated successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to migrate products pricing columns")
            raise


async def migrate_orders_pricing() -> None:
    """Convert orders pricing columns from Float to Numeric(12,2)."""
    async with AsyncSessionLocal() as db:
        try:
            logger.info("Converting orders.subtotal to NUMERIC(12,2)")
            await db.execute(
                text("""
                    ALTER TABLE orders
                    ALTER COLUMN subtotal TYPE NUMERIC(12,2) USING subtotal::NUMERIC(12,2)
                """)
            )

            logger.info("Converting orders.shipping_cost to NUMERIC(12,2)")
            await db.execute(
                text("""
                    ALTER TABLE orders
                    ALTER COLUMN shipping_cost TYPE NUMERIC(12,2) USING shipping_cost::NUMERIC(12,2)
                """)
            )

            logger.info("Converting orders.tax to NUMERIC(12,2)")
            await db.execute(
                text("""
                    ALTER TABLE orders
                    ALTER COLUMN tax TYPE NUMERIC(12,2) USING tax::NUMERIC(12,2)
                """)
            )

            logger.info("Converting orders.total to NUMERIC(12,2)")
            await db.execute(
                text("""
                    ALTER TABLE orders
                    ALTER COLUMN total TYPE NUMERIC(12,2) USING total::NUMERIC(12,2)
                """)
            )

            await db.commit()
            logger.info("Orders pricing columns migrated successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to migrate orders pricing columns")
            raise


async def migrate_order_items_pricing() -> None:
    """Convert order_items price column from Float to Numeric(12,2)."""
    async with AsyncSessionLocal() as db:
        try:
            logger.info("Converting order_items.price to NUMERIC(12,2)")
            await db.execute(
                text("""
                    ALTER TABLE order_items
                    ALTER COLUMN price TYPE NUMERIC(12,2) USING price::NUMERIC(12,2)
                """)
            )

            await db.commit()
            logger.info("Order items price column migrated successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to migrate order_items price column")
            raise


async def main():
    """
    Run the Float to Numeric migration.

    Per constitution_db.json Section 1, rollback SQL:

    -- ROLLBACK (NOT RECOMMENDED - potential precision loss)
    ALTER TABLE products ALTER COLUMN price TYPE FLOAT;
    ALTER TABLE products ALTER COLUMN original_price TYPE FLOAT;
    ALTER TABLE products ALTER COLUMN cost TYPE FLOAT;
    ALTER TABLE orders ALTER COLUMN subtotal TYPE FLOAT;
    ALTER TABLE orders ALTER COLUMN shipping_cost TYPE FLOAT;
    ALTER TABLE orders ALTER COLUMN tax TYPE FLOAT;
    ALTER TABLE orders ALTER COLUMN total TYPE FLOAT;
    ALTER TABLE order_items ALTER COLUMN price TYPE FLOAT;
    """
    logger.info("Starting migration: DB-001 Float to Numeric(12,2)")
    logger.info("Per constitution_db.json Section 5 - monetary precision")

    await migrate_products_pricing()
    await migrate_orders_pricing()
    await migrate_order_items_pricing()

    logger.info("Migration DB-001 complete")


if __name__ == "__main__":
    asyncio.run(main())
