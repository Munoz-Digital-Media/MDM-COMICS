"""
Migration: Database Improvements (DB-004, DB-005, DB-006/007/008, MED-001)

Per constitution_db.json Section 5:
- DB-004: Add indexes on FK columns for performance
- DB-005: Add audit trail columns (updated_by, update_reason)
- DB-006: Add check constraints for data integrity
- MED-001: Add missing indexes on frequently queried columns

Rollback SQL provided in docstrings.
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


async def add_fk_indexes() -> None:
    """
    DB-004: Add indexes on FK columns for JOIN performance.

    Rollback:
        DROP INDEX IF EXISTS idx_order_items_order_id;
        DROP INDEX IF EXISTS idx_order_items_product_id;
        DROP INDEX IF EXISTS idx_cart_items_user_id;
        DROP INDEX IF EXISTS idx_cart_items_product_id;
    """
    async with AsyncSessionLocal() as db:
        try:
            # Order Items indexes
            logger.info("Adding index on order_items.order_id")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_order_items_order_id
                ON order_items(order_id)
            """))

            logger.info("Adding index on order_items.product_id")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_order_items_product_id
                ON order_items(product_id)
            """))

            # Cart Items indexes
            logger.info("Adding index on cart_items.user_id")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cart_items_user_id
                ON cart_items(user_id)
            """))

            logger.info("Adding index on cart_items.product_id")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cart_items_product_id
                ON cart_items(product_id)
            """))

            # Composite index for cart lookups
            logger.info("Adding composite index on cart_items(user_id, product_id)")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cart_items_user_product
                ON cart_items(user_id, product_id)
            """))

            await db.commit()
            logger.info("DB-004: FK indexes created successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to create FK indexes")
            raise


async def add_query_indexes() -> None:
    """
    MED-001: Add indexes on frequently queried columns.

    Rollback:
        DROP INDEX IF EXISTS idx_orders_status;
        DROP INDEX IF EXISTS idx_orders_created_at;
        DROP INDEX IF EXISTS idx_orders_paid_at;
        DROP INDEX IF EXISTS idx_products_category;
    """
    async with AsyncSessionLocal() as db:
        try:
            # Orders indexes
            logger.info("Adding index on orders.status")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_orders_status
                ON orders(status)
            """))

            logger.info("Adding index on orders.created_at")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_orders_created_at
                ON orders(created_at DESC)
            """))

            logger.info("Adding index on orders.paid_at")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_orders_paid_at
                ON orders(paid_at) WHERE paid_at IS NOT NULL
            """))

            # Products indexes
            logger.info("Adding index on products.category")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_products_category
                ON products(category)
            """))

            await db.commit()
            logger.info("MED-001: Query indexes created successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to create query indexes")
            raise


async def add_check_constraints() -> None:
    """
    DB-006/007/008: Add check constraints for data integrity.

    Rollback:
        ALTER TABLE products DROP CONSTRAINT IF EXISTS check_stock_non_negative;
        ALTER TABLE products DROP CONSTRAINT IF EXISTS check_price_positive;
        ALTER TABLE shipments DROP CONSTRAINT IF EXISTS check_weight_positive;
    """
    async with AsyncSessionLocal() as db:
        try:
            # Stock must be >= 0
            logger.info("Adding check constraint for stock >= 0")
            await db.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'check_stock_non_negative'
                    ) THEN
                        ALTER TABLE products
                        ADD CONSTRAINT check_stock_non_negative
                        CHECK (stock >= 0);
                    END IF;
                END $$;
            """))

            # Price must be > 0
            logger.info("Adding check constraint for price > 0")
            await db.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'check_price_positive'
                    ) THEN
                        ALTER TABLE products
                        ADD CONSTRAINT check_price_positive
                        CHECK (price > 0);
                    END IF;
                END $$;
            """))

            # Shipment weight must be > 0 (if table exists)
            logger.info("Adding check constraint for shipment weight > 0 (if table exists)")
            await db.execute(text("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = 'shipments'
                    ) AND NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'check_weight_positive'
                    ) THEN
                        ALTER TABLE shipments
                        ADD CONSTRAINT check_weight_positive
                        CHECK (weight > 0);
                    END IF;
                END $$;
            """))

            await db.commit()
            logger.info("DB-006/007/008: Check constraints added successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to add check constraints")
            raise


async def add_audit_columns() -> None:
    """
    DB-005: Add audit trail columns to critical tables.

    Columns added:
    - updated_by: FK to users.id (who made the change)
    - update_reason: varchar(255) (why the change was made)

    Rollback:
        ALTER TABLE orders DROP COLUMN IF EXISTS updated_by;
        ALTER TABLE orders DROP COLUMN IF EXISTS update_reason;
        ALTER TABLE products DROP COLUMN IF EXISTS updated_by;
        ALTER TABLE products DROP COLUMN IF EXISTS update_reason;
    """
    async with AsyncSessionLocal() as db:
        try:
            # Orders audit columns
            logger.info("Adding audit columns to orders table")
            await db.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'orders' AND column_name = 'updated_by'
                    ) THEN
                        ALTER TABLE orders ADD COLUMN updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'orders' AND column_name = 'update_reason'
                    ) THEN
                        ALTER TABLE orders ADD COLUMN update_reason VARCHAR(255);
                    END IF;
                END $$;
            """))

            # Products audit columns
            logger.info("Adding audit columns to products table")
            await db.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'products' AND column_name = 'updated_by'
                    ) THEN
                        ALTER TABLE products ADD COLUMN updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL;
                    END IF;

                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'products' AND column_name = 'update_reason'
                    ) THEN
                        ALTER TABLE products ADD COLUMN update_reason VARCHAR(255);
                    END IF;
                END $$;
            """))

            await db.commit()
            logger.info("DB-005: Audit columns added successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to add audit columns")
            raise


async def main():
    """
    Run all database improvements.
    """
    logger.info("Starting DB improvements migration")

    await add_fk_indexes()
    await add_query_indexes()
    await add_check_constraints()
    await add_audit_columns()

    logger.info("DB improvements migration complete")


if __name__ == "__main__":
    asyncio.run(main())
