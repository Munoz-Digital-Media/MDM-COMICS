"""
Migration: Add FK cascade rules

DB-003 per constitution_db.json Section 5:
- order_items.order_id: CASCADE (delete items with order)
- order_items.product_id: SET NULL (preserve order history)
- cart_items.user_id: CASCADE (delete cart with user)
- cart_items.product_id: CASCADE (remove from cart when product deleted)
- orders.user_id: SET NULL (preserve order history)

Note: PostgreSQL requires dropping and recreating FK constraints to change cascade rules.
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


async def migrate_order_items_fks() -> None:
    """Update order_items FK cascade rules."""
    async with AsyncSessionLocal() as db:
        try:
            # Drop existing FK constraints
            logger.info("Dropping order_items FK constraints")
            await db.execute(text("""
                ALTER TABLE order_items
                DROP CONSTRAINT IF EXISTS order_items_order_id_fkey
            """))
            await db.execute(text("""
                ALTER TABLE order_items
                DROP CONSTRAINT IF EXISTS order_items_product_id_fkey
            """))

            # Add new FK constraints with cascade rules
            logger.info("Adding order_items.order_id FK with CASCADE")
            await db.execute(text("""
                ALTER TABLE order_items
                ADD CONSTRAINT order_items_order_id_fkey
                FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
            """))

            # Make product_id nullable and add SET NULL cascade
            logger.info("Updating order_items.product_id to nullable with SET NULL")
            await db.execute(text("""
                ALTER TABLE order_items
                ALTER COLUMN product_id DROP NOT NULL
            """))
            await db.execute(text("""
                ALTER TABLE order_items
                ADD CONSTRAINT order_items_product_id_fkey
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE SET NULL
            """))

            await db.commit()
            logger.info("order_items FK cascades updated successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to update order_items FK cascades")
            raise


async def migrate_cart_items_fks() -> None:
    """Update cart_items FK cascade rules."""
    async with AsyncSessionLocal() as db:
        try:
            # Drop existing FK constraints
            logger.info("Dropping cart_items FK constraints")
            await db.execute(text("""
                ALTER TABLE cart_items
                DROP CONSTRAINT IF EXISTS cart_items_user_id_fkey
            """))
            await db.execute(text("""
                ALTER TABLE cart_items
                DROP CONSTRAINT IF EXISTS cart_items_product_id_fkey
            """))

            # Add new FK constraints with CASCADE
            logger.info("Adding cart_items.user_id FK with CASCADE")
            await db.execute(text("""
                ALTER TABLE cart_items
                ADD CONSTRAINT cart_items_user_id_fkey
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            """))

            logger.info("Adding cart_items.product_id FK with CASCADE")
            await db.execute(text("""
                ALTER TABLE cart_items
                ADD CONSTRAINT cart_items_product_id_fkey
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
            """))

            await db.commit()
            logger.info("cart_items FK cascades updated successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to update cart_items FK cascades")
            raise


async def migrate_orders_user_fk() -> None:
    """Update orders.user_id FK cascade rule."""
    async with AsyncSessionLocal() as db:
        try:
            # Drop existing FK constraint
            logger.info("Dropping orders.user_id FK constraint")
            await db.execute(text("""
                ALTER TABLE orders
                DROP CONSTRAINT IF EXISTS orders_user_id_fkey
            """))

            # Make user_id nullable for SET NULL cascade
            logger.info("Updating orders.user_id to nullable")
            await db.execute(text("""
                ALTER TABLE orders
                ALTER COLUMN user_id DROP NOT NULL
            """))

            # Add new FK constraint with SET NULL
            logger.info("Adding orders.user_id FK with SET NULL")
            await db.execute(text("""
                ALTER TABLE orders
                ADD CONSTRAINT orders_user_id_fkey
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
            """))

            await db.commit()
            logger.info("orders.user_id FK cascade updated successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to update orders.user_id FK cascade")
            raise


async def main():
    """
    Run the FK cascade migration.

    Rollback SQL:
    -- Revert to no cascade (original behavior)
    ALTER TABLE order_items DROP CONSTRAINT order_items_order_id_fkey;
    ALTER TABLE order_items ADD CONSTRAINT order_items_order_id_fkey
        FOREIGN KEY (order_id) REFERENCES orders(id);
    ALTER TABLE order_items ALTER COLUMN product_id SET NOT NULL;
    ALTER TABLE order_items DROP CONSTRAINT order_items_product_id_fkey;
    ALTER TABLE order_items ADD CONSTRAINT order_items_product_id_fkey
        FOREIGN KEY (product_id) REFERENCES products(id);
    ALTER TABLE cart_items DROP CONSTRAINT cart_items_user_id_fkey;
    ALTER TABLE cart_items ADD CONSTRAINT cart_items_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id);
    ALTER TABLE cart_items DROP CONSTRAINT cart_items_product_id_fkey;
    ALTER TABLE cart_items ADD CONSTRAINT cart_items_product_id_fkey
        FOREIGN KEY (product_id) REFERENCES products(id);
    ALTER TABLE orders ALTER COLUMN user_id SET NOT NULL;
    ALTER TABLE orders DROP CONSTRAINT orders_user_id_fkey;
    ALTER TABLE orders ADD CONSTRAINT orders_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id);
    """
    logger.info("Starting migration: DB-003 FK cascades")

    await migrate_order_items_fks()
    await migrate_cart_items_fks()
    await migrate_orders_user_fk()

    logger.info("Migration DB-003 complete")


if __name__ == "__main__":
    asyncio.run(main())
