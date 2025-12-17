"""
BCW Refund Request Module v1.0.0
Migration: Add category and source columns to order_items table

These columns store a snapshot of the product category/source at time of order,
enabling refund eligibility determination without needing to query the product table.
"""
import asyncio
from sqlalchemy import text

from app.core.database import engine


async def add_refund_fields():
    """Add category and source columns to order_items table."""
    async with engine.begin() as conn:
        # Check if columns exist
        result = await conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'order_items' AND column_name IN ('category', 'source')
        """))
        existing_cols = {row[0] for row in result.fetchall()}

        if 'category' not in existing_cols:
            await conn.execute(text("""
                ALTER TABLE order_items ADD COLUMN category VARCHAR(100)
            """))
            print("Added 'category' column to order_items")
        else:
            print("'category' column already exists")

        if 'source' not in existing_cols:
            await conn.execute(text("""
                ALTER TABLE order_items ADD COLUMN source VARCHAR(50)
            """))
            print("Added 'source' column to order_items")
        else:
            print("'source' column already exists")

        # Backfill existing orders from products table where possible
        await conn.execute(text("""
            UPDATE order_items oi
            SET category = p.category, source = p.source
            FROM products p
            WHERE oi.product_id = p.id
              AND (oi.category IS NULL OR oi.source IS NULL)
        """))
        print("Backfilled category/source from products table")

    print("Migration complete!")


if __name__ == "__main__":
    asyncio.run(add_refund_fields())
