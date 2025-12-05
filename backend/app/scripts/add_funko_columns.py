"""
One-time migration to add enrichment columns to funkos table.
Run this once to add the new columns.
"""
import asyncio
import logging
from sqlalchemy import text
from app.core.database import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def add_columns():
    """Add the new enrichment columns to funkos table."""
    columns_to_add = [
        ("category", "VARCHAR(255)"),
        ("license", "VARCHAR(255)"),
        ("product_type", "VARCHAR(100)"),
        ("box_number", "VARCHAR(50)"),
        ("funko_url", "TEXT"),
    ]

    async with engine.begin() as conn:
        for col_name, col_type in columns_to_add:
            try:
                await conn.execute(text(
                    f"ALTER TABLE funkos ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
                logger.info(f"Added column: {col_name}")
            except Exception as e:
                logger.warning(f"Column {col_name} might already exist: {e}")

        # Add indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS ix_funkos_category ON funkos(category)",
            "CREATE INDEX IF NOT EXISTS ix_funkos_license ON funkos(license)",
            "CREATE INDEX IF NOT EXISTS ix_funkos_product_type ON funkos(product_type)",
            "CREATE INDEX IF NOT EXISTS ix_funkos_box_number ON funkos(box_number)",
        ]

        for idx_sql in indexes:
            try:
                await conn.execute(text(idx_sql))
                logger.info(f"Created index")
            except Exception as e:
                logger.warning(f"Index might already exist: {e}")

    logger.info("Migration complete!")


if __name__ == "__main__":
    asyncio.run(add_columns())
