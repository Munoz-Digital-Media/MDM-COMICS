"""
Migration: Cover hash prefix/bytes & reservation pricing snapshot

Adds:
- comic_issues.cover_hash_prefix / cover_hash_bytes for selective image search
- stock_reservations.unit_price_cents / currency_code / product_snapshot

Backfills data from existing records.
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


async def column_exists(db, table: str, column: str) -> bool:
    query = text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = :table
          AND column_name = :column
        """
    )
    result = await db.execute(query, {"table": table, "column": column})
    return result.scalar() is not None


async def add_cover_hash_fields() -> None:
    async with AsyncSessionLocal() as db:
        try:
            if not await column_exists(db, "comic_issues", "cover_hash_prefix"):
                logger.info("Adding comic_issues.cover_hash_prefix column")
                await db.execute(
                    text("ALTER TABLE comic_issues ADD COLUMN cover_hash_prefix VARCHAR(8)")
                )

            if not await column_exists(db, "comic_issues", "cover_hash_bytes"):
                logger.info("Adding comic_issues.cover_hash_bytes column")
                await db.execute(
                    text("ALTER TABLE comic_issues ADD COLUMN cover_hash_bytes BYTEA")
                )

            logger.info("Creating index for cover_hash_prefix")
            await db.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_comic_issues_cover_hash_prefix "
                    "ON comic_issues (cover_hash_prefix)"
                )
            )

            logger.info("Backfilling cover_hash_prefix values")
            await db.execute(
                text(
                    """
                    UPDATE comic_issues
                    SET cover_hash_prefix = SUBSTRING(cover_hash, 1, 8)
                    WHERE cover_hash IS NOT NULL
                      AND (cover_hash_prefix IS NULL OR cover_hash_prefix = '')
                    """
                )
            )

            logger.info("Backfilling cover_hash_bytes values")
            await db.execute(
                text(
                    """
                    UPDATE comic_issues
                    SET cover_hash_bytes = decode(cover_hash, 'hex')
                    WHERE cover_hash IS NOT NULL
                      AND cover_hash ~ '^[0-9a-fA-F]+$'
                      AND cover_hash_bytes IS NULL
                    """
                )
            )

            await db.commit()
        except Exception:
            await db.rollback()
            logger.exception("Failed to update comic issue hash fields")
            raise


async def add_reservation_fields() -> None:
    async with AsyncSessionLocal() as db:
        try:
            if not await column_exists(db, "stock_reservations", "unit_price_cents"):
                logger.info("Adding stock_reservations.unit_price_cents")
                await db.execute(
                    text("ALTER TABLE stock_reservations ADD COLUMN unit_price_cents INTEGER")
                )

            if not await column_exists(db, "stock_reservations", "currency_code"):
                logger.info("Adding stock_reservations.currency_code")
                await db.execute(
                    text(
                        "ALTER TABLE stock_reservations "
                        "ADD COLUMN currency_code VARCHAR(3) DEFAULT 'USD'"
                    )
                )

            if not await column_exists(db, "stock_reservations", "product_snapshot"):
                logger.info("Adding stock_reservations.product_snapshot")
                await db.execute(
                    text("ALTER TABLE stock_reservations ADD COLUMN product_snapshot JSONB")
                )

            logger.info("Backfilling reservation pricing data")
            await db.execute(
                text(
                    """
                    UPDATE stock_reservations sr
                    SET unit_price_cents = COALESCE(
                            sr.unit_price_cents,
                            GREATEST(0, ROUND(COALESCE(p.price, 0) * 100))::INTEGER
                        ),
                        currency_code = COALESCE(sr.currency_code, 'USD'),
                        product_snapshot = COALESCE(
                            sr.product_snapshot,
                            jsonb_build_object(
                                'id', p.id,
                                'name', p.name,
                                'sku', p.sku,
                                'price', p.price,
                                'image_url', p.image_url,
                                'category', p.category,
                                'subcategory', p.subcategory
                            )
                        )
                    FROM products p
                    WHERE sr.product_id = p.id
                    """
                )
            )

            logger.info("Ensuring reservation pricing columns are NOT NULL")
            await db.execute(
                text(
                    """
                    ALTER TABLE stock_reservations
                    ALTER COLUMN unit_price_cents SET NOT NULL,
                    ALTER COLUMN currency_code SET NOT NULL
                    """
                )
            )

            await db.commit()
        except Exception:
            await db.rollback()
            logger.exception("Failed to update stock reservations")
            raise


async def main():
    logger.info("Starting migration: hash + checkout enhancements")
    await add_cover_hash_fields()
    await add_reservation_fields()
    logger.info("Migration complete")


if __name__ == "__main__":
    asyncio.run(main())
