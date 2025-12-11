"""
Migration: Add cover_hash column to comic_issues table

This migration:
1. Adds cover_hash column (VARCHAR(16), indexed) to comic_issues
2. Backfills cover_hash from raw_data JSON for existing records

Part of BE-003 fix: Image search performance optimization
"""
import asyncio
import logging
import sys
import os

# Add parent directory to path so we can import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.core.database import AsyncSessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def add_cover_hash_column():
    """Add cover_hash column to comic_issues table."""
    async with AsyncSessionLocal() as db:
        try:
            # Check if column already exists
            check_query = text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'comic_issues'
                AND column_name = 'cover_hash'
            """)
            result = await db.execute(check_query)
            exists = result.scalar_one_or_none()

            if exists:
                logger.info("cover_hash column already exists, skipping creation")
            else:
                # Add the column
                logger.info("Adding cover_hash column to comic_issues...")
                await db.execute(text("""
                    ALTER TABLE comic_issues
                    ADD COLUMN cover_hash VARCHAR(16)
                """))

                # Create index for efficient lookups
                logger.info("Creating index on cover_hash...")
                await db.execute(text("""
                    CREATE INDEX IF NOT EXISTS ix_comic_issues_cover_hash
                    ON comic_issues (cover_hash)
                    WHERE cover_hash IS NOT NULL
                """))

                await db.commit()
                logger.info("Column and index created successfully")

            return True

        except Exception as e:
            logger.error(f"Error adding column: {e}")
            await db.rollback()
            return False


async def backfill_cover_hash():
    """Backfill cover_hash from raw_data JSON."""
    async with AsyncSessionLocal() as db:
        try:
            # Count records that need backfill
            count_query = text("""
                SELECT COUNT(*)
                FROM comic_issues
                WHERE raw_data IS NOT NULL
                AND raw_data->>'cover_hash' IS NOT NULL
                AND (cover_hash IS NULL OR cover_hash = '')
            """)
            result = await db.execute(count_query)
            count = result.scalar()

            if count == 0:
                logger.info("No records need backfill")
                return True

            logger.info(f"Backfilling cover_hash for {count} records...")

            # Update in batches
            batch_size = 1000
            total_updated = 0

            while True:
                update_query = text("""
                    UPDATE comic_issues
                    SET cover_hash = raw_data->>'cover_hash'
                    WHERE id IN (
                        SELECT id FROM comic_issues
                        WHERE raw_data IS NOT NULL
                        AND raw_data->>'cover_hash' IS NOT NULL
                        AND (cover_hash IS NULL OR cover_hash = '')
                        LIMIT :batch_size
                    )
                """)

                result = await db.execute(update_query, {"batch_size": batch_size})
                updated = result.rowcount

                if updated == 0:
                    break

                total_updated += updated
                await db.commit()
                logger.info(f"Updated {total_updated}/{count} records...")

            logger.info(f"Backfill complete: {total_updated} records updated")
            return True

        except Exception as e:
            logger.error(f"Error during backfill: {e}")
            await db.rollback()
            return False


async def main():
    """Run the migration."""
    logger.info("=" * 60)
    logger.info("BE-003 Migration: Adding cover_hash column for image search")
    logger.info("=" * 60)

    # Step 1: Add column
    success = await add_cover_hash_column()
    if not success:
        logger.error("Failed to add column, aborting")
        return

    # Step 2: Backfill existing data
    success = await backfill_cover_hash()
    if not success:
        logger.error("Backfill failed (column still exists)")
        return

    logger.info("=" * 60)
    logger.info("Migration complete!")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
