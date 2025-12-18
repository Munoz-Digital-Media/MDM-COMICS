"""
PriceCharting Incremental Sync Migration v1.0.0

Document ID: PC-OPT-2024-001 Phase 2
Status: APPROVED

This migration adds columns and indexes for incremental price sync:
- pricecharting_synced_at: Timestamp of last successful price sync
- Partial indexes for efficient stale-record queries

Purpose:
- Reduce daily API calls by 80%+ by only syncing stale records (>24h old)
- Improve job performance by skipping recently-synced records

Per constitution_db.json:
- DB-001: snake_case for all tables and columns
- DB-004: Indexes on query columns
- DB-011: Schema changes via migration
"""
import asyncio
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Threshold for "stale" records (default: 24 hours)
STALE_THRESHOLD_HOURS = 24


async def migrate_pricecharting_incremental_sync(engine):
    """
    Apply incremental sync schema changes.

    This is an idempotent migration - safe to run multiple times.

    Changes:
    1. Add pricecharting_synced_at to comic_issues
    2. Add pricecharting_synced_at to funkos
    3. Create partial indexes for stale-record queries
    """
    logger.info("Starting PriceCharting Incremental Sync migration...")

    async with engine.begin() as conn:
        # ==================== Comic Issues ====================
        logger.info("Phase 1: Adding pricecharting_synced_at to comic_issues...")

        # Check if column exists
        result = await conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'comic_issues'
              AND column_name = 'pricecharting_synced_at'
        """))
        col_exists = result.fetchone() is not None

        if not col_exists:
            await conn.execute(text("""
                ALTER TABLE comic_issues
                ADD COLUMN pricecharting_synced_at TIMESTAMPTZ
            """))
            await conn.execute(text("""
                COMMENT ON COLUMN comic_issues.pricecharting_synced_at
                IS 'Timestamp of last successful PriceCharting price sync (v1.1.0 incremental sync)'
            """))
            logger.info("Added pricecharting_synced_at to comic_issues")
        else:
            logger.info("comic_issues.pricecharting_synced_at already exists")

        # Create partial index for stale comic issues
        # This index only includes records that:
        # 1. Have a pricecharting_id (are matched)
        # 2. Need syncing (NULL or old synced_at)
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_comic_issues_pc_stale
            ON comic_issues (id, pricecharting_synced_at)
            WHERE pricecharting_id IS NOT NULL
        """))
        logger.info("Created partial index ix_comic_issues_pc_stale")

        # ==================== Funkos ====================
        logger.info("Phase 2: Adding pricecharting_synced_at to funkos...")

        result = await conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'funkos'
              AND column_name = 'pricecharting_synced_at'
        """))
        col_exists = result.fetchone() is not None

        if not col_exists:
            await conn.execute(text("""
                ALTER TABLE funkos
                ADD COLUMN pricecharting_synced_at TIMESTAMPTZ
            """))
            await conn.execute(text("""
                COMMENT ON COLUMN funkos.pricecharting_synced_at
                IS 'Timestamp of last successful PriceCharting price sync (v1.1.0 incremental sync)'
            """))
            logger.info("Added pricecharting_synced_at to funkos")
        else:
            logger.info("funkos.pricecharting_synced_at already exists")

        # Create partial index for stale funkos
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_funkos_pc_stale
            ON funkos (id, pricecharting_synced_at)
            WHERE pricecharting_id IS NOT NULL
        """))
        logger.info("Created partial index ix_funkos_pc_stale")

        # ==================== Backfill existing synced records ====================
        # Records that already have prices were synced "sometime in the past"
        # Set their synced_at to 25 hours ago so they get picked up on next run
        logger.info("Phase 3: Backfilling pricecharting_synced_at for existing records...")

        # Backfill comics that have prices but no synced_at
        # Note: comic_issues uses price_loose, price_cib, price_new, price_graded (not price_guide_value)
        result = await conn.execute(text("""
            UPDATE comic_issues
            SET pricecharting_synced_at = NOW() - INTERVAL '25 hours'
            WHERE pricecharting_id IS NOT NULL
              AND pricecharting_synced_at IS NULL
              AND (price_loose IS NOT NULL OR price_cib IS NOT NULL OR price_new IS NOT NULL)
        """))
        logger.info(f"Backfilled {result.rowcount} comic_issues with pricecharting_synced_at")

        # Backfill funkos that have prices but no synced_at
        result = await conn.execute(text("""
            UPDATE funkos
            SET pricecharting_synced_at = NOW() - INTERVAL '25 hours'
            WHERE pricecharting_id IS NOT NULL
              AND pricecharting_synced_at IS NULL
              AND (price_loose IS NOT NULL OR price_cib IS NOT NULL OR price_new IS NOT NULL)
        """))
        logger.info(f"Backfilled {result.rowcount} funkos with pricecharting_synced_at")

    logger.info("PriceCharting Incremental Sync migration complete!")


async def rollback_pricecharting_incremental_sync(engine):
    """
    Rollback the incremental sync schema changes.

    Per constitution_db.json Section 1:
    > "Every migration includes dry-run (shadow DB), rollback SQL"
    """
    logger.info("Rolling back PriceCharting Incremental Sync migration...")

    async with engine.begin() as conn:
        # Drop indexes first
        await conn.execute(text("""
            DROP INDEX IF EXISTS ix_comic_issues_pc_stale
        """))
        await conn.execute(text("""
            DROP INDEX IF EXISTS ix_funkos_pc_stale
        """))
        logger.info("Dropped partial indexes")

        # Remove columns
        await conn.execute(text("""
            ALTER TABLE comic_issues
            DROP COLUMN IF EXISTS pricecharting_synced_at
        """))
        await conn.execute(text("""
            ALTER TABLE funkos
            DROP COLUMN IF EXISTS pricecharting_synced_at
        """))
        logger.info("Removed pricecharting_synced_at columns")

    logger.info("PriceCharting Incremental Sync rollback complete!")


async def run_migration():
    """Run the migration using the app's database engine."""
    from app.core.database import engine

    await migrate_pricecharting_incremental_sync(engine)


if __name__ == "__main__":
    asyncio.run(run_migration())
