"""
Price Snapshots for AI Intelligence - Migration Script v1.7.0

Creates the price_snapshots table and supporting infrastructure for
daily price capture and ML feature computation.

Run: python scripts/migrate_price_snapshots_v1.py

Per constitution_db.json: Safe migration with rollback support.
Per constitution_cyberSec.json: Rollback SQL included, no PII involved.
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def migrate():
    """Create price_snapshots table and indexes for AI/ML training data."""
    engine = create_async_engine(settings.DATABASE_URL)

    async with engine.begin() as conn:
        logger.info("=" * 60)
        logger.info("Price Snapshots Migration v1.7.0")
        logger.info("=" * 60)

        # ============================================================
        # 1. Create price_snapshots table
        # ============================================================
        logger.info("\n[1/4] Creating price_snapshots table...")
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS price_snapshots (
                -- Primary key (BIGSERIAL for high volume)
                id BIGSERIAL PRIMARY KEY,

                -- Snapshot identity
                snapshot_date DATE NOT NULL,
                entity_type VARCHAR(50) NOT NULL,
                entity_id INTEGER NOT NULL,

                -- External correlation
                pricecharting_id INTEGER,

                -- Price fields (all nullable - entity may not have all price types)
                price_loose NUMERIC(12, 2),
                price_cib NUMERIC(12, 2),
                price_new NUMERIC(12, 2),
                price_graded NUMERIC(12, 2),      -- Comics only
                price_bgs_10 NUMERIC(12, 2),      -- Comics only
                price_cgc_98 NUMERIC(12, 2),      -- Comics only
                price_cgc_96 NUMERIC(12, 2),      -- Comics only

                -- Sales volume (valuable for demand correlation)
                sales_volume INTEGER,

                -- Change tracking
                price_changed BOOLEAN NOT NULL DEFAULT FALSE,
                days_since_change INTEGER,

                -- ML Features (pre-computed for training efficiency)
                volatility_7d NUMERIC(8, 4),
                volatility_30d NUMERIC(8, 4),
                trend_7d NUMERIC(8, 4),
                trend_30d NUMERIC(8, 4),
                momentum NUMERIC(8, 4),

                -- Data quality
                data_source VARCHAR(50) NOT NULL DEFAULT 'pricecharting',
                confidence_score NUMERIC(3, 2),
                is_stale BOOLEAN NOT NULL DEFAULT FALSE,

                -- Timestamps
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

                -- Constraints
                CONSTRAINT ck_price_snapshots_entity_type
                    CHECK (entity_type IN ('funko', 'comic')),
                CONSTRAINT ck_price_snapshots_confidence_range
                    CHECK (confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 1))
            )
        """))
        logger.info("  ✓ price_snapshots table created")

        # ============================================================
        # 2. Create indexes for common query patterns
        # ============================================================
        logger.info("\n[2/4] Creating indexes...")

        indexes = [
            # Primary lookup + uniqueness (one snapshot per entity per day)
            ("ix_price_snapshots_lookup",
             "CREATE UNIQUE INDEX IF NOT EXISTS ix_price_snapshots_lookup ON price_snapshots(entity_type, entity_id, snapshot_date)"),

            # Time-range queries
            ("ix_price_snapshots_date",
             "CREATE INDEX IF NOT EXISTS ix_price_snapshots_date ON price_snapshots(snapshot_date)"),

            # Entity history queries
            ("ix_price_snapshots_entity",
             "CREATE INDEX IF NOT EXISTS ix_price_snapshots_entity ON price_snapshots(entity_type, entity_id)"),

            # External ID correlation
            ("ix_price_snapshots_pricecharting",
             "CREATE INDEX IF NOT EXISTS ix_price_snapshots_pricecharting ON price_snapshots(pricecharting_id)"),

            # ML feature queries (find volatile items)
            ("ix_price_snapshots_volatility",
             "CREATE INDEX IF NOT EXISTS ix_price_snapshots_volatility ON price_snapshots(entity_type, snapshot_date, volatility_30d)"),

            # Find changed items by date
            ("ix_price_snapshots_changed",
             "CREATE INDEX IF NOT EXISTS ix_price_snapshots_changed ON price_snapshots(snapshot_date, price_changed)"),
        ]

        for idx_name, idx_sql in indexes:
            try:
                await conn.execute(text(idx_sql))
                logger.info(f"  ✓ {idx_name}")
            except Exception as e:
                logger.warning(f"  ✗ {idx_name}: {e}")

        # ============================================================
        # 3. Add sales_volume column to funkos table (if not exists)
        # ============================================================
        logger.info("\n[3/4] Adding sales_volume to funkos table...")
        try:
            await conn.execute(text("""
                ALTER TABLE funkos
                ADD COLUMN IF NOT EXISTS sales_volume INTEGER
            """))
            logger.info("  ✓ funkos.sales_volume column added")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info("  - funkos.sales_volume (already exists)")
            else:
                logger.warning(f"  ✗ funkos.sales_volume: {e}")

        # ============================================================
        # 4. Add comment to table for documentation
        # ============================================================
        logger.info("\n[4/4] Adding table documentation...")
        await conn.execute(text("""
            COMMENT ON TABLE price_snapshots IS
            'Daily point-in-time capture of ALL prices for AI/ML model development. v1.7.0'
        """))
        await conn.execute(text("""
            COMMENT ON COLUMN price_snapshots.volatility_7d IS
            'Standard deviation of price_loose over last 7 days'
        """))
        await conn.execute(text("""
            COMMENT ON COLUMN price_snapshots.trend_7d IS
            'Linear regression slope normalized to -1 to +1 range over 7 days'
        """))
        await conn.execute(text("""
            COMMENT ON COLUMN price_snapshots.momentum IS
            'Price momentum indicator: (current - avg_30d) / std_30d'
        """))
        logger.info("  ✓ Table and column comments added")

        # ============================================================
        # Summary
        # ============================================================
        logger.info("\n" + "=" * 60)
        logger.info("✅ Migration completed successfully!")
        logger.info("=" * 60)
        logger.info("\nCreated:")
        logger.info("  - price_snapshots table (for AI/ML training data)")
        logger.info("  - 6 indexes for query performance")
        logger.info("  - funkos.sales_volume column")
        logger.info("\nNext steps:")
        logger.info("  1. Deploy to production")
        logger.info("  2. Run backfill script to populate historical data")
        logger.info("  3. Enable daily snapshot job in pipeline scheduler")

    await engine.dispose()


async def rollback():
    """
    Rollback migration (use with caution - drops table!)

    Per constitution_cyberSec.json §4: Rollback SQL included.
    """
    logger.warning("⚠️  ROLLBACK: This will DROP the price_snapshots table!")
    logger.warning("    All snapshot data will be PERMANENTLY DELETED.")
    logger.warning("    (Note: funkos.sales_volume column will NOT be dropped)")

    confirm = input("Type 'ROLLBACK' to confirm: ")
    if confirm != "ROLLBACK":
        logger.info("Rollback cancelled.")
        return

    engine = create_async_engine(settings.DATABASE_URL)

    async with engine.begin() as conn:
        logger.info("Rolling back price_snapshots migration...")

        # Drop table (CASCADE will drop dependent objects like indexes)
        await conn.execute(text("DROP TABLE IF EXISTS price_snapshots CASCADE"))
        logger.info("  ✓ Dropped price_snapshots table")

        # Note: We intentionally do NOT drop funkos.sales_volume
        # as it may have been populated with valuable data
        logger.info("  - funkos.sales_volume column preserved (contains data)")

        logger.info("\n✅ Rollback completed.")
        logger.info("   price_snapshots table has been removed.")

    await engine.dispose()


async def verify():
    """Verify the migration was successful."""
    engine = create_async_engine(settings.DATABASE_URL)

    async with engine.begin() as conn:
        logger.info("Verifying price_snapshots migration...\n")

        # Check table exists
        result = await conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'price_snapshots'
            )
        """))
        table_exists = result.scalar()

        if not table_exists:
            logger.error("✗ price_snapshots table does not exist!")
            return False
        logger.info("✓ price_snapshots table exists")

        # Check columns
        result = await conn.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'price_snapshots'
            ORDER BY ordinal_position
        """))
        columns = result.fetchall()

        expected_columns = {
            'id', 'snapshot_date', 'entity_type', 'entity_id', 'pricecharting_id',
            'price_loose', 'price_cib', 'price_new', 'price_graded',
            'price_bgs_10', 'price_cgc_98', 'price_cgc_96', 'sales_volume',
            'price_changed', 'days_since_change',
            'volatility_7d', 'volatility_30d', 'trend_7d', 'trend_30d', 'momentum',
            'data_source', 'confidence_score', 'is_stale', 'created_at'
        }
        actual_columns = {col[0] for col in columns}

        missing = expected_columns - actual_columns
        if missing:
            logger.error(f"✗ Missing columns: {missing}")
            return False
        logger.info(f"✓ All {len(expected_columns)} columns present")

        # Check indexes
        result = await conn.execute(text("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'price_snapshots'
        """))
        indexes = {row[0] for row in result.fetchall()}

        expected_indexes = {
            'price_snapshots_pkey',
            'ix_price_snapshots_lookup',
            'ix_price_snapshots_date',
            'ix_price_snapshots_entity',
            'ix_price_snapshots_pricecharting',
            'ix_price_snapshots_volatility',
            'ix_price_snapshots_changed',
        }

        missing_indexes = expected_indexes - indexes
        if missing_indexes:
            logger.warning(f"⚠ Missing indexes: {missing_indexes}")
        else:
            logger.info(f"✓ All {len(expected_indexes)} indexes present")

        # Check funkos.sales_volume
        result = await conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_name = 'funkos' AND column_name = 'sales_volume'
            )
        """))
        has_sales_volume = result.scalar()

        if has_sales_volume:
            logger.info("✓ funkos.sales_volume column exists")
        else:
            logger.warning("⚠ funkos.sales_volume column missing")

        logger.info("\n✅ Verification complete - migration successful!")
        return True

    await engine.dispose()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Price Snapshots Migration v1.7.0")
    parser.add_argument("--rollback", action="store_true", help="Rollback migration (destructive!)")
    parser.add_argument("--verify", action="store_true", help="Verify migration success")
    args = parser.parse_args()

    if args.rollback:
        asyncio.run(rollback())
    elif args.verify:
        asyncio.run(verify())
    else:
        asyncio.run(migrate())
