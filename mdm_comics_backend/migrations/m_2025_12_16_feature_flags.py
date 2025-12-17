"""
Migration: Feature Flags Table v1.0.0

Per 20251216_shipping_compartmentalization_proposal.json:
- Create feature_flags table for runtime carrier toggles
- Seed initial flags (ups:enabled, usps:disabled)
- No deployment required for toggling

Per constitution_binder.json:
- snake_case for all columns
- Audit trail columns (disabled_by, disabled_at, disabled_reason)
- Indexes for lookup performance

Rollback SQL:
    DROP TABLE IF EXISTS feature_flags;
"""
import asyncio
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

from sqlalchemy import text

# Ensure app modules are importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_feature_flags_table() -> None:
    """
    Create the feature_flags table.

    Rollback:
        DROP TABLE IF EXISTS feature_flags;
    """
    async with AsyncSessionLocal() as db:
        try:
            logger.info("Creating feature_flags table...")

            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS feature_flags (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    module VARCHAR(50) NOT NULL,
                    feature VARCHAR(50) NOT NULL,
                    is_enabled BOOLEAN NOT NULL DEFAULT true,
                    config_json JSONB NOT NULL DEFAULT '{}',
                    disabled_reason TEXT,
                    disabled_at TIMESTAMPTZ,
                    disabled_by VARCHAR(100),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_feature_flags_module_feature UNIQUE (module, feature)
                );
            """))

            # Create indexes
            logger.info("Creating indexes on feature_flags...")

            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_feature_flags_lookup
                ON feature_flags(module, feature);
            """))

            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_feature_flags_enabled
                ON feature_flags(module, is_enabled);
            """))

            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_feature_flags_module
                ON feature_flags(module);
            """))

            await db.commit()
            logger.info("feature_flags table created successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to create feature_flags table")
            raise


async def seed_shipping_carrier_flags() -> None:
    """
    Seed initial shipping carrier flags.

    Per proposal:
    - ups: enabled (existing carrier)
    - usps: disabled (pending USPS API enrollment)
    """
    async with AsyncSessionLocal() as db:
        try:
            # Check if flags already exist
            result = await db.execute(text("""
                SELECT COUNT(*) FROM feature_flags WHERE module = 'shipping'
            """))
            count = result.scalar()

            if count > 0:
                logger.info(f"Shipping carrier flags already seeded ({count} found), skipping...")
                return

            logger.info("Seeding shipping carrier flags...")

            # UPS - enabled
            await db.execute(text("""
                INSERT INTO feature_flags (id, module, feature, is_enabled, config_json, created_at, updated_at)
                VALUES (
                    gen_random_uuid(),
                    'shipping',
                    'ups',
                    true,
                    '{"sandbox_mode": false}'::jsonb,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (module, feature) DO NOTHING;
            """))

            # USPS - disabled (pending API enrollment)
            await db.execute(text("""
                INSERT INTO feature_flags (
                    id, module, feature, is_enabled, config_json,
                    disabled_reason, disabled_at, disabled_by,
                    created_at, updated_at
                )
                VALUES (
                    gen_random_uuid(),
                    'shipping',
                    'usps',
                    false,
                    '{"sandbox_mode": true}'::jsonb,
                    'Pending USPS Ship API enrollment',
                    NOW(),
                    'system',
                    NOW(),
                    NOW()
                )
                ON CONFLICT (module, feature) DO NOTHING;
            """))

            await db.commit()
            logger.info("Shipping carrier flags seeded successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to seed shipping carrier flags")
            raise


async def add_updated_at_trigger() -> None:
    """
    Create trigger to auto-update updated_at column.
    """
    async with AsyncSessionLocal() as db:
        try:
            logger.info("Creating updated_at trigger for feature_flags...")

            # Create trigger function if not exists
            await db.execute(text("""
                CREATE OR REPLACE FUNCTION update_feature_flags_updated_at()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """))

            # Create trigger
            await db.execute(text("""
                DROP TRIGGER IF EXISTS trigger_feature_flags_updated_at ON feature_flags;
                CREATE TRIGGER trigger_feature_flags_updated_at
                    BEFORE UPDATE ON feature_flags
                    FOR EACH ROW
                    EXECUTE FUNCTION update_feature_flags_updated_at();
            """))

            await db.commit()
            logger.info("updated_at trigger created successfully")

        except Exception:
            await db.rollback()
            logger.exception("Failed to create updated_at trigger")
            raise


async def verify_migration() -> None:
    """
    Verify the migration was successful.
    """
    async with AsyncSessionLocal() as db:
        try:
            # Check table exists
            result = await db.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'feature_flags'
                );
            """))
            table_exists = result.scalar()

            if not table_exists:
                raise Exception("feature_flags table does not exist")

            # Check flags were seeded
            result = await db.execute(text("""
                SELECT module, feature, is_enabled FROM feature_flags
                WHERE module = 'shipping'
                ORDER BY feature;
            """))
            flags = result.fetchall()

            logger.info("Feature flags migration verified:")
            for flag in flags:
                logger.info(f"  - {flag[0]}:{flag[1]} = {'enabled' if flag[2] else 'disabled'}")

            if len(flags) < 2:
                logger.warning("Expected at least 2 shipping carrier flags, found less")

        except Exception:
            logger.exception("Migration verification failed")
            raise


async def main():
    """
    Run the feature flags migration.
    """
    logger.info("Starting feature flags migration...")

    await create_feature_flags_table()
    await add_updated_at_trigger()
    await seed_shipping_carrier_flags()
    await verify_migration()

    logger.info("Feature flags migration complete!")


if __name__ == "__main__":
    asyncio.run(main())
