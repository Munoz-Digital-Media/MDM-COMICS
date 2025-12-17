"""
Database Migration Script for Bundle Builder Tool v1.0.0

Creates the necessary tables for product bundling:
- bundles: Bundle configuration and pricing
- bundle_items: Items within bundles
- bundle_cart_items: Bundles in user carts

Per constitution_db.json:
- DB-001: Numeric(12,2) for monetary fields
- DB-003: FK with appropriate ON DELETE
- DB-004: Indexes on FKs and query columns
- DB-005: Audit columns
- DB-006: CHECK constraints for data integrity

Run this migration after deploying the bundle models.
"""
import asyncio
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


async def migrate_bundle_tables(engine):
    """
    Create bundle tables if they don't exist.

    This is an idempotent migration - safe to run multiple times.
    """
    logger.info("Starting bundle tables migration...")

    async with engine.begin() as conn:
        # ==================== bundles table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bundles (
                id SERIAL PRIMARY KEY,

                -- Identification
                sku VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                slug VARCHAR(255) UNIQUE NOT NULL,

                -- Description
                short_description VARCHAR(500),
                description TEXT,

                -- Pricing - DB-001: Numeric(12,2)
                bundle_price NUMERIC(12, 2) NOT NULL,
                compare_at_price NUMERIC(12, 2),
                cost NUMERIC(12, 2),

                -- Calculated fields
                savings_amount NUMERIC(12, 2),
                savings_percent NUMERIC(5, 2),
                margin_percent NUMERIC(5, 2),

                -- Status
                status VARCHAR(20) NOT NULL DEFAULT 'DRAFT',

                -- Inventory
                available_qty INTEGER DEFAULT 0,

                -- Display
                image_url VARCHAR(500),
                images JSONB DEFAULT '[]'::jsonb,
                badge_text VARCHAR(50),
                display_order INTEGER DEFAULT 0,

                -- Categorization
                category VARCHAR(100),
                tags JSONB DEFAULT '[]'::jsonb,

                -- Validity period
                start_date TIMESTAMP WITH TIME ZONE,
                end_date TIMESTAMP WITH TIME ZONE,

                -- Timestamps
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                published_at TIMESTAMP WITH TIME ZONE,

                -- DB-005: Audit columns
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,

                -- DB-006: Check constraints
                CONSTRAINT check_bundle_price_positive CHECK (bundle_price > 0),
                CONSTRAINT check_bundle_status CHECK (status IN ('DRAFT', 'ACTIVE', 'INACTIVE', 'ARCHIVED')),
                CONSTRAINT check_savings_percent CHECK (savings_percent IS NULL OR savings_percent >= 0),
                CONSTRAINT check_available_qty CHECK (available_qty >= 0)
            )
        """))
        logger.info("Created/verified bundles table")

        # Bundle indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bundles_sku ON bundles(sku)",
            "CREATE INDEX IF NOT EXISTS ix_bundles_slug ON bundles(slug)",
            "CREATE INDEX IF NOT EXISTS ix_bundles_name ON bundles(name)",
            "CREATE INDEX IF NOT EXISTS ix_bundles_status ON bundles(status)",
            "CREATE INDEX IF NOT EXISTS ix_bundles_category ON bundles(category)",
            "CREATE INDEX IF NOT EXISTS ix_bundles_display_order ON bundles(display_order)",
            "CREATE INDEX IF NOT EXISTS ix_bundles_created_at ON bundles(created_at)",
            "CREATE INDEX IF NOT EXISTS ix_bundles_status_category ON bundles(status, category)",
        ]:
            await conn.execute(text(idx_sql))

        # Partial index for active bundles
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bundles_active
            ON bundles(status, display_order)
            WHERE status = 'ACTIVE'
        """))
        logger.info("Created bundle indexes")

        # ==================== bundle_items table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bundle_items (
                id SERIAL PRIMARY KEY,

                -- Links
                bundle_id INTEGER NOT NULL REFERENCES bundles(id) ON DELETE CASCADE,
                product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,

                -- BCW mapping for dropship items
                bcw_mapping_id INTEGER REFERENCES bcw_product_mappings(id) ON DELETE SET NULL,

                -- Quantity
                quantity INTEGER NOT NULL DEFAULT 1,

                -- Pricing (denormalized for stability)
                unit_price NUMERIC(12, 2),
                unit_cost NUMERIC(12, 2),
                line_price NUMERIC(12, 2),
                line_cost NUMERIC(12, 2),

                -- Display
                display_order INTEGER DEFAULT 0,
                is_featured BOOLEAN DEFAULT FALSE,
                custom_label VARCHAR(100),

                -- Item-specific options
                options JSONB DEFAULT '{}'::jsonb,

                -- Timestamps
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                -- DB-006: Check constraints
                CONSTRAINT check_item_quantity_positive CHECK (quantity > 0),
                CONSTRAINT check_item_unit_price CHECK (unit_price IS NULL OR unit_price >= 0),
                CONSTRAINT check_item_unit_cost CHECK (unit_cost IS NULL OR unit_cost >= 0)
            )
        """))
        logger.info("Created/verified bundle_items table")

        # Bundle item indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bundle_items_bundle_id ON bundle_items(bundle_id)",
            "CREATE INDEX IF NOT EXISTS ix_bundle_items_product_id ON bundle_items(product_id)",
            "CREATE INDEX IF NOT EXISTS ix_bundle_items_bcw_mapping ON bundle_items(bcw_mapping_id)",
            "CREATE INDEX IF NOT EXISTS ix_bundle_items_display_order ON bundle_items(bundle_id, display_order)",
        ]:
            await conn.execute(text(idx_sql))

        # Unique constraint for bundle + product + options combination
        await conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_bundle_product_options
            ON bundle_items(bundle_id, product_id, options)
        """))
        logger.info("Created bundle_items indexes")

        # ==================== bundle_cart_items table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bundle_cart_items (
                id SERIAL PRIMARY KEY,

                -- Links
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                bundle_id INTEGER NOT NULL REFERENCES bundles(id) ON DELETE CASCADE,

                -- Quantity
                quantity INTEGER NOT NULL DEFAULT 1,

                -- Timestamp
                added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                -- Price snapshot at time of add (for price change detection)
                price_snapshot NUMERIC(12, 2),

                -- DB-006: Check constraints
                CONSTRAINT check_cart_quantity_positive CHECK (quantity > 0)
            )
        """))
        logger.info("Created/verified bundle_cart_items table")

        # Bundle cart item indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bundle_cart_user_id ON bundle_cart_items(user_id)",
            "CREATE INDEX IF NOT EXISTS ix_bundle_cart_bundle_id ON bundle_cart_items(bundle_id)",
        ]:
            await conn.execute(text(idx_sql))

        # Unique constraint for user + bundle
        await conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ix_bundle_cart_user_bundle
            ON bundle_cart_items(user_id, bundle_id)
        """))
        logger.info("Created bundle_cart_items indexes")

    logger.info("Bundle tables migration complete!")


async def run_migration():
    """Run the migration using the app's database engine."""
    from app.core.database import engine

    await migrate_bundle_tables(engine)


if __name__ == "__main__":
    asyncio.run(run_migration())
