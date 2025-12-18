"""
BCW Search Indexes Migration v1.0.0

Adds search optimization indexes for BCW catalog search functionality.
Per 20251218_bcw_search_create_proposal_v1.1.md

Run: python -m app.migrations.bcw_search_indexes
"""
import asyncio
import logging
from sqlalchemy import text
from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)


async def migrate():
    """Add BCW search indexes for optimized catalog search."""
    print("=" * 60)
    print("BCW SEARCH INDEXES MIGRATION")
    print("=" * 60)

    async with AsyncSessionLocal() as db:
        try:
            # Add pricing columns if not present
            print("\nChecking bcw_product_mappings columns...")
            await db.execute(text("""
                ALTER TABLE bcw_product_mappings
                ADD COLUMN IF NOT EXISTS bcw_cost NUMERIC(12, 2),
                ADD COLUMN IF NOT EXISTS bcw_msrp NUMERIC(12, 2),
                ADD COLUMN IF NOT EXISTS our_price NUMERIC(12, 2),
                ADD COLUMN IF NOT EXISTS min_margin_percent NUMERIC(5, 2) DEFAULT 20.00
            """))
            print("  Pricing columns verified")

            # Full-text search index
            print("\nCreating full-text search index...")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_bcw_mappings_fts
                ON bcw_product_mappings
                USING gin(to_tsvector('english', product_name || ' ' || COALESCE(bcw_category, '')))
            """))
            print("  idx_bcw_mappings_fts created")

            # Category filter index
            print("Creating category index...")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_bcw_mappings_category
                ON bcw_product_mappings(bcw_category)
                WHERE is_active = true
            """))
            print("  idx_bcw_mappings_category created")

            # MDM SKU lookup index
            print("Creating MDM SKU index...")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_bcw_mappings_mdm_sku
                ON bcw_product_mappings(mdm_sku)
            """))
            print("  idx_bcw_mappings_mdm_sku created")

            # BCW SKU lookup index
            print("Creating BCW SKU index...")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_bcw_mappings_bcw_sku
                ON bcw_product_mappings(bcw_sku)
            """))
            print("  idx_bcw_mappings_bcw_sku created")

            # Active products index
            print("Creating active products index...")
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_bcw_mappings_active
                ON bcw_product_mappings(is_active)
                WHERE is_active = true
            """))
            print("  idx_bcw_mappings_active created")

            await db.commit()

            print("\n" + "=" * 60)
            print("MIGRATION COMPLETE")
            print("=" * 60)
            print("\nIndexes created successfully for BCW catalog search optimization.")

        except Exception as e:
            print(f"\nERROR: Migration failed: {e}")
            await db.rollback()
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(migrate())
