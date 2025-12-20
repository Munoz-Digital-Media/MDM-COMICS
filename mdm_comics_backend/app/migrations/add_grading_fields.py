"""
Migration: Add grading fields to products table.

Adds support for CGC certification with fields:
- grading_company: Enum (cgc, future: cbcs, pgx)
- certification_number: String (the cert number)
- grade_label: Enum (universal, signature, qualified, restored, conserved)
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import text
from app.core.database import AsyncSessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def migrate():
    async with AsyncSessionLocal() as db:
        logger.info("Adding grading fields to products table...")

        # Create grading_company enum type
        await db.execute(text("""
            DO $$ BEGIN
                CREATE TYPE gradingcompany AS ENUM ('cgc');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """))
        logger.info("Created gradingcompany enum (or already exists)")

        # Create grade_label enum type
        await db.execute(text("""
            DO $$ BEGIN
                CREATE TYPE gradelabel AS ENUM ('universal', 'signature', 'qualified', 'restored', 'conserved');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """))
        logger.info("Created gradelabel enum (or already exists)")

        # Add grading_company column if not exists
        await db.execute(text("""
            DO $$ BEGIN
                ALTER TABLE products ADD COLUMN grading_company gradingcompany;
            EXCEPTION
                WHEN duplicate_column THEN null;
            END $$;
        """))
        logger.info("Added grading_company column (or already exists)")

        # Add certification_number column if not exists
        await db.execute(text("""
            DO $$ BEGIN
                ALTER TABLE products ADD COLUMN certification_number VARCHAR(50);
            EXCEPTION
                WHEN duplicate_column THEN null;
            END $$;
        """))
        logger.info("Added certification_number column (or already exists)")

        # Add index on certification_number for lookups
        await db.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_products_certification_number
            ON products (certification_number)
            WHERE certification_number IS NOT NULL;
        """))
        logger.info("Created index on certification_number (or already exists)")

        # Add grade_label column if not exists
        await db.execute(text("""
            DO $$ BEGIN
                ALTER TABLE products ADD COLUMN grade_label gradelabel;
            EXCEPTION
                WHEN duplicate_column THEN null;
            END $$;
        """))
        logger.info("Added grade_label column (or already exists)")

        await db.commit()
        logger.info("Migration completed successfully!")


async def rollback():
    """Rollback migration (for development only)."""
    async with AsyncSessionLocal() as db:
        logger.info("Rolling back grading fields migration...")

        await db.execute(text("ALTER TABLE products DROP COLUMN IF EXISTS grading_company;"))
        await db.execute(text("ALTER TABLE products DROP COLUMN IF EXISTS certification_number;"))
        await db.execute(text("ALTER TABLE products DROP COLUMN IF EXISTS grade_label;"))
        await db.execute(text("DROP INDEX IF EXISTS ix_products_certification_number;"))
        await db.execute(text("DROP TYPE IF EXISTS gradingcompany;"))
        await db.execute(text("DROP TYPE IF EXISTS gradelabel;"))

        await db.commit()
        logger.info("Rollback completed!")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    args = parser.parse_args()

    if args.rollback:
        asyncio.run(rollback())
    else:
        asyncio.run(migrate())
