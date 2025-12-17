"""
Migration: Add credentials_json column to carriers table

Per 20251216_shipping_compartmentalization_proposal.json:
- Adds a carrier-agnostic encrypted credentials storage column
- Maintains backward compatibility with existing credential columns
- New carriers can use credentials_json for flexible credential storage

Usage:
    python -m migrations.2025_12_16_carrier_credentials_json
"""
import asyncio
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_migration():
    """Add credentials_json column to carriers table."""
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        try:
            # Check if column already exists
            check_column = text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'carriers'
                AND column_name = 'credentials_json'
            """)
            result = await session.execute(check_column)
            if result.fetchone():
                logger.info("credentials_json column already exists, skipping...")
                return

            # Add credentials_json column
            logger.info("Adding credentials_json column to carriers table...")
            await session.execute(text("""
                ALTER TABLE carriers
                ADD COLUMN credentials_json TEXT NULL
            """))
            logger.info("credentials_json column added")

            # Add comment for documentation
            await session.execute(text("""
                COMMENT ON COLUMN carriers.credentials_json IS
                'Fernet-encrypted JSON blob containing carrier-specific credentials.
                Format varies by carrier. Used for new carriers; existing carriers
                use dedicated credential columns for backward compatibility.'
            """))

            await session.commit()
            logger.info("Migration completed successfully!")

        except Exception as e:
            await session.rollback()
            logger.error(f"Migration failed: {e}")
            raise


async def rollback_migration():
    """Remove credentials_json column from carriers table."""
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        try:
            logger.info("Removing credentials_json column from carriers table...")
            await session.execute(text("""
                ALTER TABLE carriers
                DROP COLUMN IF EXISTS credentials_json
            """))
            await session.commit()
            logger.info("Rollback completed successfully!")

        except Exception as e:
            await session.rollback()
            logger.error(f"Rollback failed: {e}")
            raise


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Carrier credentials_json migration")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    args = parser.parse_args()

    if args.rollback:
        asyncio.run(rollback_migration())
    else:
        asyncio.run(run_migration())
