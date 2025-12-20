"""
Add selectors JSON column to bcw_config table.

Per 20251219_INTEGRATED_BCW_REMEDIATION_PROPOSAL.json WS-02.

This allows DOM selectors to be stored in the database for hot-patching
without code deployment when BCW website changes.

Schema:
    bcw_config.selectors JSONB - Stores selector overrides keyed by category.key

Example:
    {
        "login.username": "input#new-email-field",
        "cart.add_to_cart": "button.new-add-btn",
        "selector_version": "1.0.1",
        "updated_at": "2025-12-19T12:00:00Z"
    }
"""
import asyncio
import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import settings

logger = logging.getLogger(__name__)


async def run_migration():
    """Add selectors column to bcw_config table."""
    engine = create_async_engine(settings.DATABASE_URL)

    async with engine.begin() as conn:
        # Check if column already exists
        result = await conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bcw_config'
            AND column_name = 'selectors'
        """))

        if result.fetchone():
            logger.info("Column 'selectors' already exists in bcw_config")
            return

        # Add selectors column as JSONB
        logger.info("Adding 'selectors' column to bcw_config table...")

        await conn.execute(text("""
            ALTER TABLE bcw_config
            ADD COLUMN IF NOT EXISTS selectors JSONB DEFAULT '{}'::jsonb
        """))

        # Add comment for documentation
        await conn.execute(text("""
            COMMENT ON COLUMN bcw_config.selectors IS
            'Dynamic DOM selector overrides for BCW automation. Keys are category.selector_name, values are CSS/XPath selectors.'
        """))

        logger.info("Successfully added 'selectors' column to bcw_config")

    await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_migration())
