"""
Migration: Add default homepage sections configuration.
"""
import asyncio
import json
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import select
from app.core.database import AsyncSessionLocal
from app.models.site_settings import SiteSettings
from app.services.homepage_service import HomepageService, HOMEPAGE_SECTIONS_KEY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def migrate():
    async with AsyncSessionLocal() as db:
        logger.info("Checking for homepage sections configuration...")
        
        result = await db.execute(
            select(SiteSettings).where(SiteSettings.key == HOMEPAGE_SECTIONS_KEY)
        )
        setting = result.scalar_one_or_none()

        if setting:
            logger.info("Homepage sections configuration already exists.")
            return

        logger.info("Configuration missing. Seeding default values...")
        
        defaults = HomepageService.get_default_sections()
        value_json = json.dumps({"sections": defaults})

        new_setting = SiteSettings(
            key=HOMEPAGE_SECTIONS_KEY,
            value=value_json,
            value_type="json",
            category="homepage",
            description="Homepage section configuration",
            updated_by=None # System migration
        )
        db.add(new_setting)
        await db.commit()
        
        logger.info("Successfully seeded homepage sections configuration.")

if __name__ == "__main__":
    asyncio.run(migrate())
