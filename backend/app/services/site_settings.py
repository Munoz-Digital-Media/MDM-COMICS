"""
Site Settings Service

v1.0.0: Database-driven site configuration for branding, feature flags, etc.
"""
from typing import Dict, Optional
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.site_settings import SiteSettings

logger = logging.getLogger(__name__)


class SiteSettingsService:
    """
    Service for reading site settings from the database.

    Provides caching and fallback to config.py values.
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self._cache: Dict[str, str] = {}

    async def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a setting value by key.

        First checks cache, then database, then falls back to default.
        """
        # Check cache first
        if key in self._cache:
            return self._cache[key]

        try:
            result = await self.db.execute(
                select(SiteSettings).where(SiteSettings.key == key)
            )
            setting = result.scalar_one_or_none()

            if setting and setting.value:
                self._cache[key] = setting.value
                return setting.value

            return default

        except Exception as e:
            logger.warning(f"Error fetching setting {key}: {e}")
            return default

    async def get_branding_urls(self) -> Dict[str, str]:
        """
        Get all branding URLs for templates.

        Returns dict with common branding keys for use in templates.
        """
        try:
            result = await self.db.execute(
                select(SiteSettings).where(SiteSettings.category == "branding")
            )
            settings_list = result.scalars().all()

            branding = {}
            for s in settings_list:
                if s.value:  # Only include non-empty values
                    branding[s.key] = s.value
                    self._cache[s.key] = s.value

            return branding

        except Exception as e:
            logger.warning(f"Error fetching branding settings: {e}")
            return {}

    async def get_rack_factor_logo_url(self) -> Optional[str]:
        """
        Get The Rack Factor logo URL.

        Falls back to config if not set in database.
        """
        url = await self.get("rack_factor_logo_url")
        if url:
            return url

        # Fallback to config
        if hasattr(settings, 'RACK_FACTOR_LOGO_URL') and settings.RACK_FACTOR_LOGO_URL:
            return settings.RACK_FACTOR_LOGO_URL

        return None

    async def get_site_logo_url(self) -> Optional[str]:
        """Get main site logo URL."""
        return await self.get("site_logo_url")

    async def get_email_logo_url(self) -> Optional[str]:
        """Get email header logo URL."""
        return await self.get("email_header_logo_url")


async def get_branding_context(db: AsyncSession) -> Dict[str, str]:
    """
    Helper to get all branding context variables for templates.

    Use this when rendering newsletter templates:

        branding = await get_branding_context(db)
        template.render(**branding, **other_vars)
    """
    service = SiteSettingsService(db)
    branding = await service.get_branding_urls()

    # Map to common template variable names
    return {
        "logo_url": branding.get("rack_factor_logo_url", ""),
        "app_logo_url": branding.get("site_logo_url", ""),
        "site_logo_url": branding.get("site_logo_url", ""),
        "site_logo_dark_url": branding.get("site_logo_dark_url", ""),
        "favicon_url": branding.get("favicon_url", ""),
        "email_header_logo_url": branding.get("email_header_logo_url", ""),
        "og_image_url": branding.get("og_default_image_url", ""),
    }
