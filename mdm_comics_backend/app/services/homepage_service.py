"""
Homepage Service
Business logic for homepage section configuration.
"""
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.site_settings import SiteSettings
from app.schemas.homepage import (
    HomepageSectionConfig,
    HomepageSectionsResponse,
    HomepageSectionsUpdateRequest
)
from app.core.redis_client import (
    get_homepage_sections_cached,
    set_homepage_sections_cached,
    invalidate_homepage_cache
)

logger = logging.getLogger(__name__)

DEFAULT_SECTIONS = [
    {"key": "bagged-boarded", "title": "Bagged & Boarded Books", "emoji": "📚", "visible": True, "display_order": 1, "max_items": 5, "category_link": "/shop/bagged-boarded", "data_source": "products"},
    {"key": "graded", "title": "Graded Books", "emoji": "🏆", "visible": True, "display_order": 2, "max_items": 5, "category_link": "/shop/graded", "data_source": "products"},
    {"key": "funko", "title": "Funko POPs", "emoji": "🎭", "visible": True, "display_order": 3, "max_items": 5, "category_link": "/shop/funko", "data_source": "products"},
    {"key": "supplies", "title": "Supplies", "emoji": "📦", "visible": True, "display_order": 4, "max_items": 5, "category_link": "/shop/supplies", "data_source": "products"},
    {"key": "bundles", "title": "Bundles", "emoji": "🎁", "visible": True, "display_order": 5, "max_items": 5, "category_link": "/shop/bundles", "data_source": "bundles"}
]

HOMEPAGE_SECTIONS_KEY = "homepage_sections"

class HomepageService:
    @staticmethod
    def get_default_sections() -> List[Dict[str, Any]]:
        """Return the default section configuration."""
        return DEFAULT_SECTIONS

    @staticmethod
    async def get_sections(db: AsyncSession) -> HomepageSectionsResponse:
        """Fetch homepage sections from database."""
        # Try cache first
        cached = await get_homepage_sections_cached()
        if cached:
            return HomepageSectionsResponse(**cached)

        result = await db.execute(
            select(SiteSettings).where(SiteSettings.key == HOMEPAGE_SECTIONS_KEY)
        )
        setting = result.scalar_one_or_none()

        if not setting:
            # Return defaults if not configured
            sections = [HomepageSectionConfig(**s) for s in DEFAULT_SECTIONS]
            response = HomepageSectionsResponse(sections=sections, updated_at=datetime.now(timezone.utc).isoformat())
            await set_homepage_sections_cached(response.model_dump())
            return response

        try:
            data = json.loads(setting.value)
            sections_data = data.get("sections", DEFAULT_SECTIONS)
            # Ensure all default sections exist (handle migration/schema drift)
            current_keys = {s["key"] for s in sections_data}
            for default in DEFAULT_SECTIONS:
                if default["key"] not in current_keys:
                    sections_data.append(default)
            
            sections = [HomepageSectionConfig(**s) for s in sections_data]
            # Sort by display order
            sections.sort(key=lambda x: x.display_order)
            
            response = HomepageSectionsResponse(
                sections=sections,
                updated_at=setting.updated_at.isoformat() if setting.updated_at else None
            )
            await set_homepage_sections_cached(response.model_dump())
            return response
        except Exception as e:
            logger.error(f"Failed to parse homepage sections: {e}")
            # Fallback to defaults
            sections = [HomepageSectionConfig(**s) for s in DEFAULT_SECTIONS]
            return HomepageSectionsResponse(sections=sections)

    @staticmethod
    async def update_sections(
        db: AsyncSession, 
        update_request: HomepageSectionsUpdateRequest,
        user_id: int
    ) -> HomepageSectionsResponse:
        """Update homepage section configuration."""
        # Get current config first
        current_response = await HomepageService.get_sections(db)
        current_sections = {s.key: s for s in current_response.sections}

        # Apply updates
        updates_map = {u.key: u for u in update_request.sections}
        
        updated_list = []
        for key, section in current_sections.items():
            if key in updates_map:
                update = updates_map[key]
                if update.visible is not None:
                    section.visible = update.visible
                if update.display_order is not None:
                    section.display_order = update.display_order
                if update.max_items is not None:
                    section.max_items = update.max_items
            updated_list.append(section)

        # Validate display_order uniqueness? 
        # Ideally yes, but let's just sort them. UI handles drag-drop.
        updated_list.sort(key=lambda x: x.display_order)

        # Persist to DB
        result = await db.execute(
            select(SiteSettings).where(SiteSettings.key == HOMEPAGE_SECTIONS_KEY)
        )
        setting = result.scalar_one_or_none()

        value_json = json.dumps({"sections": [s.model_dump() for s in updated_list]})

        if setting:
            setting.value = value_json
            setting.updated_by = user_id
            setting.updated_at = datetime.now(timezone.utc)
        else:
            setting = SiteSettings(
                key=HOMEPAGE_SECTIONS_KEY,
                value=value_json,
                value_type="json",
                category="homepage",
                description="Homepage section configuration",
                updated_by=user_id
            )
            db.add(setting)

        await db.commit()
        await db.refresh(setting)
        
        # Invalidate cache
        await invalidate_homepage_cache()

        return await HomepageService.get_sections(db)
