"""
Admin API Routes for Site Settings Management

v1.0.0: CRUD operations for site-wide settings (branding URLs, feature flags, etc.)

Per constitution_cyberSec.json Section 3:
- All admin endpoints require is_admin=True
- CSRF protection on mutations
- Input validation
"""
import logging
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.core.database import get_db
from app.api.deps import get_current_admin
from app.models.user import User
from app.models.site_settings import SiteSettings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/settings", tags=["admin-settings"])


# ----- Pydantic Schemas -----

class SettingCreateRequest(BaseModel):
    key: str = Field(..., min_length=1, max_length=100, pattern="^[a-z][a-z0-9_]*$")
    value: str
    value_type: str = Field(default="string", pattern="^(string|url|json|boolean|number)$")
    category: str = Field(default="general", pattern="^(branding|newsletter|social|system|general)$")
    description: Optional[str] = None


class SettingUpdateRequest(BaseModel):
    value: str
    description: Optional[str] = None


class BulkUpdateRequest(BaseModel):
    settings: dict[str, str]  # key -> value mapping


# ----- Endpoints -----

@router.get("/")
async def list_settings(
    category: Optional[str] = Query(None, pattern="^(branding|newsletter|social|system|general)$"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """List all site settings, optionally filtered by category."""
    query = select(SiteSettings).order_by(SiteSettings.category, SiteSettings.key)

    if category:
        query = query.where(SiteSettings.category == category)

    # Get total count
    count_query = select(func.count(SiteSettings.id))
    if category:
        count_query = count_query.where(SiteSettings.category == category)
    total_result = await db.execute(count_query)
    total = total_result.scalar()

    # Get items
    query = query.offset(offset).limit(limit)
    result = await db.execute(query)
    settings = result.scalars().all()

    # Group by category
    by_category: dict[str, list] = {}
    for s in settings:
        cat = s.category or 'general'
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append({
            "id": s.id,
            "key": s.key,
            "value": s.value,
            "value_type": s.value_type,
            "description": s.description,
            "brand_asset_id": s.brand_asset_id,
            "updated_at": s.updated_at.isoformat() if s.updated_at else None
        })

    return {
        "settings": [
            {
                "id": s.id,
                "key": s.key,
                "value": s.value,
                "value_type": s.value_type,
                "category": s.category,
                "description": s.description,
                "brand_asset_id": s.brand_asset_id,
                "updated_at": s.updated_at.isoformat() if s.updated_at else None
            }
            for s in settings
        ],
        "by_category": by_category,
        "total": total
    }


@router.get("/{key}")
async def get_setting(
    key: str,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get a single setting by key."""
    result = await db.execute(
        select(SiteSettings).where(SiteSettings.key == key)
    )
    setting = result.scalar_one_or_none()

    if not setting:
        raise HTTPException(status_code=404, detail=f"Setting '{key}' not found")

    return {
        "id": setting.id,
        "key": setting.key,
        "value": setting.value,
        "value_type": setting.value_type,
        "category": setting.category,
        "description": setting.description,
        "brand_asset_id": setting.brand_asset_id,
        "updated_at": setting.updated_at.isoformat() if setting.updated_at else None
    }


@router.post("", status_code=201)
async def create_setting(
    request: SettingCreateRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Create a new site setting."""
    # Check if key already exists
    existing = await db.execute(
        select(SiteSettings).where(SiteSettings.key == request.key)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail=f"Setting '{request.key}' already exists")

    setting = SiteSettings(
        key=request.key,
        value=request.value,
        value_type=request.value_type,
        category=request.category,
        description=request.description,
        updated_by=current_user.id
    )
    db.add(setting)
    await db.commit()
    await db.refresh(setting)

    logger.info(f"Created setting {request.key} by user {current_user.id}")

    return {
        "id": setting.id,
        "key": setting.key,
        "value": setting.value,
        "value_type": setting.value_type,
        "category": setting.category
    }


@router.put("/{key}")
async def update_setting(
    key: str,
    request: SettingUpdateRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Update an existing site setting."""
    result = await db.execute(
        select(SiteSettings).where(SiteSettings.key == key)
    )
    setting = result.scalar_one_or_none()

    if not setting:
        raise HTTPException(status_code=404, detail=f"Setting '{key}' not found")

    old_value = setting.value
    setting.value = request.value
    if request.description is not None:
        setting.description = request.description
    setting.updated_at = datetime.now(timezone.utc)
    setting.updated_by = current_user.id

    await db.commit()

    logger.info(f"Updated setting {key}: '{old_value[:50]}...' -> '{request.value[:50]}...' by user {current_user.id}")

    return {
        "key": key,
        "value": setting.value,
        "updated_at": setting.updated_at.isoformat()
    }


@router.delete("/{key}")
async def delete_setting(
    key: str,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Delete a site setting."""
    result = await db.execute(
        select(SiteSettings).where(SiteSettings.key == key)
    )
    setting = result.scalar_one_or_none()

    if not setting:
        raise HTTPException(status_code=404, detail=f"Setting '{key}' not found")

    await db.delete(setting)
    await db.commit()

    logger.info(f"Deleted setting {key} by user {current_user.id}")

    return {"status": "deleted", "key": key}


@router.post("/bulk")
async def bulk_update_settings(
    request: BulkUpdateRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Update multiple settings at once."""
    updated = 0
    errors = []

    for key, value in request.settings.items():
        result = await db.execute(
            select(SiteSettings).where(SiteSettings.key == key)
        )
        setting = result.scalar_one_or_none()

        if not setting:
            errors.append(f"Setting '{key}' not found")
            continue

        setting.value = value
        setting.updated_at = datetime.now(timezone.utc)
        setting.updated_by = current_user.id
        updated += 1

    await db.commit()

    logger.info(f"Bulk updated {updated} settings by user {current_user.id}")

    return {
        "updated": updated,
        "errors": errors if errors else None
    }


# ----- Public endpoint for frontend -----

@router.get("/public/branding")
async def get_public_branding(
    db: AsyncSession = Depends(get_db)
):
    """
    Get public branding settings (no auth required).

    This endpoint is for the frontend to fetch logo URLs, etc.
    """
    result = await db.execute(
        select(SiteSettings).where(SiteSettings.category == "branding")
    )
    settings = result.scalars().all()

    return {
        key: val
        for s in settings
        for key, val in [(s.key, s.value)]
        if s.value  # Only include non-empty values
    }
