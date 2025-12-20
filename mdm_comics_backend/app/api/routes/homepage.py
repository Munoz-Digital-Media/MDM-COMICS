"""
Homepage API Routes
"""
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_current_admin
from app.models.user import User
from app.schemas.homepage import (
    HomepageSectionsResponse,
    HomepageSectionsUpdateRequest
)
from app.services.homepage_service import HomepageService

router = APIRouter()

@router.get("/homepage/sections", response_model=HomepageSectionsResponse)
async def get_homepage_sections(
    db: AsyncSession = Depends(get_db)
) -> Any:
    """
    Get homepage section configuration.
    Public endpoint.
    """
    return await HomepageService.get_sections(db)

@router.put("/admin/homepage/sections", response_model=HomepageSectionsResponse)
async def update_homepage_sections(
    update_data: HomepageSectionsUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
) -> Any:
    """
    Update homepage section configuration.
    Admin only.
    """
    return await HomepageService.update_sections(db, update_data, current_user.id)
