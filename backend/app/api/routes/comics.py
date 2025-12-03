"""
Comic search and lookup routes using Metron API
All searches are cached to local database for data capture.
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.services.comic_cache import comic_cache
from app.services.metron import metron_service

router = APIRouter(prefix="/comics", tags=["comics"])


@router.get("/search")
async def search_comics(
    request: Request,
    series: Optional[str] = Query(None, description="Series name (e.g., 'amazing spider-man')"),
    number: Optional[str] = Query(None, description="Issue number (e.g., '300')"),
    publisher: Optional[str] = Query(None, description="Publisher name (e.g., 'marvel')"),
    year: Optional[int] = Query(None, description="Cover year"),
    page: int = Query(1, ge=1, description="Page number"),
    db: AsyncSession = Depends(get_db)
):
    """
    Search for comic issues in the Metron database.
    All results are cached locally for future use.
    """
    try:
        ip_address = request.client.host if request.client else None

        results = await comic_cache.search_issues(
            db=db,
            series_name=series,
            number=number,
            publisher_name=publisher,
            cover_year=year,
            page=page,
            ip_address=ip_address
        )
        return results
    except Exception as e:
        # Fallback to direct Metron call if caching fails
        try:
            results = await metron_service.search_issues(
                series_name=series,
                number=number,
                publisher_name=publisher,
                cover_year=year,
                page=page
            )
            return results
        except Exception as e2:
            raise HTTPException(status_code=500, detail=f"Error searching comics: {str(e2)}")


@router.get("/issue/{issue_id}")
async def get_issue(
    issue_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """
    Get detailed information about a specific comic issue.
    Full data including credits, characters, etc. is cached locally.
    """
    try:
        ip_address = request.client.host if request.client else None

        result = await comic_cache.get_issue_detail(
            db=db,
            issue_id=issue_id,
            ip_address=ip_address
        )
        return result
    except Exception as e:
        try:
            result = await metron_service.get_issue(issue_id)
            return result
        except Exception as e2:
            raise HTTPException(status_code=500, detail=f"Error fetching issue: {str(e2)}")


@router.get("/series")
async def search_series(
    request: Request,
    name: Optional[str] = Query(None, description="Series name"),
    publisher: Optional[str] = Query(None, description="Publisher name"),
    year: Optional[int] = Query(None, description="Year series began"),
    page: int = Query(1, ge=1, description="Page number"),
    db: AsyncSession = Depends(get_db)
):
    """
    Search for comic series. Results are cached locally.
    """
    try:
        ip_address = request.client.host if request.client else None

        results = await comic_cache.search_series(
            db=db,
            name=name,
            publisher_name=publisher,
            year_began=year,
            page=page,
            ip_address=ip_address
        )
        return results
    except Exception as e:
        try:
            results = await metron_service.search_series(
                name=name,
                publisher_name=publisher,
                year_began=year,
                page=page
            )
            return results
        except Exception as e2:
            raise HTTPException(status_code=500, detail=f"Error searching series: {str(e2)}")


@router.get("/series/{series_id}")
async def get_series(series_id: int):
    """Get detailed information about a specific series."""
    try:
        result = await metron_service.get_series(series_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching series: {str(e)}")


@router.get("/publishers")
async def get_publishers(
    request: Request,
    page: int = Query(1, ge=1),
    db: AsyncSession = Depends(get_db)
):
    """Get list of comic publishers. All are cached locally."""
    try:
        ip_address = request.client.host if request.client else None

        results = await comic_cache.get_publishers(
            db=db,
            page=page,
            ip_address=ip_address
        )
        return results
    except Exception as e:
        try:
            results = await metron_service.get_publishers(page=page)
            return results
        except Exception as e2:
            raise HTTPException(status_code=500, detail=f"Error fetching publishers: {str(e2)}")


@router.get("/characters")
async def search_characters(
    name: Optional[str] = Query(None, description="Character name"),
    page: int = Query(1, ge=1)
):
    """Search for characters in the Metron database."""
    try:
        results = await metron_service.search_characters(name=name, page=page)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching characters: {str(e)}")


@router.get("/creators")
async def search_creators(
    name: Optional[str] = Query(None, description="Creator name"),
    page: int = Query(1, ge=1)
):
    """Search for creators (writers, artists, etc.)."""
    try:
        results = await metron_service.search_creators(name=name, page=page)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching creators: {str(e)}")
