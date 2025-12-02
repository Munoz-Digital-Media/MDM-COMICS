"""
Comic search and lookup routes using Metron API
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from ...services.metron import metron_service

router = APIRouter(prefix="/comics", tags=["comics"])


@router.get("/search")
async def search_comics(
    series: Optional[str] = Query(None, description="Series name (e.g., 'amazing spider-man')"),
    number: Optional[str] = Query(None, description="Issue number (e.g., '300')"),
    publisher: Optional[str] = Query(None, description="Publisher name (e.g., 'marvel')"),
    year: Optional[int] = Query(None, description="Cover year"),
    page: int = Query(1, ge=1, description="Page number")
):
    """
    Search for comic issues in the Metron database.

    Returns paginated results with cover images, series info, and issue details.
    """
    try:
        results = await metron_service.search_issues(
            series_name=series,
            number=number,
            publisher_name=publisher,
            cover_year=year,
            page=page
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching comics: {str(e)}")


@router.get("/issue/{issue_id}")
async def get_issue(issue_id: int):
    """
    Get detailed information about a specific comic issue.

    Returns full details including cover image, credits, characters, etc.
    """
    try:
        result = await metron_service.get_issue(issue_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching issue: {str(e)}")


@router.get("/series")
async def search_series(
    name: Optional[str] = Query(None, description="Series name"),
    publisher: Optional[str] = Query(None, description="Publisher name"),
    year: Optional[int] = Query(None, description="Year series began"),
    page: int = Query(1, ge=1, description="Page number")
):
    """
    Search for comic series.

    Returns series info including volume, year began, and publisher.
    """
    try:
        results = await metron_service.search_series(
            name=name,
            publisher_name=publisher,
            year_began=year,
            page=page
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching series: {str(e)}")


@router.get("/series/{series_id}")
async def get_series(series_id: int):
    """
    Get detailed information about a specific series.
    """
    try:
        result = await metron_service.get_series(series_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching series: {str(e)}")


@router.get("/publishers")
async def get_publishers(page: int = Query(1, ge=1)):
    """
    Get list of comic publishers.
    """
    try:
        results = await metron_service.get_publishers(page=page)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching publishers: {str(e)}")


@router.get("/characters")
async def search_characters(
    name: Optional[str] = Query(None, description="Character name"),
    page: int = Query(1, ge=1)
):
    """
    Search for characters in the Metron database.
    """
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
    """
    Search for creators (writers, artists, etc.).
    """
    try:
        results = await metron_service.search_creators(name=name, page=page)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching creators: {str(e)}")
