"""
Comic search and lookup routes using Metron API
All searches are cached to local database for data capture.
"""
from typing import Optional
import io
from fastapi import APIRouter, HTTPException, Query, Depends, Request, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from PIL import Image
import imagehash

from app.core.database import get_db
from app.services.comic_cache import comic_cache
from app.services.metron import metron_service
from app.models.comic_data import ComicIssue, ComicSeries

router = APIRouter(prefix="/comics", tags=["comics"])


@router.get("/search")
async def search_comics(
    request: Request,
    series: Optional[str] = Query(None, description="Series name (e.g., 'amazing spider-man')"),
    number: Optional[str] = Query(None, description="Issue number (e.g., '300')"),
    publisher: Optional[str] = Query(None, description="Publisher name (e.g., 'marvel')"),
    year: Optional[int] = Query(None, description="Cover year"),
    upc: Optional[str] = Query(None, description="UPC barcode number"),
    page: int = Query(1, ge=1, description="Page number"),
    db: AsyncSession = Depends(get_db)
):
    """
    Search for comic issues in the Metron database.
    All results are cached locally for future use.
    Supports UPC barcode search for exact match.
    """
    try:
        ip_address = request.client.host if request.client else None

        results = await comic_cache.search_issues(
            db=db,
            series_name=series,
            number=number,
            publisher_name=publisher,
            cover_year=year,
            upc=upc,
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
                upc=upc,
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


@router.post("/search-by-image")
async def search_by_image(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db)
):
    """
    Search for comics by cover image using perceptual hashing.
    Returns potential matches ranked by similarity.

    BE-003 OPTIMIZATION: Uses indexed cover_hash column instead of
    scanning raw_data JSON. Only queries issues that HAVE a cover_hash.
    """
    # Validate file type
    if file.content_type not in ["image/jpeg", "image/png"]:
        raise HTTPException(status_code=400, detail="Only JPEG and PNG images allowed")

    # Read and validate file size (10MB max)
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Image must be under 10MB")

    try:
        # Compute perceptual hash of uploaded image
        img = Image.open(io.BytesIO(content))
        uploaded_hash = imagehash.phash(img)
        uploaded_hash_str = str(uploaded_hash)

        # BE-003 FIX: Query ONLY issues with cover_hash populated (uses index)
        # This avoids full table scan of raw_data JSON
        from sqlalchemy.orm import selectinload

        result = await db.execute(
            select(ComicIssue)
            .options(selectinload(ComicIssue.series))
            .where(ComicIssue.cover_hash.isnot(None))
        )
        issues = result.scalars().all()

        # Calculate hamming distance for each
        matches = []
        for issue in issues:
            try:
                db_hash = imagehash.hex_to_hash(issue.cover_hash)
                distance = db_hash - uploaded_hash

                # Lower distance = better match (0 = identical)
                # Threshold of 15 is reasonable for comic covers
                if distance <= 15:
                    confidence = max(0, 1 - (distance / 15))

                    # Get series name from relationship (eager loaded)
                    series_name = issue.series.name if issue.series else ""

                    matches.append({
                        "id": issue.metron_id,
                        "issue": f"{series_name} #{issue.number}" if series_name else f"Issue #{issue.number}",
                        "series": {"name": series_name},
                        "number": issue.number,
                        "image": issue.image,
                        "cover_date": str(issue.cover_date) if issue.cover_date else None,
                        "confidence": round(confidence, 2),
                        "distance": distance
                    })
            except Exception:
                continue

        # Sort by confidence (highest first)
        matches.sort(key=lambda x: x["confidence"], reverse=True)

        # Return top 10 matches
        return {"matches": matches[:10]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Image processing failed: {str(e)}")
