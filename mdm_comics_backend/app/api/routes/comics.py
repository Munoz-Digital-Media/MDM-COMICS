"""
Comic search and lookup routes using Metron API
All searches are cached to local database for data capture.

P2-1: Image upload validation with magic bytes check
"""
from typing import Optional, List
import io
from fastapi import APIRouter, HTTPException, Query, Depends, Request, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from PIL import Image
import imagehash

from app.core.database import get_db
from app.core.upload_validation import validate_image_upload
from app.services.comic_cache import comic_cache
from app.services.metron import metron_service
from app.services.multi_source_search import multi_source_search
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
    Search for comic issues across multiple data sources.
    Uses multi-source fallback: Local cache → Metron → Fandom wikis.
    No single point of failure - if one source is rate limited, others are tried.
    """
    import asyncio
    import logging
    logger = logging.getLogger(__name__)

    try:
        ip_address = request.client.host if request.client else None

        # Use multi-source search with automatic failover
        results = await asyncio.wait_for(
            multi_source_search.search_issues(
                db=db,
                series_name=series,
                number=number,
                publisher_name=publisher,
                cover_year=year,
                upc=upc,
                page=page,
                ip_address=ip_address
            ),
            timeout=25.0  # Fail before Railway's 30s timeout
        )

        # Log which sources were used
        sources = results.get("_sources_tried", [])
        failed = results.get("_sources_failed", [])
        if failed:
            logger.info(f"[comics/search] Sources tried: {sources}, failed: {failed}")

        return results
    except asyncio.TimeoutError:
        logger.warning(f"[comics/search] Timeout searching for series={series}, number={number}")
        return {"results": [], "count": 0, "next": None, "previous": None, "message": "Search timed out across all sources. Try again later."}
    except Exception as e:
        logger.error(f"[comics/search] Error: {e}")
        raise HTTPException(status_code=500, detail=f"Error searching comics: {str(e)}")


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
    Search for comic series across multiple sources.
    Falls back to local cache if external APIs are rate limited.
    """
    import asyncio
    import logging
    logger = logging.getLogger(__name__)

    try:
        ip_address = request.client.host if request.client else None

        # Try external API first with short timeout
        try:
            results = await asyncio.wait_for(
                comic_cache.search_series(
                    db=db,
                    name=name,
                    publisher_name=publisher,
                    year_began=year,
                    page=page,
                    ip_address=ip_address
                ),
                timeout=10.0
            )
            return results
        except (asyncio.TimeoutError, Exception) as api_error:
            error_str = str(api_error).lower()
            is_rate_limited = "429" in error_str or "rate limit" in error_str or isinstance(api_error, asyncio.TimeoutError)

            if is_rate_limited:
                logger.info(f"[comics/series] API unavailable, falling back to local cache")

                # Fallback to local database search
                from sqlalchemy import select
                query = select(ComicSeries)

                if name:
                    query = query.where(ComicSeries.name.ilike(f"%{name}%"))
                if publisher:
                    query = query.where(ComicSeries.publisher_name.ilike(f"%{publisher}%"))
                if year:
                    query = query.where(ComicSeries.year_began == year)

                query = query.limit(20)
                result = await db.execute(query)
                series_list = result.scalars().all()

                return {
                    "results": [
                        {
                            "id": s.metron_id or s.id,
                            "name": s.name,
                            "publisher": {"name": s.publisher_name},
                            "year_began": s.year_began,
                            "issue_count": s.issue_count,
                            "_source": "local_cache",
                        }
                        for s in series_list
                    ],
                    "count": len(series_list),
                    "next": None,
                    "previous": None,
                    "message": "Showing cached results. External API temporarily unavailable.",
                }
            else:
                raise api_error

    except asyncio.TimeoutError:
        logger.warning(f"[comics/series] Timeout searching for name={name}")
        return {"results": [], "count": 0, "next": None, "previous": None, "message": "Search timed out across all sources."}
    except Exception as e:
        logger.error(f"[comics/series] Error: {e}")
        raise HTTPException(status_code=500, detail=f"Error searching series: {str(e)}")


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

    P2-1: Enhanced image validation with magic bytes check.
    """
    # P2-1: Comprehensive image validation (type, size, magic bytes, dimensions)
    content = await validate_image_upload(
        file,
        allowed_types=["image/jpeg", "image/png"],
        max_size_mb=10,
        validate_dimensions=True,
        max_dimension=8192
    )

    try:
        # Compute perceptual hash of uploaded image
        img = Image.open(io.BytesIO(content))
        uploaded_hash = imagehash.phash(img)
        uploaded_hash_str = str(uploaded_hash).lower()

        prefix = uploaded_hash_str[:8]

        prefix_candidates: List[str] = []
        if prefix and all(c in "0123456789abcdef" for c in prefix):
            try:
                prefix_value = int(prefix, 16)
                prefix_candidates.append(f"{prefix_value:08x}")
                # Include neighboring prefixes to widen the search window
                if prefix_value > 0:
                    prefix_candidates.append(f"{prefix_value - 1:08x}")
                if prefix_value < 0xFFFFFFFF:
                    prefix_candidates.append(f"{prefix_value + 1:08x}")
            except ValueError:
                prefix_candidates = []

        query = (
            select(ComicIssue)
            .options(selectinload(ComicIssue.series))
            .where(ComicIssue.cover_hash.isnot(None))
        )

        if prefix_candidates:
            query = query.where(ComicIssue.cover_hash_prefix.in_(prefix_candidates))

        # Bound the candidate set to avoid loading the entire table
        result = await db.execute(query.limit(500))
        issues = result.scalars().all()

        if not issues:
            fallback = await db.execute(
                select(ComicIssue)
                .options(selectinload(ComicIssue.series))
                .where(ComicIssue.cover_hash.isnot(None))
                .limit(200)
            )
            issues = fallback.scalars().all()

        uploaded_hash_int = int(uploaded_hash_str, 16)

        # Calculate hamming distance for each
        matches = []
        for issue in issues:
            try:
                if issue.cover_hash_bytes:
                    db_hash_int = int.from_bytes(bytes(issue.cover_hash_bytes), "big")
                elif issue.cover_hash:
                    db_hash_int = int(issue.cover_hash, 16)
                else:
                    continue
            except (ValueError, TypeError):
                continue

            distance = bin(uploaded_hash_int ^ db_hash_int).count("1")

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

        # Sort by confidence (highest first)
        matches.sort(key=lambda x: x["confidence"], reverse=True)

        # Return top 10 matches
        return {"matches": matches[:10]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Image processing failed: {str(e)}")
