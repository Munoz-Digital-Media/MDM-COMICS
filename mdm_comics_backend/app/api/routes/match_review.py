"""
Match Review Queue API Routes

Per constitution_cyberSec.json:
- JWT verification required
- Admin role required
- Input validation via Pydantic
- Audit logging
"""

import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.api.deps import get_current_user, get_current_admin
from app.models.user import User
from app.models.match_review import MatchReviewQueue
from app.schemas.match_review import (
    MatchQueueFilter, MatchApproval, MatchRejection, MatchSkip,
    ManualLink, BulkApproval, ManualSearch,
    MatchQueueResponse, MatchQueueItem, MatchQueueStats,
    MatchActionResult, BulkApprovalResult, ManualSearchResponse,
    EntitySummary, CandidateSummary
)
from app.services.match_review_service import MatchReviewService
from app.services.storage import StorageService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/match-queue", tags=["Match Review"])


# =============================================================
# List & Stats
# =============================================================

@router.post("", response_model=MatchQueueResponse)
async def list_match_queue(
    filter: MatchQueueFilter,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """
    List pending matches for review.

    Requires admin role.
    """
    service = MatchReviewService(db)
    items, total = await service.get_queue(filter, current_user.id)

    # Get counts
    stats = await service.get_stats()

    # Transform to response
    response_items = []
    for item in items:
        # Build entity summary (pass candidate_data for cover uploads where entity_id=0)
        entity = await _get_entity_summary(
            db, item.entity_type, item.entity_id, item.candidate_data
        )

        # Build candidate summary
        candidate = CandidateSummary(
            source=item.candidate_source,
            id=item.candidate_id,
            name=item.candidate_name or "",
            price_loose=item.candidate_data.get('price_loose') if item.candidate_data else None,
            price_cib=item.candidate_data.get('price_cib') if item.candidate_data else None,
            price_graded=item.candidate_data.get('price_graded') if item.candidate_data else None,
            url=item.candidate_data.get('url') if item.candidate_data else None
        )

        response_items.append(MatchQueueItem(
            id=item.id,
            entity=entity,
            candidate=candidate,
            match_method=item.match_method,
            match_score=item.match_score,
            match_details=item.match_details,
            status=item.status,
            is_escalated=item.is_escalated,
            can_bulk_approve=item.can_bulk_approve,
            is_locked=item.is_locked,
            locked_by_current_user=(item.locked_by == current_user.id),
            created_at=item.created_at,
            expires_at=item.expires_at
        ))

    return MatchQueueResponse(
        items=response_items,
        total=total,
        pending_count=stats.pending_count,
        escalated_count=stats.escalated_count,
        limit=filter.limit,
        offset=filter.offset
    )


@router.get("/stats", response_model=MatchQueueStats)
async def get_queue_stats(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """Get queue statistics for dashboard."""
    service = MatchReviewService(db)
    return await service.get_stats()


@router.get("/cover/{match_id}")
async def get_cover_image(
    match_id: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Serve cover image for a match review item.

    NOTE: This endpoint is intentionally unauthenticated because browsers
    load <img src="..."> without sending Authorization headers. The images
    themselves are non-sensitive cover art.

    Handles both S3-hosted images and local files:
    - If s3_url exists: redirect to S3
    - If s3_key exists: construct URL from key
    - If local file_path exists: serve file directly
    """
    match = await db.get(MatchReviewQueue, match_id)
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")

    candidate_data = match.candidate_data or {}

    # Check for S3 URL first
    s3_url = candidate_data.get('s3_url')
    if s3_url:
        logger.debug(f"Redirecting to s3_url for match {match_id}")
        return RedirectResponse(url=s3_url)

    # Check for S3 key - construct URL if storage is configured
    s3_key = candidate_data.get('s3_key')
    if s3_key:
        try:
            storage = StorageService()
            if storage.is_configured():
                constructed_url = storage.get_public_url(s3_key)
                logger.debug(f"Constructed S3 URL from key for match {match_id}: {constructed_url}")
                return RedirectResponse(url=constructed_url)
        except Exception as e:
            logger.warning(f"Failed to construct S3 URL from key: {e}")

    # Check for local file path
    file_path = candidate_data.get('file_path')
    if file_path and os.path.exists(file_path):
        # Determine content type from extension
        ext = Path(file_path).suffix.lower()
        content_types = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.webp': 'image/webp',
            '.gif': 'image/gif',
        }
        content_type = content_types.get(ext, 'image/jpeg')

        return FileResponse(
            path=file_path,
            media_type=content_type,
            filename=Path(file_path).name
        )

    # Log what we have for debugging
    logger.warning(
        f"Cover not found for match {match_id}: "
        f"s3_url={s3_url is not None}, s3_key={s3_key is not None}, "
        f"file_path={file_path}, file_exists={file_path and os.path.exists(file_path) if file_path else False}"
    )

    raise HTTPException(status_code=404, detail="Cover image not found")


# =============================================================
# Individual Match Actions
# =============================================================

@router.get("/{match_id}")
async def get_match_details(
    match_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """Get full details of a match for review."""
    match = await db.get(MatchReviewQueue, match_id)
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")

    # Acquire lock for this user
    service = MatchReviewService(db)
    await service.acquire_lock(match_id, current_user.id)

    entity = await _get_entity_summary(
        db, match.entity_type, match.entity_id, match.candidate_data
    )

    candidate = CandidateSummary(
        source=match.candidate_source,
        id=match.candidate_id,
        name=match.candidate_name or "",
        price_loose=match.candidate_data.get('price_loose') if match.candidate_data else None,
        price_cib=match.candidate_data.get('price_cib') if match.candidate_data else None,
        price_graded=match.candidate_data.get('price_graded') if match.candidate_data else None,
        url=match.candidate_data.get('url') if match.candidate_data else None
    )

    return MatchQueueItem(
        id=match.id,
        entity=entity,
        candidate=candidate,
        match_method=match.match_method,
        match_score=match.match_score,
        match_details=match.match_details,
        status=match.status,
        is_escalated=match.is_escalated,
        can_bulk_approve=match.can_bulk_approve,
        is_locked=match.is_locked,
        locked_by_current_user=True,
        created_at=match.created_at,
        expires_at=match.expires_at
    )


@router.post("/{match_id}/approve", response_model=MatchActionResult)
async def approve_match(
    match_id: int,
    approval: MatchApproval,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """Approve a match and link the entity to PriceCharting."""
    service = MatchReviewService(db)
    success, message = await service.approve_match(
        match_id=match_id,
        user_id=current_user.id,
        notes=approval.notes
    )

    if not success:
        raise HTTPException(status_code=400, detail=message)

    # Get next pending match for continuous review flow
    next_match = await _get_next_pending_match(db, match_id)

    return MatchActionResult(
        success=True,
        match_id=match_id,
        action='approved',
        message=message,
        next_match_id=next_match.id if next_match else None
    )


@router.post("/{match_id}/reject", response_model=MatchActionResult)
async def reject_match(
    match_id: int,
    rejection: MatchRejection,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """Reject a match candidate."""
    service = MatchReviewService(db)
    success, message = await service.reject_match(
        match_id=match_id,
        user_id=current_user.id,
        reason=rejection.reason,
        notes=rejection.notes
    )

    if not success:
        raise HTTPException(status_code=400, detail=message)

    # Get next pending match
    next_match = await _get_next_pending_match(db, match_id)

    return MatchActionResult(
        success=True,
        match_id=match_id,
        action='rejected',
        message=message,
        next_match_id=next_match.id if next_match else None
    )


@router.post("/{match_id}/skip", response_model=MatchActionResult)
async def skip_match(
    match_id: int,
    skip: MatchSkip,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """Skip a match for later review."""
    match = await db.get(MatchReviewQueue, match_id)
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")

    # Release lock
    service = MatchReviewService(db)
    await service.release_lock(match_id, current_user.id)

    # Get next pending match
    next_match = await _get_next_pending_match(db, match_id)

    return MatchActionResult(
        success=True,
        match_id=match_id,
        action='skipped',
        message="Match skipped",
        next_match_id=next_match.id if next_match else None
    )


# =============================================================
# Bulk Operations
# =============================================================

@router.post("/bulk-approve", response_model=BulkApprovalResult)
async def bulk_approve_matches(
    bulk: BulkApproval,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """
    Bulk approve matches with score >= 8.

    Only matches with can_bulk_approve=True will be approved.
    """
    service = MatchReviewService(db)
    approved_count, failed_ids = await service.bulk_approve(
        match_ids=bulk.match_ids,
        user_id=current_user.id,
        notes=bulk.notes
    )

    return BulkApprovalResult(
        success=len(failed_ids) == 0,
        approved_count=approved_count,
        failed_count=len(failed_ids),
        failed_ids=failed_ids,
        message=f"Approved {approved_count} matches" + (f", {len(failed_ids)} failed" if failed_ids else "")
    )


# =============================================================
# Manual Linking
# =============================================================

@router.post("/manual-link", response_model=MatchActionResult)
async def manual_link(
    link: ManualLink,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """Manually link an entity to a PriceCharting product."""
    from datetime import datetime

    if link.entity_type == 'comic':
        from app.models.comic_data import ComicIssue
        entity = await db.get(ComicIssue, link.entity_id)
    else:
        from app.models.funko import Funko
        entity = await db.get(Funko, link.entity_id)

    if not entity:
        raise HTTPException(status_code=404, detail=f"{link.entity_type} not found")

    # Update entity
    entity.pricecharting_id = link.pricecharting_id
    entity.pricecharting_match_method = 'manual'
    entity.pricecharting_match_score = 10  # Manual = highest confidence
    entity.pricecharting_matched_at = datetime.utcnow()
    entity.pricecharting_matched_by = current_user.id

    await db.commit()

    # Log audit
    service = MatchReviewService(db)
    await service._log_action(
        action='manual_link',
        entity_type=link.entity_type,
        entity_id=link.entity_id,
        actor_type='user',
        actor_id=current_user.id,
        match_source='pricecharting',
        match_id=link.pricecharting_id,
        match_method='manual',
        match_score=10
    )

    return MatchActionResult(
        success=True,
        match_id=0,
        action='manual_link',
        message=f"Linked {link.entity_type} {link.entity_id} to PriceCharting {link.pricecharting_id}"
    )


@router.post("/search", response_model=ManualSearchResponse)
async def search_pricecharting(
    search: ManualSearch,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """Search PriceCharting for manual linking."""
    import os
    import httpx

    pc_token = os.getenv("PRICECHARTING_API_TOKEN")
    if not pc_token:
        raise HTTPException(status_code=500, detail="PriceCharting API not configured")

    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://www.pricecharting.com/api/products",
            params={
                "t": pc_token,
                "q": search.query,
                "console-name": "Comics" if search.entity_type == 'comic' else "Funko"
            },
            timeout=30.0
        )

        if response.status_code != 200:
            raise HTTPException(status_code=502, detail="PriceCharting API error")

        data = response.json()
        products = data.get("products", [])

        from app.schemas.match_review import SearchResult
        results = [
            SearchResult(
                id=str(p.get("id")),
                name=p.get("product-name", ""),
                console=p.get("console-name", ""),
                price_loose=p.get("loose-price"),
                price_cib=p.get("cib-price"),
                price_graded=p.get("graded-price")
            )
            for p in products[:20]
        ]

        return ManualSearchResponse(
            query=search.query,
            results=results,
            result_count=len(results)
        )


# =============================================================
# Helpers
# =============================================================

async def _get_entity_summary(
    db: AsyncSession,
    entity_type: str,
    entity_id: int,
    candidate_data: Optional[dict] = None
) -> EntitySummary:
    """
    Get summary of entity for display.

    For cover uploads (entity_id=0), uses candidate_data which contains:
    - publisher, series, volume, issue_number, variant_code
    - s3_url (cover image), cgc_grade, original_filename
    """
    # Cover upload case: entity_id=0, data is in candidate_data
    if entity_id == 0 and candidate_data:
        # Build display name from metadata
        series = candidate_data.get('series', 'Unknown')
        volume = candidate_data.get('volume', 1)
        issue = candidate_data.get('issue_number', '1')
        variant = candidate_data.get('variant_code', '')
        cgc = candidate_data.get('cgc_grade')

        name = f"{series} v{volume} #{issue}"
        if variant:
            name += f" ({variant})"
        if cgc:
            name += f" CGC {cgc}"

        return EntitySummary(
            id=0,
            type=entity_type,
            name=name,
            series_name=series,
            issue_number=str(issue),
            publisher=candidate_data.get('publisher'),
            year=None,  # Not in folder metadata
            isbn=candidate_data.get('isbn'),
            upc=candidate_data.get('upc'),
            cover_image_url=candidate_data.get('s3_url')
        )

    # Standard comic lookup
    if entity_type == 'comic':
        from app.models.comic_data import ComicIssue
        entity = await db.get(ComicIssue, entity_id)
        if entity:
            return EntitySummary(
                id=entity.id,
                type='comic',
                name=entity.issue_name or entity.series_name or f"Comic #{entity_id}",
                series_name=entity.series_name,
                issue_number=entity.number,
                publisher=entity.publisher_name,
                year=entity.cover_date.year if entity.cover_date else None,
                isbn=entity.isbn_normalized or entity.isbn,
                upc=entity.upc,
                cover_image_url=entity.cover_image_url
            )

    # Standard funko lookup
    elif entity_type == 'funko':
        from app.models.funko import Funko
        entity = await db.get(Funko, entity_id)
        if entity:
            return EntitySummary(
                id=entity.id,
                type='funko',
                name=entity.title or f"Funko #{entity_id}",
                upc=entity.upc
            )

    # Fallback for unknown entity
    return EntitySummary(
        id=entity_id,
        type=entity_type,
        name=f"{entity_type.title()} #{entity_id}"
    )


async def _get_next_pending_match(db: AsyncSession, current_id: int) -> Optional[MatchReviewQueue]:
    """Get next pending match for continuous review flow."""
    from sqlalchemy import select

    result = await db.execute(
        select(MatchReviewQueue)
        .where(MatchReviewQueue.status == 'pending')
        .where(MatchReviewQueue.id != current_id)
        .order_by(MatchReviewQueue.is_escalated.desc(), MatchReviewQueue.created_at.asc())
        .limit(1)
    )
    return result.scalar_one_or_none()


# =============================================================
# Cover Upload CLI Support (v1.21.4)
# =============================================================

from pydantic import BaseModel
from typing import Dict, Any


class CoverQueueRequest(BaseModel):
    """Request to queue a cover for review."""
    source_type: str = "cover_upload"
    source_id: str  # File hash
    candidate_data: Dict[str, Any]
    confidence_score: float = 5.0
    disposition: str = "review"


class CoverQueueResponse(BaseModel):
    """Response from queueing a cover."""
    success: bool
    id: Optional[int] = None
    message: Optional[str] = None


class HashCheckResponse(BaseModel):
    """Response from hash check."""
    exists: bool
    queue_id: Optional[int] = None


@router.post("/queue-cover", response_model=CoverQueueResponse)
async def queue_cover_for_review(
    request: CoverQueueRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """
    Queue a cover image for Match Review.

    Called by the local upload_covers.py script after uploading to S3.
    Creates a Match Review queue entry for human approval.
    """
    from sqlalchemy import select
    from datetime import datetime, timezone, timedelta

    # Check if already exists
    existing = await db.execute(
        select(MatchReviewQueue).where(
            MatchReviewQueue.entity_type == request.source_type,
            MatchReviewQueue.candidate_id == request.source_id
        )
    )
    existing_item = existing.scalar_one_or_none()

    if existing_item:
        return CoverQueueResponse(
            success=True,
            id=existing_item.id,
            message="Already in queue"
        )

    # Build product name from metadata
    data = request.candidate_data
    product_name = f"{data.get('series', 'Unknown')} v{data.get('volume', 1)} #{data.get('issue_number', '1')}"
    if data.get('variant_code'):
        product_name += f" ({data['variant_code']})"
    if data.get('cgc_grade'):
        product_name += f" CGC {data['cgc_grade']}"

    # Create queue entry
    queue_item = MatchReviewQueue(
        entity_type=request.source_type,
        entity_id=0,  # No entity yet - created on approval
        candidate_source="local_upload",
        candidate_id=request.source_id,
        candidate_name=product_name,
        candidate_data=request.candidate_data,
        match_method="cover_upload",
        match_score=int(request.confidence_score),
        status="pending",
        expires_at=datetime.now(timezone.utc) + timedelta(days=30)
    )

    db.add(queue_item)
    await db.flush()

    logger.info(f"Queued cover for review: {product_name} (#{queue_item.id})")

    return CoverQueueResponse(
        success=True,
        id=queue_item.id,
        message="Queued for review"
    )


@router.get("/check-hash/{file_hash}", response_model=HashCheckResponse)
async def check_cover_hash(
    file_hash: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """
    Check if a cover with this file hash already exists in the queue.

    Used by upload_covers.py to skip already-processed files.
    """
    from sqlalchemy import select

    result = await db.execute(
        select(MatchReviewQueue).where(
            MatchReviewQueue.entity_type.in_(["cover_ingestion", "cover_upload"]),
            MatchReviewQueue.candidate_id == file_hash
        )
    )
    existing = result.scalar_one_or_none()

    return HashCheckResponse(
        exists=existing is not None,
        queue_id=existing.id if existing else None
    )
