"""
Match Review Queue API Routes

Per constitution_cyberSec.json:
- JWT verification required
- Admin role required
- Input validation via Pydantic
- Audit logging
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user, require_admin
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

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/match-queue", tags=["Match Review"])


# =============================================================
# List & Stats
# =============================================================

@router.post("", response_model=MatchQueueResponse)
async def list_match_queue(
    filter: MatchQueueFilter,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_admin)
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
        # Build entity summary
        entity = await _get_entity_summary(db, item.entity_type, item.entity_id)

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
    current_user: User = Depends(require_admin)
):
    """Get queue statistics for dashboard."""
    service = MatchReviewService(db)
    return await service.get_stats()


# =============================================================
# Individual Match Actions
# =============================================================

@router.get("/{match_id}")
async def get_match_details(
    match_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_admin)
):
    """Get full details of a match for review."""
    match = await db.get(MatchReviewQueue, match_id)
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")

    # Acquire lock for this user
    service = MatchReviewService(db)
    await service.acquire_lock(match_id, current_user.id)

    entity = await _get_entity_summary(db, match.entity_type, match.entity_id)

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
    current_user: User = Depends(require_admin)
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
    current_user: User = Depends(require_admin)
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
    current_user: User = Depends(require_admin)
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
    current_user: User = Depends(require_admin)
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
    current_user: User = Depends(require_admin)
):
    """Manually link an entity to a PriceCharting product."""
    from datetime import datetime

    if link.entity_type == 'comic':
        from app.models.comic_issue import ComicIssue
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
    current_user: User = Depends(require_admin)
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

async def _get_entity_summary(db: AsyncSession, entity_type: str, entity_id: int) -> EntitySummary:
    """Get summary of entity for display."""
    if entity_type == 'comic':
        from app.models.comic_issue import ComicIssue
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
    else:
        from app.models.funko import Funko
        entity = await db.get(Funko, entity_id)
        if entity:
            return EntitySummary(
                id=entity.id,
                type='funko',
                name=entity.title or f"Funko #{entity_id}",
                upc=entity.upc
            )

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
