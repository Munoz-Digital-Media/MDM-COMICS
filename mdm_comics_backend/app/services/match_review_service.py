"""
Match Review Service

Handles:
- Confidence-based routing
- Queue management
- Audit logging (hash-chained per constitution_logging.json)
- Optimistic locking
"""

import hashlib
import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

from sqlalchemy import select, update, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.match_review import MatchReviewQueue, MatchAuditLog, IsbnSource
from app.schemas.match_review import (
    MatchQueueFilter, MatchApproval, MatchRejection, ManualLink,
    MatchQueueItem, MatchQueueStats, EntitySummary, CandidateSummary
)

logger = logging.getLogger(__name__)

# Notification threshold per user requirement
QUEUE_NOTIFICATION_THRESHOLD = 20

# Escalation period
ESCALATION_DAYS = 30


class MatchDisposition(Enum):
    """Match routing disposition."""
    AUTO_LINK = "auto_link"      # Score >= 9, link immediately
    REVIEW_QUEUE = "review"      # Score 5-8, human review
    NO_MATCH = "no_match"        # Score < 5, queue for manual search


@dataclass
class MatchResult:
    """Result of match attempt."""
    disposition: MatchDisposition
    candidate_id: Optional[str]
    candidate_name: Optional[str]
    candidate_data: Optional[Dict[str, Any]]
    score: int
    method: str
    details: Dict[str, Any]


def route_match(method: str, score: int, candidate_count: int) -> MatchDisposition:
    """
    Route match to appropriate disposition based on confidence.

    Thresholds (confirmed 2025-12-13):
    - Score >= 9: AUTO_LINK
    - Score >= 8: REVIEW_QUEUE (bulk approve unlocked)
    - Score 5-7: REVIEW_QUEUE (individual review required)
    - Score < 5: NO_MATCH (queue for manual search)
    - ISBN/UPC exact: AUTO_LINK if single match
    """
    # Exact identifier matches
    if method in ('isbn', 'upc'):
        if candidate_count == 1:
            return MatchDisposition.AUTO_LINK
        else:
            # Multiple matches for same ISBN/UPC - unusual, needs review
            return MatchDisposition.REVIEW_QUEUE

    # Fuzzy matches by score
    if score >= 9:
        return MatchDisposition.AUTO_LINK
    elif score >= 5:
        return MatchDisposition.REVIEW_QUEUE
    else:
        return MatchDisposition.NO_MATCH


class MatchReviewService:
    """Service for match review queue operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # =========================================================
    # Queue Operations
    # =========================================================

    async def add_to_queue(
        self,
        entity_type: str,
        entity_id: int,
        candidate_source: str,
        candidate_id: str,
        candidate_name: str,
        candidate_data: Dict[str, Any],
        match_method: str,
        match_score: int,
        match_details: Dict[str, Any]
    ) -> MatchReviewQueue:
        """Add a match to the review queue."""

        # Check if already exists
        existing = await self.db.execute(
            select(MatchReviewQueue).where(
                and_(
                    MatchReviewQueue.entity_type == entity_type,
                    MatchReviewQueue.entity_id == entity_id,
                    MatchReviewQueue.candidate_source == candidate_source,
                    MatchReviewQueue.candidate_id == candidate_id
                )
            )
        )
        if existing.scalar_one_or_none():
            logger.debug(f"Match already in queue: {entity_type}:{entity_id} -> {candidate_id}")
            return existing.scalar_one_or_none()

        queue_item = MatchReviewQueue(
            entity_type=entity_type,
            entity_id=entity_id,
            candidate_source=candidate_source,
            candidate_id=candidate_id,
            candidate_name=candidate_name,
            candidate_data=candidate_data,
            match_method=match_method,
            match_score=match_score,
            match_details=match_details,
            status='pending',
            expires_at=datetime.utcnow() + timedelta(days=ESCALATION_DAYS)
        )

        self.db.add(queue_item)
        await self.db.commit()
        await self.db.refresh(queue_item)

        logger.info(f"Added to review queue: {entity_type}:{entity_id} -> {candidate_id} (score={match_score})")
        return queue_item

    async def get_queue(
        self,
        filter: MatchQueueFilter,
        current_user_id: int
    ) -> Tuple[List[MatchReviewQueue], int]:
        """Get paginated queue items."""

        query = select(MatchReviewQueue)

        # Apply filters
        if filter.status != 'all':
            query = query.where(MatchReviewQueue.status == filter.status)

        if filter.entity_type:
            query = query.where(MatchReviewQueue.entity_type == filter.entity_type)

        if filter.min_score is not None:
            query = query.where(MatchReviewQueue.match_score >= filter.min_score)

        if filter.max_score is not None:
            query = query.where(MatchReviewQueue.match_score <= filter.max_score)

        if filter.escalated_only:
            query = query.where(MatchReviewQueue.is_escalated == True)

        # Get total count
        count_query = select(func.count()).select_from(query.subquery())
        total = await self.db.scalar(count_query)

        # Order: escalated first, then by created_at
        query = query.order_by(
            MatchReviewQueue.is_escalated.desc(),
            MatchReviewQueue.created_at.asc()
        )

        # Pagination
        query = query.offset(filter.offset).limit(filter.limit)

        result = await self.db.execute(query)
        items = result.scalars().all()

        return items, total

    async def get_stats(self) -> MatchQueueStats:
        """Get queue statistics."""

        # Pending count
        pending = await self.db.scalar(
            select(func.count()).where(MatchReviewQueue.status == 'pending')
        )

        # Escalated count
        escalated = await self.db.scalar(
            select(func.count()).where(
                and_(
                    MatchReviewQueue.status == 'pending',
                    MatchReviewQueue.is_escalated == True
                )
            )
        )

        # Approved today
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        approved_today = await self.db.scalar(
            select(func.count()).where(
                and_(
                    MatchReviewQueue.status == 'approved',
                    MatchReviewQueue.reviewed_at >= today_start
                )
            )
        )

        # Rejected today
        rejected_today = await self.db.scalar(
            select(func.count()).where(
                and_(
                    MatchReviewQueue.status == 'rejected',
                    MatchReviewQueue.reviewed_at >= today_start
                )
            )
        )

        return MatchQueueStats(
            pending_count=pending or 0,
            escalated_count=escalated or 0,
            approved_today=approved_today or 0,
            rejected_today=rejected_today or 0,
            avg_review_time_seconds=None,  # TODO: Calculate
            threshold_exceeded=(pending or 0) > QUEUE_NOTIFICATION_THRESHOLD
        )

    # =========================================================
    # Locking
    # =========================================================

    async def acquire_lock(self, match_id: int, user_id: int) -> bool:
        """Acquire optimistic lock on a match."""

        match = await self.db.get(MatchReviewQueue, match_id)
        if not match:
            return False

        # Check if already locked by another user
        if match.is_locked and match.locked_by != user_id:
            return False

        # Acquire lock
        match.locked_by = user_id
        match.locked_at = datetime.utcnow()
        await self.db.commit()

        return True

    async def release_lock(self, match_id: int, user_id: int) -> bool:
        """Release lock on a match."""

        match = await self.db.get(MatchReviewQueue, match_id)
        if not match:
            return False

        if match.locked_by == user_id:
            match.locked_by = None
            match.locked_at = None
            await self.db.commit()
            return True

        return False

    # =========================================================
    # Actions
    # =========================================================

    async def approve_match(
        self,
        match_id: int,
        user_id: int,
        notes: Optional[str] = None
    ) -> Tuple[bool, str]:
        """Approve a match and link the entity."""

        match = await self.db.get(MatchReviewQueue, match_id)
        if not match:
            return False, "Match not found"

        if match.status != 'pending':
            return False, f"Match already {match.status}"

        # Check lock
        if match.is_locked and match.locked_by != user_id:
            return False, "Match is locked by another user"

        # Get before state for audit
        before_state = {"pricecharting_id": None}

        # Update entity with pricecharting_id
        if match.entity_type == 'comic':
            from app.models.comic_data import ComicIssue
            entity = await self.db.get(ComicIssue, match.entity_id)
            if entity:
                before_state["pricecharting_id"] = entity.pricecharting_id
                entity.pricecharting_id = match.candidate_id
                entity.pricecharting_match_method = match.match_method
                entity.pricecharting_match_score = match.match_score
                entity.pricecharting_matched_at = datetime.utcnow()
                entity.pricecharting_matched_by = user_id

        elif match.entity_type == 'funko':
            from app.models.funko import Funko
            entity = await self.db.get(Funko, match.entity_id)
            if entity:
                before_state["pricecharting_id"] = entity.pricecharting_id
                entity.pricecharting_id = match.candidate_id
                entity.pricecharting_match_method = match.match_method
                entity.pricecharting_match_score = match.match_score
                entity.pricecharting_matched_at = datetime.utcnow()
                entity.pricecharting_matched_by = user_id

        # Update queue item
        match.status = 'approved'
        match.reviewed_by = user_id
        match.reviewed_at = datetime.utcnow()
        match.resolution_notes = notes
        match.locked_by = None
        match.locked_at = None

        await self.db.commit()

        # Log audit
        after_state = {"pricecharting_id": match.candidate_id}
        await self._log_action(
            action='manual_approve',
            entity_type=match.entity_type,
            entity_id=match.entity_id,
            actor_type='user',
            actor_id=user_id,
            match_source=match.candidate_source,
            match_id=match.candidate_id,
            match_method=match.match_method,
            match_score=match.match_score,
            before_state=before_state,
            after_state=after_state
        )

        return True, "Match approved"

    async def reject_match(
        self,
        match_id: int,
        user_id: int,
        reason: str,
        notes: Optional[str] = None
    ) -> Tuple[bool, str]:
        """Reject a match."""

        match = await self.db.get(MatchReviewQueue, match_id)
        if not match:
            return False, "Match not found"

        if match.status != 'pending':
            return False, f"Match already {match.status}"

        # Check lock
        if match.is_locked and match.locked_by != user_id:
            return False, "Match is locked by another user"

        # Update queue item
        match.status = 'rejected'
        match.reviewed_by = user_id
        match.reviewed_at = datetime.utcnow()
        match.resolution_notes = f"Reason: {reason}. {notes or ''}"
        match.locked_by = None
        match.locked_at = None

        await self.db.commit()

        # Log audit
        await self._log_action(
            action='manual_reject',
            entity_type=match.entity_type,
            entity_id=match.entity_id,
            actor_type='user',
            actor_id=user_id,
            match_source=match.candidate_source,
            match_id=match.candidate_id,
            match_method=match.match_method,
            match_score=match.match_score
        )

        return True, "Match rejected"

    async def bulk_approve(
        self,
        match_ids: List[int],
        user_id: int,
        notes: Optional[str] = None
    ) -> Tuple[int, List[int]]:
        """
        Bulk approve matches with score >= 8.

        Returns: (approved_count, failed_ids)
        """
        approved = 0
        failed = []

        for match_id in match_ids:
            match = await self.db.get(MatchReviewQueue, match_id)

            if not match:
                failed.append(match_id)
                continue

            # Only allow bulk approve for score >= 8
            if not match.can_bulk_approve:
                failed.append(match_id)
                continue

            success, _ = await self.approve_match(match_id, user_id, notes)
            if success:
                approved += 1
            else:
                failed.append(match_id)

        return approved, failed

    # =========================================================
    # Escalation
    # =========================================================

    async def escalate_old_matches(self) -> int:
        """
        Escalate matches older than 30 days.
        Called by scheduled job.
        """
        cutoff = datetime.utcnow() - timedelta(days=ESCALATION_DAYS)

        result = await self.db.execute(
            update(MatchReviewQueue)
            .where(
                and_(
                    MatchReviewQueue.status == 'pending',
                    MatchReviewQueue.is_escalated == False,
                    MatchReviewQueue.created_at < cutoff
                )
            )
            .values(is_escalated=True)
        )

        await self.db.commit()
        escalated_count = result.rowcount

        if escalated_count > 0:
            logger.warning(f"Escalated {escalated_count} matches older than {ESCALATION_DAYS} days")

        return escalated_count

    # =========================================================
    # Audit Logging (hash-chained per constitution_logging.json)
    # =========================================================

    async def _log_action(
        self,
        action: str,
        entity_type: str,
        entity_id: int,
        actor_type: str,
        actor_id: Optional[int] = None,
        match_source: Optional[str] = None,
        match_id: Optional[str] = None,
        match_method: Optional[str] = None,
        match_score: Optional[int] = None,
        before_state: Optional[dict] = None,
        after_state: Optional[dict] = None
    ):
        """
        Create immutable audit log entry per constitution_logging.json.

        - Actor ID hashed (pseudonymized)
        - State changes hashed
        - Log entry hash-chained to previous
        """
        # Get previous hash for chain
        prev_result = await self.db.execute(
            select(MatchAuditLog.log_hash)
            .order_by(MatchAuditLog.id.desc())
            .limit(1)
        )
        previous_hash = prev_result.scalar() or "GENESIS"

        # Hash actor ID (never store raw per constitution_logging.json)
        actor_id_hash = None
        if actor_id:
            actor_id_hash = hashlib.sha512(f"actor:{actor_id}".encode()).hexdigest()

        # Hash state changes
        before_hash = None
        after_hash = None
        if before_state:
            before_hash = hashlib.sha512(json.dumps(before_state, sort_keys=True).encode()).hexdigest()
        if after_state:
            after_hash = hashlib.sha512(json.dumps(after_state, sort_keys=True).encode()).hexdigest()

        # Create log entry
        now = datetime.utcnow()
        entry = MatchAuditLog(
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            actor_type=actor_type,
            actor_id_hash=actor_id_hash,
            match_source=match_source,
            match_id=match_id,
            match_method=match_method,
            match_score=match_score,
            before_state_hash=before_hash,
            after_state_hash=after_hash,
            previous_hash=previous_hash,
            created_at=now
        )

        # Compute log hash (chain link)
        chain_data = f"{now.isoformat()}:{action}:{entity_type}:{entity_id}:{previous_hash}"
        entry.log_hash = hashlib.sha512(chain_data.encode()).hexdigest()

        self.db.add(entry)
        await self.db.commit()

        logger.debug(f"Audit log: {action} on {entity_type}:{entity_id}")
