"""
Session Replay Retrieval Service

Handles replay playback and search.
"""

import gzip
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.analytics import SessionReplay, SessionReplayChunk, AnalyticsSession

logger = logging.getLogger(__name__)


class ReplayService:
    """
    Session replay retrieval and management.
    """

    async def get_replay(
        self,
        db: AsyncSession,
        session_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get full replay data for a session.

        Returns decompressed rrweb events.
        """
        result = await db.execute(
            select(SessionReplay).where(SessionReplay.session_id == session_id)
        )
        replay = result.scalar_one_or_none()

        if not replay:
            return None

        # Get all chunks
        result = await db.execute(
            select(SessionReplayChunk)
            .where(SessionReplayChunk.replay_id == replay.id)
            .order_by(SessionReplayChunk.chunk_index)
        )
        chunks = result.scalars().all()

        # Decompress and combine events
        all_events = []
        for chunk in chunks:
            try:
                decompressed = gzip.decompress(chunk.data)
                events = json.loads(decompressed)
                all_events.extend(events)
            except Exception as e:
                logger.error(f"Failed to decompress chunk {chunk.id}: {e}")

        return {
            "session_id": session_id,
            "started_at": replay.started_at.isoformat() if replay.started_at else None,
            "duration_seconds": replay.duration_seconds,
            "has_errors": replay.has_errors,
            "has_cart_abandonment": replay.has_cart_abandonment,
            "events": all_events,
        }

    async def search_replays(
        self,
        db: AsyncSession,
        filters: Dict[str, Any],
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Search replays with filters.

        Filters:
        - has_errors: bool
        - has_cart_abandonment: bool
        - user_id: int
        - date_from: datetime
        - date_to: datetime
        - min_duration: int (seconds)
        """
        query = select(SessionReplay, AnalyticsSession).join(
            AnalyticsSession,
            SessionReplay.session_id == AnalyticsSession.session_id,
        )

        conditions = []

        if filters.get("has_errors"):
            conditions.append(SessionReplay.has_errors == True)

        if filters.get("has_cart_abandonment"):
            conditions.append(SessionReplay.has_cart_abandonment == True)

        if filters.get("user_id"):
            conditions.append(SessionReplay.user_id == filters["user_id"])

        if filters.get("date_from"):
            conditions.append(SessionReplay.started_at >= filters["date_from"])

        if filters.get("date_to"):
            conditions.append(SessionReplay.started_at <= filters["date_to"])

        if filters.get("min_duration"):
            conditions.append(SessionReplay.duration_seconds >= filters["min_duration"])

        if conditions:
            query = query.where(and_(*conditions))

        query = query.order_by(SessionReplay.started_at.desc()).limit(limit).offset(offset)

        result = await db.execute(query)
        rows = result.all()

        return [
            {
                "session_id": replay.session_id,
                "user_id": replay.user_id,
                "started_at": replay.started_at.isoformat() if replay.started_at else None,
                "duration_seconds": replay.duration_seconds,
                "has_errors": replay.has_errors,
                "has_cart_abandonment": replay.has_cart_abandonment,
                "landing_page": session.landing_page,
                "converted": session.converted,
            }
            for replay, session in rows
        ]


# Singleton
_service: Optional[ReplayService] = None


def get_replay_service() -> ReplayService:
    global _service
    if _service is None:
        _service = ReplayService()
    return _service
