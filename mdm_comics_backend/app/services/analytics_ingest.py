"""
Analytics Ingestion Service

Handles high-volume event ingestion with batching and compression support.
"""

import gzip
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
import hashlib

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from user_agents import parse as parse_user_agent

from app.models.analytics import (
    AnalyticsSession, AnalyticsEvent, SearchQuery, ProductView,
    CartSnapshot, CartEvent, SessionReplay, SessionReplayChunk,
    WebVital, ErrorEvent
)
from app.core.config import settings
from app.core.monitoring import metrics

logger = logging.getLogger(__name__)


class AnalyticsIngestService:
    """
    High-performance analytics ingestion.

    Features:
    - Batched writes
    - Compression support (gzip)
    - User agent parsing
    - IP hashing (for privacy)
    - Session management
    """

    def __init__(self):
        self._session_cache: Dict[str, datetime] = {}  # session_id -> last_seen

    def _hash_ip(self, ip: str) -> str:
        """Hash IP for privacy while maintaining consistency."""
        salt = settings.SECRET_KEY[:16]  # Use part of secret as salt
        return hashlib.sha256(f"{salt}:{ip}".encode()).hexdigest()

    def _parse_user_agent(self, ua_string: str) -> Dict[str, Any]:
        """Parse user agent into structured data."""
        if not ua_string:
            return {}

        try:
            ua = parse_user_agent(ua_string)
            return {
                "browser": ua.browser.family,
                "browser_version": ua.browser.version_string,
                "os": ua.os.family,
                "os_version": ua.os.version_string,
                "device": ua.device.family,
                "device_type": "mobile" if ua.is_mobile else "tablet" if ua.is_tablet else "desktop",
                "is_bot": ua.is_bot,
            }
        except Exception:
            return {}

    async def get_or_create_session(
        self,
        db: AsyncSession,
        session_id: str,
        context: Dict[str, Any],
    ) -> AnalyticsSession:
        """Get existing session or create new one."""
        # Check cache first
        now = datetime.now(timezone.utc)

        result = await db.execute(
            select(AnalyticsSession).where(AnalyticsSession.session_id == session_id)
        )
        session = result.scalar_one_or_none()

        if session:
            # Update last activity
            session.last_activity_at = now
            session.event_count = (session.event_count or 0) + 1
            return session

        # Create new session
        ua_parsed = self._parse_user_agent(context.get("user_agent", ""))

        session = AnalyticsSession(
            session_id=session_id,
            user_id=context.get("user_id"),
            started_at=now,
            last_activity_at=now,
            landing_page=context.get("landing_page"),
            referrer=context.get("referrer"),
            referrer_domain=self._extract_domain(context.get("referrer")),
            utm_source=context.get("utm_source"),
            utm_medium=context.get("utm_medium"),
            utm_campaign=context.get("utm_campaign"),
            utm_term=context.get("utm_term"),
            utm_content=context.get("utm_content"),
            user_agent=context.get("user_agent"),
            user_agent_parsed=ua_parsed,
            viewport_width=context.get("viewport_width"),
            viewport_height=context.get("viewport_height"),
            screen_width=context.get("screen_width"),
            screen_height=context.get("screen_height"),
            device_pixel_ratio=context.get("device_pixel_ratio"),
            connection_type=context.get("connection_type"),
            connection_downlink=context.get("connection_downlink"),
            ip_hash=self._hash_ip(context.get("ip_address", "")),
            country_code=context.get("country_code"),
            region=context.get("region"),
            city=context.get("city"),
            page_count=1,
            event_count=1,
        )

        db.add(session)
        await db.flush()

        logger.debug(f"Created session: {session_id}")
        return session

    def _extract_domain(self, url: Optional[str]) -> Optional[str]:
        """Extract domain from URL."""
        if not url:
            return None
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc
        except Exception:
            return None

    def _parse_client_timestamp(self, value: Optional[str]) -> Tuple[datetime, bool]:
        """Parse ISO8601 timestamps defensively."""
        if not value:
            metrics.increment("analytics_ingest_timestamp_parse_failures")
            return datetime.now(timezone.utc), False

        candidate = value.strip()
        if not candidate:
            metrics.increment("analytics_ingest_timestamp_parse_failures")
            return datetime.now(timezone.utc), False

        normalized = candidate.replace("Z", "+00:00")
        if "+" not in normalized[-6:]:
            normalized = f"{normalized}+00:00"

        try:
            return datetime.fromisoformat(normalized), True
        except ValueError:
            metrics.increment("analytics_ingest_timestamp_parse_failures")
            logger.warning("Invalid analytics timestamp", extra={"timestamp": value})
            return datetime.now(timezone.utc), False

    async def ingest_events(
        self,
        db: AsyncSession,
        session_id: str,
        events: List[Dict[str, Any]],
        context: Dict[str, Any],
    ) -> int:
        """
        Ingest a batch of events.

        Returns number of events processed.
        """
        session = await self.get_or_create_session(db, session_id, context)

        user_id = context.get("user_id") or session.user_id
        processed = 0

        for i, event_data in enumerate(events):
            event_type = event_data.get("type", "unknown")
            event_category = event_type.split(".")[0] if "." in event_type else "custom"

            client_timestamp, timestamp_valid = self._parse_client_timestamp(event_data.get("timestamp"))
            payload = event_data.get("payload", {}) or {}
            if not timestamp_valid:
                payload = dict(payload)
                payload["_timestamp_invalid"] = True

            event = AnalyticsEvent(
                session_id=session_id,
                user_id=user_id,
                event_type=event_type,
                event_category=event_category,
                payload=payload,
                page_url=event_data.get("page_url"),
                page_route=event_data.get("page_route"),
                client_timestamp=client_timestamp,
                sequence_number=event_data.get("sequence", i),
            )

            db.add(event)
            processed += 1

            # Handle special event types with dedicated tables
            await self._process_special_event(db, session_id, user_id, event_type, event_data)

        # Update session counts
        session.event_count = (session.event_count or 0) + processed

        await db.flush()
        return processed

    async def _process_special_event(
        self,
        db: AsyncSession,
        session_id: str,
        user_id: Optional[int],
        event_type: str,
        event_data: Dict[str, Any],
    ):
        """Process events that need dedicated table storage."""
        payload = event_data.get("payload", {})

        # Search events
        if event_type == "search.query":
            query = SearchQuery(
                session_id=session_id,
                user_id=user_id,
                query_text=payload.get("query", ""),
                query_normalized=payload.get("query", "").lower().strip(),
                search_type=payload.get("search_type", "all"),
                result_count=payload.get("result_count", 0),
                had_results=payload.get("result_count", 0) > 0,
                filters=payload.get("filters", {}),
            )
            db.add(query)

        # Product views
        elif event_type == "product.view":
            view = ProductView(
                session_id=session_id,
                user_id=user_id,
                product_id=payload.get("product_id"),
                source_type=payload.get("source", "direct"),
                source_query=payload.get("source_query"),
                source_page=payload.get("source_page"),
            )
            db.add(view)

        # Cart events
        elif event_type.startswith("cart."):
            cart_event = CartEvent(
                cart_id=payload.get("cart_id", session_id),
                session_id=session_id,
                user_id=user_id,
                event_type=event_type.replace("cart.", ""),
                product_id=payload.get("product_id"),
                product_name=payload.get("product_name"),
                product_price=payload.get("product_price"),
                quantity_before=payload.get("quantity_before"),
                quantity_after=payload.get("quantity_after"),
                quantity_delta=payload.get("quantity_delta"),
                cart_item_count_after=payload.get("cart_item_count", 0),
                cart_value_after=payload.get("cart_value", 0),
                source_page=event_data.get("page_route"),
            )
            db.add(cart_event)

            # Also create snapshot for cart adds and checkout starts
            if event_type in ("cart.add", "checkout.start"):
                snapshot = CartSnapshot(
                    cart_id=payload.get("cart_id", session_id),
                    session_id=session_id,
                    user_id=user_id,
                    snapshot_type="checkout_started" if event_type == "checkout.start" else "updated",
                    items=payload.get("items", []),
                    item_count=payload.get("cart_item_count", 0),
                    subtotal=payload.get("cart_value", 0),
                )
                db.add(snapshot)

        # Error events
        elif event_type.startswith("error."):
            error = ErrorEvent(
                session_id=session_id,
                user_id=user_id,
                error_type=event_type.replace("error.", ""),
                message=payload.get("message", "Unknown error"),
                stack_trace=payload.get("stack"),
                filename=payload.get("filename"),
                line_number=payload.get("line"),
                column_number=payload.get("column"),
                page_url=event_data.get("page_url"),
                page_route=event_data.get("page_route"),
                user_action_context=payload.get("context"),
                request_url=payload.get("request_url"),
                request_method=payload.get("request_method"),
                response_status=payload.get("response_status"),
            )
            db.add(error)

    async def ingest_web_vitals(
        self,
        db: AsyncSession,
        session_id: str,
        vitals_data: Dict[str, Any],
    ):
        """Ingest Core Web Vitals measurement."""
        vital = WebVital(
            session_id=session_id,
            route=vitals_data.get("route", "/"),
            page_url=vitals_data.get("page_url"),
            lcp_ms=vitals_data.get("lcp"),
            lcp_element=vitals_data.get("lcp_element"),
            fid_ms=vitals_data.get("fid"),
            fid_event=vitals_data.get("fid_event"),
            cls=vitals_data.get("cls"),
            inp_ms=vitals_data.get("inp"),
            ttfb_ms=vitals_data.get("ttfb"),
            dns_ms=vitals_data.get("dns"),
            tcp_ms=vitals_data.get("tcp"),
            tls_ms=vitals_data.get("tls"),
            request_ms=vitals_data.get("request"),
            response_ms=vitals_data.get("response"),
            dom_interactive_ms=vitals_data.get("dom_interactive"),
            dom_complete_ms=vitals_data.get("dom_complete"),
            load_ms=vitals_data.get("load"),
            resource_count=vitals_data.get("resource_count"),
            resource_total_bytes=vitals_data.get("resource_bytes"),
            resource_cached_count=vitals_data.get("resource_cached"),
            device_type=vitals_data.get("device_type"),
            connection_type=vitals_data.get("connection_type"),
        )

        db.add(vital)
        await db.flush()

    async def ingest_replay_chunk(
        self,
        db: AsyncSession,
        session_id: str,
        chunk_data: bytes,
        chunk_index: int,
        metadata: Dict[str, Any],
    ):
        """Ingest a session replay chunk."""
        # Get or create replay record
        result = await db.execute(
            select(SessionReplay).where(SessionReplay.session_id == session_id)
        )
        replay = result.scalar_one_or_none()

        if not replay:
            replay = SessionReplay(
                session_id=session_id,
                user_id=metadata.get("user_id"),
                started_at=datetime.now(timezone.utc),
                compressed=True,
            )
            db.add(replay)
            await db.flush()

        # Compress chunk if not already compressed
        if not metadata.get("compressed", False):
            chunk_data = gzip.compress(chunk_data)

        chunk = SessionReplayChunk(
            replay_id=replay.id,
            chunk_index=chunk_index,
            data=chunk_data,
            event_count=metadata.get("event_count", 0),
            start_timestamp=metadata.get("start_timestamp", 0),
            end_timestamp=metadata.get("end_timestamp", 0),
            size_bytes=len(chunk_data),
        )

        db.add(chunk)

        # Update replay metadata
        replay.chunk_count = (replay.chunk_count or 0) + 1
        replay.total_size_bytes = (replay.total_size_bytes or 0) + len(chunk_data)
        replay.has_errors = replay.has_errors or metadata.get("has_errors", False)

        await db.flush()

    async def identify_user(
        self,
        db: AsyncSession,
        session_id: str,
        user_id: int,
    ):
        """Associate a user ID with an existing session (on login/register)."""
        result = await db.execute(
            select(AnalyticsSession).where(AnalyticsSession.session_id == session_id)
        )
        session = result.scalar_one_or_none()

        if session and not session.user_id:
            session.user_id = user_id
            await db.flush()
            logger.info(f"Identified session {session_id} as user {user_id}")


# Singleton
_service: Optional[AnalyticsIngestService] = None


def get_analytics_ingest_service() -> AnalyticsIngestService:
    global _service
    if _service is None:
        _service = AnalyticsIngestService()
    return _service
