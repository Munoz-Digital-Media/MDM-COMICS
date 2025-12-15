"""
Analytics API Routes

High-volume beacon endpoints for telemetry ingestion.
"""

import gzip
import json
import logging
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, Request, Response, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.api.deps import get_optional_user
from app.models.user import User
from app.services.analytics_ingest import get_analytics_ingest_service

logger = logging.getLogger(__name__)
router = APIRouter()


# ============================================================================
# REQUEST MODELS
# ============================================================================

class SessionContext(BaseModel):
    """Session initialization context."""
    session_id: str
    landing_page: Optional[str] = None
    referrer: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None
    viewport_width: Optional[int] = None
    viewport_height: Optional[int] = None
    screen_width: Optional[int] = None
    screen_height: Optional[int] = None
    device_pixel_ratio: Optional[float] = None
    connection_type: Optional[str] = None
    connection_downlink: Optional[float] = None


class EventBatch(BaseModel):
    """Batch of events to ingest."""
    session_id: str
    events: List[Dict[str, Any]]
    context: Optional[Dict[str, Any]] = Field(default_factory=dict)


class VitalsPayload(BaseModel):
    """Core Web Vitals data."""
    session_id: str
    route: str
    page_url: Optional[str] = None
    lcp: Optional[int] = None
    lcp_element: Optional[str] = None
    fid: Optional[int] = None
    fid_event: Optional[str] = None
    cls: Optional[float] = None
    inp: Optional[int] = None
    ttfb: Optional[int] = None
    # Navigation timing
    dns: Optional[int] = None
    tcp: Optional[int] = None
    tls: Optional[int] = None
    request: Optional[int] = None
    response: Optional[int] = None
    dom_interactive: Optional[int] = None
    dom_complete: Optional[int] = None
    load: Optional[int] = None
    # Resources
    resource_count: Optional[int] = None
    resource_bytes: Optional[int] = None
    resource_cached: Optional[int] = None
    # Device
    device_type: Optional[str] = None
    connection_type: Optional[str] = None


class ReplayChunkPayload(BaseModel):
    """Session replay chunk metadata."""
    session_id: str
    chunk_index: int
    event_count: int
    start_timestamp: int
    end_timestamp: int
    compressed: bool = False
    has_errors: bool = False


class IdentifyPayload(BaseModel):
    """User identification payload."""
    session_id: str


# ============================================================================
# BEACON ENDPOINTS
# ============================================================================

@router.post("/beacon/events", status_code=status.HTTP_202_ACCEPTED)
async def ingest_events(
    request: Request,
    payload: EventBatch,
    db: AsyncSession = Depends(get_db),
    user: Optional[User] = Depends(get_optional_user),
):
    """
    Ingest a batch of analytics events.

    Accepts compressed (gzip) or uncompressed JSON payloads.
    """
    service = get_analytics_ingest_service()

    # Build context from request
    context = payload.context or {}
    context["user_agent"] = request.headers.get("User-Agent")
    context["ip_address"] = request.headers.get("X-Forwarded-For", request.client.host)

    if user:
        context["user_id"] = user.id

    try:
        count = await service.ingest_events(
            db=db,
            session_id=payload.session_id,
            events=payload.events,
            context=context,
        )
        await db.commit()

        return {"accepted": True, "count": count}
    except Exception as e:
        logger.error(f"Event ingestion failed: {e}")
        await db.rollback()
        # Return success to client - don't break their experience
        return {"accepted": True, "count": 0}


@router.post("/beacon/vitals", status_code=status.HTTP_202_ACCEPTED)
async def ingest_vitals(
    payload: VitalsPayload,
    db: AsyncSession = Depends(get_db),
):
    """Ingest Core Web Vitals measurement."""
    service = get_analytics_ingest_service()

    try:
        await service.ingest_web_vitals(db, payload.session_id, payload.model_dump())
        await db.commit()
    except Exception as e:
        logger.error(f"Vitals ingestion failed: {e}")
        await db.rollback()

    return {"accepted": True}


@router.options("/beacon/replay")
async def replay_preflight():
    """
    Handle CORS preflight for replay endpoint.

    The CORSMiddleware should handle this, but adding explicit handler
    to ensure OPTIONS doesn't hit the POST handler's validation.
    """
    return Response(status_code=200)


@router.post("/beacon/replay", status_code=status.HTTP_202_ACCEPTED)
async def ingest_replay(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: Optional[User] = Depends(get_optional_user),
):
    """
    Ingest session replay chunk.

    Expects multipart or raw binary data with metadata in headers.
    """
    service = get_analytics_ingest_service()

    # Get metadata from headers
    session_id = request.headers.get("X-Session-ID")
    chunk_index = int(request.headers.get("X-Chunk-Index", 0))
    event_count = int(request.headers.get("X-Event-Count", 0))
    start_ts = int(request.headers.get("X-Start-Timestamp", 0))
    end_ts = int(request.headers.get("X-End-Timestamp", 0))
    is_compressed = request.headers.get("Content-Encoding") == "gzip"
    has_errors = request.headers.get("X-Has-Errors", "false").lower() == "true"

    if not session_id:
        raise HTTPException(status_code=400, detail="X-Session-ID header required")

    # Read body
    body = await request.body()

    metadata = {
        "user_id": user.id if user else None,
        "event_count": event_count,
        "start_timestamp": start_ts,
        "end_timestamp": end_ts,
        "compressed": is_compressed,
        "has_errors": has_errors,
    }

    try:
        await service.ingest_replay_chunk(
            db=db,
            session_id=session_id,
            chunk_data=body,
            chunk_index=chunk_index,
            metadata=metadata,
        )
        await db.commit()
    except Exception as e:
        logger.error(f"Replay ingestion failed: {e}")
        await db.rollback()

    return {"accepted": True}


@router.post("/identify", status_code=status.HTTP_200_OK)
async def identify_session(
    payload: IdentifyPayload,
    user: User = Depends(get_optional_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Associate current user with a session.

    Called on login/registration to link anonymous session to user.
    """
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")

    service = get_analytics_ingest_service()
    await service.identify_user(db, payload.session_id, user.id)
    await db.commit()

    return {"identified": True, "user_id": user.id}


# ============================================================================
# OPT-OUT ENDPOINT
# ============================================================================

@router.post("/opt-out")
async def opt_out(request: Request, response: Response):
    """
    Opt out of analytics tracking.

    Sets a cookie to disable tracking on future visits.
    """
    response.set_cookie(
        key="mdm_analytics_optout",
        value="1",
        max_age=365 * 24 * 60 * 60,  # 1 year
        httponly=True,
        secure=True,
        samesite="lax",
    )

    return {"opted_out": True}


@router.get("/opt-out/status")
async def opt_out_status(request: Request):
    """Check if user has opted out."""
    opted_out = request.cookies.get("mdm_analytics_optout") == "1"
    return {"opted_out": opted_out}
