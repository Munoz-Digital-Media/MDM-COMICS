"""
Client Error Reporting API Routes
FE-ERR-001: Receives error reports from frontend for production monitoring

This endpoint accepts error reports from the frontend error tracking service
and logs them for analysis. In production, these could be forwarded to
Sentry, Datadog, or another error monitoring service.
"""
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.core.rate_limit import limiter
from app.core.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/errors", tags=["errors"])


class ErrorContext(BaseModel):
    """Browser context from error report"""
    url: Optional[str] = None
    userAgent: Optional[str] = None
    screenSize: Optional[str] = None
    timestamp: Optional[str] = None
    sessionId: Optional[str] = None
    referrer: Optional[str] = None
    online: Optional[bool] = None


class ClientErrorReport(BaseModel):
    """Error report from frontend"""
    name: str
    message: str
    stack: Optional[str] = None
    componentStack: Optional[str] = None
    context: Optional[ErrorContext] = None
    extra: Optional[Dict[str, Any]] = None
    level: Optional[str] = "error"


@router.post("/client")
@limiter.limit("30/minute")
async def report_client_error(
    request: Request,
    error_report: ClientErrorReport,
):
    """
    Receive and log client-side error reports.

    This endpoint is rate-limited to prevent abuse.
    Errors are logged with structured data for analysis.
    """
    # Extract client IP for logging
    forwarded = request.headers.get("x-forwarded-for")
    client_ip = forwarded.split(",")[0].strip() if forwarded else (
        request.client.host if request.client else "unknown"
    )

    # Build structured log entry
    log_data = {
        "type": "client_error",
        "error_name": error_report.name,
        "error_message": error_report.message,
        "level": error_report.level or "error",
        "client_ip": client_ip,
        "received_at": datetime.now(timezone.utc).isoformat(),
    }

    # Add context if available
    if error_report.context:
        log_data.update({
            "url": error_report.context.url,
            "user_agent": error_report.context.userAgent,
            "session_id": error_report.context.sessionId,
            "screen_size": error_report.context.screenSize,
        })

    # Add extra context
    if error_report.extra:
        log_data["extra"] = error_report.extra

    # Log based on level
    log_level = error_report.level or "error"
    if log_level == "error":
        logger.error(
            f"CLIENT_ERROR: {error_report.name}: {error_report.message}",
            extra=log_data
        )
        # Log stack trace separately for readability
        if error_report.stack:
            logger.error(f"CLIENT_ERROR_STACK:\n{error_report.stack}")
        if error_report.componentStack:
            logger.error(f"CLIENT_ERROR_COMPONENT_STACK:\n{error_report.componentStack}")
    elif log_level == "warning":
        logger.warning(
            f"CLIENT_WARNING: {error_report.name}: {error_report.message}",
            extra=log_data
        )
    else:
        logger.info(
            f"CLIENT_INFO: {error_report.name}: {error_report.message}",
            extra=log_data
        )

    # TODO: Forward to external error tracking service (Sentry, Datadog, etc.)
    # if settings.SENTRY_DSN:
    #     sentry_sdk.capture_message(error_report.message, level=log_level, extras=log_data)

    return {"status": "received"}
