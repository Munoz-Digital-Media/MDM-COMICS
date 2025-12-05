"""
P1-3: Rate Limiting Configuration

Uses SlowAPI for in-memory rate limiting (production should use Redis).
Configurable via environment variables.
"""
import logging
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.core.config import settings

logger = logging.getLogger(__name__)


def get_client_ip(request: Request) -> str:
    """
    Get client IP, respecting X-Forwarded-For for proxied requests.
    Falls back to direct IP if header not present.
    """
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        # First IP in the chain is the original client
        return forwarded.split(",")[0].strip()
    return get_remote_address(request)


# Create limiter instance
# Uses in-memory storage by default (suitable for single-instance)
# For multi-instance deployments, configure Redis:
# limiter = Limiter(key_func=get_client_ip, storage_uri="redis://localhost:6379")
limiter = Limiter(
    key_func=get_client_ip,
    enabled=settings.RATE_LIMIT_ENABLED,
    default_limits=[settings.RATE_LIMIT_DEFAULT],
)


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """
    Custom handler for rate limit exceeded errors.
    Returns structured JSON response with retry-after header.
    """
    logger.warning(
        f"Rate limit exceeded: {get_client_ip(request)} on {request.url.path}"
    )

    retry_after = exc.detail.split("per")[0].strip() if exc.detail else "1 minute"

    return JSONResponse(
        status_code=429,
        content={
            "error": "rate_limit_exceeded",
            "message": f"Too many requests. Please try again in {retry_after}.",
            "retry_after": retry_after,
        },
        headers={"Retry-After": "60"},
    )


# Pre-configured rate limit decorators for common use cases
# Usage: @rate_limit_auth on login/register endpoints
def get_auth_limit():
    """Rate limit for auth endpoints (stricter)."""
    return limiter.limit(settings.RATE_LIMIT_AUTH)


def get_checkout_limit():
    """Rate limit for checkout endpoints."""
    return limiter.limit(settings.RATE_LIMIT_CHECKOUT)


def get_default_limit():
    """Default rate limit."""
    return limiter.limit(settings.RATE_LIMIT_DEFAULT)
