"""
CSRF Protection Middleware

Validates CSRF tokens on state-changing requests (POST, PUT, PATCH, DELETE).
Tokens are set in cookies and must match the X-CSRF-Token header.
"""
import logging
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

logger = logging.getLogger(__name__)

# Methods that don't modify state (safe methods)
SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}

# Paths that don't require CSRF (webhooks, public APIs)
CSRF_EXEMPT_PATHS = {
    "/api/webhooks/stripe",
    "/api/webhooks/sendgrid",
    "/api/health",
    "/api/config",
    "/docs",
    "/redoc",
    "/openapi.json",
}


class CSRFMiddleware(BaseHTTPMiddleware):
    """
    CSRF protection middleware.
    
    For state-changing requests, validates that:
    1. A csrf_token cookie exists
    2. An X-CSRF-Token header is present
    3. Both values match
    
    Safe methods (GET, HEAD, OPTIONS) are allowed without CSRF validation.
    Certain paths (webhooks) are exempt from CSRF checks.
    """
    
    async def dispatch(self, request: Request, call_next) -> Response:
        # Allow safe methods
        if request.method in SAFE_METHODS:
            return await call_next(request)
        
        # Check exempt paths
        path = request.url.path
        if any(path.startswith(exempt) for exempt in CSRF_EXEMPT_PATHS):
            return await call_next(request)
        
        # Validate CSRF token
        csrf_cookie = request.cookies.get("csrf_token")
        csrf_header = request.headers.get("X-CSRF-Token")
        
        if not csrf_cookie:
            logger.warning(f"CSRF validation failed: missing cookie for {request.method} {path}")
            raise HTTPException(
                status_code=403,
                detail="CSRF validation failed: missing token"
            )
        
        if not csrf_header:
            logger.warning(f"CSRF validation failed: missing header for {request.method} {path}")
            raise HTTPException(
                status_code=403,
                detail="CSRF validation failed: missing header"
            )
        
        if csrf_cookie != csrf_header:
            logger.warning(f"CSRF validation failed: token mismatch for {request.method} {path}")
            raise HTTPException(
                status_code=403,
                detail="CSRF validation failed: token mismatch"
            )
        
        return await call_next(request)
