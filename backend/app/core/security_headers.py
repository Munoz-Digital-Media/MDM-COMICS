"""
Security headers middleware

P2-7: Content-Security-Policy and other security headers
P2-10: Additional security headers for defense in depth
"""
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.core.config import settings


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Add security headers to all responses.

    Headers added:
    - Content-Security-Policy (P2-7)
    - X-Content-Type-Options
    - X-Frame-Options
    - X-XSS-Protection (legacy browsers)
    - Referrer-Policy
    - Permissions-Policy
    """

    # CSP directives - adjust based on your frontend needs
    # This is a strict policy suitable for an API backend
    CSP_DIRECTIVES = {
        "default-src": "'self'",
        "script-src": "'self'",
        "style-src": "'self' 'unsafe-inline'",  # Allow inline styles for Swagger UI
        "img-src": "'self' data: https:",  # Allow images from HTTPS and data URIs
        "font-src": "'self'",
        "connect-src": "'self'",
        "frame-ancestors": "'none'",  # Prevent framing (clickjacking protection)
        "form-action": "'self'",
        "base-uri": "'self'",
        "object-src": "'none'",  # Disallow plugins
    }

    # For API docs (Swagger/ReDoc), we need slightly relaxed CSP
    DOCS_CSP_DIRECTIVES = {
        "default-src": "'self'",
        "script-src": "'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net",
        "style-src": "'self' 'unsafe-inline' https://cdn.jsdelivr.net",
        "img-src": "'self' data: https:",
        "font-src": "'self' https://cdn.jsdelivr.net",
        "connect-src": "'self'",
        "frame-ancestors": "'self'",
        "form-action": "'self'",
        "base-uri": "'self'",
        "object-src": "'none'",
    }

    def _build_csp(self, directives: dict) -> str:
        """Build CSP header string from directives."""
        return "; ".join(f"{key} {value}" for key, value in directives.items())

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)

        # Determine if this is a docs path
        path = request.url.path
        is_docs_path = path in ("/docs", "/redoc", "/openapi.json")

        # P2-7: Content-Security-Policy
        if is_docs_path:
            csp = self._build_csp(self.DOCS_CSP_DIRECTIVES)
        else:
            csp = self._build_csp(self.CSP_DIRECTIVES)

        response.headers["Content-Security-Policy"] = csp

        # X-Content-Type-Options: Prevent MIME type sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"

        # X-Frame-Options: Prevent clickjacking (backup for CSP frame-ancestors)
        response.headers["X-Frame-Options"] = "DENY"

        # X-XSS-Protection: Legacy XSS protection for older browsers
        # Note: Modern browsers ignore this in favor of CSP
        response.headers["X-XSS-Protection"] = "1; mode=block"

        # Referrer-Policy: Control referrer information
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        # Permissions-Policy: Disable unnecessary browser features
        response.headers["Permissions-Policy"] = (
            "camera=(), "
            "microphone=(), "
            "geolocation=(), "
            "payment=(), "
            "usb=()"
        )

        # Strict-Transport-Security: Enforce HTTPS (only in production)
        if settings.ENVIRONMENT == "production" and not settings.DEBUG:
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )

        return response
