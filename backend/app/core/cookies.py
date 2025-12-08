"""
P1-5: Cookie Management Utilities

Centralized cookie handling for secure auth token storage.
"""
from typing import Optional
from fastapi import Response
from starlette.requests import Request

from app.core.config import settings
from app.core.csrf import generate_csrf_token


# Cookie names
ACCESS_TOKEN_COOKIE = "mdm_access_token"
REFRESH_TOKEN_COOKIE = "mdm_refresh_token"
CSRF_TOKEN_COOKIE = "mdm_csrf_token"


def get_cookie_domain(request: Request) -> Optional[str]:
    """
    Get the cookie domain from settings or auto-detect from request.

    Returns None for localhost to let the browser auto-set.
    For production, returns the root domain with leading dot for cross-subdomain sharing.
    """
    if settings.COOKIE_DOMAIN:
        return settings.COOKIE_DOMAIN

    # Auto-detect from request host
    host = request.headers.get("host", "").split(":")[0]

    # Don't set domain for localhost (browser handles it)
    if host in ("localhost", "127.0.0.1"):
        return None

    # For production domains, extract root domain for cross-subdomain cookies
    # e.g., api.mdmcomics.com -> .mdmcomics.com
    parts = host.split(".")
    if len(parts) >= 2:
        # Get the root domain (last two parts)
        root_domain = ".".join(parts[-2:])
        return f".{root_domain}"

    return host


def set_auth_cookies(
    response: Response,
    request: Request,
    access_token: str,
    refresh_token: str,
) -> str:
    """
    Set authentication cookies on response.

    Sets:
    - access_token: HttpOnly, Secure cookie (not readable by JS)
    - refresh_token: HttpOnly, Secure cookie (not readable by JS)
    - csrf_token: Non-HttpOnly cookie (readable by JS for header)

    Returns the CSRF token for including in response body if needed.
    """
    domain = get_cookie_domain(request)
    csrf_token = generate_csrf_token()

    # Access token - HttpOnly (XSS protection)
    response.set_cookie(
        key=ACCESS_TOKEN_COOKIE,
        value=access_token,
        httponly=True,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        domain=domain,
        max_age=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        path="/",
    )

    # Refresh token - HttpOnly (XSS protection), longer expiry
    response.set_cookie(
        key=REFRESH_TOKEN_COOKIE,
        value=refresh_token,
        httponly=True,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        domain=domain,
        max_age=settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60,
        path="/api/auth/refresh",  # Only sent to refresh endpoint
    )

    # CSRF token - NOT HttpOnly (must be readable by JS)
    response.set_cookie(
        key=CSRF_TOKEN_COOKIE,
        value=csrf_token,
        httponly=False,  # JS needs to read this
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        domain=domain,
        max_age=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        path="/",
    )

    return csrf_token


def clear_auth_cookies(response: Response, request: Request) -> None:
    """Clear all authentication cookies."""
    domain = get_cookie_domain(request)

    for cookie_name in [ACCESS_TOKEN_COOKIE, REFRESH_TOKEN_COOKIE, CSRF_TOKEN_COOKIE]:
        response.delete_cookie(
            key=cookie_name,
            domain=domain,
            path="/" if cookie_name != REFRESH_TOKEN_COOKIE else "/api/auth/refresh",
        )


def get_access_token_from_cookie(request: Request) -> Optional[str]:
    """Extract access token from cookie."""
    return request.cookies.get(ACCESS_TOKEN_COOKIE)


def get_refresh_token_from_cookie(request: Request) -> Optional[str]:
    """Extract refresh token from cookie."""
    return request.cookies.get(REFRESH_TOKEN_COOKIE)


def get_csrf_token_from_cookie(request: Request) -> Optional[str]:
    """Extract CSRF token from cookie."""
    return request.cookies.get(CSRF_TOKEN_COOKIE)


def get_csrf_token_from_header(request: Request) -> Optional[str]:
    """Extract CSRF token from header."""
    return request.headers.get("X-CSRF-Token")
