"""
API dependencies

P1-5: Updated to support both cookie-based and header-based auth.
Cookie-based is preferred for security (HttpOnly, CSRF protected).
Header-based is kept for API compatibility and mobile apps.

P2-8: Token revocation support via blacklist checking.
"""
from datetime import datetime, timezone
from typing import Optional
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.database import get_db
from app.core.security import decode_token
from app.core.cookies import (
    get_access_token_from_cookie,
    get_csrf_token_from_cookie,
    get_csrf_token_from_header,
)
from app.core.csrf import tokens_match
from app.core.token_blacklist import token_blacklist
from app.models.user import User

# Optional bearer - doesn't fail if no Authorization header
security = HTTPBearer(auto_error=False)


def get_token_from_request(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = None,
) -> Optional[str]:
    """
    Extract access token from request.

    Priority:
    1. Authorization header (Bearer token) - for API/mobile compatibility
    2. HttpOnly cookie - for web browser security

    Returns None if no token found.
    """
    # Check header first (API compatibility)
    if credentials and credentials.credentials:
        return credentials.credentials

    # Fall back to cookie
    return get_access_token_from_cookie(request)


def validate_csrf_for_mutation(request: Request) -> bool:
    """
    Validate CSRF token for state-changing requests.

    Only required for cookie-based auth on mutation endpoints.
    Returns True if:
    - Request uses header-based auth (no CSRF needed)
    - Request is a GET/HEAD/OPTIONS (safe methods)
    - CSRF tokens match (cookie == header)
    """
    # Safe methods don't need CSRF
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return True

    # Check if using cookie-based auth
    cookie_token = get_access_token_from_cookie(request)
    if not cookie_token:
        # Using header-based auth, no CSRF needed
        return True

    # Cookie-based auth requires CSRF validation
    csrf_cookie = get_csrf_token_from_cookie(request)
    csrf_header = get_csrf_token_from_header(request)

    return tokens_match(csrf_cookie, csrf_header)


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: AsyncSession = Depends(get_db)
) -> User:
    """
    Get current authenticated user.

    P1-5: Supports both cookie and header-based authentication.
    For cookie-based auth, validates CSRF token on mutations.
    """
    token = get_token_from_request(request, credentials)

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )

    # Validate CSRF for mutations with cookie auth
    if not validate_csrf_for_mutation(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF token missing or invalid"
        )

    payload = decode_token(token)

    if not payload or payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token"
        )

    user_id = payload.get("sub")

    # P2-8: Check if token has been revoked
    jti = payload.get("jti")
    iat = payload.get("iat")
    issued_at = datetime.fromtimestamp(iat, tz=timezone.utc) if iat else datetime.now(timezone.utc)

    if token_blacklist.is_token_revoked(jti, int(user_id), issued_at):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has been revoked"
        )

    result = await db.execute(select(User).where(User.id == int(user_id)))
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account is disabled"
        )

    return user


async def get_current_admin(user: User = Depends(get_current_user)) -> User:
    """Require admin user"""
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return user


async def get_optional_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: AsyncSession = Depends(get_db)
) -> User | None:
    """Get current user if authenticated, None otherwise"""
    token = get_token_from_request(request, credentials)

    if not token:
        return None

    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        return None

    user_id = payload.get("sub")
    result = await db.execute(select(User).where(User.id == int(user_id)))
    return result.scalar_one_or_none()
