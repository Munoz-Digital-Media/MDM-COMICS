"""
P1-5: CSRF Token Management

Implements double-submit cookie pattern for CSRF protection:
1. Server generates CSRF token and sets it as a non-HttpOnly cookie
2. Frontend reads the cookie and includes token in X-CSRF-Token header
3. Server validates that cookie value matches header value

This works because:
- Attackers can't read cookies from other domains (same-origin policy)
- Attackers can't set custom headers in cross-origin requests (CORS)
"""
import hashlib
import hmac
import secrets
import time
from typing import Optional

from app.core.config import settings


def get_csrf_secret() -> str:
    """Get CSRF secret key, deriving from main SECRET_KEY if not set."""
    if settings.CSRF_SECRET_KEY:
        return settings.CSRF_SECRET_KEY
    # Derive from main secret key
    return hashlib.sha256(f"{settings.SECRET_KEY}_csrf".encode()).hexdigest()


def generate_csrf_token() -> str:
    """
    Generate a new CSRF token.

    Format: {random_bytes}.{timestamp}.{signature}
    - random_bytes: 32 bytes of random data (hex encoded)
    - timestamp: Unix timestamp for optional expiration
    - signature: HMAC-SHA256 of random_bytes.timestamp
    """
    random_part = secrets.token_hex(32)
    timestamp = str(int(time.time()))

    # Sign the token
    message = f"{random_part}.{timestamp}"
    signature = hmac.new(
        get_csrf_secret().encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()

    return f"{random_part}.{timestamp}.{signature}"


def validate_csrf_token(token: str, max_age_seconds: int = 86400) -> bool:
    """
    Validate a CSRF token.

    Args:
        token: The CSRF token to validate
        max_age_seconds: Maximum token age in seconds (default 24 hours)

    Returns:
        True if token is valid, False otherwise
    """
    if not token:
        return False

    try:
        parts = token.split(".")
        if len(parts) != 3:
            return False

        random_part, timestamp_str, signature = parts

        # Verify signature
        message = f"{random_part}.{timestamp_str}"
        expected_signature = hmac.new(
            get_csrf_secret().encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(signature, expected_signature):
            return False

        # Check timestamp (optional expiration)
        if max_age_seconds > 0:
            timestamp = int(timestamp_str)
            if time.time() - timestamp > max_age_seconds:
                return False

        return True

    except (ValueError, TypeError):
        return False


def tokens_match(cookie_token: Optional[str], header_token: Optional[str]) -> bool:
    """
    Validate that cookie and header CSRF tokens match.

    Uses constant-time comparison to prevent timing attacks.
    """
    if not cookie_token or not header_token:
        return False

    # Both tokens must be valid and identical
    if not validate_csrf_token(cookie_token) or not validate_csrf_token(header_token):
        return False

    return hmac.compare_digest(cookie_token, header_token)
