"""
Request utility functions

Extracted from auth.py and admin_dsar.py to reduce code duplication.
"""
from typing import Optional
from fastapi import Request


def extract_client_ip(request: Request) -> Optional[str]:
    """
    Extract client IP from request, handling proxy headers.
    
    Checks X-Forwarded-For header first (for requests behind load balancers),
    then falls back to the direct client IP.
    """
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        # X-Forwarded-For can contain multiple IPs; first is the original client
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else None


def extract_user_agent(request: Request) -> Optional[str]:
    """Extract user agent string from request."""
    return request.headers.get("user-agent")
