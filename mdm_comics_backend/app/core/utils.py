"""
Core Utilities

Shared helpers used across the application.
v1.5.0: Outreach System
"""
from datetime import datetime, timezone


def utcnow() -> datetime:
    """
    Return timezone-aware UTC datetime.

    Use this instead of datetime.utcnow() which returns naive datetime.
    """
    return datetime.now(timezone.utc)
