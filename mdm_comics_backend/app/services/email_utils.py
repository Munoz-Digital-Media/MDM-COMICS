"""
Email Utilities

v1.5.0: Outreach System - Email hashing for GDPR compliance
"""
import hmac
import hashlib
from app.core.config import settings


def compute_email_hash(email: str) -> str:
    """
    Compute HMAC-SHA256 hash of email for GDPR lookup.

    Uses PII_PEPPER as secret key to prevent rainbow table attacks.
    """
    pepper = settings.PII_PEPPER or settings.SECRET_KEY
    normalized = email.strip().lower()
    return hmac.new(
        pepper.encode(),
        normalized.encode(),
        hashlib.sha256
    ).hexdigest()
