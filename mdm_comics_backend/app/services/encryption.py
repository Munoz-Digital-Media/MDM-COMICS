"""
Encryption service for UPS Shipping Integration v1.28.0

Provides AES-256 encryption for PII fields (addresses, phone numbers, names).
Uses Fernet (symmetric encryption) with key derived from SECRET_KEY.

Per constitution_binder.json: All PII must be encrypted at rest.
"""
import base64
import hashlib
import logging
import os
import re
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from app.core.config import settings

logger = logging.getLogger(__name__)

# Salt for key derivation (should be stored securely, using app secret for simplicity)
_ENCRYPTION_SALT = b"mdm_comics_ups_shipping_v1"

# Cached Fernet instance
_fernet: Optional[Fernet] = None


def _get_fernet() -> Fernet:
    """Get or create Fernet instance with derived key."""
    global _fernet

    if _fernet is None:
        # Derive a proper 32-byte key from SECRET_KEY
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=_ENCRYPTION_SALT,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(settings.SECRET_KEY.encode()))
        _fernet = Fernet(key)

    return _fernet


def encrypt_pii(plaintext: str) -> str:
    """
    Encrypt a PII string using AES-256 (Fernet).

    Args:
        plaintext: The sensitive data to encrypt

    Returns:
        Base64-encoded encrypted string
    """
    if not plaintext:
        return ""

    try:
        fernet = _get_fernet()
        encrypted = fernet.encrypt(plaintext.encode())
        return encrypted.decode()
    except Exception as e:
        logger.error(f"Encryption failed: {e}")
        raise ValueError("Failed to encrypt sensitive data")


def decrypt_pii(ciphertext: str) -> str:
    """
    Decrypt a PII string.

    Args:
        ciphertext: Base64-encoded encrypted string

    Returns:
        Decrypted plaintext
    """
    if not ciphertext:
        return ""

    try:
        fernet = _get_fernet()
        decrypted = fernet.decrypt(ciphertext.encode())
        return decrypted.decode()
    except InvalidToken:
        logger.error("Decryption failed: Invalid token (wrong key or corrupted data)")
        raise ValueError("Failed to decrypt data - invalid token")
    except Exception as e:
        logger.error(f"Decryption failed: {e}")
        raise ValueError("Failed to decrypt sensitive data")


def hash_phone(phone: str, salt: Optional[str] = None) -> str:
    """
    Create a SHA-256 hash of a phone number for lookup.

    Normalizes the phone to digits only before hashing.

    Args:
        phone: Phone number to hash
        salt: Optional salt (defaults to settings.SECRET_KEY)

    Returns:
        SHA-256 hash prefixed with 'sha256:'
    """
    if not phone:
        return ""

    # Normalize: keep only digits
    digits = re.sub(r'\D', '', phone)

    if not digits:
        return ""

    # Use provided salt or derive from secret
    hash_salt = salt or settings.SECRET_KEY

    # Create salted hash
    salted = f"{hash_salt}:{digits}"
    full_hash = hashlib.sha256(salted.encode()).hexdigest()

    return f"sha256:{full_hash}"


def get_phone_last4(phone: str) -> str:
    """
    Get last 4 digits of a phone number for display.

    Args:
        phone: Phone number

    Returns:
        Last 4 digits or empty string
    """
    if not phone:
        return ""

    digits = re.sub(r'\D', '', phone)
    return digits[-4:] if len(digits) >= 4 else digits


def mask_email(email: str) -> str:
    """
    Mask an email for display (e.g., j***@example.com).

    Args:
        email: Email address

    Returns:
        Masked email
    """
    if not email or "@" not in email:
        return "***"

    local, domain = email.rsplit("@", 1)

    if len(local) <= 1:
        masked_local = "*"
    elif len(local) <= 3:
        masked_local = local[0] + "*" * (len(local) - 1)
    else:
        masked_local = local[0] + "***"

    return f"{masked_local}@{domain}"


def mask_address_line(address: str) -> str:
    """
    Mask an address line for display (e.g., 123 M*** S***).

    Args:
        address: Address line

    Returns:
        Masked address
    """
    if not address:
        return "***"

    words = address.split()
    masked = []

    for i, word in enumerate(words):
        if i == 0 and word.isdigit():
            # Keep street number
            masked.append(word)
        elif len(word) <= 2:
            masked.append("*" * len(word))
        else:
            masked.append(word[0] + "***")

    return " ".join(masked)


def sanitize_for_logging(text: str, max_length: int = 500) -> str:
    """
    Remove PII from text for safe logging.

    Args:
        text: Text that may contain PII
        max_length: Maximum length of result

    Returns:
        Sanitized text safe for logging
    """
    if not text:
        return ""

    sanitized = text[:max_length]

    # Patterns to redact
    patterns = [
        # Phone numbers
        (r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b', '[PHONE]'),
        (r'\b\(\d{3}\)\s*\d{3}[-.\s]?\d{4}\b', '[PHONE]'),
        (r'\b1[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b', '[PHONE]'),
        (r'\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}\b', '[PHONE]'),
        # Emails
        (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]'),
        # Postal codes (various formats)
        (r'\b\d{5}-\d{4}\b', '[ZIP]'),
        (r'\b\d{5}\b', '[ZIP]'),
        (r'\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b', '[POSTAL]'),  # UK
        (r'\b[A-Z]\d[A-Z]\s*\d[A-Z]\d\b', '[POSTAL]'),  # Canada
    ]

    for pattern, replacement in patterns:
        sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)

    return sanitized


# Environment variable for custom encryption key (optional)
ENV_ENCRYPTION_KEY = "PII_ENCRYPTION_KEY"


def get_custom_encryption_key() -> Optional[bytes]:
    """
    Get custom encryption key from environment if set.

    For production, consider using a dedicated key management service.
    """
    key = os.environ.get(ENV_ENCRYPTION_KEY)
    if key:
        # Validate it's a valid Fernet key
        try:
            Fernet(key.encode())
            return key.encode()
        except Exception:
            logger.warning("Invalid PII_ENCRYPTION_KEY format, using derived key")
    return None
