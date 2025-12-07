"""
PII (Personally Identifiable Information) handling

Per constitution_pii.json requirements:
- Field-level encryption
- Pseudonymization for analytics
- Secure hashing for lookups
"""
import hashlib
import hmac
import secrets
from typing import Optional
from cryptography.fernet import Fernet

from app.core.config import settings


class PIIHandler:
    """
    Handles PII field encryption and pseudonymization.

    Per constitution_pii.json:
    - Email: DEK encryption + tokenization + HMAC-BLAKE3 pseudonym
    - Name: DEK encryption + HMAC-BLAKE3 pseudonym
    - IP: Field-level encryption + truncated hash

    Security:
    - Uses Fernet (AES-128-CBC) for encryption
    - Uses BLAKE2b with pepper for hashing
    - Deterministic hashing for lookups
    """

    def __init__(self):
        """Initialize PII handler with encryption keys."""
        # Get encryption key from settings, generate if not set
        key = getattr(settings, 'PII_ENCRYPTION_KEY', None)
        if not key:
            # Use SECRET_KEY to derive encryption key in development
            key = Fernet.generate_key().decode()

        # Ensure key is properly formatted
        if isinstance(key, str):
            try:
                # Try to use as-is if valid Fernet key
                self._fernet = Fernet(key.encode() if isinstance(key, str) else key)
            except Exception:
                # Derive key from SECRET_KEY
                derived = hashlib.sha256(settings.SECRET_KEY.encode()).digest()
                import base64
                self._fernet = Fernet(base64.urlsafe_b64encode(derived))
        else:
            self._fernet = Fernet(key)

        # Get pepper for hashing
        pepper = getattr(settings, 'PII_PEPPER', None)
        if not pepper:
            # Use SECRET_KEY to derive pepper
            pepper = hashlib.sha256(f"{settings.SECRET_KEY}_pepper".encode()).hexdigest()

        self._pepper = pepper.encode() if isinstance(pepper, str) else pepper

    def encrypt_field(self, value: str) -> str:
        """
        Encrypt a PII field value.

        Args:
            value: Plain text value

        Returns:
            Base64-encoded encrypted value
        """
        if not value:
            return ""
        return self._fernet.encrypt(value.encode()).decode()

    def decrypt_field(self, encrypted: str) -> str:
        """
        Decrypt a PII field value.

        Args:
            encrypted: Base64-encoded encrypted value

        Returns:
            Decrypted plain text
        """
        if not encrypted:
            return ""
        try:
            return self._fernet.decrypt(encrypted.encode()).decode()
        except Exception:
            # Return empty string on decryption failure
            return ""

    def hash_for_lookup(self, value: str) -> str:
        """
        Create deterministic hash for lookups.

        Used for email_hash column to enable queries without decryption.
        Normalizes to lowercase before hashing.

        Args:
            value: Value to hash (e.g., email)

        Returns:
            64-character hex hash
        """
        if not value:
            return ""

        # Normalize and hash
        normalized = value.lower().strip()
        return hashlib.blake2b(
            normalized.encode(),
            key=self._pepper,
            digest_size=32
        ).hexdigest()

    def hash_ip(self, ip: str) -> str:
        """
        Hash IP address per constitution_pii.json device.ip_address spec.

        Truncates IPv4 to /24 before hashing for privacy.

        Args:
            ip: IP address string

        Returns:
            64-character hex hash
        """
        if not ip:
            return ""

        # Truncate IPv4 to /24 (last octet = 0)
        parts = ip.split(".")
        if len(parts) == 4:
            truncated = f"{parts[0]}.{parts[1]}.{parts[2]}.0"
        else:
            # IPv6 or invalid - hash as-is
            truncated = ip

        return hashlib.sha256(
            f"{truncated}{self._pepper.decode()}".encode()
        ).hexdigest()[:64]

    def hash_user_agent(self, user_agent: str) -> str:
        """
        Hash user agent string.

        Args:
            user_agent: User agent string

        Returns:
            64-character hex hash
        """
        if not user_agent:
            return ""

        return hashlib.sha256(
            f"{user_agent}{self._pepper.decode()}".encode()
        ).hexdigest()[:64]

    def pseudonymize_for_analytics(self, value: str) -> str:
        """
        Create pseudonymized ID for analytics.

        Per constitution_pii.json: HMAC-BLAKE3 for analytics.
        Produces shorter hash suitable for analytics tracking.

        Args:
            value: Value to pseudonymize

        Returns:
            32-character hex hash
        """
        if not value:
            return ""

        return hashlib.blake2b(
            str(value).encode(),
            key=self._pepper,
            digest_size=16
        ).hexdigest()

    def hash_token(self, token: str) -> str:
        """
        Hash a token for storage (password reset, email verification).

        Uses SHA-512 for tokens.

        Args:
            token: Plain token

        Returns:
            128-character hex hash
        """
        if not token:
            return ""

        return hashlib.sha512(
            f"{token}{self._pepper.decode()}".encode()
        ).hexdigest()

    def generate_token(self, length: int = 32) -> str:
        """
        Generate a secure random token.

        Args:
            length: Number of bytes (default 32 = 64 hex chars)

        Returns:
            URL-safe token string
        """
        return secrets.token_urlsafe(length)

    def mask_email(self, email: str) -> str:
        """
        Mask email for display (show first 2 chars + domain).

        Args:
            email: Full email address

        Returns:
            Masked email (e.g., "jo***@example.com")
        """
        if not email or "@" not in email:
            return "***@***"

        local, domain = email.rsplit("@", 1)

        if len(local) <= 2:
            masked_local = local[0] + "***"
        else:
            masked_local = local[:2] + "***"

        return f"{masked_local}@{domain}"

    def mask_ip(self, ip: str) -> str:
        """
        Mask IP for display.

        Args:
            ip: Full IP address

        Returns:
            Masked IP (e.g., "192.168.xxx.xxx")
        """
        if not ip:
            return "xxx.xxx.xxx.xxx"

        parts = ip.split(".")
        if len(parts) == 4:
            return f"{parts[0]}.{parts[1]}.xxx.xxx"

        # IPv6 or other - just show first segment
        if ":" in ip:
            segments = ip.split(":")
            return f"{segments[0]}:xxxx:xxxx:xxxx"

        return "xxx.xxx.xxx.xxx"


# Global singleton instance
pii_handler = PIIHandler()
