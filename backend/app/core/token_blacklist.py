"""
Token blacklist for session management

P2-8: Implements token revocation capabilities
- Revoke specific tokens (logout)
- Revoke all tokens for a user (logout from all devices)
- Automatic cleanup of expired entries

Uses in-memory storage with expiry tracking.
For production scaling, consider Redis-based implementation.
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Set
from threading import Lock

from app.core.config import settings

logger = logging.getLogger(__name__)


class TokenBlacklist:
    """
    In-memory token blacklist with automatic cleanup.

    Stores:
    - Token JTIs (JWT IDs) that have been revoked
    - User IDs for "logout all" functionality
    - Expiry times for automatic cleanup
    """

    def __init__(self):
        # {jti: expiry_datetime}
        self._revoked_tokens: dict[str, datetime] = {}
        # {user_id: revoked_before_datetime} - tokens issued before this time are invalid
        self._user_revocations: dict[int, datetime] = {}
        self._lock = Lock()
        self._cleanup_task: Optional[asyncio.Task] = None

    def revoke_token(self, jti: str, expiry: datetime) -> None:
        """
        Revoke a specific token by its JTI.

        Args:
            jti: The JWT ID to revoke
            expiry: When the token expires (for cleanup)
        """
        with self._lock:
            self._revoked_tokens[jti] = expiry
            logger.info(f"Token revoked: {jti[:8]}...")

    def revoke_all_user_tokens(self, user_id: int) -> None:
        """
        Revoke all tokens for a user (logout from all devices).

        All tokens issued before now will be considered invalid.

        Args:
            user_id: The user ID to revoke all tokens for
        """
        with self._lock:
            self._user_revocations[user_id] = datetime.now(timezone.utc)
            logger.info(f"All tokens revoked for user {user_id}")

    def is_token_revoked(self, jti: Optional[str], user_id: int, issued_at: datetime) -> bool:
        """
        Check if a token has been revoked.

        Args:
            jti: The JWT ID (if present)
            user_id: The user ID from the token
            issued_at: When the token was issued

        Returns:
            True if the token is revoked
        """
        with self._lock:
            # Check specific token revocation
            if jti and jti in self._revoked_tokens:
                return True

            # Check user-wide revocation
            if user_id in self._user_revocations:
                revoked_before = self._user_revocations[user_id]
                if issued_at < revoked_before:
                    return True

        return False

    def cleanup_expired(self) -> int:
        """
        Remove expired entries from the blacklist.

        Returns:
            Number of entries removed
        """
        now = datetime.now(timezone.utc)
        removed = 0

        with self._lock:
            # Clean expired tokens
            expired_tokens = [
                jti for jti, expiry in self._revoked_tokens.items()
                if expiry < now
            ]
            for jti in expired_tokens:
                del self._revoked_tokens[jti]
                removed += 1

            # Clean old user revocations (keep for refresh token lifetime + buffer)
            max_revocation_age = timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS + 1)
            old_revocations = [
                user_id for user_id, revoked_at in self._user_revocations.items()
                if now - revoked_at > max_revocation_age
            ]
            for user_id in old_revocations:
                del self._user_revocations[user_id]
                removed += 1

        if removed > 0:
            logger.debug(f"Token blacklist cleanup: removed {removed} entries")

        return removed

    async def start_cleanup_task(self, interval_minutes: int = 60) -> None:
        """Start background cleanup task."""
        async def cleanup_loop():
            while True:
                await asyncio.sleep(interval_minutes * 60)
                self.cleanup_expired()

        self._cleanup_task = asyncio.create_task(cleanup_loop())
        logger.info(f"Token blacklist cleanup task started (interval: {interval_minutes} min)")

    async def stop_cleanup_task(self) -> None:
        """Stop background cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            logger.info("Token blacklist cleanup task stopped")

    def stats(self) -> dict:
        """Get blacklist statistics."""
        with self._lock:
            return {
                "revoked_tokens": len(self._revoked_tokens),
                "user_revocations": len(self._user_revocations),
            }


# Global singleton
token_blacklist = TokenBlacklist()
