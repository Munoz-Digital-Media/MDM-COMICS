"""
Token blacklist for session management

P2-8: Implements token revocation capabilities
NASTY-003: Uses Redis for persistence across restarts (with in-memory fallback)

- Revoke specific tokens (logout)
- Revoke all tokens for a user (logout from all devices)
- Automatic cleanup of expired entries
- Redis-backed for persistence and scaling
"""
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from threading import Lock

from app.core.config import settings

logger = logging.getLogger(__name__)

# Redis key prefixes
TOKEN_BLACKLIST_PREFIX = "blacklist:token:"
USER_REVOCATION_PREFIX = "blacklist:user:"


class TokenBlacklist:
    """
    Token blacklist with Redis persistence and in-memory fallback.

    NASTY-003: Uses Redis when available for persistence across restarts
    and scaling across multiple instances. Falls back to in-memory when
    Redis is unavailable.

    Stores:
    - Token JTIs (JWT IDs) that have been revoked
    - User IDs for "logout all" functionality
    - Expiry times for automatic cleanup
    """

    def __init__(self):
        # In-memory fallback
        self._revoked_tokens: dict[str, datetime] = {}
        self._user_revocations: dict[int, datetime] = {}
        self._lock = Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._redis_available: Optional[bool] = None

    async def _get_redis(self):
        """Get Redis client if available."""
        from app.core.redis_client import get_redis
        client = await get_redis()

        if client and self._redis_available is None:
            self._redis_available = True
            logger.info("NASTY-003: Token blacklist using Redis for persistence")
        elif not client and self._redis_available is None:
            self._redis_available = False
            logger.warning(
                "NASTY-003: Token blacklist using in-memory storage only. "
                "Token revocations will NOT persist across restarts. "
                "Configure REDIS_URL for persistence."
            )

        return client

    async def revoke_token_async(self, jti: str, expiry: datetime) -> None:
        """
        Revoke a specific token by its JTI (async version).

        Args:
            jti: The JWT ID to revoke
            expiry: When the token expires (for cleanup)
        """
        redis = await self._get_redis()

        if redis:
            try:
                # Calculate TTL in seconds
                now = datetime.now(timezone.utc)
                ttl_seconds = max(int((expiry - now).total_seconds()), 1)

                await redis.setex(
                    f"{TOKEN_BLACKLIST_PREFIX}{jti}",
                    ttl_seconds,
                    expiry.isoformat()
                )
                logger.info(f"Token revoked in Redis: {jti[:8]}...")
                return
            except Exception as e:
                logger.warning(f"Redis token revoke failed, using fallback: {e}")

        # Fallback to in-memory
        with self._lock:
            self._revoked_tokens[jti] = expiry
            logger.info(f"Token revoked in memory: {jti[:8]}...")

    def revoke_token(self, jti: str, expiry: datetime) -> None:
        """
        Revoke a specific token by its JTI (sync version for compatibility).

        Note: This only uses in-memory storage. Use revoke_token_async for Redis.
        """
        with self._lock:
            self._revoked_tokens[jti] = expiry
            logger.info(f"Token revoked (sync): {jti[:8]}...")

    async def revoke_all_user_tokens_async(self, user_id: int) -> None:
        """
        Revoke all tokens for a user (logout from all devices) - async version.

        All tokens issued before now will be considered invalid.
        """
        revoked_at = datetime.now(timezone.utc)
        redis = await self._get_redis()

        if redis:
            try:
                # Store with TTL = refresh token lifetime + buffer
                ttl_seconds = (settings.REFRESH_TOKEN_EXPIRE_DAYS + 1) * 86400

                await redis.setex(
                    f"{USER_REVOCATION_PREFIX}{user_id}",
                    ttl_seconds,
                    revoked_at.isoformat()
                )
                logger.info(f"All tokens revoked for user {user_id} in Redis")
                return
            except Exception as e:
                logger.warning(f"Redis user revocation failed, using fallback: {e}")

        # Fallback to in-memory
        with self._lock:
            self._user_revocations[user_id] = revoked_at
            logger.info(f"All tokens revoked for user {user_id} in memory")

    def revoke_all_user_tokens(self, user_id: int) -> None:
        """
        Revoke all tokens for a user (sync version for compatibility).

        Note: This only uses in-memory storage. Use revoke_all_user_tokens_async for Redis.
        """
        with self._lock:
            self._user_revocations[user_id] = datetime.now(timezone.utc)
            logger.info(f"All tokens revoked for user {user_id} (sync)")

    async def is_token_revoked_async(
        self,
        jti: Optional[str],
        user_id: int,
        issued_at: datetime
    ) -> bool:
        """
        Check if a token has been revoked (async version).

        Args:
            jti: The JWT ID (if present)
            user_id: The user ID from the token
            issued_at: When the token was issued

        Returns:
            True if the token is revoked
        """
        redis = await self._get_redis()

        if redis:
            try:
                # Check specific token revocation
                if jti:
                    if await redis.exists(f"{TOKEN_BLACKLIST_PREFIX}{jti}"):
                        return True

                # Check user-wide revocation
                revoked_at_str = await redis.get(f"{USER_REVOCATION_PREFIX}{user_id}")
                if revoked_at_str:
                    revoked_at = datetime.fromisoformat(revoked_at_str)
                    if issued_at < revoked_at:
                        return True

                return False
            except Exception as e:
                logger.warning(f"Redis revocation check failed, using fallback: {e}")

        # Fallback to in-memory
        return self.is_token_revoked(jti, user_id, issued_at)

    def is_token_revoked(
        self,
        jti: Optional[str],
        user_id: int,
        issued_at: datetime
    ) -> bool:
        """
        Check if a token has been revoked (sync version - in-memory only).
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
        Remove expired entries from the in-memory blacklist.
        Redis entries expire automatically via TTL.

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

    async def stats_async(self) -> dict:
        """Get blacklist statistics including Redis if available."""
        redis = await self._get_redis()
        stats = {
            "memory_revoked_tokens": len(self._revoked_tokens),
            "memory_user_revocations": len(self._user_revocations),
            "redis_available": redis is not None,
        }

        if redis:
            try:
                # Count Redis keys (approximate)
                token_keys = await redis.keys(f"{TOKEN_BLACKLIST_PREFIX}*")
                user_keys = await redis.keys(f"{USER_REVOCATION_PREFIX}*")
                stats["redis_revoked_tokens"] = len(token_keys)
                stats["redis_user_revocations"] = len(user_keys)
            except Exception as e:
                logger.warning(f"Failed to get Redis stats: {e}")

        return stats

    def stats(self) -> dict:
        """Get in-memory blacklist statistics."""
        with self._lock:
            return {
                "revoked_tokens": len(self._revoked_tokens),
                "user_revocations": len(self._user_revocations),
            }


# Global singleton
token_blacklist = TokenBlacklist()
