"""
Metron Rate Limiter - Global rate limiting for Metron API calls

IMPL-2025-1221-METRON-RL: Rate limit hardening to prevent 429 storms.

Controls:
- Max 1 concurrent request (semaphore=1) - no parallel Metron calls
- Max 30 requests per minute (token bucket RPM)
- Max 9,500 requests per day (buffer before 10k limit)
- Persistent daily counter with UTC midnight reset
- Shared cooldown after 429 response
- Feature flag gated: METRON_RL_HARDENING_ENABLED

v1.0.0 - Initial implementation
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass
class MetronRateLimiterConfig:
    """Configuration for Metron rate limiting."""
    max_rps: float = 1.0              # Max requests per second
    max_rpm: int = 30                  # Max requests per minute
    max_daily: int = 9500              # Max daily requests (buffer before 10k)
    daily_reset_utc_hour: int = 0      # UTC hour to reset daily counter
    cooldown_seconds: float = 60.0     # Cooldown after 429 response
    backlog_cap: int = 100             # Max pending requests in queue
    enabled: bool = False              # Feature flag - disabled by default


class MetronRateLimiter:
    """
    Global rate limiter for Metron API with strict serialization.

    Guarantees:
    - Max concurrency = 1 (no parallel Metron calls)
    - Max 30 calls per rolling minute
    - Max 9,500 calls per UTC day
    - Automatic cooldown after 429 responses

    Thread-safe and async-compatible.
    """

    _instance: Optional['MetronRateLimiter'] = None
    _lock: asyncio.Lock = None

    def __new__(cls):
        """Singleton pattern - one global rate limiter."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize rate limiter state."""
        if self._initialized:
            return

        self._initialized = True
        self._config = MetronRateLimiterConfig()

        # Concurrency control - semaphore = 1 for strict serialization
        self._semaphore = asyncio.Semaphore(1)

        # RPM tracking (rolling minute)
        self._minute_requests: list = []  # Timestamps of requests in current minute

        # Daily counter
        self._daily_count: int = 0
        self._daily_reset_date: Optional[str] = None  # YYYY-MM-DD in UTC

        # Cooldown state
        self._cooldown_until: float = 0.0  # Unix timestamp
        self._consecutive_429s: int = 0

        # Queue depth tracking
        self._pending_requests: int = 0

        # Persistence flag
        self._persistence_available: bool = False
        self._db_session_factory = None

        # Metrics
        self._metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "rate_limited_requests": 0,
            "budget_exhausted_rejections": 0,
            "cooldown_rejections": 0,
            "queue_depth_rejections": 0,
        }

        logger.info("[MetronRL] Rate limiter initialized (singleton)")

    def configure(
        self,
        max_rps: Optional[float] = None,
        max_rpm: Optional[int] = None,
        max_daily: Optional[int] = None,
        cooldown_seconds: Optional[float] = None,
        backlog_cap: Optional[int] = None,
        enabled: Optional[bool] = None,
        db_session_factory = None,
    ) -> None:
        """
        Configure rate limiter settings.

        Called during app startup with environment variables.
        """
        if max_rps is not None:
            self._config.max_rps = max_rps
        if max_rpm is not None:
            self._config.max_rpm = max_rpm
        if max_daily is not None:
            self._config.max_daily = max_daily
        if cooldown_seconds is not None:
            self._config.cooldown_seconds = cooldown_seconds
        if backlog_cap is not None:
            self._config.backlog_cap = backlog_cap
        if enabled is not None:
            self._config.enabled = enabled
        if db_session_factory is not None:
            self._db_session_factory = db_session_factory
            self._persistence_available = True

        logger.info(
            f"[MetronRL] Configured: enabled={self._config.enabled}, "
            f"max_rpm={self._config.max_rpm}, max_daily={self._config.max_daily}, "
            f"cooldown={self._config.cooldown_seconds}s"
        )

    @property
    def is_enabled(self) -> bool:
        """Check if rate limit hardening is enabled."""
        return self._config.enabled

    @property
    def remaining_daily_budget(self) -> int:
        """Get remaining daily budget."""
        self._check_daily_reset()
        return max(0, self._config.max_daily - self._daily_count)

    @property
    def is_in_cooldown(self) -> bool:
        """Check if currently in cooldown period."""
        return time.time() < self._cooldown_until

    @property
    def cooldown_remaining(self) -> float:
        """Get remaining cooldown time in seconds."""
        return max(0, self._cooldown_until - time.time())

    @property
    def pending_requests(self) -> int:
        """Get current queue depth."""
        return self._pending_requests

    @property
    def metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return {
            **self._metrics,
            "remaining_budget": self.remaining_daily_budget,
            "in_cooldown": self.is_in_cooldown,
            "cooldown_remaining_seconds": self.cooldown_remaining,
            "pending_requests": self._pending_requests,
            "rpm_current": self._get_rpm_count(),
        }

    def _check_daily_reset(self) -> None:
        """Check and perform daily reset if needed."""
        now_utc = datetime.now(timezone.utc)
        current_date = now_utc.strftime("%Y-%m-%d")

        if self._daily_reset_date != current_date:
            old_count = self._daily_count
            self._daily_count = 0
            self._daily_reset_date = current_date
            self._consecutive_429s = 0

            if old_count > 0:
                logger.info(
                    f"[MetronRL] Daily reset: {old_count} requests yesterday, "
                    f"new date={current_date}"
                )

    def _get_rpm_count(self) -> int:
        """Get requests in the current rolling minute."""
        now = time.time()
        cutoff = now - 60

        # Clean old entries
        self._minute_requests = [t for t in self._minute_requests if t > cutoff]

        return len(self._minute_requests)

    def _can_make_request(self) -> tuple[bool, str]:
        """
        Check if a request can be made.

        Returns (can_proceed, reason).
        """
        # Check cooldown
        if self.is_in_cooldown:
            return False, f"cooldown_{self.cooldown_remaining:.0f}s"

        # Check daily budget
        if self.remaining_daily_budget <= 0:
            return False, "budget_exhausted"

        # Check RPM
        if self._get_rpm_count() >= self._config.max_rpm:
            return False, "rpm_exceeded"

        # Check queue depth
        if self._pending_requests >= self._config.backlog_cap:
            return False, "queue_full"

        return True, "ok"

    async def acquire(
        self,
        request_id: Optional[str] = None,
        comic_id: Optional[int] = None,
        timeout: float = 30.0,
    ) -> tuple[bool, str]:
        """
        Acquire permission to make a Metron request.

        Blocks until slot is available or timeout/rejection.

        Args:
            request_id: Unique request identifier for logging
            comic_id: Comic being enriched (for logging)
            timeout: Max seconds to wait for slot

        Returns:
            (success, reason) - True if request can proceed
        """
        if not self._config.enabled:
            # Feature flag off - allow request (legacy behavior)
            return True, "feature_disabled"

        request_id = request_id or str(uuid4())[:8]
        self._metrics["total_requests"] += 1

        # Pre-flight check before queuing
        can_proceed, reason = self._can_make_request()
        if not can_proceed:
            if reason == "budget_exhausted":
                self._metrics["budget_exhausted_rejections"] += 1
            elif reason.startswith("cooldown"):
                self._metrics["cooldown_rejections"] += 1
            elif reason == "queue_full":
                self._metrics["queue_depth_rejections"] += 1

            logger.debug(
                f"[MetronRL] REJECT {request_id} comic={comic_id}: {reason}"
            )
            return False, reason

        # Queue for semaphore
        self._pending_requests += 1

        try:
            # Wait for semaphore with timeout
            try:
                acquired = await asyncio.wait_for(
                    self._semaphore.acquire(),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                self._pending_requests -= 1
                logger.warning(
                    f"[MetronRL] TIMEOUT {request_id} comic={comic_id}: "
                    f"waited {timeout}s for slot"
                )
                return False, "timeout"

            # Recheck after acquiring semaphore (conditions may have changed)
            can_proceed, reason = self._can_make_request()
            if not can_proceed:
                self._semaphore.release()
                self._pending_requests -= 1
                logger.debug(
                    f"[MetronRL] REJECT {request_id} comic={comic_id} (post-acquire): {reason}"
                )
                return False, reason

            # Enforce RPS delay
            await asyncio.sleep(1.0 / self._config.max_rps)

            # Record request
            self._minute_requests.append(time.time())
            self._daily_count += 1
            self._check_daily_reset()

            # Persist counter if available
            if self._persistence_available:
                await self._persist_counter()

            logger.debug(
                f"[MetronRL] ALLOW {request_id} comic={comic_id}: "
                f"daily={self._daily_count}/{self._config.max_daily}, "
                f"rpm={self._get_rpm_count()}/{self._config.max_rpm}"
            )

            return True, "ok"

        except Exception as e:
            self._pending_requests -= 1
            logger.error(f"[MetronRL] Error in acquire: {e}")
            return False, f"error: {e}"

    def release(self, success: bool = True, response_code: Optional[int] = None) -> None:
        """
        Release the request slot after completion.

        Args:
            success: Whether the request succeeded
            response_code: HTTP response code (for 429 detection)
        """
        if not self._config.enabled:
            return

        self._pending_requests = max(0, self._pending_requests - 1)

        try:
            self._semaphore.release()
        except ValueError:
            # Semaphore not acquired (edge case)
            pass

        if success:
            self._metrics["successful_requests"] += 1
            self._consecutive_429s = 0

        if response_code == 429:
            self._record_rate_limit()

    def _record_rate_limit(self, retry_after: Optional[float] = None) -> None:
        """Record a 429 rate limit response."""
        self._consecutive_429s += 1
        self._metrics["rate_limited_requests"] += 1

        # Calculate cooldown with exponential backoff
        if retry_after:
            cooldown = retry_after
        else:
            cooldown = min(
                300,  # Cap at 5 minutes
                self._config.cooldown_seconds * (2 ** (self._consecutive_429s - 1))
            )

        self._cooldown_until = time.time() + cooldown

        logger.warning(
            f"[MetronRL] 429 RECEIVED (#{self._consecutive_429s}): "
            f"cooldown {cooldown:.0f}s, budget remaining={self.remaining_daily_budget}"
        )

    def record_rate_limit(self, retry_after: Optional[float] = None) -> None:
        """Public method to record 429 from external callers."""
        self._record_rate_limit(retry_after)

    async def _persist_counter(self) -> None:
        """Persist daily counter to database."""
        if not self._db_session_factory:
            return

        try:
            from sqlalchemy import text

            async with self._db_session_factory() as db:
                await db.execute(text("""
                    INSERT INTO metron_rate_budget (
                        date_utc, request_count, last_updated
                    ) VALUES (
                        :date, :count, NOW()
                    )
                    ON CONFLICT (date_utc) DO UPDATE SET
                        request_count = :count,
                        last_updated = NOW()
                """), {
                    "date": self._daily_reset_date,
                    "count": self._daily_count,
                })
                await db.commit()
        except Exception as e:
            # Non-fatal - log and continue with in-memory counter
            logger.warning(f"[MetronRL] Failed to persist counter: {e}")

    async def load_from_persistence(self) -> None:
        """Load daily counter from database on startup."""
        if not self._db_session_factory:
            return

        try:
            from sqlalchemy import text

            self._check_daily_reset()

            async with self._db_session_factory() as db:
                result = await db.execute(text("""
                    SELECT request_count FROM metron_rate_budget
                    WHERE date_utc = :date
                """), {"date": self._daily_reset_date})

                row = result.fetchone()
                if row:
                    self._daily_count = row.request_count
                    logger.info(
                        f"[MetronRL] Loaded from persistence: "
                        f"date={self._daily_reset_date}, count={self._daily_count}"
                    )
        except Exception as e:
            logger.warning(f"[MetronRL] Failed to load from persistence: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive rate limiter status."""
        return {
            "enabled": self._config.enabled,
            "config": {
                "max_rps": self._config.max_rps,
                "max_rpm": self._config.max_rpm,
                "max_daily": self._config.max_daily,
                "cooldown_seconds": self._config.cooldown_seconds,
                "backlog_cap": self._config.backlog_cap,
            },
            "state": {
                "daily_count": self._daily_count,
                "daily_reset_date": self._daily_reset_date,
                "remaining_budget": self.remaining_daily_budget,
                "rpm_current": self._get_rpm_count(),
                "in_cooldown": self.is_in_cooldown,
                "cooldown_remaining_seconds": self.cooldown_remaining,
                "consecutive_429s": self._consecutive_429s,
                "pending_requests": self._pending_requests,
            },
            "metrics": self._metrics,
            "persistence_available": self._persistence_available,
        }


# Global singleton instance
_metron_rate_limiter: Optional[MetronRateLimiter] = None


def get_metron_rate_limiter() -> MetronRateLimiter:
    """Get the global Metron rate limiter instance."""
    global _metron_rate_limiter
    if _metron_rate_limiter is None:
        _metron_rate_limiter = MetronRateLimiter()
    return _metron_rate_limiter


async def init_metron_rate_limiter(
    db_session_factory = None,
    enabled: bool = False,
    max_rpm: int = 30,
    max_daily: int = 9500,
    cooldown_seconds: float = 60.0,
) -> MetronRateLimiter:
    """
    Initialize the Metron rate limiter with configuration.

    Called during app startup.
    """
    limiter = get_metron_rate_limiter()
    limiter.configure(
        enabled=enabled,
        max_rpm=max_rpm,
        max_daily=max_daily,
        cooldown_seconds=cooldown_seconds,
        db_session_factory=db_session_factory,
    )

    if db_session_factory and enabled:
        await limiter.load_from_persistence()

    return limiter
