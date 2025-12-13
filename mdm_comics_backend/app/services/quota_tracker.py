"""
Database-Backed Quota Tracker v1.10.0

Multi-worker safe rate limiting with atomic database operations.

Per constitution_telemetry.json: Defensive telemetry for rate limiting.
Per constitution_observability.json: Metrics for throttling decisions.

Review Notes Applied:
- Atomic increment with `UPDATE ... WHERE requests_today < daily_limit RETURNING`
- Rolling window for daily reset (no nightly job needed)
- Both per-second and per-day bucket tracking
- Metrics exposure for auditing
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

from sqlalchemy import select, update, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.source_quota import SourceQuota

logger = logging.getLogger(__name__)


@dataclass
class QuotaConfig:
    """Configuration for a source's rate limits."""
    source_name: str
    daily_limit: int
    requests_per_second: float
    burst_limit: int = 5  # Max requests before enforcing per-second limit


@dataclass
class QuotaStatus:
    """Current quota status for a source."""
    source_name: str
    requests_today: int
    daily_limit: int
    remaining_today: int
    requests_per_second: float
    is_healthy: bool
    circuit_state: str
    last_request_at: Optional[datetime]
    can_request: bool
    wait_seconds: float  # Seconds to wait before next request


# Default quota configurations for known sources
DEFAULT_QUOTAS: Dict[str, QuotaConfig] = {
    "metron": QuotaConfig(
        source_name="metron",
        daily_limit=172800,  # 2 req/sec * 86400 sec
        requests_per_second=2.0,
        burst_limit=5,
    ),
    "comicvine": QuotaConfig(
        source_name="comicvine",
        daily_limit=4800,  # 200 req/hr * 24 hr
        requests_per_second=0.055,  # ~200/hr = 0.055/sec
        burst_limit=3,
    ),
    "comicbookrealm": QuotaConfig(
        source_name="comicbookrealm",
        daily_limit=43200,  # 0.5 req/sec * 86400 sec
        requests_per_second=0.5,
        burst_limit=2,
    ),
    "mycomicshop": QuotaConfig(
        source_name="mycomicshop",
        daily_limit=25920,  # 0.3 req/sec * 86400 sec
        requests_per_second=0.3,
        burst_limit=2,
    ),
    "gradingtool": QuotaConfig(
        source_name="gradingtool",
        daily_limit=8640,  # 0.1 req/sec * 86400 sec
        requests_per_second=0.1,
        burst_limit=1,
    ),
}


class QuotaTracker:
    """
    Database-backed quota tracker with atomic operations.

    Designed for multi-worker deployments where in-memory tracking
    would allow quota bursts across workers.

    Usage:
        tracker = QuotaTracker()

        # Check if we can make a request
        can_request, wait_time = await tracker.can_acquire(db, "metron")
        if not can_request:
            await asyncio.sleep(wait_time)

        # Acquire quota (atomic)
        acquired = await tracker.acquire(db, "metron")
        if acquired:
            # Make request
            ...
            await tracker.record_success(db, "metron")
        else:
            # Quota exhausted
            ...
    """

    def __init__(self):
        self._local_rate_limiters: Dict[str, float] = {}  # source -> last_request_time
        self._lock = asyncio.Lock()

    async def ensure_source_exists(
        self,
        db: AsyncSession,
        source_name: str,
        config: Optional[QuotaConfig] = None
    ) -> SourceQuota:
        """
        Ensure a source quota record exists, creating if needed.

        Uses INSERT ... ON CONFLICT DO NOTHING for atomicity.
        """
        if config is None:
            config = DEFAULT_QUOTAS.get(source_name)
            if config is None:
                raise ValueError(f"Unknown source '{source_name}' and no config provided")

        # Upsert with ON CONFLICT
        stmt = insert(SourceQuota).values(
            source_name=config.source_name,
            daily_limit=config.daily_limit,
            requests_per_second=float(config.requests_per_second),
            requests_today=0,
            is_healthy=True,
            consecutive_failures=0,
            circuit_state="closed",
        ).on_conflict_do_nothing(index_elements=["source_name"])

        await db.execute(stmt)
        await db.commit()

        # Fetch the record
        result = await db.execute(
            select(SourceQuota).where(SourceQuota.source_name == source_name)
        )
        return result.scalar_one()

    async def get_status(self, db: AsyncSession, source_name: str) -> QuotaStatus:
        """Get current quota status for a source."""
        quota = await self.ensure_source_exists(db, source_name)

        # Check if we need to reset (rolling 24-hour window)
        now = datetime.now(timezone.utc)
        if quota.last_reset_at:
            hours_since_reset = (now - quota.last_reset_at).total_seconds() / 3600
            if hours_since_reset >= 24:
                # Reset the counter
                await self._reset_daily_quota(db, source_name)
                quota = await self.ensure_source_exists(db, source_name)

        # Calculate wait time for per-second limit
        wait_seconds = 0.0
        if quota.last_request_at:
            min_interval = 1.0 / float(quota.requests_per_second)
            elapsed = (now - quota.last_request_at).total_seconds()
            if elapsed < min_interval:
                wait_seconds = min_interval - elapsed

        # Check if quota allows request
        can_request = (
            quota.is_healthy and
            quota.circuit_state != "open" and
            quota.requests_today < quota.daily_limit
        )

        return QuotaStatus(
            source_name=quota.source_name,
            requests_today=quota.requests_today,
            daily_limit=quota.daily_limit,
            remaining_today=quota.daily_limit - quota.requests_today,
            requests_per_second=float(quota.requests_per_second),
            is_healthy=quota.is_healthy,
            circuit_state=quota.circuit_state,
            last_request_at=quota.last_request_at,
            can_request=can_request,
            wait_seconds=wait_seconds,
        )

    async def can_acquire(
        self,
        db: AsyncSession,
        source_name: str
    ) -> Tuple[bool, float]:
        """
        Check if a request can be made to the source.

        Returns:
            Tuple of (can_request, wait_seconds)
        """
        status = await self.get_status(db, source_name)
        return status.can_request, status.wait_seconds

    async def acquire(self, db: AsyncSession, source_name: str) -> bool:
        """
        Atomically acquire a quota slot.

        Uses atomic UPDATE with condition to prevent over-quota.

        Returns:
            True if quota acquired, False if exhausted or unhealthy.
        """
        async with self._lock:
            # Ensure source exists
            await self.ensure_source_exists(db, source_name)

            now = datetime.now(timezone.utc)

            # Atomic increment with conditions
            # Only succeeds if: healthy, circuit not open, under daily limit
            stmt = (
                update(SourceQuota)
                .where(
                    SourceQuota.source_name == source_name,
                    SourceQuota.is_healthy == True,
                    SourceQuota.circuit_state != "open",
                    SourceQuota.requests_today < SourceQuota.daily_limit,
                )
                .values(
                    requests_today=SourceQuota.requests_today + 1,
                    last_request_at=now,
                    updated_at=now,
                )
                .returning(SourceQuota.id)
            )

            result = await db.execute(stmt)
            await db.commit()

            # Check if update succeeded
            row = result.fetchone()
            if row:
                logger.debug(f"[QUOTA] Acquired slot for {source_name}")
                return True
            else:
                logger.warning(f"[QUOTA] Failed to acquire slot for {source_name}")
                return False

    async def record_success(self, db: AsyncSession, source_name: str) -> None:
        """Record a successful request (for circuit breaker)."""
        now = datetime.now(timezone.utc)

        stmt = (
            update(SourceQuota)
            .where(SourceQuota.source_name == source_name)
            .values(
                consecutive_failures=0,
                last_success_at=now,
                is_healthy=True,
                # If half_open, close the circuit
                circuit_state=text(
                    "CASE WHEN circuit_state = 'half_open' THEN 'closed' ELSE circuit_state END"
                ),
                updated_at=now,
            )
        )
        await db.execute(stmt)
        await db.commit()

    async def record_failure(
        self,
        db: AsyncSession,
        source_name: str,
        failure_threshold: int = 5
    ) -> None:
        """
        Record a failed request (for circuit breaker).

        Opens circuit after consecutive failures exceed threshold.
        """
        now = datetime.now(timezone.utc)

        # First, increment failure count
        stmt = (
            update(SourceQuota)
            .where(SourceQuota.source_name == source_name)
            .values(
                consecutive_failures=SourceQuota.consecutive_failures + 1,
                last_failure_at=now,
                updated_at=now,
            )
            .returning(SourceQuota.consecutive_failures)
        )
        result = await db.execute(stmt)
        row = result.fetchone()

        if row and row[0] >= failure_threshold:
            # Open the circuit
            stmt = (
                update(SourceQuota)
                .where(SourceQuota.source_name == source_name)
                .values(
                    circuit_state="open",
                    circuit_opened_at=now,
                    is_healthy=False,
                    updated_at=now,
                )
            )
            await db.execute(stmt)
            logger.warning(f"[CIRCUIT] Opened circuit for {source_name} after {row[0]} failures")

        await db.commit()

    async def try_half_open(
        self,
        db: AsyncSession,
        source_name: str,
        recovery_seconds: float = 300.0
    ) -> bool:
        """
        Try to move circuit from open to half_open for testing.

        Returns:
            True if moved to half_open, False if not ready.
        """
        now = datetime.now(timezone.utc)

        # Only transition if circuit has been open long enough
        stmt = (
            update(SourceQuota)
            .where(
                SourceQuota.source_name == source_name,
                SourceQuota.circuit_state == "open",
                SourceQuota.circuit_opened_at < now - timedelta(seconds=recovery_seconds),
            )
            .values(
                circuit_state="half_open",
                consecutive_failures=0,  # Reset for fresh test
                updated_at=now,
            )
            .returning(SourceQuota.id)
        )

        result = await db.execute(stmt)
        await db.commit()

        row = result.fetchone()
        if row:
            logger.info(f"[CIRCUIT] Moved {source_name} to half_open for testing")
            return True
        return False

    async def _reset_daily_quota(self, db: AsyncSession, source_name: str) -> None:
        """Reset daily quota counter (rolling 24-hour window)."""
        now = datetime.now(timezone.utc)

        stmt = (
            update(SourceQuota)
            .where(SourceQuota.source_name == source_name)
            .values(
                requests_today=0,
                last_reset_at=now,
                updated_at=now,
            )
        )
        await db.execute(stmt)
        await db.commit()
        logger.info(f"[QUOTA] Reset daily quota for {source_name}")

    async def get_all_statuses(self, db: AsyncSession) -> Dict[str, QuotaStatus]:
        """Get quota status for all registered sources."""
        result = await db.execute(select(SourceQuota))
        quotas = result.scalars().all()

        statuses = {}
        for quota in quotas:
            statuses[quota.source_name] = await self.get_status(db, quota.source_name)

        return statuses

    async def wait_for_quota(
        self,
        db: AsyncSession,
        source_name: str,
        max_wait: float = 60.0
    ) -> bool:
        """
        Wait until quota is available, up to max_wait seconds.

        Returns:
            True if quota acquired, False if timed out.
        """
        start = time.monotonic()

        while time.monotonic() - start < max_wait:
            can_request, wait_seconds = await self.can_acquire(db, source_name)

            if can_request:
                return await self.acquire(db, source_name)

            # Wait the minimum of wait_seconds and remaining time
            remaining = max_wait - (time.monotonic() - start)
            sleep_time = min(wait_seconds, remaining, 1.0)

            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

        logger.warning(f"[QUOTA] Timed out waiting for {source_name} (waited {max_wait}s)")
        return False


# Global quota tracker instance
quota_tracker = QuotaTracker()
