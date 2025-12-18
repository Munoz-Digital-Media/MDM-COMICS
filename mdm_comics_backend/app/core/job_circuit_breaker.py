"""
Per-Job Circuit Breaker Wrapper v1.0.0

Document ID: PC-OPT-2024-001 Phase 5
Status: APPROVED

Provides job-specific circuit breaker isolation to prevent one failing job
from affecting others. Wraps the core CircuitBreaker with job-specific
configuration and isolation guarantees.

Problem:
- Global circuit breaker state can cause all 4 PriceCharting jobs to fail
  when only one encounters issues
- Lack of job-specific failure tracking

Solution:
- Each job gets its own isolated circuit breaker instance
- Configuration tuned per job type (match vs sync)
- Automatic DB state persistence
- Isolated failure counting

Per constitution_devops_doctrine.json:
- "circuit_breakers": true
- "auto_rollback_on_slo_breach": true
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    log_circuit_event_to_audit,
)

logger = logging.getLogger(__name__)


@dataclass
class JobCircuitConfig:
    """Configuration for a job-specific circuit breaker."""
    failure_threshold: int = 5  # Consecutive failures to open
    recovery_timeout: int = 60  # Base seconds before retry
    error_rate_threshold: float = 0.5  # Error rate to trigger open
    max_backoff_minutes: int = 15  # Maximum backoff time


# Pre-defined configurations per job type
JOB_CONFIGS = {
    # Match jobs: More lenient (fewer failures expected, more tolerant)
    "funko_pricecharting_match": JobCircuitConfig(
        failure_threshold=8,
        recovery_timeout=60,
        error_rate_threshold=0.4,
        max_backoff_minutes=10,
    ),
    "comic_pricecharting_match": JobCircuitConfig(
        failure_threshold=8,
        recovery_timeout=60,
        error_rate_threshold=0.4,
        max_backoff_minutes=10,
    ),
    # Sync jobs: Stricter (API quota-critical, faster recovery needed)
    "funko_price_sync": JobCircuitConfig(
        failure_threshold=5,
        recovery_timeout=45,
        error_rate_threshold=0.3,
        max_backoff_minutes=8,
    ),
    "comic_price_sync": JobCircuitConfig(
        failure_threshold=5,
        recovery_timeout=45,
        error_rate_threshold=0.3,
        max_backoff_minutes=8,
    ),
}


class JobCircuitBreaker:
    """
    Job-specific circuit breaker wrapper with isolation guarantees.

    Usage:
        async with AsyncSessionLocal() as db:
            jcb = JobCircuitBreaker("funko_pricecharting_match")
            await jcb.restore_from_db(db)

            # Use in job
            try:
                result = await jcb.execute(api_call_func)
            except CircuitOpenError:
                # Circuit is open, skip this item
                pass

            # Persist state after batch
            await jcb.save_to_db(db)
    """

    def __init__(self, job_name: str, config: JobCircuitConfig = None):
        """
        Initialize job-specific circuit breaker.

        Args:
            job_name: Unique job identifier
            config: Optional custom config (defaults to JOB_CONFIGS)
        """
        self.job_name = job_name
        self.config = config or JOB_CONFIGS.get(job_name, JobCircuitConfig())

        # Create underlying circuit breaker with job-specific settings
        max_backoff_mult = (self.config.max_backoff_minutes * 60) // self.config.recovery_timeout
        self._breaker = CircuitBreaker(
            name=f"job_{job_name}",
            failure_threshold=self.config.failure_threshold,
            recovery_timeout=self.config.recovery_timeout,
            error_rate_threshold=self.config.error_rate_threshold,
        )
        self._breaker.MAX_BACKOFF_MULTIPLIER = max_backoff_mult

        # Track if state was restored from DB
        self._restored_from_db = False

    @property
    def state(self) -> CircuitState:
        """Current circuit state."""
        return self._breaker.state

    @property
    def is_open(self) -> bool:
        """Check if circuit is currently open (blocking calls)."""
        return self._breaker.state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (allowing calls)."""
        return self._breaker.state == CircuitState.CLOSED

    @property
    def failure_count(self) -> int:
        """Current failure count."""
        return self._breaker.failure_count

    @property
    def backoff_multiplier(self) -> int:
        """Current backoff multiplier."""
        return self._breaker.backoff_multiplier

    def is_call_permitted(self) -> bool:
        """Check if a call is currently permitted."""
        return self._breaker.is_call_permitted()

    def get_retry_after_seconds(self) -> float:
        """Get seconds until circuit will attempt reset."""
        return self._breaker.get_retry_after_seconds()

    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit protection.

        Args:
            func: Async function to execute
            *args, **kwargs: Arguments for func

        Returns:
            Result from func

        Raises:
            CircuitOpenError: If circuit is open
        """
        return await self._breaker.execute(func, *args, **kwargs)

    def record_success(self):
        """Manually record a successful call."""
        self._breaker._on_success()

    def record_failure(self, error: Exception = None):
        """Manually record a failed call."""
        self._breaker._on_failure(error or Exception("Manual failure"))

    async def restore_from_db(self, db: AsyncSession) -> None:
        """
        Restore circuit state from database checkpoint.

        Args:
            db: Database session
        """
        result = await db.execute(text("""
            SELECT circuit_state, circuit_failure_count,
                   circuit_last_failure, circuit_backoff_multiplier
            FROM pipeline_checkpoints
            WHERE job_name = :job_name
        """), {"job_name": self.job_name})
        row = result.fetchone()

        if row:
            state_str = row[0] or "CLOSED"
            self._breaker._state = CircuitState(state_str)
            self._breaker.failure_count = row[1] or 0
            self._breaker.last_failure_time = row[2]
            self._breaker.backoff_multiplier = row[3] or 1

            logger.info(
                f"[JobCircuit:{self.job_name}] Restored - "
                f"state={state_str}, failures={self._breaker.failure_count}, "
                f"backoff={self._breaker.backoff_multiplier}x"
            )
        else:
            logger.info(
                f"[JobCircuit:{self.job_name}] No checkpoint found, starting fresh"
            )

        self._restored_from_db = True

    async def save_to_db(self, db: AsyncSession) -> None:
        """
        Save circuit state to database checkpoint.

        Args:
            db: Database session
        """
        await db.execute(text("""
            UPDATE pipeline_checkpoints
            SET circuit_state = :state,
                circuit_failure_count = :failures,
                circuit_last_failure = :last_failure,
                circuit_backoff_multiplier = :backoff,
                updated_at = NOW()
            WHERE job_name = :job_name
        """), {
            "job_name": self.job_name,
            "state": self._breaker._state.value,
            "failures": self._breaker.failure_count,
            "last_failure": self._breaker.last_failure_time,
            "backoff": self._breaker.backoff_multiplier,
        })
        await db.commit()

        logger.debug(
            f"[JobCircuit:{self.job_name}] Saved - "
            f"state={self._breaker._state.value}, failures={self._breaker.failure_count}"
        )

    async def log_state_change(self, db: AsyncSession, action: str) -> None:
        """
        Log circuit state change to audit trail.

        Args:
            db: Database session
            action: Event type (CIRCUIT_OPENED, CIRCUIT_CLOSED, etc.)
        """
        await log_circuit_event_to_audit(
            db,
            action=action,
            job_name=self.job_name,
            details={
                "state": self._breaker._state.value,
                "failure_count": self._breaker.failure_count,
                "backoff_multiplier": self._breaker.backoff_multiplier,
                "retry_after_seconds": self.get_retry_after_seconds(),
            }
        )
        await db.commit()

    def reset(self) -> None:
        """Manually reset circuit to CLOSED state."""
        self._breaker.reset()
        logger.info(f"[JobCircuit:{self.job_name}] Manually reset to CLOSED")

    def get_status(self) -> dict:
        """Get current circuit status for monitoring."""
        return {
            "job_name": self.job_name,
            "state": self._breaker._state.value,
            "failure_count": self._breaker.failure_count,
            "success_count": self._breaker.success_count,
            "backoff_multiplier": self._breaker.backoff_multiplier,
            "retry_after_seconds": self.get_retry_after_seconds(),
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "recovery_timeout": self.config.recovery_timeout,
                "error_rate_threshold": self.config.error_rate_threshold,
                "max_backoff_minutes": self.config.max_backoff_minutes,
            },
            "metrics": self._breaker.get_metrics(),
        }


# Registry of job circuit breakers
_job_circuits: dict[str, JobCircuitBreaker] = {}


def get_job_circuit_breaker(job_name: str) -> JobCircuitBreaker:
    """
    Get or create a job-specific circuit breaker.

    Args:
        job_name: Job identifier

    Returns:
        JobCircuitBreaker instance
    """
    if job_name not in _job_circuits:
        _job_circuits[job_name] = JobCircuitBreaker(job_name)
    return _job_circuits[job_name]


def get_all_job_circuits() -> dict[str, JobCircuitBreaker]:
    """Get all registered job circuit breakers."""
    return _job_circuits.copy()


async def get_job_circuit_from_db(
    db: AsyncSession,
    job_name: str
) -> JobCircuitBreaker:
    """
    Get job circuit breaker with state restored from database.

    Args:
        db: Database session
        job_name: Job identifier

    Returns:
        JobCircuitBreaker with DB state restored
    """
    jcb = get_job_circuit_breaker(job_name)
    if not jcb._restored_from_db:
        await jcb.restore_from_db(db)
    return jcb
