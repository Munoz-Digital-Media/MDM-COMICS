"""
Circuit Breaker Pattern Implementation v1.0.0

Document ID: IMPL-PC-2025-12-17
Per constitution_devops_doctrine.json Section "runtime":
> "circuit_breakers": true

States:
- CLOSED: Normal operation, requests pass through
- OPEN: Failures exceeded threshold, requests blocked
- HALF_OPEN: Testing if service recovered

Per constitution_devops_doctrine.json:
- "timeouts_retries": "Edge 1s, Node 3s, background 10s"
- "auto_rollback_on_slo_breach": true
"""
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(Exception):
    """Raised when circuit breaker is OPEN and blocking requests."""

    def __init__(self, circuit_name: str, retry_after_seconds: float):
        self.circuit_name = circuit_name
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"Circuit '{circuit_name}' is OPEN. Retry after {retry_after_seconds:.0f} seconds."
        )


class CircuitBreaker:
    """
    Circuit breaker for external API calls.

    Implements the circuit breaker pattern to protect against cascading failures
    when external services are unavailable or degraded.

    Per constitution_cyberSec.json Section 5:
    > "All partner API clients implement retries with bounded budgets and circuit breakers"

    Attributes:
        name: Identifier for this circuit breaker
        failure_threshold: Number of consecutive failures before opening
        recovery_timeout: Base seconds to wait before attempting reset
        error_rate_threshold: Error rate (0.0-1.0) that triggers open
    """

    # Class-level constants
    FAILURE_THRESHOLD = 5
    RECOVERY_TIMEOUT = 60  # Base seconds before trying again
    ERROR_RATE_THRESHOLD = 0.50  # 50% error rate triggers open
    MAX_BACKOFF_MULTIPLIER = 16  # Maximum backoff: 16 * 60 = 16 minutes

    def __init__(
        self,
        name: str,
        failure_threshold: int = None,
        recovery_timeout: int = None,
        error_rate_threshold: float = None,
    ):
        """
        Initialize circuit breaker.

        Args:
            name: Unique identifier for this circuit
            failure_threshold: Override default failure threshold
            recovery_timeout: Override default recovery timeout (seconds)
            error_rate_threshold: Override default error rate threshold
        """
        self.name = name
        self.failure_threshold = failure_threshold or self.FAILURE_THRESHOLD
        self.recovery_timeout = recovery_timeout or self.RECOVERY_TIMEOUT
        self.error_rate_threshold = error_rate_threshold or self.ERROR_RATE_THRESHOLD

        # State
        self._state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.backoff_multiplier = 1

        # Metrics
        self.total_calls = 0
        self.total_failures = 0
        self.total_blocked = 0
        self.last_state_change: Optional[datetime] = None

        logger.info(f"[CircuitBreaker:{self.name}] Initialized with "
                    f"failure_threshold={self.failure_threshold}, "
                    f"recovery_timeout={self.recovery_timeout}s")

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    @state.setter
    def state(self, new_state: CircuitState):
        """Set circuit state with logging."""
        if new_state != self._state:
            old_state = self._state
            self._state = new_state
            self.last_state_change = datetime.now(timezone.utc)
            logger.info(
                f"[CircuitBreaker:{self.name}] State changed: {old_state} -> {new_state}"
            )

    def get_retry_after_seconds(self) -> float:
        """Calculate seconds until circuit will attempt reset."""
        if self._state != CircuitState.OPEN or not self.last_failure_time:
            return 0

        elapsed = (datetime.now(timezone.utc) - self.last_failure_time).total_seconds()
        wait_time = self.recovery_timeout * self.backoff_multiplier
        remaining = max(0, wait_time - elapsed)
        return remaining

    def is_call_permitted(self) -> bool:
        """Check if a call is permitted through the circuit."""
        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.HALF_OPEN:
            return True

        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
                return True
            return False

        return False

    async def execute(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute function with circuit breaker protection.

        Args:
            func: Async function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Result from func

        Raises:
            CircuitOpenError: If circuit is OPEN and blocking requests
            Exception: Re-raises any exception from func
        """
        self.total_calls += 1

        if not self.is_call_permitted():
            self.total_blocked += 1
            retry_after = self.get_retry_after_seconds()
            logger.warning(
                f"[CircuitBreaker:{self.name}] Call blocked - circuit OPEN. "
                f"Retry after {retry_after:.0f}s"
            )
            raise CircuitOpenError(self.name, retry_after)

        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure(e)
            raise

    def _on_success(self):
        """Handle successful call."""
        if self._state == CircuitState.HALF_OPEN:
            # Service recovered, close the circuit
            self.state = CircuitState.CLOSED
            self.backoff_multiplier = 1
            self.failure_count = 0
            logger.info(
                f"[CircuitBreaker:{self.name}] CLOSED - service recovered"
            )

        self.success_count += 1
        # Decay failure count on success (gradual recovery)
        self.failure_count = max(0, self.failure_count - 1)

    def _on_failure(self, error: Exception):
        """Handle failed call."""
        self.failure_count += 1
        self.total_failures += 1
        self.last_failure_time = datetime.now(timezone.utc)

        # Calculate rolling error rate
        total = self.failure_count + self.success_count
        error_rate = self.failure_count / max(total, 1)

        # Check if we should open the circuit
        should_open = (
            self.failure_count >= self.failure_threshold or
            (total >= 10 and error_rate >= self.error_rate_threshold)
        )

        if self._state == CircuitState.HALF_OPEN:
            # Failed during recovery test, back to OPEN with increased backoff
            self.state = CircuitState.OPEN
            self.backoff_multiplier = min(
                self.backoff_multiplier * 2,
                self.MAX_BACKOFF_MULTIPLIER
            )
            logger.warning(
                f"[CircuitBreaker:{self.name}] OPENED (half-open test failed) - "
                f"error={type(error).__name__}, backoff={self.backoff_multiplier}x"
            )
        elif should_open and self._state == CircuitState.CLOSED:
            self.state = CircuitState.OPEN
            self.backoff_multiplier = min(
                self.backoff_multiplier * 2,
                self.MAX_BACKOFF_MULTIPLIER
            )
            logger.warning(
                f"[CircuitBreaker:{self.name}] OPENED - "
                f"failures={self.failure_count}, error_rate={error_rate:.1%}, "
                f"backoff={self.backoff_multiplier}x, error={type(error).__name__}"
            )

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt circuit reset."""
        if not self.last_failure_time:
            return True

        elapsed = (datetime.now(timezone.utc) - self.last_failure_time).total_seconds()
        wait_time = self.recovery_timeout * self.backoff_multiplier
        return elapsed >= wait_time

    def reset(self):
        """Manually reset the circuit breaker to CLOSED state."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.backoff_multiplier = 1
        self.last_failure_time = None
        logger.info(f"[CircuitBreaker:{self.name}] Manually reset to CLOSED")

    def get_metrics(self) -> dict:
        """Get circuit breaker metrics for observability."""
        return {
            "name": self.name,
            "state": self._state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "backoff_multiplier": self.backoff_multiplier,
            "total_calls": self.total_calls,
            "total_failures": self.total_failures,
            "total_blocked": self.total_blocked,
            "retry_after_seconds": self.get_retry_after_seconds(),
            "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None,
            "last_state_change": self.last_state_change.isoformat() if self.last_state_change else None,
        }

    def to_db_state(self) -> dict:
        """Convert to database-storable state for checkpoint persistence."""
        return {
            "circuit_state": self._state.value,
            "circuit_failure_count": self.failure_count,
            "circuit_last_failure": self.last_failure_time,
            "circuit_backoff_multiplier": self.backoff_multiplier,
        }

    @classmethod
    def from_db_state(cls, name: str, db_row: dict) -> "CircuitBreaker":
        """Restore circuit breaker from database checkpoint state."""
        cb = cls(name)

        if db_row:
            state_str = db_row.get("circuit_state", "CLOSED")
            cb._state = CircuitState(state_str) if state_str else CircuitState.CLOSED
            cb.failure_count = db_row.get("circuit_failure_count", 0) or 0
            cb.last_failure_time = db_row.get("circuit_last_failure")
            cb.backoff_multiplier = db_row.get("circuit_backoff_multiplier", 1) or 1

            logger.info(
                f"[CircuitBreaker:{name}] Restored from DB - "
                f"state={cb._state.value}, failures={cb.failure_count}, "
                f"backoff={cb.backoff_multiplier}x"
            )

        return cb


# Registry of circuit breakers for global access
_circuit_breakers: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(name: str, **kwargs) -> CircuitBreaker:
    """
    Get or create a circuit breaker by name.

    Args:
        name: Unique identifier for the circuit breaker
        **kwargs: Arguments passed to CircuitBreaker constructor if creating

    Returns:
        CircuitBreaker instance
    """
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(name, **kwargs)
    return _circuit_breakers[name]


def get_all_circuit_breakers() -> dict[str, CircuitBreaker]:
    """Get all registered circuit breakers."""
    return _circuit_breakers.copy()


async def log_circuit_event_to_audit(
    db,
    action: str,
    job_name: str,
    details: dict,
) -> None:
    """
    Log circuit breaker event to self_healing_audit table.

    Per constitution_logging.json:
    > "Hash-chain logs for tamper evidence"

    Args:
        db: Database session
        action: Event type (CIRCUIT_OPENED, CIRCUIT_CLOSED, etc.)
        job_name: Job name associated with the circuit
        details: Additional event details as JSON
    """
    from sqlalchemy import text
    import json

    # Get last hash for chain
    result = await db.execute(text("""
        SELECT hash_chain FROM self_healing_audit
        ORDER BY id DESC LIMIT 1
    """))
    row = result.fetchone()
    prev_hash = row[0] if row else "GENESIS"

    # Create hash chain entry
    payload = f"{prev_hash}|{action}|{job_name}|{json.dumps(details, sort_keys=True)}"
    new_hash = hashlib.sha512(payload.encode()).hexdigest()

    # Insert audit record
    await db.execute(text("""
        INSERT INTO self_healing_audit (action, job_name, details, hash_chain)
        VALUES (:action, :job_name, :details::jsonb, :hash)
    """), {
        "action": action,
        "job_name": job_name,
        "details": json.dumps(details),
        "hash": new_hash,
    })
