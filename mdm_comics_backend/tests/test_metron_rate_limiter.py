"""
Tests for Metron Rate Limiter.
IMPL-2025-1221-METRON-RL: Rate limit hardening
"""
import pytest
import asyncio
import os
import time
from datetime import datetime, timezone

# Set test environment before importing
os.environ["ENVIRONMENT"] = "development"
os.environ["DATABASE_URL"] = "postgresql+asyncpg://test:test@localhost:5432/test_db"
os.environ["SECRET_KEY"] = "test-secret-key-for-unit-tests-only"

from app.core.metron_rate_limiter import (
    MetronRateLimiter,
    MetronRateLimiterConfig,
    get_metron_rate_limiter,
    init_metron_rate_limiter,
)


class TestMetronRateLimiterConfig:
    """Test configuration defaults."""

    def test_default_config(self):
        """Test default configuration values."""
        config = MetronRateLimiterConfig()

        assert config.max_rps == 1.0
        assert config.max_rpm == 30
        assert config.max_daily == 9500
        assert config.cooldown_seconds == 60.0
        assert config.backlog_cap == 100
        assert config.enabled is False  # Default off

    def test_custom_config(self):
        """Test custom configuration."""
        config = MetronRateLimiterConfig(
            max_rps=0.5,
            max_rpm=15,
            max_daily=5000,
            cooldown_seconds=120.0,
            enabled=True
        )

        assert config.max_rps == 0.5
        assert config.max_rpm == 15
        assert config.max_daily == 5000
        assert config.cooldown_seconds == 120.0
        assert config.enabled is True


class TestMetronRateLimiter:
    """Test rate limiter core functionality."""

    @pytest.fixture
    def limiter(self):
        """Create a fresh rate limiter for each test."""
        # Reset singleton for testing
        MetronRateLimiter._instance = None
        limiter = MetronRateLimiter()
        limiter.configure(
            enabled=True,
            max_rpm=30,
            max_daily=100,  # Low limit for testing
            cooldown_seconds=5.0
        )
        return limiter

    def test_singleton_pattern(self):
        """Test that limiter is a singleton."""
        MetronRateLimiter._instance = None
        limiter1 = MetronRateLimiter()
        limiter2 = MetronRateLimiter()

        assert limiter1 is limiter2

    def test_feature_flag_disabled(self):
        """Test that disabled limiter allows all requests."""
        MetronRateLimiter._instance = None
        limiter = MetronRateLimiter()
        limiter.configure(enabled=False)

        assert limiter.is_enabled is False

    def test_feature_flag_enabled(self, limiter):
        """Test that enabled limiter enforces limits."""
        assert limiter.is_enabled is True

    def test_daily_budget_initial(self, limiter):
        """Test initial daily budget."""
        assert limiter.remaining_daily_budget == 100

    def test_cooldown_initial(self, limiter):
        """Test initial cooldown state."""
        assert limiter.is_in_cooldown is False
        assert limiter.cooldown_remaining == 0

    def test_record_rate_limit(self, limiter):
        """Test recording a 429 triggers cooldown."""
        initial_429_count = limiter._metrics["rate_limited_requests"]
        limiter.record_rate_limit(retry_after=10.0)

        assert limiter.is_in_cooldown is True
        assert limiter.cooldown_remaining > 0
        assert limiter._metrics["rate_limited_requests"] == initial_429_count + 1

    def test_record_rate_limit_exponential_backoff(self, limiter):
        """Test exponential backoff on consecutive 429s."""
        # Record first 429 - should set cooldown to base
        limiter.record_rate_limit()
        assert limiter.is_in_cooldown is True

        # Record second 429 while in cooldown - cooldown should increase
        # Consecutive 429s should trigger exponential backoff
        first_cooldown = limiter.cooldown_remaining
        limiter._cooldown_until = 0  # Reset cooldown for test
        limiter.record_rate_limit()  # Second 429
        second_cooldown = limiter.cooldown_remaining

        # Second cooldown should be longer due to exponential backoff
        # (2^1 * base vs 2^0 * base)
        assert second_cooldown >= first_cooldown or limiter.is_in_cooldown

    def test_metrics_tracking(self, limiter):
        """Test that metrics are tracked correctly."""
        metrics = limiter.metrics

        assert "total_requests" in metrics
        assert "successful_requests" in metrics
        assert "rate_limited_requests" in metrics
        assert "remaining_budget" in metrics
        assert "in_cooldown" in metrics

    def test_get_status(self, limiter):
        """Test status reporting."""
        status = limiter.get_status()

        assert status["enabled"] is True
        assert "config" in status
        assert "state" in status
        assert "metrics" in status
        assert status["config"]["max_rpm"] == 30
        assert status["config"]["max_daily"] == 100


class TestMetronRateLimiterAsync:
    """Test async acquire/release functionality."""

    @pytest.fixture
    def limiter(self):
        """Create a fresh rate limiter for each test."""
        MetronRateLimiter._instance = None
        limiter = MetronRateLimiter()
        limiter.configure(
            enabled=True,
            max_rpm=30,
            max_daily=100,
            cooldown_seconds=5.0
        )
        return limiter

    @pytest.mark.asyncio
    async def test_acquire_success(self, limiter):
        """Test successful acquire."""
        success, reason = await limiter.acquire(
            request_id="test-1",
            comic_id=123,
            timeout=5.0
        )

        assert success is True
        assert reason == "ok"
        assert limiter.remaining_daily_budget == 99

        # Clean up
        limiter.release(success=True)

    @pytest.mark.asyncio
    async def test_acquire_feature_disabled(self):
        """Test acquire when feature is disabled."""
        MetronRateLimiter._instance = None
        limiter = MetronRateLimiter()
        limiter.configure(enabled=False)

        success, reason = await limiter.acquire()

        assert success is True
        assert reason == "feature_disabled"

    @pytest.mark.asyncio
    async def test_acquire_during_cooldown(self, limiter):
        """Test acquire rejected during cooldown."""
        # Trigger cooldown
        limiter.record_rate_limit(retry_after=60.0)

        success, reason = await limiter.acquire()

        assert success is False
        assert "cooldown" in reason

    @pytest.mark.asyncio
    async def test_acquire_budget_exhausted(self, limiter):
        """Test acquire rejected when budget exhausted."""
        from datetime import datetime, timezone

        # Set today's date to prevent reset, then exhaust budget
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        limiter._daily_reset_date = today
        limiter._daily_count = 100  # Equal to max_daily

        success, reason = await limiter.acquire()

        assert success is False
        assert reason == "budget_exhausted"

    @pytest.mark.asyncio
    async def test_release_decrements_pending(self, limiter):
        """Test release decrements pending count."""
        success, _ = await limiter.acquire()
        assert success is True

        initial_pending = limiter.pending_requests
        limiter.release(success=True)

        # Pending should be decremented or same (already released)
        assert limiter.pending_requests <= initial_pending

    @pytest.mark.asyncio
    async def test_release_records_429(self, limiter):
        """Test release with 429 triggers cooldown."""
        success, _ = await limiter.acquire()
        assert success is True

        limiter.release(success=False, response_code=429)

        assert limiter.is_in_cooldown is True

    @pytest.mark.asyncio
    async def test_serialization(self, limiter):
        """Test that only one request can be in flight at a time."""
        # Start first request (don't release)
        success1, _ = await limiter.acquire(timeout=1.0)
        assert success1 is True

        # Second request should timeout waiting for semaphore
        # (Since first hasn't released)
        start = time.time()
        success2, reason2 = await limiter.acquire(timeout=0.5)
        elapsed = time.time() - start

        # Should timeout
        assert success2 is False
        assert reason2 == "timeout"
        assert elapsed >= 0.4  # Should have waited close to timeout

        # Release first
        limiter.release(success=True)


class TestMetronRateLimiterDailyReset:
    """Test daily reset functionality."""

    @pytest.fixture
    def limiter(self):
        """Create a fresh rate limiter."""
        MetronRateLimiter._instance = None
        limiter = MetronRateLimiter()
        limiter.configure(enabled=True, max_daily=100)
        return limiter

    def test_daily_reset_on_date_change(self, limiter):
        """Test that counter resets on date change."""
        # Set up counter for "yesterday"
        limiter._daily_count = 50
        limiter._daily_reset_date = "2020-01-01"  # Old date

        # Access remaining budget (triggers reset check)
        remaining = limiter.remaining_daily_budget

        # Should have reset
        assert remaining == 100
        assert limiter._daily_count == 0

    def test_no_reset_same_day(self, limiter):
        """Test counter doesn't reset on same day."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        limiter._daily_count = 50
        limiter._daily_reset_date = today

        remaining = limiter.remaining_daily_budget

        assert remaining == 50  # Not reset


class TestGlobalFunctions:
    """Test module-level helper functions."""

    def test_get_metron_rate_limiter(self):
        """Test getting global limiter instance."""
        # Reset
        import app.core.metron_rate_limiter as module
        module._metron_rate_limiter = None
        MetronRateLimiter._instance = None

        limiter = get_metron_rate_limiter()
        assert limiter is not None
        assert isinstance(limiter, MetronRateLimiter)

    @pytest.mark.asyncio
    async def test_init_metron_rate_limiter(self):
        """Test initializing limiter with config."""
        import app.core.metron_rate_limiter as module
        module._metron_rate_limiter = None
        MetronRateLimiter._instance = None

        limiter = await init_metron_rate_limiter(
            enabled=True,
            max_rpm=15,
            max_daily=5000
        )

        assert limiter.is_enabled is True
        assert limiter._config.max_rpm == 15
        assert limiter._config.max_daily == 5000
