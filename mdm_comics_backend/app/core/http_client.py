"""
Resilient HTTP Client for External API Calls

Per 20251207_MDM_COMICS_DATA_ACQUISITION_PIPELINE.json v1.1.0:
- Exponential backoff with jitter to prevent thundering herd
- 429 detection with Retry-After header respect
- Configurable rate limiting per-host
- Circuit breaker pattern for repeated failures
- Proper async implementation (no blocking calls)

CRITICAL: This client is designed to PREVENT API BANS.
All external API calls MUST use this client.

v2.0.0 (2024-12-18): Per-host request serialization to fix race condition
- HostLockManager ensures only ONE request per host at a time
- Pre-flight block check before queuing requests
- RateLimitExceeded exception for fail-fast on long waits
"""
import asyncio
import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Optional, Callable

import httpx

logger = logging.getLogger(__name__)


class RateLimitExceeded(Exception):
    """
    Raised when a host is rate-limited for longer than MAX_RATE_LIMIT_WAIT.

    This allows callers to fail fast and try alternative sources instead
    of blocking for extended periods (e.g., 37 minutes).
    """
    def __init__(self, host: str, wait_time: float):
        self.host = host
        self.wait_time = wait_time
        super().__init__(f"Rate limited by {host} for {wait_time:.0f}s")


class HostLockManager:
    """
    Manages per-host locks to serialize requests.

    CRITICAL: This fixes the race condition where 5 concurrent comics
    would fire requests SIMULTANEOUSLY before rate limit state propagated.

    With this, only ONE request to any given host can be in-flight at a time.
    """
    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._lock = asyncio.Lock()  # Protects _locks dict creation

    async def get_lock(self, host: str) -> asyncio.Lock:
        """Get or create a lock for a specific host."""
        async with self._lock:
            if host not in self._locks:
                self._locks[host] = asyncio.Lock()
            return self._locks[host]


# Global host lock manager - shared across all client instances
_host_locks = HostLockManager()


class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting a specific host."""
    requests_per_second: float = 2.0  # Max requests per second
    burst_limit: int = 5              # Max burst before throttling
    min_request_interval: float = 0.5  # Minimum seconds between requests
    
    
@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 5
    base_delay: float = 1.0           # Base delay in seconds
    max_delay: float = 60.0           # Maximum delay cap
    exponential_base: float = 2.0     # Exponential backoff multiplier
    jitter_factor: float = 0.5        # Random jitter (0-1)
    
    # Status codes that should trigger retry
    retryable_status_codes: tuple = (429, 500, 502, 503, 504)
    
    # Don't retry these - they're permanent failures
    fatal_status_codes: tuple = (400, 401, 403, 404)


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5        # Failures before opening circuit
    success_threshold: int = 2        # Successes to close circuit
    timeout_seconds: float = 60.0     # Time before half-open test
    

@dataclass
class HostState:
    """Tracks state for a specific host."""
    last_request_time: float = 0.0
    request_count: int = 0
    window_start: float = field(default_factory=time.time)

    # Circuit breaker state
    circuit_state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0

    # Rate limit state from server
    rate_limit_reset: Optional[float] = None
    retry_after: Optional[float] = None

    # Blocked until timestamp (for pre-flight checks)
    # This is set when we receive a 429 and should block ALL requests
    blocked_until: Optional[float] = None


class ResilientHTTPClient:
    """
    Async HTTP client with built-in resilience patterns.
    
    Features:
    - Exponential backoff with jitter
    - Per-host rate limiting
    - 429 Retry-After header respect
    - Circuit breaker pattern
    - Request deduplication (optional)
    
    Usage:
        async with ResilientHTTPClient() as client:
            response = await client.get("https://api.example.com/data")
    """
    
    def __init__(
        self,
        rate_limit_config: Optional[RateLimitConfig] = None,
        retry_config: Optional[RetryConfig] = None,
        circuit_config: Optional[CircuitBreakerConfig] = None,
        timeout: float = 30.0,
        default_headers: Optional[Dict[str, str]] = None,
    ):
        self.rate_limit_config = rate_limit_config or RateLimitConfig()
        self.retry_config = retry_config or RetryConfig()
        self.circuit_config = circuit_config or CircuitBreakerConfig()
        self.timeout = timeout
        self.default_headers = default_headers or {}
        
        self._client: Optional[httpx.AsyncClient] = None
        self._host_states: Dict[str, HostState] = {}
        self._lock = asyncio.Lock()
        
    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            headers=self.default_headers,
            follow_redirects=True,  # v2.2.1: Fix 301 redirect handling
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def init(self):
        """Initialize client without context manager. Must call close() when done."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers=self.default_headers,
                follow_redirects=True,  # v2.2.1: Fix 301 redirect handling
            )
        return self

    async def close(self):
        """Close the client. Use this when not using context manager."""
        if self._client:
            await self._client.aclose()
            self._client = None
            
    def _get_host(self, url: str) -> str:
        """Extract host from URL for per-host tracking."""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc
        
    def _get_host_state(self, host: str) -> HostState:
        """Get or create state for a host."""
        if host not in self._host_states:
            self._host_states[host] = HostState()
        return self._host_states[host]
        
    def _calculate_backoff(self, attempt: int) -> float:
        """
        Calculate delay with exponential backoff and jitter.
        
        Formula: min(base * (exp_base ^ attempt) + jitter, max_delay)
        Jitter prevents thundering herd when multiple clients retry simultaneously.
        """
        cfg = self.retry_config
        
        # Exponential delay
        delay = cfg.base_delay * (cfg.exponential_base ** attempt)
        
        # Add random jitter (±jitter_factor of the delay)
        jitter = delay * cfg.jitter_factor * (2 * random.random() - 1)
        delay += jitter
        
        # Cap at maximum
        return min(delay, cfg.max_delay)
        
    async def _wait_for_rate_limit(self, host: str) -> None:
        """
        Wait if necessary to respect rate limits.

        Implements token bucket algorithm with server-side Retry-After respect.
        IMPORTANT: Fails fast if wait time exceeds MAX_RATE_LIMIT_WAIT (60s).
        This prevents blocking for absurdly long periods (e.g., 37 minutes).
        """
        MAX_RATE_LIMIT_WAIT = 60.0  # Maximum seconds we'll wait for rate limit

        async with self._lock:
            state = self._get_host_state(host)
            now = time.time()

            # Check if server told us to wait (429 Retry-After)
            if state.retry_after and now < state.retry_after:
                wait_time = state.retry_after - now

                # FAIL FAST: Don't block for absurdly long rate limits
                if wait_time > MAX_RATE_LIMIT_WAIT:
                    logger.warning(
                        f"[RATE_LIMIT] {host}: Rate limited for {wait_time:.0f}s - "
                        f"failing fast (max wait: {MAX_RATE_LIMIT_WAIT}s). Try other sources."
                    )
                    raise httpx.HTTPStatusError(
                        f"Rate limited for {wait_time:.0f}s (exceeds max wait)",
                        request=None,
                        response=httpx.Response(429)
                    )

                logger.info(f"[RATE_LIMIT] {host}: Server requested wait of {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
                state.retry_after = None
                return

            # Check if rate limit reset time has passed
            if state.rate_limit_reset and now < state.rate_limit_reset:
                wait_time = state.rate_limit_reset - now

                # FAIL FAST: Same check for rate limit reset
                if wait_time > MAX_RATE_LIMIT_WAIT:
                    logger.warning(
                        f"[RATE_LIMIT] {host}: Rate limit reset in {wait_time:.0f}s - "
                        f"failing fast. Try other sources."
                    )
                    raise httpx.HTTPStatusError(
                        f"Rate limit reset in {wait_time:.0f}s (exceeds max wait)",
                        request=None,
                        response=httpx.Response(429)
                    )

                logger.info(f"[RATE_LIMIT] {host}: Waiting {wait_time:.1f}s for rate limit reset")
                await asyncio.sleep(wait_time)
                state.rate_limit_reset = None
                
            # Enforce minimum interval between requests
            cfg = self.rate_limit_config
            elapsed = now - state.last_request_time
            if elapsed < cfg.min_request_interval:
                wait_time = cfg.min_request_interval - elapsed
                logger.debug(f"[RATE_LIMIT] {host}: Throttling {wait_time:.2f}s (min interval)")
                await asyncio.sleep(wait_time)
                
            # Reset window if needed
            if now - state.window_start > 1.0:
                state.window_start = now
                state.request_count = 0
                
            # Check burst limit
            if state.request_count >= cfg.burst_limit:
                wait_time = 1.0 - (now - state.window_start)
                if wait_time > 0:
                    logger.info(f"[RATE_LIMIT] {host}: Burst limit reached, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    state.window_start = time.time()
                    state.request_count = 0
                    
            state.request_count += 1
            state.last_request_time = time.time()
            
    def _check_circuit_breaker(self, host: str) -> bool:
        """
        Check if circuit breaker allows request.
        
        Returns True if request should proceed, False if circuit is open.
        """
        state = self._get_host_state(host)
        cfg = self.circuit_config
        now = time.time()
        
        if state.circuit_state == CircuitState.CLOSED:
            return True
            
        if state.circuit_state == CircuitState.OPEN:
            # Check if timeout has passed to try half-open
            if now - state.last_failure_time > cfg.timeout_seconds:
                logger.info(f"[CIRCUIT] {host}: Moving to HALF_OPEN for test request")
                state.circuit_state = CircuitState.HALF_OPEN
                state.success_count = 0
                return True
            else:
                remaining = cfg.timeout_seconds - (now - state.last_failure_time)
                logger.warning(f"[CIRCUIT] {host}: OPEN, rejecting request ({remaining:.1f}s until retry)")
                return False
                
        # HALF_OPEN - allow limited requests to test
        return True
        
    def _record_success(self, host: str) -> None:
        """Record successful request for circuit breaker."""
        state = self._get_host_state(host)
        cfg = self.circuit_config
        
        state.failure_count = 0
        
        if state.circuit_state == CircuitState.HALF_OPEN:
            state.success_count += 1
            if state.success_count >= cfg.success_threshold:
                logger.info(f"[CIRCUIT] {host}: Closing circuit after {state.success_count} successes")
                state.circuit_state = CircuitState.CLOSED
                
    def _record_failure(self, host: str) -> None:
        """Record failed request for circuit breaker."""
        state = self._get_host_state(host)
        cfg = self.circuit_config
        
        state.failure_count += 1
        state.last_failure_time = time.time()
        state.success_count = 0
        
        if state.circuit_state == CircuitState.HALF_OPEN:
            logger.warning(f"[CIRCUIT] {host}: Test request failed, reopening circuit")
            state.circuit_state = CircuitState.OPEN
        elif state.failure_count >= cfg.failure_threshold:
            logger.error(f"[CIRCUIT] {host}: Opening circuit after {state.failure_count} failures")
            state.circuit_state = CircuitState.OPEN
            
    def _parse_retry_after(self, response: httpx.Response) -> Optional[float]:
        """Parse Retry-After header, returns absolute timestamp."""
        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return None
            
        try:
            # Try as seconds first
            seconds = int(retry_after)
            return time.time() + seconds
        except ValueError:
            pass
            
        try:
            # Try as HTTP date
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(retry_after)
            return dt.timestamp()
        except (ValueError, TypeError):
            pass
            
        return None
        
    def _extract_rate_limit_headers(self, response: httpx.Response, host: str) -> None:
        """Extract and store rate limit info from response headers."""
        state = self._get_host_state(host)
        
        # Common rate limit headers
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset = response.headers.get("X-RateLimit-Reset")
        
        if remaining is not None and int(remaining) <= 1:
            if reset:
                try:
                    state.rate_limit_reset = float(reset)
                    logger.info(f"[RATE_LIMIT] {host}: Approaching limit, reset at {reset}")
                except ValueError:
                    pass
                    
    async def _check_preflight_block(self, host: str) -> None:
        """
        PRE-FLIGHT CHECK: Verify host is not blocked before even queuing.

        This runs BEFORE acquiring the per-host lock to fail fast.
        If blocked for > MAX_RATE_LIMIT_WAIT, raises RateLimitExceeded.
        If blocked for shorter, waits then clears the block.
        """
        MAX_RATE_LIMIT_WAIT = 60.0

        state = self._get_host_state(host)
        now = time.time()

        if state.blocked_until and now < state.blocked_until:
            wait_time = state.blocked_until - now

            if wait_time > MAX_RATE_LIMIT_WAIT:
                logger.warning(
                    f"[PRE-FLIGHT] {host}: Blocked for {wait_time:.0f}s - "
                    f"failing fast (max: {MAX_RATE_LIMIT_WAIT}s)"
                )
                raise RateLimitExceeded(host, wait_time)

            logger.info(f"[PRE-FLIGHT] {host}: Waiting {wait_time:.1f}s for block to clear")
            await asyncio.sleep(wait_time)
            state.blocked_until = None

    async def request(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> httpx.Response:
        """
        Make an HTTP request with full resilience.

        v2.0.0: Now serializes requests per-host to prevent race conditions.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Target URL
            **kwargs: Additional arguments passed to httpx

        Returns:
            httpx.Response on success

        Raises:
            httpx.HTTPStatusError: On non-retryable error
            RateLimitExceeded: On rate limit exceeding MAX_RATE_LIMIT_WAIT
            Exception: On circuit breaker rejection or max retries exceeded
        """
        # Auto-initialize if not using context manager
        if not self._client:
            await self.init()

        host = self._get_host(url)
        cfg = self.retry_config

        # PRE-FLIGHT CHECK: Fail fast if host is blocked
        await self._check_preflight_block(host)

        # Check circuit breaker
        if not self._check_circuit_breaker(host):
            raise Exception(f"Circuit breaker OPEN for {host}. Request rejected.")

        # SERIALIZE: Acquire per-host lock to ensure only ONE request at a time
        host_lock = await _host_locks.get_lock(host)

        async with host_lock:
            return await self._do_request_with_retry(method, url, host, cfg, **kwargs)

    async def _do_request_with_retry(
        self,
        method: str,
        url: str,
        host: str,
        cfg: RetryConfig,
        **kwargs
    ) -> httpx.Response:
        """Execute request with retry logic. Called under per-host lock."""
        last_exception = None

        for attempt in range(cfg.max_retries + 1):
            try:
                # Wait for rate limiting
                await self._wait_for_rate_limit(host)
                
                # Make request
                logger.debug(f"[HTTP] {method} {url} (attempt {attempt + 1}/{cfg.max_retries + 1})")
                response = await self._client.request(method, url, **kwargs)
                
                # Extract rate limit headers for future requests
                self._extract_rate_limit_headers(response, host)
                
                # Handle 429 specifically - CRITICAL for race condition fix
                if response.status_code == 429:
                    retry_after = self._parse_retry_after(response)
                    state = self._get_host_state(host)

                    if retry_after:
                        wait_time = retry_after - time.time()
                        # SET blocked_until IMMEDIATELY so other requests see it
                        state.blocked_until = retry_after
                        state.retry_after = retry_after
                        logger.warning(
                            f"[429] {host}: Rate limited for {wait_time:.1f}s - "
                            f"blocking ALL requests until {retry_after}"
                        )
                    else:
                        # Default backoff if no Retry-After header
                        wait_time = self._calculate_backoff(attempt)
                        state.blocked_until = time.time() + wait_time
                        logger.warning(f"[429] {host}: Rate limited, backing off {wait_time:.1f}s")

                    # FAIL FAST: If wait is too long, raise immediately
                    MAX_RATE_LIMIT_WAIT = 60.0
                    if wait_time > MAX_RATE_LIMIT_WAIT:
                        logger.error(
                            f"[429] {host}: Wait time {wait_time:.0f}s exceeds max "
                            f"({MAX_RATE_LIMIT_WAIT}s) - failing fast"
                        )
                        raise RateLimitExceeded(host, wait_time)

                    if attempt < cfg.max_retries:
                        await asyncio.sleep(min(wait_time, MAX_RATE_LIMIT_WAIT))
                        state.blocked_until = None  # Clear after waiting
                        continue
                        
                # Handle other retryable errors
                if response.status_code in cfg.retryable_status_codes:
                    self._record_failure(host)
                    
                    if attempt < cfg.max_retries:
                        delay = self._calculate_backoff(attempt)
                        logger.warning(
                            f"[HTTP] {host}: Status {response.status_code}, "
                            f"retrying in {delay:.1f}s (attempt {attempt + 1})"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        response.raise_for_status()
                        
                # Handle fatal errors (no retry)
                if response.status_code in cfg.fatal_status_codes:
                    logger.error(f"[HTTP] {host}: Fatal status {response.status_code}, not retrying")
                    response.raise_for_status()
                    
                # Success!
                self._record_success(host)
                return response
                
            except httpx.TimeoutException as e:
                self._record_failure(host)
                last_exception = e
                
                if attempt < cfg.max_retries:
                    delay = self._calculate_backoff(attempt)
                    logger.warning(f"[HTTP] {host}: Timeout, retrying in {delay:.1f}s")
                    await asyncio.sleep(delay)
                    continue
                    
            except httpx.ConnectError as e:
                self._record_failure(host)
                last_exception = e
                
                if attempt < cfg.max_retries:
                    delay = self._calculate_backoff(attempt)
                    logger.warning(f"[HTTP] {host}: Connection error, retrying in {delay:.1f}s")
                    await asyncio.sleep(delay)
                    continue
                    
        # All retries exhausted
        logger.error(f"[HTTP] {host}: All {cfg.max_retries + 1} attempts failed")
        if last_exception:
            raise last_exception
        raise Exception(f"Request to {url} failed after {cfg.max_retries + 1} attempts")
        
    async def get(self, url: str, **kwargs) -> httpx.Response:
        """GET request with resilience."""
        return await self.request("GET", url, **kwargs)
        
    async def post(self, url: str, **kwargs) -> httpx.Response:
        """POST request with resilience."""
        return await self.request("POST", url, **kwargs)
        
    async def put(self, url: str, **kwargs) -> httpx.Response:
        """PUT request with resilience."""
        return await self.request("PUT", url, **kwargs)
        
    async def delete(self, url: str, **kwargs) -> httpx.Response:
        """DELETE request with resilience."""
        return await self.request("DELETE", url, **kwargs)


# Pre-configured clients for specific APIs
# These use conservative settings to avoid bans

def get_pricecharting_client() -> ResilientHTTPClient:
    """
    Get client configured for PriceCharting API.
    
    PriceCharting is rate-sensitive. Use conservative limits.
    """
    return ResilientHTTPClient(
        rate_limit_config=RateLimitConfig(
            requests_per_second=1.0,     # Very conservative
            burst_limit=3,               # Small bursts only
            min_request_interval=1.0,    # At least 1 second between requests
        ),
        retry_config=RetryConfig(
            max_retries=5,
            base_delay=2.0,              # Start with 2 second delay
            max_delay=120.0,             # Up to 2 minutes
            exponential_base=2.0,
            jitter_factor=0.5,
        ),
        circuit_config=CircuitBreakerConfig(
            failure_threshold=3,         # Open circuit after 3 failures
            success_threshold=2,
            timeout_seconds=120.0,       # Wait 2 minutes before retry
        ),
        timeout=30.0,
    )
    

def get_metron_client() -> ResilientHTTPClient:
    """
    Get client configured for Metron API.

    Metron enforces: 30 requests/minute, 10,000 requests/day
    We use 0.4 req/sec (24/min) for safety margin.

    Per Mokkari docs: https://github.com/Metron-Project/mokkari
    """
    return ResilientHTTPClient(
        rate_limit_config=RateLimitConfig(
            requests_per_second=0.4,       # 24 req/min (Metron limit: 30/min)
            burst_limit=2,                 # Max 2 requests in quick succession
            min_request_interval=2.5,      # At least 2.5s between requests
        ),
        retry_config=RetryConfig(
            max_retries=3,                 # Fewer retries - respect the API
            base_delay=5.0,                # Start with 5s (longer for rate limits)
            max_delay=120.0,               # Up to 2 minutes on 429
            exponential_base=2.0,
            jitter_factor=0.5,
        ),
        circuit_config=CircuitBreakerConfig(
            failure_threshold=3,           # Open circuit after 3 failures
            success_threshold=2,
            timeout_seconds=120.0,         # Wait 2 min before retry after circuit opens
        ),
        timeout=30.0,
    )


def get_gcd_client() -> ResilientHTTPClient:
    """
    Get client configured for Grand Comics Database API.
    
    GCD is a free/open service - be extra respectful.
    """
    return ResilientHTTPClient(
        rate_limit_config=RateLimitConfig(
            requests_per_second=1.0,
            burst_limit=3,
            min_request_interval=1.0,
        ),
        retry_config=RetryConfig(
            max_retries=3,
            base_delay=2.0,
            max_delay=60.0,
        ),
        timeout=30.0,
    )
