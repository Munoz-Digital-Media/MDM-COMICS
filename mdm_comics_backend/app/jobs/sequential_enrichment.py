"""
Sequential Exhaustive Enrichment Job v1.16.0

MSE-002+: Complete Fandom wiki expansion + MyComicShop sources
- Marvel Fandom: TEXT ONLY per CC BY-SA license (no images)
- DC Fandom: TEXT ONLY - DC/Vertigo/WildStorm only
- Image Fandom: TEXT ONLY - Image Comics only
- IDW Fandom: TEXT ONLY - IDW Publishing only
- Dark Horse Fandom: TEXT ONLY - Dark Horse Comics only
- Dynamite Fandom: TEXT ONLY - Dynamite Entertainment only
- MyComicShop: Bibliographic data only (no inventory/availability)
- Expanded CBR coverage for isbn, description, cover_date, store_date


v1.16.0 Changes (Assessment SEJ-ASSESS-001 Solutions):
- SOLUTION-001: Fixed zero-value detection (0 is valid for price/page_count)
- SOLUTION-002: Added adapter cleanup guards (no silent failures)
- SOLUTION-003: Centralized imports at module level
- SOLUTION-004: Added retry decorator for transient errors
- SOLUTION-005: HTTP client pool for connection reuse
- SOLUTION-006: Volume detection enhancement (uses series_year_began)

All Fandom sources are publisher-filtered - won't query wrong wikis for content.

Implements the USER-REQUESTED algorithm:
1. Process ONE row at a time
2. Identify ALL missing fields
3. Query Source 1, import deltas
4. RE-EVALUATE what's still missing (search refines as data is added)
5. Query Source 2, import deltas
6. Repeat until ALL sources exhausted
7. Move to next row

Key difference from parallel jobs:
- After each source, we re-check what's missing - added data refines subsequent searches
- We never "give up" on a field until ALL sources have been exhausted for that row
- Intelligent per-source rate limiting with exponential backoff

Rate Limiting Intelligence:
- Track rate limits per source (429 responses, X-RateLimit headers)
- Exponential backoff per source when rate limited
- Source rotation when one source is rate limited (try others first)
- Adaptive delay based on recent error rates
"""
import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Set, Callable, TypeVar
from uuid import uuid4

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.core.utils import utcnow

logger = logging.getLogger(__name__)

# Type variable for retry decorator
T = TypeVar('T')

# =============================================================================
# SOLUTION-004: RETRY DECORATOR FOR TRANSIENT ERRORS
# =============================================================================

async def with_retry(
    func: Callable[[], T],
    max_retries: int = 3,
    backoff_base: float = 2.0,
    retryable_exceptions: tuple = (httpx.NetworkError, httpx.TimeoutException, ConnectionError)
) -> T:
    """
    Execute an async function with exponential backoff retry.

    Args:
        func: Async callable to execute
        max_retries: Maximum number of retry attempts
        backoff_base: Base for exponential backoff (delay = base ** attempt)
        retryable_exceptions: Tuple of exception types to retry on

    Returns:
        Result of the function call

    Raises:
        Last exception if all retries fail
    """
    last_exception = None
    for attempt in range(max_retries):
        try:
            return await func()
        except retryable_exceptions as e:
            last_exception = e
            if attempt < max_retries - 1:
                delay = backoff_base ** attempt
                logger.debug(f"[Retry] Attempt {attempt + 1}/{max_retries} failed: {e}, waiting {delay:.1f}s")
                await asyncio.sleep(delay)
    raise last_exception


# =============================================================================
# SOLUTION-005: HTTP CLIENT POOL FOR CONNECTION REUSE
# =============================================================================

class HTTPClientPool:
    """
    Manages a pool of HTTP clients for connection reuse.

    Benefits:
    - Reuses TCP connections (avoids handshake overhead)
    - Keeps connections warm (faster requests)
    - Limits concurrent connections per source
    """

    def __init__(self):
        self._clients: Dict[str, httpx.AsyncClient] = {}
        self._lock = asyncio.Lock()

    async def get_client(self, source_name: str, timeout: float = 30.0) -> httpx.AsyncClient:
        """Get or create an HTTP client for a source."""
        async with self._lock:
            if source_name not in self._clients:
                self._clients[source_name] = httpx.AsyncClient(
                    timeout=timeout,
                    headers={"User-Agent": f"MDM Comics Enrichment/1.16 ({source_name})"},
                    follow_redirects=True,
                    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
                )
                logger.debug(f"[HTTPPool] Created client for {source_name}")
            return self._clients[source_name]

    async def close_all(self):
        """Close all HTTP clients. Call this when job completes."""
        async with self._lock:
            for source_name, client in self._clients.items():
                try:
                    await client.aclose()
                    logger.debug(f"[HTTPPool] Closed client for {source_name}")
                except Exception as e:
                    logger.debug(f"[HTTPPool] Error closing client for {source_name}: {e}")
            self._clients.clear()


# Global HTTP client pool (singleton)
_http_pool: Optional[HTTPClientPool] = None


def get_http_pool() -> HTTPClientPool:
    """Get the global HTTP client pool (creates if needed)."""
    global _http_pool
    if _http_pool is None:
        _http_pool = HTTPClientPool()
    return _http_pool


async def cleanup_http_pool():
    """Cleanup the global HTTP client pool. Call at job end."""
    global _http_pool
    if _http_pool is not None:
        await _http_pool.close_all()
        _http_pool = None


# =============================================================================
# SOLUTION-006: VOLUME DETECTION ENHANCEMENT
# =============================================================================

# Known volume years for major series
SERIES_VOLUME_YEARS = {
    # Marvel
    "amazing spider-man": [(1963, 1), (1999, 2), (2003, 1), (2014, 3), (2015, 4), (2018, 5), (2022, 6)],
    "spectacular spider-man": [(1976, 1), (2003, 2), (2017, 1)],
    "x-men": [(1963, 1), (1991, 2), (2010, 3), (2013, 4), (2019, 5), (2021, 6)],
    "uncanny x-men": [(1963, 1), (2013, 2), (2016, 3), (2019, 5)],
    "avengers": [(1963, 1), (1996, 2), (1998, 3), (2010, 4), (2012, 5), (2018, 8)],
    "fantastic four": [(1961, 1), (1996, 2), (1998, 3), (2013, 4), (2014, 5), (2018, 6)],
    "iron man": [(1968, 1), (1996, 2), (1998, 3), (2005, 4), (2013, 5), (2015, 1), (2020, 6)],
    "captain america": [(1968, 1), (1996, 2), (1998, 3), (2002, 4), (2005, 5), (2011, 6), (2013, 7), (2017, 8)],
    "thor": [(1966, 1), (1998, 2), (2007, 3), (2014, 4), (2015, 1), (2018, 5), (2020, 6)],
    "hulk": [(1962, 1), (1968, 2), (1999, 2), (2008, 3), (2014, 4), (2016, 1), (2021, 1)],
    "daredevil": [(1964, 1), (1998, 2), (2011, 3), (2014, 4), (2016, 5), (2019, 6)],
    # DC
    "batman": [(1940, 1), (2011, 2), (2016, 3)],
    "detective comics": [(1937, 1), (2011, 2), (2016, 1)],
    "superman": [(1939, 1), (1987, 2), (2006, 1), (2011, 2), (2016, 3), (2018, 4)],
    "action comics": [(1938, 1), (2011, 2), (2016, 1)],
    "wonder woman": [(1942, 1), (1987, 2), (2006, 3), (2011, 4), (2016, 5)],
    "justice league": [(1987, 1), (2006, 2), (2011, 2), (2016, 3), (2018, 4)],
    "flash": [(1959, 1), (1987, 2), (2010, 3), (2011, 4), (2016, 5)],
    "green lantern": [(1960, 1), (1990, 3), (2005, 4), (2011, 5), (2016, 1), (2021, 1)],
    "aquaman": [(1962, 1), (1986, 2), (1991, 3), (1994, 4), (2003, 5), (2011, 7), (2016, 8)],
    # Image
    "spawn": [(1992, 1)],
    "invincible": [(2003, 1)],
    "savage dragon": [(1993, 1)],
    "walking dead": [(2003, 1)],
}


def estimate_volume(series_name: str, year: Optional[int]) -> int:
    """
    Estimate the volume number for a series based on the publication year.

    Args:
        series_name: Name of the comic series
        year: Publication year (from cover_date or series_year_began)

    Returns:
        Estimated volume number (defaults to 1 if unknown)
    """
    if not year:
        return 1

    series_lower = series_name.lower().strip()

    # Check for exact match
    if series_lower in SERIES_VOLUME_YEARS:
        volume_years = SERIES_VOLUME_YEARS[series_lower]
        best_volume = 1
        for vol_year, vol_num in sorted(volume_years):
            if vol_year <= year:
                best_volume = vol_num
        return best_volume

    # Check for partial match
    for known_series, volume_years in SERIES_VOLUME_YEARS.items():
        if known_series in series_lower or series_lower in known_series:
            best_volume = 1
            for vol_year, vol_num in sorted(volume_years):
                if vol_year <= year:
                    best_volume = vol_num
            return best_volume

    return 1




# =============================================================================
# INTELLIGENT RATE LIMITER
# =============================================================================

@dataclass
class SourceRateLimiter:
    """Tracks rate limits and manages backoff per source."""

    source_name: str
    base_delay: float = 1.0  # Base delay between requests (seconds)

    # State tracking
    last_request_time: float = 0.0
    consecutive_429s: int = 0
    total_429s_today: int = 0
    last_429_time: float = 0.0
    current_backoff: float = 0.0
    blocked_until: float = 0.0

    # Adaptive limits (learned from responses)
    requests_this_minute: int = 0
    minute_start: float = 0.0
    rate_limit_ceiling: Optional[int] = None  # From X-RateLimit-Limit header

    def can_make_request(self) -> bool:
        """Check if we can make a request to this source."""
        now = time.time()

        # Check if we're in a backoff period
        if now < self.blocked_until:
            return False

        # Check if we've exceeded known rate limit
        if self.rate_limit_ceiling:
            if now - self.minute_start < 60:
                if self.requests_this_minute >= self.rate_limit_ceiling - 1:
                    return False
            else:
                # Reset minute counter
                self.minute_start = now
                self.requests_this_minute = 0

        return True

    def get_required_delay(self) -> float:
        """Get the delay needed before next request.

        IMPORTANT: Sources that have NOT been rate-limited should run with
        minimal delay. Only rate-limited sources get backoff applied.
        This prevents healthy sources from being slowed by unhealthy ones.
        """
        now = time.time()

        # If blocked due to rate limiting, return time until unblocked
        if now < self.blocked_until:
            return self.blocked_until - now

        # KEY INSIGHT: If this source has NO recent rate limit issues,
        # only enforce the minimum polite delay (base_delay)
        # Don't let other sources' problems slow us down!
        if self.consecutive_429s == 0:
            time_since_last = now - self.last_request_time
            # Only enforce base delay to be polite, no backoff penalty
            remaining = self.base_delay - time_since_last
            return max(0, remaining)

        # This source has had rate limit issues - use backoff + base delay
        time_since_last = now - self.last_request_time
        required_delay = self.base_delay + self.current_backoff

        remaining = required_delay - time_since_last
        return max(0, remaining)

    def is_healthy(self) -> bool:
        """Check if this source is healthy (no recent rate limit issues)."""
        return self.consecutive_429s == 0 and time.time() >= self.blocked_until

    def record_request(self):
        """Record that a request was made."""
        now = time.time()
        self.last_request_time = now

        # Track per-minute requests
        if now - self.minute_start >= 60:
            self.minute_start = now
            self.requests_this_minute = 0
        self.requests_this_minute += 1

    def record_success(self):
        """Record successful response - decay backoff."""
        self.consecutive_429s = 0

        # Decay backoff on success (halve it, min 0)
        self.current_backoff = max(0, self.current_backoff / 2)

    def record_rate_limit(self, retry_after: Optional[float] = None):
        """Record 429 response - increase backoff."""
        now = time.time()
        self.consecutive_429s += 1
        self.total_429s_today += 1
        self.last_429_time = now

        # Exponential backoff: 2^consecutive * base, capped at 5 minutes
        if retry_after:
            self.current_backoff = retry_after
        else:
            self.current_backoff = min(300, self.base_delay * (2 ** self.consecutive_429s))

        # Block for backoff duration
        self.blocked_until = now + self.current_backoff

        logger.warning(
            f"[RateLimit] {self.source_name}: 429 received (#{self.consecutive_429s}), "
            f"backing off {self.current_backoff:.1f}s"
        )

    def update_from_headers(self, headers: Dict[str, str]):
        """Update limits from response headers (X-RateLimit-*)."""
        if "x-ratelimit-limit" in headers:
            try:
                self.rate_limit_ceiling = int(headers["x-ratelimit-limit"])
            except ValueError:
                pass

        if "x-ratelimit-remaining" in headers:
            try:
                remaining = int(headers["x-ratelimit-remaining"])
                if remaining <= 1:
                    # Preemptively slow down
                    self.current_backoff = max(self.current_backoff, 2.0)
            except ValueError:
                pass


class RateLimitManager:
    """Manages rate limiting across all sources."""

    def __init__(self):
        self.limiters: Dict[str, SourceRateLimiter] = {}

        # Configure base delays per source (known limits)
        self._base_delays = {
            "metron": 1.0,           # Metron is generous, 1 req/sec is safe
            "comicvine": 1.5,        # ComicVine: ~200/hour = 1 per 18s, but bursty OK
            "pricecharting": 2.0,    # PriceCharting: aggressive rate limiting
            "comicbookrealm": 1.5,   # CBR: Unknown, be conservative
            "mycomicshop": 3.0,      # MCS: Scraping, be polite (conservative)
            # Fandom wikis (MediaWiki API) - all generous, 1 req/sec is safe
            "marvel_fandom": 1.0,
            "dc_fandom": 1.0,
            "image_fandom": 1.0,
            "idw_fandom": 1.0,
            "darkhorse_fandom": 1.0,
            "dynamite_fandom": 1.0,
        }

    def get_limiter(self, source_name: str) -> SourceRateLimiter:
        """Get or create limiter for a source."""
        if source_name not in self.limiters:
            base_delay = self._base_delays.get(source_name, 1.5)
            self.limiters[source_name] = SourceRateLimiter(
                source_name=source_name,
                base_delay=base_delay
            )
        return self.limiters[source_name]

    async def wait_for_source(self, source_name: str) -> bool:
        """Wait until we can make a request to a source.

        Returns True if request can proceed, False if source is blocked.

        IMPORTANT: Healthy sources (no rate limit issues) get minimal delay.
        This prevents unhealthy sources from slowing down healthy ones.
        """
        limiter = self.get_limiter(source_name)

        # If source is blocked, don't wait - return False so caller can try others
        if not limiter.can_make_request():
            logger.debug(f"[RateLimit] {source_name} blocked, skipping for now")
            return False

        delay = limiter.get_required_delay()
        if delay > 0:
            # Only log delays > 1 second to reduce noise
            if delay > 1.0:
                logger.debug(f"[RateLimit] Waiting {delay:.1f}s for {source_name}")
            await asyncio.sleep(delay)

        limiter.record_request()
        return True

    def get_available_sources(self, sources: List[str]) -> List[str]:
        """Return sources sorted by health/availability.

        Healthy sources (no rate limit issues) come first.
        Blocked sources come last but are still included in case they unblock.
        """
        now = time.time()

        def availability_score(source: str) -> tuple:
            limiter = self.get_limiter(source)

            # Tier 1: Healthy sources (no consecutive 429s)
            if limiter.is_healthy():
                return (0, limiter.get_required_delay())

            # Tier 2: Recovering sources (have had issues but not blocked)
            if now >= limiter.blocked_until:
                return (1, limiter.get_required_delay())

            # Tier 3: Blocked sources
            return (2, limiter.blocked_until - now)

        return sorted(sources, key=availability_score)

    def get_healthy_sources(self, sources: List[str]) -> List[str]:
        """Return only sources that are healthy (no rate limit issues)."""
        return [s for s in sources if self.get_limiter(s).is_healthy()]

    def get_blocked_sources(self, sources: List[str]) -> List[str]:
        """Return sources that are currently blocked."""
        now = time.time()
        return [s for s in sources if now < self.get_limiter(s).blocked_until]


# Global rate limit manager
rate_limiter = RateLimitManager()


# =============================================================================
# FIELD DETECTION
# =============================================================================

# Fields we want to enrich and which sources can provide them
# MSE-002+: Expanded sources - All Fandom wikis (publisher-filtered), MyComicShop, CBR expansion
# Fandom sources are publisher-specific: each only queries for its publisher's comics
#   - marvel_fandom: Marvel only
#   - dc_fandom: DC/Vertigo/WildStorm only
#   - image_fandom: Image Comics only
#   - idw_fandom: IDW Publishing only
#   - darkhorse_fandom: Dark Horse only
#   - dynamite_fandom: Dynamite Entertainment only
ENRICHABLE_FIELDS = {
    # Core identifiers
    "metron_id": {"sources": ["metron"], "required_for_lookup": False},
    "comicvine_id": {"sources": ["comicvine"], "required_for_lookup": False},
    "pricecharting_id": {"sources": ["pricecharting"], "required_for_lookup": False},

    # Barcodes - EXPANDED with mycomicshop
    "upc": {"sources": ["metron", "comicvine", "comicbookrealm", "mycomicshop"], "required_for_lookup": False},
    "isbn": {"sources": ["metron", "comicvine", "comicbookrealm", "mycomicshop"], "required_for_lookup": False},

    # Bibliographic - EXPANDED with ALL Fandom wikis (TEXT ONLY), mycomicshop, cbr
    # NOTE: Fandom sources filter by publisher automatically - no cross-wiki queries
    "description": {"sources": ["metron", "comicvine", "marvel_fandom", "dc_fandom", "image_fandom", "idw_fandom", "darkhorse_fandom", "dynamite_fandom", "comicbookrealm", "mycomicshop"], "required_for_lookup": False},
    "page_count": {"sources": ["metron", "comicvine", "marvel_fandom", "dc_fandom", "image_fandom", "idw_fandom", "darkhorse_fandom", "dynamite_fandom", "comicbookrealm", "mycomicshop"], "required_for_lookup": False},
    "price": {"sources": ["metron", "comicvine", "comicbookrealm", "mycomicshop"], "required_for_lookup": False},
    "cover_date": {"sources": ["metron", "comicvine", "marvel_fandom", "dc_fandom", "image_fandom", "idw_fandom", "darkhorse_fandom", "dynamite_fandom", "comicbookrealm", "mycomicshop"], "required_for_lookup": False},
    "store_date": {"sources": ["metron", "comicvine", "marvel_fandom", "dc_fandom", "image_fandom", "idw_fandom", "darkhorse_fandom", "dynamite_fandom", "comicbookrealm", "mycomicshop"], "required_for_lookup": False},

    # Market data (from PriceCharting once we have PC ID)
    "price_loose": {"sources": ["pricecharting"], "required_for_lookup": True},
    "price_graded": {"sources": ["pricecharting"], "required_for_lookup": True},

    # Images - NO Fandom (CC BY-SA license prohibits image redistribution)
    "image": {"sources": ["metron", "comicvine", "comicbookrealm", "mycomicshop"], "required_for_lookup": False},
}


def identify_missing_fields(comic: Dict[str, Any]) -> Set[str]:
    """Identify which enrichable fields are missing from this comic."""
    missing = set()

    for field_name, config in ENRICHABLE_FIELDS.items():
        value = comic.get(field_name)

        # Check if field is empty/null
        # SOLUTION-001: 0 is valid for price/page_count, so don't treat as empty
        is_empty = (
            value is None or
            value == "" or
            (isinstance(value, (list, dict)) and len(value) == 0)
        )

        if is_empty:
            # If field requires a lookup ID (e.g., pricecharting_id for prices),
            # check if we have the required ID
            if config.get("required_for_lookup"):
                # price_loose/price_graded require pricecharting_id
                if field_name in ("price_loose", "price_graded"):
                    if not comic.get("pricecharting_id"):
                        continue  # Skip until we have the ID
            missing.add(field_name)

    return missing


def get_sources_for_fields(missing_fields: Set[str]) -> Set[str]:
    """Get the set of sources that can help fill these fields."""
    sources = set()
    for field in missing_fields:
        if field in ENRICHABLE_FIELDS:
            sources.update(ENRICHABLE_FIELDS[field]["sources"])
    return sources


# =============================================================================
# SOURCE ADAPTERS (Thin wrappers that handle rate limiting)
# =============================================================================

async def query_metron(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """Query Metron for missing fields. Returns dict of field->value."""
    from app.adapters.metron_adapter import MetronAdapter
    from app.core.http_client import get_metron_client

    updates = {}
    source = "metron"

    try:
        # Properly initialize the HTTP client
        client = get_metron_client()
        await client.__aenter__()
        adapter = MetronAdapter(client=client)
        
        if not await adapter.health_check():
            logger.warning(f"[{source}] Health check failed")
            await client.__aexit__(None, None, None)
            return updates

        # Check if source is available (non-blocking check)
        if not await rate_mgr.wait_for_source(source):
            # Source is blocked - return empty, caller will try other sources
            return updates

        # Strategy: If we have metron_id, direct lookup. Otherwise, search.
        metron_id = comic.get("metron_id")

        if metron_id:
            # Direct lookup
            data = await adapter.fetch_by_id(str(metron_id), endpoint="issue")
            if data:
                rate_mgr.get_limiter(source).record_success()

                # Extract available fields
                if "upc" in missing_fields and data.get("upc"):
                    updates["upc"] = data["upc"]
                if "isbn" in missing_fields and data.get("isbn"):
                    updates["isbn"] = data["isbn"]
                if "description" in missing_fields and data.get("description"):
                    updates["description"] = data["description"]
                if "page_count" in missing_fields and data.get("page_count"):
                    updates["page_count"] = data["page_count"]
                if "cover_date" in missing_fields and data.get("cover_date"):
                    updates["cover_date"] = data["cover_date"]
                if "store_date" in missing_fields and data.get("store_date"):
                    updates["store_date"] = data["store_date"]
                if "image" in missing_fields and data.get("image"):
                    updates["image"] = data["image"]
                if "price" in missing_fields and data.get("price"):
                    updates["price"] = data["price"]
        else:
            # Search by series + issue
            if comic.get("series_name") and comic.get("number"):
                if not await rate_mgr.wait_for_source(source):
                    return updates

                search_result = await adapter.search_issues(
                    series_name=comic["series_name"],
                    number=str(comic["number"]),
                    publisher_name=comic.get("publisher_name"),
                )

                if search_result.success and search_result.records:
                    rate_mgr.get_limiter(source).record_success()

                    # Find best match
                    best = _find_best_match(comic, search_result.records)
                    if best:
                        # If we found a match, store the metron_id for future lookups
                        if best.get("id"):
                            updates["metron_id"] = best["id"]

                        # Fetch full details
                        if best.get("id"):
                            if not await rate_mgr.wait_for_source(source):
                                return updates  # Return what we have so far
                            data = await adapter.fetch_by_id(str(best["id"]), endpoint="issue")
                            if data:
                                rate_mgr.get_limiter(source).record_success()

                                if "upc" in missing_fields and data.get("upc"):
                                    updates["upc"] = data["upc"]
                                if "isbn" in missing_fields and data.get("isbn"):
                                    updates["isbn"] = data["isbn"]
                                if "description" in missing_fields and data.get("description"):
                                    updates["description"] = data["description"]
                                if "page_count" in missing_fields and data.get("page_count"):
                                    updates["page_count"] = data["page_count"]
                                if "cover_date" in missing_fields and data.get("cover_date"):
                                    updates["cover_date"] = data["cover_date"]
                                if "store_date" in missing_fields and data.get("store_date"):
                                    updates["store_date"] = data["store_date"]
                                if "image" in missing_fields and data.get("image"):
                                    updates["image"] = data["image"]
                else:
                    rate_mgr.get_limiter(source).record_success()  # Search worked, just no results

    except Exception as e:
        error_str = str(e).lower()
        if "429" in error_str or "rate" in error_str:
            rate_mgr.get_limiter(source).record_rate_limit()
        else:
            logger.warning(f"[{source}] Error querying: {e}")
    finally:
        # Cleanup client - SOLUTION-002: Log cleanup errors
        try:
            await client.__aexit__(None, None, None)
        except Exception as e:
            logger.debug(f"[{source}] Client cleanup error: {e}")

    if updates:
        logger.info(f"[{source}] Found {len(updates)} fields for comic {comic.get('id')}")

    return updates


async def query_comicvine(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """Query ComicVine for missing fields."""
    # SOLUTION-003: Imports moved to module level
    updates = {}
    source = "comicvine"

    api_key = os.environ.get("COMIC_VINE_API_KEY") or os.environ.get("COMICVINE_API_KEY")
    if not api_key:
        return updates

    try:
        # Check if source is available
        if not await rate_mgr.wait_for_source(source):
            return updates

        async with httpx.AsyncClient(timeout=30.0) as client:
            cv_id = comic.get("comicvine_id")

            if cv_id:
                # Direct lookup by CV ID
                response = await client.get(
                    f"https://comicvine.gamespot.com/api/issue/4000-{cv_id}/",
                    params={"api_key": api_key, "format": "json"},
                    headers={"User-Agent": "MDM Comics Enrichment/1.0"}
                )

                # Check rate limit headers
                rate_mgr.get_limiter(source).update_from_headers(
                    {k.lower(): v for k, v in response.headers.items()}
                )

                if response.status_code == 429:
                    rate_mgr.get_limiter(source).record_rate_limit()
                    return updates

                rate_mgr.get_limiter(source).record_success()

                if response.status_code == 200:
                    data = response.json().get("results", {})

                    if "description" in missing_fields and data.get("description"):
                        # Strip HTML tags
                        import re
                        desc = re.sub(r'<[^>]+>', '', data["description"])
                        updates["description"] = desc[:5000]  # Limit length

                    if "cover_date" in missing_fields and data.get("cover_date"):
                        updates["cover_date"] = data["cover_date"]

                    if "store_date" in missing_fields and data.get("store_date"):
                        updates["store_date"] = data["store_date"]

                    if "image" in missing_fields and data.get("image", {}).get("original_url"):
                        updates["image"] = data["image"]["original_url"]
            else:
                # Search by series + issue
                if comic.get("series_name") and comic.get("number"):
                    query = f"{comic['series_name']} {comic['number']}"

                    response = await client.get(
                        "https://comicvine.gamespot.com/api/search/",
                        params={
                            "api_key": api_key,
                            "format": "json",
                            "query": query,
                            "resources": "issue",
                            "limit": 5,
                        },
                        headers={"User-Agent": "MDM Comics Enrichment/1.0"}
                    )

                    rate_mgr.get_limiter(source).update_from_headers(
                        {k.lower(): v for k, v in response.headers.items()}
                    )

                    if response.status_code == 429:
                        rate_mgr.get_limiter(source).record_rate_limit()
                        return updates

                    rate_mgr.get_limiter(source).record_success()

                    if response.status_code == 200:
                        results = response.json().get("results", [])
                        best = _find_best_match(comic, results, cv_format=True)

                        if best and best.get("id"):
                            updates["comicvine_id"] = best["id"]

                            # Fetch full details
                            if not await rate_mgr.wait_for_source(source):
                                return updates  # Return what we have
                            detail_resp = await client.get(
                                f"https://comicvine.gamespot.com/api/issue/4000-{best['id']}/",
                                params={"api_key": api_key, "format": "json"},
                                headers={"User-Agent": "MDM Comics Enrichment/1.0"}
                            )

                            if detail_resp.status_code == 200:
                                rate_mgr.get_limiter(source).record_success()
                                data = detail_resp.json().get("results", {})

                                if "description" in missing_fields and data.get("description"):
                                    desc = re.sub(r'<[^>]+>', '', data["description"])
                                    updates["description"] = desc[:5000]

                                if "cover_date" in missing_fields and data.get("cover_date"):
                                    updates["cover_date"] = data["cover_date"]

                                if "image" in missing_fields and data.get("image", {}).get("original_url"):
                                    updates["image"] = data["image"]["original_url"]

    except Exception as e:
        error_str = str(e).lower()
        if "429" in error_str or "rate" in error_str:
            rate_mgr.get_limiter(source).record_rate_limit()
        else:
            logger.warning(f"[{source}] Error querying: {e}")

    if updates:
        logger.info(f"[{source}] Found {len(updates)} fields for comic {comic.get('id')}")

    return updates


async def query_pricecharting(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """Query PriceCharting for ID and/or prices."""
    # SOLUTION-003: Imports moved to module level
    updates = {}
    source = "pricecharting"

    api_token = os.environ.get("PRICECHARTING_API_TOKEN")
    if not api_token:
        return updates

    try:
        if not await rate_mgr.wait_for_source(source):
            return updates

        async with httpx.AsyncClient(timeout=30.0) as client:
            pc_id = comic.get("pricecharting_id")

            # If we don't have PC ID, try to find it
            if not pc_id and "pricecharting_id" in missing_fields:
                # Try UPC first (most accurate)
                if comic.get("upc"):
                    upc = "".join(c for c in str(comic["upc"]) if c.isdigit())
                    if len(upc) >= 10:
                        response = await client.get(
                            "https://www.pricecharting.com/api/products",
                            params={"t": api_token, "upc": upc}
                        )

                        if response.status_code == 429:
                            rate_mgr.get_limiter(source).record_rate_limit()
                            return updates

                        rate_mgr.get_limiter(source).record_success()

                        if response.status_code == 200:
                            products = response.json().get("products", [])
                            if products:
                                updates["pricecharting_id"] = int(products[0]["id"])
                                pc_id = products[0]["id"]

                # If no UPC match, try title search
                if not pc_id and comic.get("series_name") and comic.get("number"):
                    if not await rate_mgr.wait_for_source(source):
                        return updates

                    query = f"{comic['series_name']} {comic['number']}"
                    response = await client.get(
                        "https://www.pricecharting.com/api/products",
                        params={"t": api_token, "q": query, "console-name": "Comics"}
                    )

                    if response.status_code == 429:
                        rate_mgr.get_limiter(source).record_rate_limit()
                        return updates

                    rate_mgr.get_limiter(source).record_success()

                    if response.status_code == 200:
                        products = response.json().get("products", [])
                        best = _find_best_pc_match(comic, products)
                        if best:
                            updates["pricecharting_id"] = int(best["id"])
                            pc_id = best["id"]

            # If we have PC ID, get prices
            if pc_id and ("price_loose" in missing_fields or "price_graded" in missing_fields):
                if not await rate_mgr.wait_for_source(source):
                    return updates

                response = await client.get(
                    f"https://www.pricecharting.com/api/product",
                    params={"t": api_token, "id": pc_id}
                )

                if response.status_code == 429:
                    rate_mgr.get_limiter(source).record_rate_limit()
                    return updates

                rate_mgr.get_limiter(source).record_success()

                if response.status_code == 200:
                    data = response.json()

                    if "price_loose" in missing_fields and data.get("loose-price"):
                        updates["price_loose"] = data["loose-price"] / 100  # Convert cents

                    if "price_graded" in missing_fields and data.get("graded-price"):
                        updates["price_graded"] = data["graded-price"] / 100

    except Exception as e:
        error_str = str(e).lower()
        if "429" in error_str or "rate" in error_str:
            rate_mgr.get_limiter(source).record_rate_limit()
        else:
            logger.warning(f"[{source}] Error querying: {e}")

    if updates:
        logger.info(f"[{source}] Found {len(updates)} fields for comic {comic.get('id')}")

    return updates


async def query_comicbookrealm(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """Query ComicBookRealm for missing fields (web scraping)."""
    updates = {}
    source = "comicbookrealm"

    try:
        from app.adapters.comicbookrealm_adapter import create_comicbookrealm_adapter

        adapter = await create_comicbookrealm_adapter()

        if not await rate_mgr.wait_for_source(source):
            return updates

        if comic.get("series_name") and comic.get("number"):
            # CBR search - use search_issues (plural) which returns list
            fetch_result = await adapter.search_issues(
                series_name=comic["series_name"],
                issue_number=str(comic["number"]),
            )
            # FetchResult has .records attribute containing the list
            results = fetch_result.records if fetch_result.success else []
            result = results[0] if results else None

            if result:
                rate_mgr.get_limiter(source).record_success()

                if "upc" in missing_fields and result.get("upc"):
                    updates["upc"] = result["upc"]
                if "page_count" in missing_fields and result.get("page_count"):
                    updates["page_count"] = result["page_count"]
                if "price" in missing_fields and result.get("price"):
                    updates["price"] = result["price"]
                if "image" in missing_fields and result.get("cover_url"):
                    updates["image"] = result["cover_url"]
            else:
                rate_mgr.get_limiter(source).record_success()  # Request worked, no result

    except Exception as e:
        error_str = str(e).lower()
        if "429" in error_str or "rate" in error_str or "too many" in error_str:
            rate_mgr.get_limiter(source).record_rate_limit()
        else:
            logger.warning(f"[{source}] Error querying: {e}")

    if updates:
        logger.info(f"[{source}] Found {len(updates)} fields for comic {comic.get('id')}")

    return updates


async def query_marvel_fandom(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """
    Query Marvel Fandom (Marvel Database) for missing fields.

    TEXT ONLY per CC BY-SA license - NO images allowed.
    Provides: description, cover_date, store_date, page_count
    """
    updates = {}
    source = "marvel_fandom"

    # Only useful for Marvel comics
    publisher = str(comic.get("publisher_name", "")).lower()
    if "marvel" not in publisher:
        return updates

    # Fields Marvel Fandom can provide (TEXT ONLY)
    fandom_fields = {"description", "cover_date", "store_date", "page_count"}
    relevant_missing = missing_fields & fandom_fields
    if not relevant_missing:
        return updates

    try:
        from app.adapters.marvel_fandom import MarvelFandomAdapter

        adapter = MarvelFandomAdapter()

        if not await rate_mgr.wait_for_source(source):
            return updates

        series_name = comic.get("series_name", "")
        issue_number = str(comic.get("number", ""))

        if not series_name or not issue_number:
            return updates

        # SOLUTION-006: Use intelligent volume detection
        year = None
        if comic.get("series_year_began"):
            year = comic["series_year_began"]
        elif comic.get("cover_date"):
            # Try to extract year from cover_date
            try:
                year = int(str(comic["cover_date"])[:4])
            except (ValueError, TypeError):
                pass
        volume = estimate_volume(series_name, year)

        # Fetch issue data from Marvel Fandom
        data = await adapter.fetch_issue_credits(series_name, volume, issue_number)

        if data:
            rate_mgr.get_limiter(source).record_success()

            # Extract description (synopsis) - TEXT ONLY
            if "description" in missing_fields:
                # Try multiple synopsis fields
                synopsis = None
                for story in data.get("stories", []):
                    if story.get("synopsis"):
                        synopsis = story["synopsis"]
                        break
                if synopsis:
                    updates["description"] = synopsis[:5000]  # Limit length

            # Extract cover_date
            if "cover_date" in missing_fields and data.get("cover_date"):
                updates["cover_date"] = data["cover_date"]

            # Extract store_date (release_date)
            if "store_date" in missing_fields and data.get("release_date"):
                updates["store_date"] = data["release_date"]

            # page_count not typically available from Marvel Fandom
        else:
            rate_mgr.get_limiter(source).record_success()  # API worked, just no match

        # Cleanup - SOLUTION-002: Log cleanup errors
        if hasattr(adapter, 'client') and adapter.client:
            try:
                await adapter.client.close()
            except Exception as e:
                logger.debug(f"[{source}] Adapter cleanup error: {e}")

    except Exception as e:
        error_str = str(e).lower()
        if "429" in error_str or "rate" in error_str:
            rate_mgr.get_limiter(source).record_rate_limit()
        else:
            logger.warning(f"[{source}] Error querying: {e}")

    if updates:
        logger.info(f"[{source}] Found {len(updates)} fields for comic {comic.get('id')}")

    return updates


async def query_dc_fandom(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """
    Query DC Fandom (DC Database) for missing fields.

    TEXT ONLY per CC BY-SA license - NO images allowed.
    Provides: description, cover_date, store_date, page_count
    """
    return await _query_generic_fandom(
        comic, missing_fields, rate_mgr,
        wiki_key="dc_fandom",
        publisher_patterns=["dc", "dc comics", "vertigo", "wildstorm"]
    )


async def query_image_fandom(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """
    Query Image Comics Fandom for missing fields.

    TEXT ONLY per CC BY-SA license - NO images allowed.
    Provides: description, cover_date, store_date, page_count
    """
    return await _query_generic_fandom(
        comic, missing_fields, rate_mgr,
        wiki_key="image_fandom",
        publisher_patterns=["image", "image comics"]
    )


async def query_idw_fandom(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """
    Query IDW Publishing Database (Fandom) for missing fields.

    TEXT ONLY per CC BY-SA license - NO images allowed.
    Provides: description, cover_date, store_date, page_count
    """
    return await _query_generic_fandom(
        comic, missing_fields, rate_mgr,
        wiki_key="idw_fandom",
        publisher_patterns=["idw", "idw publishing"]
    )


async def query_darkhorse_fandom(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """
    Query Dark Horse Comics Database (Fandom) for missing fields.

    TEXT ONLY per CC BY-SA license - NO images allowed.
    Provides: description, cover_date, store_date, page_count
    """
    return await _query_generic_fandom(
        comic, missing_fields, rate_mgr,
        wiki_key="darkhorse_fandom",
        publisher_patterns=["dark horse", "dark horse comics"]
    )


async def query_dynamite_fandom(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """
    Query Dynamite Entertainment Database (Fandom) for missing fields.

    TEXT ONLY per CC BY-SA license - NO images allowed.
    Provides: description, cover_date, store_date, page_count
    """
    return await _query_generic_fandom(
        comic, missing_fields, rate_mgr,
        wiki_key="dynamite_fandom",
        publisher_patterns=["dynamite", "dynamite entertainment"]
    )


async def _query_generic_fandom(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager,
    wiki_key: str,
    publisher_patterns: List[str]
) -> Dict[str, Any]:
    """
    Generic Fandom query function for publisher-specific wikis.

    TEXT ONLY per CC BY-SA license - NO images allowed.
    Provides: description, cover_date, store_date, page_count
    """
    updates = {}
    source = wiki_key

    # Only query if publisher matches
    publisher = str(comic.get("publisher_name", "")).lower()
    if not any(p in publisher for p in publisher_patterns):
        return updates

    # Fields Fandom can provide (TEXT ONLY)
    fandom_fields = {"description", "cover_date", "store_date", "page_count"}
    relevant_missing = missing_fields & fandom_fields
    if not relevant_missing:
        return updates

    try:
        from app.adapters.fandom_adapter import FandomAdapter

        adapter = FandomAdapter(wiki_key)

        if not await rate_mgr.wait_for_source(source):
            return updates

        series_name = comic.get("series_name", "")
        issue_number = str(comic.get("number", ""))

        if not series_name or not issue_number:
            return updates

        # SOLUTION-006: Use intelligent volume detection
        year = None
        if comic.get("series_year_began"):
            year = comic["series_year_began"]
        elif comic.get("cover_date"):
            try:
                year = int(str(comic["cover_date"])[:4])
            except (ValueError, TypeError):
                pass
        volume = estimate_volume(series_name, year)

        # Fetch issue data from Fandom wiki
        data = await adapter.fetch_issue_data(series_name, volume, issue_number)

        if data:
            rate_mgr.get_limiter(source).record_success()

            # Extract description - TEXT ONLY
            if "description" in missing_fields and data.get("description"):
                updates["description"] = data["description"][:5000]  # Limit length

            # Extract cover_date
            if "cover_date" in missing_fields and data.get("cover_date"):
                updates["cover_date"] = data["cover_date"]

            # Extract store_date (release_date)
            if "store_date" in missing_fields and data.get("release_date"):
                updates["store_date"] = data["release_date"]

            # Extract page_count
            if "page_count" in missing_fields and data.get("page_count"):
                updates["page_count"] = data["page_count"]
        else:
            rate_mgr.get_limiter(source).record_success()  # API worked, just no match

        # Cleanup
        await adapter.close()

    except Exception as e:
        error_str = str(e).lower()
        if "429" in error_str or "rate" in error_str:
            rate_mgr.get_limiter(source).record_rate_limit()
        else:
            logger.warning(f"[{source}] Error querying: {e}")

    if updates:
        logger.info(f"[{source}] Found {len(updates)} fields for comic {comic.get('id')}")

    return updates


async def query_mycomicshop(
    comic: Dict[str, Any],
    missing_fields: Set[str],
    rate_mgr: RateLimitManager
) -> Dict[str, Any]:
    """
    Query MyComicShop for missing fields (bibliographic data only).

    Provides: upc, isbn, description, cover_date, price, image
    EXCLUDED per user spec: inventory/availability data
    """
    updates = {}
    source = "mycomicshop"

    # Fields MyComicShop can provide
    mcs_fields = {"upc", "isbn", "description", "cover_date", "price", "image"}
    relevant_missing = missing_fields & mcs_fields
    if not relevant_missing:
        return updates

    try:
        from app.adapters.mycomicshop_adapter import create_mycomicshop_adapter

        adapter = await create_mycomicshop_adapter()

        if not await rate_mgr.wait_for_source(source):
            return updates

        series_name = comic.get("series_name", "")
        issue_number = str(comic.get("number", ""))

        if not series_name or not issue_number:
            return updates

        # Search for the issue
        search_result = await adapter.search_issues(
            series_name=series_name,
            issue_number=issue_number,
            limit=5
        )

        if search_result.success and search_result.records:
            rate_mgr.get_limiter(source).record_success()

            # Find best match from results
            best = None
            our_series = series_name.lower()
            our_num = issue_number.lstrip("0") or "0"

            for record in search_result.records:
                title = record.get("title", "").lower()
                # Check if series and number appear in title
                if our_series.split()[0] in title and f"#{our_num}" in title:
                    best = record
                    break

            if not best and search_result.records:
                best = search_result.records[0]  # Fall back to first result

            if best:
                # Get full details if we have a URL
                detail = None
                if best.get("url"):
                    if not await rate_mgr.wait_for_source(source):
                        return updates
                    detail = await adapter.fetch_by_id(best["url"])
                    if detail:
                        rate_mgr.get_limiter(source).record_success()

                # Use detail data or search result
                data = detail or best

                # Extract fields (bibliographic only, no inventory)
                if "image" in missing_fields and data.get("cover_url"):
                    updates["image"] = data["cover_url"]

                if "description" in missing_fields and data.get("description"):
                    updates["description"] = data["description"][:5000]

                if "cover_date" in missing_fields and data.get("cover_date"):
                    updates["cover_date"] = data["cover_date"]

                if "price" in missing_fields and data.get("cover_price"):
                    updates["price"] = data["cover_price"]

                # UPC/ISBN from detail page if available
                if "upc" in missing_fields and data.get("upc"):
                    updates["upc"] = data["upc"]

                if "isbn" in missing_fields and data.get("isbn"):
                    updates["isbn"] = data["isbn"]
        else:
            rate_mgr.get_limiter(source).record_success()  # Request worked, no results

    except Exception as e:
        error_str = str(e).lower()
        if "429" in error_str or "rate" in error_str or "too many" in error_str:
            rate_mgr.get_limiter(source).record_rate_limit()
        else:
            logger.warning(f"[{source}] Error querying: {e}")

    if updates:
        logger.info(f"[{source}] Found {len(updates)} fields for comic {comic.get('id')}")

    return updates


# =============================================================================
# MATCHING HELPERS
# =============================================================================

def _find_best_match(
    comic: Dict[str, Any],
    candidates: List[Dict[str, Any]],
    cv_format: bool = False
) -> Optional[Dict[str, Any]]:
    """Find best matching record using fuzzy scoring."""
    if not candidates:
        return None

    best = None
    best_score = 0

    for candidate in candidates[:10]:  # Limit to top 10
        score = 0

        # Series name match
        if cv_format:
            cand_series = candidate.get("volume", {}).get("name", "")
        else:
            cand_series = candidate.get("series_name", "") or candidate.get("series", {}).get("name", "")

        our_series = comic.get("series_name", "").lower()
        cand_series_lower = str(cand_series).lower()

        if our_series and cand_series_lower:
            if our_series == cand_series_lower:
                score += 5
            elif our_series in cand_series_lower or cand_series_lower in our_series:
                score += 3
            else:
                continue  # Must have some series overlap

        # Issue number match
        our_num = str(comic.get("number", "")).lstrip("0") or "0"

        if cv_format:
            cand_num = str(candidate.get("issue_number", "")).lstrip("0") or "0"
        else:
            cand_num = str(candidate.get("number", "")).lstrip("0") or "0"

        if our_num == cand_num:
            score += 3
        elif our_num in cand_num or cand_num in our_num:
            score += 1

        # Year match
        if comic.get("cover_date"):
            try:
                our_year = str(comic["cover_date"])[:4]
                if cv_format:
                    cand_year = str(candidate.get("cover_date", ""))[:4]
                else:
                    cand_year = str(candidate.get("cover_date", "") or candidate.get("year", ""))[:4]

                if our_year == cand_year:
                    score += 2
            except:
                pass

        if score > best_score:
            best_score = score
            best = candidate

    # Require minimum score (series + number match)
    if best_score >= 5:
        return best

    return None


def _find_best_pc_match(
    comic: Dict[str, Any],
    products: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Find best PriceCharting match."""
    if not products:
        return None

    our_series = comic.get("series_name", "").lower()
    our_num = str(comic.get("number", "")).lstrip("0")

    for product in products[:5]:
        name = product.get("product-name", "").lower()

        # Check if series and number appear in product name
        if our_series and our_series.split()[0] in name:
            if our_num and f"#{our_num}" in name:
                return product

    return None


# =============================================================================
# MAIN JOB
# =============================================================================

async def run_sequential_exhaustive_enrichment_job(
    batch_size: int = 10,
    max_records: int = 0
) -> Dict[str, Any]:
    """
    Sequential Exhaustive Enrichment - Process ONE row at a time, exhaust ALL sources.

    This implements the user's requested algorithm:
    1. For each comic row:
       a. Identify ALL missing fields
       b. Query Source 1, apply updates
       c. RE-EVALUATE what's still missing (refines subsequent searches!)
       d. Query Source 2, apply updates
       e. Repeat until ALL sources exhausted
    2. Move to next row

    The key insight: After each source, we re-check missing fields. If Metron gives us
    a UPC, that UPC can then be used for a more accurate PriceCharting lookup.
    """
    job_name = "sequential_enrichment"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting Sequential Exhaustive Enrichment (batch: {batch_id})")

    # Source query functions in preferred order
    # MSE-002+: All Fandom wikis + MyComicShop sources
    # NOTE: Fandom sources auto-filter by publisher - won't query wrong wikis
    SOURCES = [
        ("metron", query_metron),
        ("comicvine", query_comicvine),
        # Fandom wikis (TEXT ONLY per CC BY-SA - no images)
        # Each filters by publisher_name - no cross-wiki queries
        ("marvel_fandom", query_marvel_fandom),      # Marvel only
        ("dc_fandom", query_dc_fandom),              # DC/Vertigo/WildStorm only
        ("image_fandom", query_image_fandom),        # Image Comics only
        ("idw_fandom", query_idw_fandom),            # IDW only
        ("darkhorse_fandom", query_darkhorse_fandom),  # Dark Horse only
        ("dynamite_fandom", query_dynamite_fandom),    # Dynamite only
        ("mycomicshop", query_mycomicshop),          # Bibliographic only, no inventory
        ("pricecharting", query_pricecharting),
        ("comicbookrealm", query_comicbookrealm),
    ]

    stats = {
        "processed": 0,
        "enriched": 0,
        "fields_filled": 0,
        "by_source": {s[0]: 0 for s in SOURCES},
        "fully_enriched": 0,  # Rows where ALL fields are now filled
        "errors": 0,
    }

    # Rate limit manager (persists across rows)
    rate_mgr = RateLimitManager()

    async with AsyncSessionLocal() as db:
        # Initialize checkpoint
        await db.execute(text("""
            INSERT INTO pipeline_checkpoints (job_name, job_type, created_at, updated_at)
            VALUES (:name, 'enrichment', NOW(), NOW())
            ON CONFLICT (job_name) DO NOTHING
        """), {"name": job_name})

        # Claim job (atomic)
        result = await db.execute(text("""
            UPDATE pipeline_checkpoints
            SET is_running = true,
                last_run_started = NOW(),
                current_batch_id = :batch_id,
                updated_at = NOW()
            WHERE job_name = :name
            AND (is_running = false OR is_running IS NULL)
            RETURNING id, state_data
        """), {"name": job_name, "batch_id": batch_id})
        claim = result.fetchone()

        if not claim:
            logger.warning(f"[{job_name}] Job already running, skipping")
            return {"status": "skipped", "message": "Job already running"}

        await db.commit()

        state_data = claim.state_data or {}
        last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

        try:
            while True:
                # Fetch batch of comics that need enrichment
                result = await db.execute(text("""
                    SELECT id, metron_id, comicvine_id, gcd_id, pricecharting_id,
                           series_name, number, issue_name, publisher_name,
                           cover_date, store_date, description, page_count, price,
                           upc, isbn, isbn_normalized, image,
                           price_loose, price_graded
                    FROM comic_issues
                    WHERE id > :last_id
                    AND series_name IS NOT NULL
                    ORDER BY id
                    LIMIT :limit
                """), {"last_id": last_id, "limit": batch_size})

                comics = result.fetchall()

                if not comics:
                    logger.info(f"[{job_name}] No more comics to process")
                    break

                for comic_row in comics:
                    # Convert to dict for easier manipulation
                    comic = {
                        "id": comic_row.id,
                        "metron_id": comic_row.metron_id,
                        "comicvine_id": comic_row.comicvine_id,
                        "gcd_id": comic_row.gcd_id,
                        "pricecharting_id": comic_row.pricecharting_id,
                        "series_name": comic_row.series_name,
                        "number": comic_row.number,
                        "issue_name": comic_row.issue_name,
                        "publisher_name": comic_row.publisher_name,
                        "cover_date": comic_row.cover_date,
                        "store_date": comic_row.store_date,
                        "description": comic_row.description,
                        "page_count": comic_row.page_count,
                        "price": comic_row.price,
                        "upc": comic_row.upc,
                        "isbn": comic_row.isbn,
                        "image": comic_row.image,
                        "price_loose": comic_row.price_loose,
                        "price_graded": comic_row.price_graded,
                    }

                    comic_id = comic["id"]
                    last_id = comic_id
                    stats["processed"] += 1

                    if max_records > 0 and stats["processed"] > max_records:
                        logger.info(f"[{job_name}] Reached max_records limit ({max_records})")
                        break

                    # =========================================================
                    # STEP 1: Initial field analysis
                    # =========================================================
                    all_updates = {}

                    # Get initial missing fields
                    missing = identify_missing_fields(comic)

                    if not missing:
                        # Already fully enriched
                        stats["fully_enriched"] += 1
                        continue

                    logger.debug(
                        f"[{job_name}] Comic {comic_id}: {len(missing)} missing fields: "
                        f"{', '.join(list(missing)[:5])}..."
                    )

                    # =========================================================
                    # STEP 2: Query sources SEQUENTIALLY, re-evaluate after each
                    # =========================================================

                    # Sort sources by availability (least rate-limited first)
                    available_sources = rate_mgr.get_available_sources([s[0] for s in SOURCES])
                    source_funcs = {s[0]: s[1] for s in SOURCES}

                    for source_name in available_sources:
                        # Get sources that can help with CURRENT missing fields
                        helpful_sources = get_sources_for_fields(missing)

                        if source_name not in helpful_sources:
                            continue  # This source can't help with what's missing

                        # Check if source is available
                        if not rate_mgr.get_limiter(source_name).can_make_request():
                            logger.debug(f"[{job_name}] {source_name} rate limited, skipping")
                            continue

                        try:
                            # Query the source
                            query_func = source_funcs[source_name]
                            updates = await query_func(comic, missing, rate_mgr)

                            if updates:
                                # Apply updates to our working copy
                                for field, value in updates.items():
                                    if field in missing or field.endswith("_id"):
                                        comic[field] = value
                                        all_updates[field] = value
                                        stats["by_source"][source_name] += 1

                                # =========================================
                                # KEY: RE-EVALUATE missing fields!
                                # This is what makes sequential better -
                                # subsequent searches can use newly acquired data
                                # =========================================
                                missing = identify_missing_fields(comic)

                                if not missing:
                                    logger.info(f"[{job_name}] Comic {comic_id} fully enriched!")
                                    stats["fully_enriched"] += 1
                                    break

                        except Exception as e:
                            logger.warning(f"[{job_name}] Error querying {source_name}: {e}")
                            stats["errors"] += 1

                    # =========================================================
                    # STEP 3: Apply all accumulated updates to database
                    # =========================================================
                    if all_updates:
                        try:
                            # Use savepoint for transaction safety
                            async with db.begin_nested():
                                # Build dynamic UPDATE
                                set_clauses = []
                                params = {"id": comic_id}

                                for field, value in all_updates.items():
                                    set_clauses.append(f"{field} = :{field}")
                                    params[field] = value

                                set_clauses.append("updated_at = NOW()")

                                sql = f"""
                                    UPDATE comic_issues
                                    SET {', '.join(set_clauses)}
                                    WHERE id = :id
                                """

                                await db.execute(text(sql), params)

                            stats["enriched"] += 1
                            stats["fields_filled"] += len(all_updates)

                            logger.info(
                                f"[{job_name}] Comic {comic_id}: Updated {len(all_updates)} fields"
                            )

                        except Exception as e:
                            logger.error(f"[{job_name}] Failed to update comic {comic_id}: {e}")
                            stats["errors"] += 1

                    # Checkpoint every 10 rows
                    if stats["processed"] % 10 == 0:
                        await db.execute(text("""
                            UPDATE pipeline_checkpoints
                            SET state_data = jsonb_build_object('last_id', CAST(:last_id AS integer)),
                                total_processed = COALESCE(total_processed, 0) + :batch_processed,
                                updated_at = NOW()
                            WHERE job_name = :name
                        """), {
                            "name": job_name,
                            "last_id": last_id,
                            "batch_processed": 10
                        })
                        await db.commit()

                # Commit batch
                await db.commit()

                if max_records > 0 and stats["processed"] >= max_records:
                    break

            # Final checkpoint update
            await db.execute(text("""
                UPDATE pipeline_checkpoints
                SET is_running = false,
                    state_data = jsonb_build_object('last_id', CAST(:last_id AS integer)),
                    total_processed = COALESCE(total_processed, 0) + :batch_processed,
                    last_run_completed = NOW(),
                    updated_at = NOW()
                WHERE job_name = :name
            """), {
                "name": job_name,
                "last_id": last_id,
                "batch_processed": stats["processed"] % 10  # Remaining
            })
            await db.commit()

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            import traceback
            traceback.print_exc()

            await db.execute(text("""
                UPDATE pipeline_checkpoints
                SET is_running = false,
                    last_error = :error,
                    state_data = jsonb_build_object('last_id', CAST(:last_id AS integer)),
                    updated_at = NOW()
                WHERE job_name = :name
            """), {"name": job_name, "error": str(e)[:500], "last_id": last_id})
            await db.commit()

            stats["error"] = str(e)

    logger.info(f"[{job_name}] Complete! Stats: {stats}")
    return stats
