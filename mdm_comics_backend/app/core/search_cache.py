"""
Search Result Cache for PriceCharting API v1.0.0

Per constitution_devops_doctrine.json: Minimize redundant external calls
Per proposal PC-OPT-2024-001 Phase 1: LRU cache with TTL

Purpose:
- Reduces duplicate API calls for same-series comics
- Cache key: MD5 hash of search query + filters
- TTL: 1 hour (configurable)
- Max size: 1000 entries (LRU eviction)

Usage:
    from app.core.search_cache import pricecharting_search_cache

    # Get or fetch with automatic caching
    results = await pricecharting_search_cache.get_or_fetch(
        query="Spider-Man #1",
        fetch_func=do_api_search,
        console_name="Comics"
    )

    # Manual get/set
    cached = pricecharting_search_cache.get("Spider-Man #1", console_name="Comics")
    if cached is None:
        results = await do_api_search(...)
        pricecharting_search_cache.set("Spider-Man #1", results, console_name="Comics")
"""
import hashlib
import logging
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class SearchCache:
    """
    LRU cache with TTL for PriceCharting search results.

    Reduces duplicate API calls for same-series comics.
    Thread-safe for single-threaded async usage (standard in asyncio).

    Attributes:
        ttl_seconds: Time-to-live for cache entries (default: 3600 = 1 hour)
        max_size: Maximum cache entries before LRU eviction (default: 1000)
    """

    def __init__(self, ttl_seconds: int = 3600, max_size: int = 1000):
        """
        Initialize the search cache.

        Args:
            ttl_seconds: Time-to-live for cache entries in seconds
            max_size: Maximum number of cache entries
        """
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self._cache: OrderedDict[str, Tuple[float, List[Dict]]] = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def _make_key(self, query: str, **filters) -> str:
        """
        Generate cache key from query and filters.

        Args:
            query: Search query string
            **filters: Additional filter parameters (console_name, etc.)

        Returns:
            MD5 hash of normalized query + filters
        """
        # Normalize query: lowercase, strip whitespace
        normalized_query = query.lower().strip()

        # Build key from query + sorted filters for deterministic hashing
        key_parts = [normalized_query]
        for k, v in sorted(filters.items()):
            if v is not None:
                key_parts.append(f"{k}={str(v).lower().strip()}")

        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()

    def get(self, query: str, **filters) -> Optional[List[Dict]]:
        """
        Get cached results if valid.

        Args:
            query: Search query string
            **filters: Additional filter parameters

        Returns:
            Cached results list or None if not found/expired
        """
        key = self._make_key(query, **filters)

        if key not in self._cache:
            self._misses += 1
            return None

        timestamp, results = self._cache[key]

        # Check expiration
        if time.time() - timestamp > self.ttl_seconds:
            # Expired - remove and return miss
            del self._cache[key]
            self._misses += 1
            logger.debug(f"[SEARCH_CACHE] Expired: {query[:50]}...")
            return None

        # Move to end (most recently used)
        self._cache.move_to_end(key)
        self._hits += 1
        logger.debug(f"[SEARCH_CACHE] Hit: {query[:50]}... ({len(results)} results)")
        return results

    def set(self, query: str, results: List[Dict], **filters) -> None:
        """
        Cache search results.

        Args:
            query: Search query string
            results: Search results to cache
            **filters: Additional filter parameters
        """
        key = self._make_key(query, **filters)

        # Evict oldest entries if at capacity
        while len(self._cache) >= self.max_size:
            evicted_key, _ = self._cache.popitem(last=False)
            self._evictions += 1
            logger.debug(f"[SEARCH_CACHE] Evicted oldest entry (capacity)")

        self._cache[key] = (time.time(), results)
        logger.debug(f"[SEARCH_CACHE] Stored: {query[:50]}... ({len(results)} results)")

    async def get_or_fetch(
        self,
        query: str,
        fetch_func: Callable[..., Any],
        **filters
    ) -> List[Dict]:
        """
        Get from cache or fetch and cache.

        This is the primary interface for cached lookups.

        Args:
            query: Search query string
            fetch_func: Async function to call if cache miss
                       Should accept (query, **filters) and return List[Dict]
            **filters: Additional filter parameters passed to fetch_func

        Returns:
            Search results (from cache or fresh fetch)
        """
        # Try cache first
        cached = self.get(query, **filters)
        if cached is not None:
            return cached

        # Cache miss - fetch from API
        try:
            results = await fetch_func(query, **filters)
            # Only cache successful non-empty responses
            if isinstance(results, list):
                self.set(query, results, **filters)
            return results if results else []
        except Exception as e:
            logger.warning(f"[SEARCH_CACHE] Fetch failed for '{query[:50]}...': {e}")
            raise

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dict with hits, misses, hit_rate, size, etc.
        """
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(self._hits / total, 4) if total > 0 else 0.0,
            "size": len(self._cache),
            "max_size": self.max_size,
            "ttl_seconds": self.ttl_seconds,
            "evictions": self._evictions,
        }

    def clear(self) -> None:
        """Clear all cached entries."""
        count = len(self._cache)
        self._cache.clear()
        logger.info(f"[SEARCH_CACHE] Cleared {count} entries")

    def invalidate(self, query: str, **filters) -> bool:
        """
        Invalidate a specific cache entry.

        Args:
            query: Search query string
            **filters: Additional filter parameters

        Returns:
            True if entry was found and removed, False otherwise
        """
        key = self._make_key(query, **filters)
        if key in self._cache:
            del self._cache[key]
            logger.debug(f"[SEARCH_CACHE] Invalidated: {query[:50]}...")
            return True
        return False

    def reset_stats(self) -> None:
        """Reset hit/miss/eviction counters."""
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        logger.info("[SEARCH_CACHE] Stats reset")


# Global instance for PriceCharting searches
# TTL: 1 hour (prices don't change frequently)
# Max size: 1000 entries (covers most daily operations)
pricecharting_search_cache = SearchCache(ttl_seconds=3600, max_size=1000)
