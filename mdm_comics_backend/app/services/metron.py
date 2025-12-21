"""
Metron Comic Database API Service
https://metron.cloud/

v2.0.0: REFACTOR - Consolidated to use MetronAdapter (Mokkari)
        - All Metron traffic now routes through rate-limited MetronAdapter
        - Fixes split-brain issue causing 429 errors (IMP-20251220-METRON-FIX)
        - Mokkari handles: 30/min, 10,000/day rate limits with SQLite persistence

v1.6.0: Fixed client lifecycle management (RISK-010 from pipeline spec)
        - Proper async context manager for client lifecycle
        - Registered shutdown hook for cleanup
        - Client is now properly closed on service shutdown

v1.1.0: Refactored to use ResilientHTTPClient for retry logic,
        exponential backoff, and rate limiting per pipeline spec.
"""
import logging
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

from ..core.config import settings
from ..adapters.metron_adapter import MetronAdapter, get_metron_stats

logger = logging.getLogger(__name__)


class MetronService:
    """
    Service for interacting with the Metron comic database API.

    v2.0.0: REFACTORED to use MetronAdapter (Mokkari library)
    - All traffic now goes through rate-limited Mokkari client
    - 30 req/min, 10,000 req/day with SQLite persistence
    - No more split-brain between raw httpx and Mokkari
    - Fixes IMP-20251220-METRON-FIX (429 Too Many Requests errors)

    Previous versions used ResilientHTTPClient which bypassed Mokkari's
    rate limiting, causing 429 errors and risk of IP ban.
    """

    def __init__(self):
        self.base_url = settings.METRON_API_BASE
        self._adapter: Optional[MetronAdapter] = None

    async def __aenter__(self):
        """Async context manager entry - initialize adapter."""
        self._ensure_adapter()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - no cleanup needed for Mokkari singleton."""
        pass

    def _ensure_adapter(self):
        """Ensure MetronAdapter is initialized."""
        if self._adapter is None:
            self._adapter = MetronAdapter()
            logger.debug("[METRON] MetronAdapter initialized (using Mokkari)")

    async def close(self):
        """No cleanup needed - Mokkari manages its own lifecycle."""
        pass

    def get_stats(self) -> Dict[str, Any]:
        """Get Metron API request statistics."""
        return get_metron_stats()

    async def search_issues(
        self,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
        publisher_name: Optional[str] = None,
        cover_year: Optional[int] = None,
        upc: Optional[str] = None,
        isbn: Optional[str] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """
        Search for comic issues.

        Search priority (exact identifiers first):
        1. UPC - exact barcode match, fastest lookup
        2. ISBN - exact ISBN match
        3. Series name + filters - requires series ID lookup

        Args:
            series_name: Name of the series (e.g., "amazing spider-man")
            number: Issue number (e.g., "300")
            publisher_name: Publisher name (e.g., "marvel")
            cover_year: Year of cover date
            upc: UPC barcode number (highest priority - exact match)
            isbn: ISBN number (exact match)
            page: Page number for pagination

        Returns:
            Dict with 'results' list and pagination info
        """
        self._ensure_adapter()
        result = await self._adapter.search_issues(
            series_name=series_name,
            number=number,
            publisher_name=publisher_name,
            cover_year=cover_year,
            upc=upc,
            isbn=isbn,
            page=page
        )
        # Convert FetchResult to legacy dict format
        response = {
            "results": result.records if result.success else [],
            "next": None if not result.has_more else f"page={page + 1}",
            "count": result.total_count or len(result.records),
        }

        # CRITICAL: Propagate errors so multi_source_search can trigger fallback
        if not result.success and result.errors:
            error_info = result.errors[0] if result.errors else {}
            error_type = error_info.get("error", "unknown")
            response["error"] = error_type
            if "retry_after" in error_info:
                response["retry_after"] = error_info["retry_after"]

        return response

    async def get_issue(self, issue_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific issue."""
        self._ensure_adapter()
        result = await self._adapter.fetch_by_id(str(issue_id), endpoint="issue")
        if result is None:
            raise ValueError(f"Issue {issue_id} not found")
        return result

    async def search_series(
        self,
        name: Optional[str] = None,
        publisher_name: Optional[str] = None,
        year_began: Optional[int] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """
        Search for comic series.

        Args:
            name: Series name
            publisher_name: Publisher name
            year_began: Year the series started
            page: Page number for pagination
        """
        self._ensure_adapter()
        filters = {}
        if name:
            filters["name"] = name
        if publisher_name:
            filters["publisher_name"] = publisher_name
        if year_began:
            filters["year_began"] = year_began

        result = await self._adapter.fetch_page(page=page, endpoint="series", **filters)
        return {
            "results": result.records if result.success else [],
            "next": None if not result.has_more else f"page={page + 1}",
            "count": result.total_count or len(result.records),
        }

    async def get_series(self, series_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific series."""
        self._ensure_adapter()
        result = await self._adapter.fetch_by_id(str(series_id), endpoint="series")
        if result is None:
            raise ValueError(f"Series {series_id} not found")
        return result

    async def get_publishers(self, page: int = 1) -> Dict[str, Any]:
        """Get list of publishers."""
        self._ensure_adapter()
        result = await self._adapter.fetch_page(page=page, endpoint="publisher")
        return {
            "results": result.records if result.success else [],
            "next": None if not result.has_more else f"page={page + 1}",
            "count": result.total_count or len(result.records),
        }

    async def search_characters(
        self,
        name: Optional[str] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """Search for characters."""
        self._ensure_adapter()
        filters = {}
        if name:
            filters["name"] = name
        result = await self._adapter.fetch_page(page=page, endpoint="character", **filters)
        return {
            "results": result.records if result.success else [],
            "next": None if not result.has_more else f"page={page + 1}",
            "count": result.total_count or len(result.records),
        }

    async def search_creators(
        self,
        name: Optional[str] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """Search for creators (writers, artists, etc.)."""
        self._ensure_adapter()
        filters = {}
        if name:
            filters["name"] = name
        result = await self._adapter.fetch_page(page=page, endpoint="creator", **filters)
        return {
            "results": result.records if result.success else [],
            "next": None if not result.has_more else f"page={page + 1}",
            "count": result.total_count or len(result.records),
        }

    async def fetch_all_pages(
        self,
        method: str,
        max_pages: int = 100,
        **kwargs
    ) -> list:
        """
        Fetch all pages of results from a paginated endpoint.

        Respects rate limits automatically via MetronAdapter (Mokkari).

        Args:
            method: Method name to call (e.g., 'search_issues')
            max_pages: Maximum pages to fetch (safety limit)
            **kwargs: Arguments to pass to the method

        Returns:
            List of all results combined
        """
        all_results = []
        page = 1

        while page <= max_pages:
            logger.info(f"[METRON] Fetching page {page}...")

            # Call the appropriate method
            func = getattr(self, method)
            response = await func(page=page, **kwargs)

            results = response.get("results", [])
            all_results.extend(results)

            # Check if more pages exist
            next_url = response.get("next")
            if not next_url or not results:
                logger.info(f"[METRON] Completed after {page} pages, {len(all_results)} total results")
                break

            page += 1

        return all_results


# Singleton instance
metron_service = MetronService()
