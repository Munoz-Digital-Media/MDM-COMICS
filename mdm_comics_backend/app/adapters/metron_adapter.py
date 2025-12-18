"""
Metron Adapter v2.0.0

Adapter for Metron API using official Mokkari library.
https://metron.cloud/
https://github.com/Metron-Project/mokkari

v2.0.0 Changes:
- REFACTOR: Use official Mokkari library instead of custom HTTP client
- Built-in rate limiting: 30 req/min, 10,000 req/day (SQLite persisted)
- Proper RateLimitError handling with retry_after
- No more manual rate limit management needed

Per pipeline spec:
- Rich comic metadata (issues, variants, creators, publisher, print runs, story arcs)
"""
import asyncio
import os
import logging
from typing import Any, Dict, List, Optional

from app.core.adapter_registry import (
    DataSourceAdapter,
    AdapterConfig,
    FetchResult,
    METRON_CONFIG,
)

logger = logging.getLogger(__name__)

# Lazy import mokkari to avoid import errors if not installed
_mokkari_client = None


class RateLimitError(Exception):
    """Rate limit exceeded - wraps mokkari.exceptions.RateLimitError."""
    def __init__(self, message: str, retry_after: float = 60.0):
        super().__init__(message)
        self.retry_after = retry_after


def _get_mokkari_client():
    """Get or create the Mokkari client singleton."""
    global _mokkari_client
    if _mokkari_client is None:
        try:
            import mokkari
            username = os.getenv("METRON_USERNAME", "")
            password = os.getenv("METRON_PASSWORD", "")
            if username and password:
                _mokkari_client = mokkari.api(username, password)
                logger.info("[Metron] Mokkari client initialized with built-in rate limiting")
            else:
                logger.warning("[Metron] Credentials not configured")
        except ImportError:
            logger.error("[Metron] mokkari package not installed - run: pip install mokkari")
        except Exception as e:
            logger.error(f"[Metron] Failed to initialize Mokkari client: {e}")
    return _mokkari_client


class MetronAdapter(DataSourceAdapter):
    """
    Adapter for Metron comic database API using official Mokkari library.

    Mokkari handles:
    - Rate limiting (30/min, 10,000/day) with SQLite persistence
    - Authentication
    - Retry logic with proper backoff
    - RateLimitError with retry_after attribute
    """

    def __init__(
        self,
        config: AdapterConfig = METRON_CONFIG,
        client = None  # Ignored - we use Mokkari singleton
    ):
        # Don't call super().__init__ with client - we manage our own
        self.config = config
        self._name = config.name  # Store name separately (base class has read-only property)
        self._client = None
        self._status = "enabled"

    @property
    def name(self) -> str:
        """Override base class property."""
        return self._name

    def _get_client(self):
        """Get the Mokkari client."""
        if self._client is None:
            self._client = _get_mokkari_client()
        return self._client

    async def health_check(self) -> bool:
        """Check if Metron API is reachable and credentials are valid."""
        client = self._get_client()
        if client is None:
            return False

        try:
            # Use a lightweight call - get a single publisher
            # Run sync mokkari in thread pool
            result = await asyncio.to_thread(client.publishers_list, {"name": "marvel"})
            return result is not None
        except Exception as e:
            logger.error(f"[{self.name}] Health check failed: {e}")
            return False

    async def fetch_page(
        self,
        page: int = 1,
        page_size: int = 100,
        cursor: Optional[str] = None,
        endpoint: str = "issue",
        **filters
    ) -> FetchResult:
        """
        Fetch a page of records from Metron.

        Args:
            page: Page number (1-indexed)
            page_size: Not used - Metron has fixed page sizes
            cursor: Not used - Metron uses page numbers
            endpoint: API endpoint (issue, series, publisher, character, creator)
            **filters: Metron-specific filters
        """
        client = self._get_client()
        if client is None:
            return FetchResult(
                success=False,
                errors=[{"error": "Mokkari client not initialized"}]
            )

        try:
            import mokkari.exceptions as mokkari_exc

            params = {"page": page, **filters}

            # Map endpoint to Mokkari method
            method_map = {
                "issue": client.issues_list,
                "series": client.series_list,
                "publisher": client.publishers_list,
                "character": client.characters_list,
                "creator": client.creators_list,
            }

            method = method_map.get(endpoint)
            if not method:
                return FetchResult(
                    success=False,
                    errors=[{"error": f"Unknown endpoint: {endpoint}"}]
                )

            # Run sync mokkari in thread pool
            results = await asyncio.to_thread(method, params)

            # Convert Mokkari objects to dicts
            records = []
            for item in results:
                if hasattr(item, '__dict__'):
                    records.append(self._mokkari_obj_to_dict(item))
                else:
                    records.append(item)

            return FetchResult(
                success=True,
                records=records,
                has_more=len(records) >= 20,  # Metron default page size
                total_count=len(records),
            )

        except Exception as e:
            # Check for rate limit error
            error_str = str(e).lower()
            if "rate" in error_str or "429" in error_str:
                retry_after = getattr(e, 'retry_after', 60.0)
                logger.warning(f"[{self.name}] Rate limited, retry after {retry_after}s")
                return FetchResult(
                    success=False,
                    errors=[{"error": "rate_limited", "retry_after": retry_after}]
                )

            logger.error(f"[{self.name}] Fetch failed: {e}")
            return FetchResult(
                success=False,
                errors=[{"error": str(e)}]
            )

    async def fetch_by_id(self, external_id: str, endpoint: str = "issue") -> Optional[Dict[str, Any]]:
        """Fetch a single record by Metron ID."""
        client = self._get_client()
        if client is None:
            return None

        try:
            import mokkari.exceptions as mokkari_exc

            # Map endpoint to Mokkari method
            method_map = {
                "issue": client.issue,
                "series": client.series,
                "publisher": client.publisher,
                "character": client.character,
                "creator": client.creator,
            }

            method = method_map.get(endpoint)
            if not method:
                logger.error(f"[{self.name}] Unknown endpoint: {endpoint}")
                return None

            # Run sync mokkari in thread pool
            result = await asyncio.to_thread(method, int(external_id))

            if result:
                return self._mokkari_obj_to_dict(result)
            return None

        except Exception as e:
            error_str = str(e).lower()
            if "rate" in error_str or "429" in error_str:
                retry_after = getattr(e, 'retry_after', 60.0)
                logger.warning(f"[{self.name}] Rate limited on fetch_by_id, retry after {retry_after}s")
                raise RateLimitError(f"Rate limited: {e}", retry_after)

            logger.error(f"[{self.name}] Fetch by ID {external_id} failed: {e}")
            return None

    def _mokkari_obj_to_dict(self, obj) -> Dict[str, Any]:
        """Convert a Mokkari pydantic object to a dict."""
        if hasattr(obj, 'model_dump'):
            # Pydantic v2
            return obj.model_dump()
        elif hasattr(obj, 'dict'):
            # Pydantic v1
            return obj.dict()
        elif hasattr(obj, '__dict__'):
            return dict(obj.__dict__)
        return {}

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize Metron issue record to canonical schema.
        """
        # Handle series info
        series = record.get("series", {}) or {}
        if hasattr(series, '__dict__'):
            series = self._mokkari_obj_to_dict(series)

        publisher = record.get("publisher", {}) or series.get("publisher", {}) or {}
        if hasattr(publisher, '__dict__'):
            publisher = self._mokkari_obj_to_dict(publisher)

        # Parse cover date
        cover_date = record.get("cover_date")

        return {
            # Identifiers
            "metron_id": record.get("id"),
            "upc": record.get("upc"),
            "sku": record.get("sku"),
            "isbn": record.get("isbn"),

            # Series info
            "series_id": series.get("id") if isinstance(series, dict) else getattr(series, 'id', None),
            "series_name": series.get("name") if isinstance(series, dict) else getattr(series, 'name', None),
            "series_volume": series.get("volume") if isinstance(series, dict) else getattr(series, 'volume', None),

            # Publisher info
            "publisher_id": publisher.get("id") if isinstance(publisher, dict) else getattr(publisher, 'id', None),
            "publisher_name": publisher.get("name") if isinstance(publisher, dict) else getattr(publisher, 'name', None),

            # Issue details
            "number": record.get("number"),
            "issue_name": record.get("issue_name") or record.get("name"),
            "cover_date": cover_date,
            "store_date": record.get("store_date"),

            # Cover image
            "image": record.get("image"),

            # Specs
            "price": record.get("price"),
            "page_count": record.get("page_count"),

            # Description
            "description": record.get("desc") or record.get("description"),

            # Variant info
            "is_variant": record.get("is_variant", False),
            "variant_name": record.get("variant_name"),

            # Ratings
            "rating": record.get("rating", {}).get("name") if isinstance(record.get("rating"), dict) else None,

            # Store raw data for future parsing
            "raw_data": record,

            # Source tracking
            "_source": self.name,
            "_source_id": str(record.get("id")),
        }

    async def search_issues(
        self,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
        publisher_name: Optional[str] = None,
        cover_year: Optional[int] = None,
        upc: Optional[str] = None,
        page: int = 1
    ) -> FetchResult:
        """Search for comic issues with filters."""
        filters = {}
        if series_name:
            filters["series_name"] = series_name
        if number:
            filters["number"] = number
        if publisher_name:
            filters["publisher_name"] = publisher_name
        if cover_year:
            filters["cover_year"] = cover_year
        if upc:
            filters["upc"] = upc

        return await self.fetch_page(page=page, endpoint="issue", **filters)
