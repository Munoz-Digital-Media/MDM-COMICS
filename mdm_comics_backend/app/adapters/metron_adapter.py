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
from datetime import datetime
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


class MetronRequestLogger:
    """Track Metron API request metrics for observability."""

    def __init__(self):
        self.requests_total = 0
        self.requests_success = 0
        self.requests_rate_limited = 0
        self.requests_failed = 0
        self.last_request_at = None
        self.last_success_at = None
        self.last_rate_limit_at = None
        self.current_retry_after = 0

    def log_request(self):
        self.requests_total += 1
        self.last_request_at = datetime.now().isoformat()
        logger.info(f"[Metron] REQUEST #{self.requests_total} at {self.last_request_at}")

    def log_success(self, endpoint: str, result_count: int = 0):
        self.requests_success += 1
        self.last_success_at = datetime.now().isoformat()
        logger.info(f"[Metron] SUCCESS #{self.requests_success}/{self.requests_total} - {endpoint} returned {result_count} results")

    def log_rate_limit(self, retry_after: float):
        self.requests_rate_limited += 1
        self.last_rate_limit_at = datetime.now().isoformat()
        self.current_retry_after = retry_after
        logger.warning(f"[Metron] RATE_LIMITED #{self.requests_rate_limited} - retry_after={retry_after}s ({retry_after/60:.1f}min)")

    def log_error(self, error: str):
        self.requests_failed += 1
        logger.error(f"[Metron] ERROR #{self.requests_failed}/{self.requests_total} - {error}")

    def get_stats(self) -> dict:
        return {
            "requests_total": self.requests_total,
            "requests_success": self.requests_success,
            "requests_rate_limited": self.requests_rate_limited,
            "requests_failed": self.requests_failed,
            "success_rate": f"{(self.requests_success/self.requests_total*100):.1f}%" if self.requests_total > 0 else "N/A",
            "last_request_at": self.last_request_at,
            "last_success_at": self.last_success_at,
            "last_rate_limit_at": self.last_rate_limit_at,
            "current_retry_after_seconds": self.current_retry_after,
        }


# Global request logger
_request_logger = MetronRequestLogger()


def get_metron_stats() -> dict:
    """Get Metron API request statistics."""
    return _request_logger.get_stats()


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
                logger.info("[Metron] Mokkari client initialized - rate limits: 30/min, 10000/day (SQLite persisted)")
            else:
                logger.warning("[Metron] Credentials not configured")
        except ImportError:
            logger.error("[Metron] mokkari package not installed - run: pip install mokkari")
        except Exception as e:
            logger.error(f"[Metron] Failed to initialize Mokkari client: {e}")
    return _mokkari_client


# Global request queue and worker task
_metron_request_queue: asyncio.Queue = asyncio.Queue()
_metron_worker_task: Optional[asyncio.Task] = None


async def metron_worker():
    """
    Dedicated worker to process Metron requests serially.
    Ensures exactly 1 request per second across the entire application.
    """
    logger.info("[MetronWorker] Started serial request processor")
    while True:
        try:
            sync_func, args, kwargs, future = await _metron_request_queue.get()
            
            # Check if caller cancelled
            if future.done():
                _metron_request_queue.task_done()
                continue

            try:
                # Log request start
                _request_logger.log_request()
                
                # Execute sync function in thread pool
                result = await asyncio.to_thread(sync_func, *args, **kwargs)
                
                if not future.done():
                    future.set_result(result)
            except Exception as e:
                if not future.done():
                    future.set_exception(e)
            finally:
                _metron_request_queue.task_done()
                # Strict rate limiting spacing
                await asyncio.sleep(1.0)
                
        except asyncio.CancelledError:
            logger.info("[MetronWorker] Worker cancelled")
            break
        except Exception as e:
            logger.error(f"[MetronWorker] Unexpected error: {e}")
            await asyncio.sleep(1.0)


async def start_metron_worker():
    """Start the global Metron worker if not running."""
    global _metron_worker_task
    if _metron_worker_task is None or _metron_worker_task.done():
        _metron_worker_task = asyncio.create_task(metron_worker())
        logger.info("[MetronAdapter] Global worker started")


async def stop_metron_worker():
    """Stop the global Metron worker."""
    global _metron_worker_task
    if _metron_worker_task and not _metron_worker_task.done():
        _metron_worker_task.cancel()
        try:
            await _metron_worker_task
        except asyncio.CancelledError:
            pass
        _metron_worker_task = None
        logger.info("[MetronAdapter] Global worker stopped")


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

            # Enqueue request to global worker
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            
            # Ensure worker is running (lazy start if needed, though explicit start preferred)
            if _metron_worker_task is None:
                 await start_metron_worker()

            await _metron_request_queue.put((method, (params,), {}, future))

            try:
                # Wait for result with timeout (allow queue backlog)
                results = await asyncio.wait_for(future, timeout=120.0)
                
                # Convert Mokkari objects to dicts
                records = []
                for item in results:
                    if hasattr(item, '__dict__'):
                        records.append(self._mokkari_obj_to_dict(item))
                    else:
                        records.append(item)

                # Log success (worker logs request start)
                _request_logger.log_success(endpoint, len(records))

                return FetchResult(
                    success=True,
                    records=records,
                    has_more=len(records) >= 20,  # Metron default page size
                    total_count=len(records),
                )
            except asyncio.TimeoutError:
                future.cancel()
                return FetchResult(success=False, errors=[{"error": "Queue timeout"}])

        except Exception as e:
            # Check for rate limit error
            error_str = str(e).lower()
            if "rate" in error_str or "429" in error_str:
                retry_after = getattr(e, 'retry_after', 60.0)
                _request_logger.log_rate_limit(retry_after)
                return FetchResult(
                    success=False,
                    errors=[{"error": "rate_limited", "retry_after": retry_after}]
                )

            _request_logger.log_error(str(e))
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

            # Enqueue request
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            
            if _metron_worker_task is None:
                 await start_metron_worker()

            await _metron_request_queue.put((method, (int(external_id),), {}, future))

            try:
                result = await asyncio.wait_for(future, timeout=120.0)
                
                if result:
                    _request_logger.log_success(f"{endpoint}/{external_id}", 1)
                    return self._mokkari_obj_to_dict(result)
                return None
            except asyncio.TimeoutError:
                future.cancel()
                logger.warning(f"[{self.name}] Queue timeout fetching {external_id}")
                return None

        except Exception as e:
            error_str = str(e).lower()
            if "rate" in error_str or "429" in error_str:
                retry_after = getattr(e, 'retry_after', 60.0)
                _request_logger.log_rate_limit(retry_after)
                raise RateLimitError(f"Rate limited: {e}", retry_after)

            _request_logger.log_error(f"fetch_by_id({external_id}): {e}")
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

    async def search_series(
        self,
        name: Optional[str] = None,
        publisher_name: Optional[str] = None,
        year_began: Optional[int] = None,
        page: int = 1
    ) -> FetchResult:
        """Search for comic series by name."""
        filters = {}
        if name:
            filters["name"] = name
        if publisher_name:
            filters["publisher"] = publisher_name
        if year_began:
            filters["year_began"] = year_began

        return await self.fetch_page(page=page, endpoint="series", **filters)

    async def search_issues(
        self,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
        publisher_name: Optional[str] = None,
        cover_year: Optional[int] = None,
        upc: Optional[str] = None,
        isbn: Optional[str] = None,
        page: int = 1
    ) -> FetchResult:
        """
        Search for comic issues with filters.

        Search Priority (exact identifiers first):
        1. UPC - exact match, skip series lookup entirely
        2. ISBN - exact match, skip series lookup entirely
        3. Series name + filters - requires series ID lookup first

        Note: Metron's issues endpoint requires a series ID for series-based search,
        but UPC/ISBN can be searched directly without series context.
        """
        # PRIORITY 1: UPC is an exact identifier - use it directly
        if upc:
            logger.debug(f"[METRON] Searching by UPC (exact match): {upc}")
            filters = {"upc": upc}
            if number:
                filters["number"] = number
            return await self.fetch_page(page=page, endpoint="issue", **filters)

        # PRIORITY 2: ISBN is an exact identifier - use it directly
        if isbn:
            logger.debug(f"[METRON] Searching by ISBN (exact match): {isbn}")
            filters = {"isbn": isbn}
            if number:
                filters["number"] = number
            return await self.fetch_page(page=page, endpoint="issue", **filters)

        # PRIORITY 3: Series name search - requires series ID lookup first
        series_ids = []
        if series_name:
            series_result = await self.search_series(name=series_name, publisher_name=publisher_name)
            if series_result.success and series_result.records:
                # Get up to 5 matching series IDs
                series_ids = [r.get("id") for r in series_result.records[:5] if r.get("id")]
                logger.debug(f"[METRON] Found {len(series_ids)} series matching '{series_name}'")

            if not series_ids:
                # No matching series found
                logger.debug(f"[METRON] No series found matching '{series_name}'")
                return FetchResult(success=True, records=[], total_count=0)

        # Build issue search filters
        filters = {}
        if number:
            filters["number"] = number
        if cover_year:
            filters["cover_year"] = cover_year

        # If we have series IDs, search issues for each series
        if series_ids:
            all_records = []
            last_error = None
            for series_id in series_ids[:3]:  # Limit to first 3 series to avoid too many requests
                filters["series"] = series_id
                result = await self.fetch_page(page=page, endpoint="issue", **filters)
                if result.success and result.records:
                    all_records.extend(result.records)
                elif not result.success and result.errors:
                    # Track the error for propagation
                    last_error = result.errors

            # If we got results, return success
            if all_records:
                return FetchResult(
                    success=True,
                    records=all_records[:20],
                    total_count=len(all_records),
                    has_more=len(all_records) > 20
                )
            # If no results AND we had errors, propagate the error
            elif last_error:
                return FetchResult(
                    success=False,
                    records=[],
                    errors=last_error
                )
            # No results, no errors - genuine empty result
            return FetchResult(
                success=True,
                records=[],
                total_count=0
            )
        else:
            # No series filter - need at least one filter to search
            if not filters:
                return FetchResult(success=True, records=[], total_count=0)
            return await self.fetch_page(page=page, endpoint="issue", **filters)
