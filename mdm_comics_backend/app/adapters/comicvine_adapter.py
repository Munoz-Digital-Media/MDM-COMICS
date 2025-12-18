"""
Comic Vine API Adapter v1.10.0

Integration with Comic Vine API for comic metadata enrichment.

API Docs: https://comicvine.gamespot.com/api/documentation
Rate Limit: 200 requests/hour (non-commercial use only)

Per constitution_data_hygiene.json: No PII storage.
Per constitution_logging.json: Structured logging with correlation IDs.
"""
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from app.core.adapter_registry import (
    DataSourceAdapter, AdapterConfig, FetchResult, DataSourceType
)
from app.core.http_client import ResilientHTTPClient, RateLimitConfig, RetryConfig

logger = logging.getLogger(__name__)

# Comic Vine API configuration
COMICVINE_API_BASE = "https://comicvine.gamespot.com/api"
COMICVINE_API_KEY_ENV = "COMIC_VINE_API_KEY"


class ComicVineAdapter(DataSourceAdapter):
    """
    Adapter for Comic Vine API.

    Provides:
    - Issue search and details
    - Series/volume information
    - Creator credits
    - Character appearances
    - Cover images

    Usage:
        config = COMICVINE_CONFIG
        async with get_comicvine_client() as client:
            adapter = ComicVineAdapter(config, client)
            result = await adapter.search_issues("Amazing Spider-Man 300")
    """

    def __init__(
        self,
        config: AdapterConfig,
        client: ResilientHTTPClient,
        api_key: Optional[str] = None,
    ):
        super().__init__(config, client)
        self.api_key = api_key or os.getenv(COMICVINE_API_KEY_ENV, "")
        self.base_url = COMICVINE_API_BASE

        if not self.api_key:
            logger.warning(
                f"[COMICVINE] No API key set. Set {COMICVINE_API_KEY_ENV} env var."
            )

    def _build_url(self, endpoint: str, params: Optional[Dict] = None) -> str:
        """Build API URL with required parameters."""
        params = params or {}
        params["api_key"] = self.api_key
        params["format"] = "json"

        query = urlencode(params)
        return f"{self.base_url}/{endpoint}?{query}"

    def _parse_response(self, response) -> Dict[str, Any]:
        """
        Safely parse JSON response with HTTP status code checking.

        Returns:
            Parsed JSON dict, or error dict with status_code=-1
        """
        # Check HTTP status first
        if response.status_code == 401:
            logger.error("[COMICVINE] 401 Unauthorized - invalid API key")
            return {"status_code": -1, "error": "Invalid API key (401)"}
        elif response.status_code == 403:
            logger.error("[COMICVINE] 403 Forbidden - check API key permissions")
            return {"status_code": -1, "error": "API key forbidden (403)"}
        elif response.status_code == 420:
            logger.warning("[COMICVINE] 420 Rate limited - slow down requests")
            return {"status_code": -1, "error": "Rate limited (420)"}
        elif response.status_code == 429:
            logger.warning("[COMICVINE] 429 Too Many Requests - rate limited")
            return {"status_code": -1, "error": "Rate limited (429)"}
        elif response.status_code >= 500:
            logger.error(f"[COMICVINE] Server error ({response.status_code})")
            return {"status_code": -1, "error": f"Server error ({response.status_code})"}
        elif response.status_code != 200:
            logger.warning(f"[COMICVINE] Unexpected status: {response.status_code}")

        # Try to parse JSON
        try:
            text = response.text
            if not text or not text.strip():
                logger.error("[COMICVINE] Empty response body")
                return {"status_code": -1, "error": "Empty response body"}
            return response.json()
        except Exception as e:
            logger.error(f"[COMICVINE] JSON parse error: {e}, body: {response.text[:200] if response.text else 'empty'}")
            return {"status_code": -1, "error": f"JSON parse error: {e}"}

    async def health_check(self) -> bool:
        """Check if Comic Vine API is reachable."""
        if not self.api_key:
            return False

        try:
            url = self._build_url("types", {"limit": 1})
            response = await self.client.get(url)
            data = self._parse_response(response)
            return data.get("status_code") == 1
        except Exception as e:
            logger.error(f"[COMICVINE] Health check failed: {e}")
            return False

    async def fetch_page(
        self,
        page: int = 1,
        page_size: int = 100,
        cursor: Optional[str] = None,
        **filters
    ) -> FetchResult:
        """
        Fetch a page of issues.

        Args:
            page: Page number (1-indexed)
            page_size: Results per page (max 100)
            cursor: Not used (Comic Vine uses offset pagination)
            **filters: Additional filters (volume_id, name, etc.)
        """
        if not self.api_key:
            return FetchResult(
                success=False,
                errors=[{"message": "No API key configured"}],
            )

        try:
            offset = (page - 1) * page_size
            params = {
                "limit": min(page_size, 100),
                "offset": offset,
            }

            # Add filters
            filter_parts = []
            if "volume_id" in filters:
                filter_parts.append(f"volume:{filters['volume_id']}")
            if "name" in filters:
                filter_parts.append(f"name:{filters['name']}")

            if filter_parts:
                params["filter"] = ",".join(filter_parts)

            url = self._build_url("issues", params)
            response = await self.client.get(url)
            data = self._parse_response(response)

            if data.get("status_code") != 1:
                return FetchResult(
                    success=False,
                    errors=[{"message": data.get("error", "Unknown error")}],
                )

            results = data.get("results", [])
            total = data.get("number_of_total_results", 0)

            return FetchResult(
                success=True,
                records=results,
                has_more=offset + len(results) < total,
                total_count=total,
            )

        except Exception as e:
            logger.error(f"[COMICVINE] Fetch page failed: {e}")
            return FetchResult(
                success=False,
                errors=[{"message": str(e)}],
            )

    async def fetch_by_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single issue by Comic Vine ID."""
        if not self.api_key:
            return None

        try:
            # Comic Vine IDs are in format "4000-XXXXX" for issues
            if not external_id.startswith("4000-"):
                external_id = f"4000-{external_id}"

            url = self._build_url(f"issue/{external_id}")
            response = await self.client.get(url)
            data = self._parse_response(response)

            if data.get("status_code") != 1:
                logger.warning(f"[COMICVINE] Issue {external_id} not found: {data.get('error', 'unknown')}")
                return None

            return data.get("results")

        except Exception as e:
            logger.error(f"[COMICVINE] Fetch by ID failed: {e}")
            return None

    async def search_issues(
        self,
        query: str,
        limit: int = 10,
    ) -> FetchResult:
        """
        Search for issues by query string.

        Args:
            query: Search query (e.g., "Amazing Spider-Man 300")
            limit: Maximum results

        Returns:
            FetchResult with matching issues
        """
        if not self.api_key:
            return FetchResult(
                success=False,
                errors=[{"message": "No API key configured"}],
            )

        try:
            params = {
                "query": query,
                "resources": "issue",
                "limit": min(limit, 100),
            }

            url = self._build_url("search", params)
            response = await self.client.get(url)
            data = self._parse_response(response)

            if data.get("status_code") != 1:
                return FetchResult(
                    success=False,
                    errors=[{"message": data.get("error", "Unknown error")}],
                )

            return FetchResult(
                success=True,
                records=data.get("results", []),
                total_count=data.get("number_of_total_results", 0),
            )

        except Exception as e:
            logger.error(f"[COMICVINE] Search issues failed: {e}")
            return FetchResult(
                success=False,
                errors=[{"message": str(e)}],
            )

    async def fetch_volume(self, volume_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a volume (series) by ID."""
        if not self.api_key:
            return None

        try:
            if not volume_id.startswith("4050-"):
                volume_id = f"4050-{volume_id}"

            url = self._build_url(f"volume/{volume_id}")
            response = await self.client.get(url)
            data = self._parse_response(response)

            if data.get("status_code") != 1:
                logger.warning(f"[COMICVINE] Volume {volume_id} not found: {data.get('error', 'unknown')}")
                return None

            return data.get("results")

        except Exception as e:
            logger.error(f"[COMICVINE] Fetch volume failed: {e}")
            return None

    async def search_volumes(
        self,
        query: str,
        limit: int = 10,
    ) -> FetchResult:
        """Search for volumes (series) by name."""
        if not self.api_key:
            return FetchResult(
                success=False,
                errors=[{"message": "No API key configured"}],
            )

        try:
            params = {
                "query": query,
                "resources": "volume",
                "limit": min(limit, 100),
            }

            url = self._build_url("search", params)
            response = await self.client.get(url)
            data = self._parse_response(response)

            if data.get("status_code") != 1:
                return FetchResult(
                    success=False,
                    errors=[{"message": data.get("error", "Unknown error")}],
                )

            return FetchResult(
                success=True,
                records=data.get("results", []),
                total_count=data.get("number_of_total_results", 0),
            )

        except Exception as e:
            logger.error(f"[COMICVINE] Search volumes failed: {e}")
            return FetchResult(
                success=False,
                errors=[{"message": str(e)}],
            )

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a Comic Vine record to our canonical schema.

        Maps Comic Vine fields to our ComicIssue model.
        """
        # Extract volume info
        volume = record.get("volume", {}) or {}

        # Extract cover image
        image = record.get("image", {}) or {}
        cover_url = (
            image.get("original_url") or
            image.get("medium_url") or
            image.get("small_url")
        )

        # Extract creators
        creators = []
        for person in record.get("person_credits", []) or []:
            creators.append({
                "name": person.get("name"),
                "role": person.get("role", "unknown"),
                "comicvine_id": person.get("id"),
            })

        # Extract characters
        characters = []
        for char in record.get("character_credits", []) or []:
            characters.append({
                "name": char.get("name"),
                "comicvine_id": char.get("id"),
            })

        # Parse cover date
        cover_date = record.get("cover_date")
        if cover_date:
            try:
                cover_date = datetime.strptime(cover_date, "%Y-%m-%d").date()
            except ValueError:
                cover_date = None

        # Parse store date
        store_date = record.get("store_date")
        if store_date:
            try:
                store_date = datetime.strptime(store_date, "%Y-%m-%d").date()
            except ValueError:
                store_date = None

        return {
            # External IDs
            "comicvine_id": record.get("id"),
            "comicvine_api_url": record.get("api_detail_url"),

            # Core fields
            "issue_name": record.get("name"),
            "issue_number": record.get("issue_number"),
            "description": self._clean_html(record.get("description") or ""),

            # Series/Volume
            "series_name": volume.get("name"),
            "series_comicvine_id": volume.get("id"),

            # Dates
            "cover_date": cover_date,
            "store_date": store_date,

            # Images
            "image": cover_url,
            "cover_url": cover_url,

            # Related data
            "creators": creators,
            "characters": characters,

            # Aliases and extras
            "aliases": record.get("aliases"),
            "site_detail_url": record.get("site_detail_url"),

            # Source tracking
            "_source": "comicvine",
            "_raw": record,
        }

    def _clean_html(self, html: str) -> str:
        """Remove HTML tags from description."""
        if not html:
            return ""

        import re
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', html)
        # Decode common entities
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&quot;', '"')
        text = text.replace('&#39;', "'")
        text = text.replace('&nbsp;', ' ')
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text


# Adapter configuration
COMICVINE_CONFIG = AdapterConfig(
    name="comicvine",
    source_type=DataSourceType.API,
    enabled=True,
    priority=20,
    requests_per_second=0.055,  # ~200/hour
    burst_limit=3,
    auth_type="api_key",
    api_key_env_var=COMICVINE_API_KEY_ENV,
    license_type="proprietary",
    requires_attribution=False,
    images_allowed=True,
    extra={
        "api_base": COMICVINE_API_BASE,
        "rate_limit_per_hour": 200,
    },
)


def get_comicvine_client() -> ResilientHTTPClient:
    """Get HTTP client configured for Comic Vine API."""
    return ResilientHTTPClient(
        rate_limit_config=RateLimitConfig(
            requests_per_second=0.055,  # 200/hour = 0.055/sec
            burst_limit=3,
            min_request_interval=18.0,  # 1 request per 18 seconds
        ),
        retry_config=RetryConfig(
            max_retries=3,
            base_delay=2.0,
            max_delay=60.0,
        ),
        timeout=30.0,
        default_headers={
            "User-Agent": "MDMComicsBot/1.0 (+https://mdmcomics.com/bot)",
            "Accept": "application/json",
        },
    )


async def create_comicvine_adapter() -> Optional[ComicVineAdapter]:
    """Create and return a Comic Vine adapter instance.
    
    Note: v1.12.2 - Client is now auto-initialized via __aenter__.
    """
    api_key = os.getenv(COMICVINE_API_KEY_ENV)
    if not api_key:
        logger.warning(f"[COMICVINE] {COMICVINE_API_KEY_ENV} not set")
        return None

    client = get_comicvine_client()
    # v1.12.2: Initialize the httpx client
    await client.__aenter__()
    
    adapter = ComicVineAdapter(COMICVINE_CONFIG, client, api_key)

    # Register with global registry
    from app.core.adapter_registry import adapter_registry
    adapter_registry._adapters["comicvine"] = adapter
    adapter_registry._configs["comicvine"] = COMICVINE_CONFIG

    return adapter
