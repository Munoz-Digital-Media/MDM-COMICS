"""
Metron Adapter v1.0.0

Adapter for Metron API - rich comic metadata.
https://metron.cloud/

Per pipeline spec:
- Rich comic metadata (issues, variants, creators, publisher, print runs, story arcs)
- Uses existing MetronService as base
"""
import os
import base64
import logging
from typing import Any, Dict, Optional

from app.core.adapter_registry import (
    DataSourceAdapter,
    AdapterConfig,
    FetchResult,
    METRON_CONFIG,
)
from app.core.http_client import ResilientHTTPClient

logger = logging.getLogger(__name__)


class MetronAdapter(DataSourceAdapter):
    """
    Adapter for Metron comic database API.

    Provides rich metadata for comic issues, series, characters, and creators.
    """

    BASE_URL = "https://metron.cloud/api"

    def __init__(
        self,
        config: AdapterConfig = METRON_CONFIG,
        client: Optional[ResilientHTTPClient] = None
    ):
        if client is None:
            from app.core.http_client import get_metron_client
            client = get_metron_client()

        super().__init__(config, client)

        self._username = os.getenv(config.username_env_var or "METRON_USERNAME", "")
        self._password = os.getenv(config.password_env_var or "METRON_PASSWORD", "")

        if not self._username or not self._password:
            logger.warning(f"[{self.name}] Credentials not configured - adapter will not function")

    def _build_auth_header(self) -> Dict[str, str]:
        """Build Basic Auth header."""
        credentials = f"{self._username}:{self._password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    async def health_check(self) -> bool:
        """Check if Metron API is reachable and credentials are valid."""
        if not self._username or not self._password:
            return False

        try:
            response = await self.client.get(
                f"{self.BASE_URL}/publisher/",
                headers=self._build_auth_header(),
                params={"page": 1}
            )
            return response.status_code == 200
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
        if not self._username or not self._password:
            return FetchResult(
                success=False,
                errors=[{"error": "Credentials not configured"}]
            )

        try:
            params = {"page": page}
            params.update(filters)

            response = await self.client.get(
                f"{self.BASE_URL}/{endpoint}/",
                headers=self._build_auth_header(),
                params=params
            )

            if response.status_code != 200:
                return FetchResult(
                    success=False,
                    errors=[{"error": f"API returned {response.status_code}"}]
                )

            data = response.json()
            results = data.get("results", [])
            next_url = data.get("next")

            return FetchResult(
                success=True,
                records=results,
                has_more=next_url is not None,
                total_count=data.get("count"),
            )

        except Exception as e:
            logger.error(f"[{self.name}] Fetch failed: {e}")
            return FetchResult(
                success=False,
                errors=[{"error": str(e)}]
            )

    async def fetch_by_id(self, external_id: str, endpoint: str = "issue") -> Optional[Dict[str, Any]]:
        """Fetch a single record by Metron ID."""
        if not self._username or not self._password:
            return None

        try:
            response = await self.client.get(
                f"{self.BASE_URL}/{endpoint}/{external_id}/",
                headers=self._build_auth_header()
            )

            if response.status_code != 200:
                return None

            return response.json()

        except Exception as e:
            logger.error(f"[{self.name}] Fetch by ID {external_id} failed: {e}")
            return None

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize Metron issue record to canonical schema.
        """
        # Handle series info
        series = record.get("series", {}) or {}
        publisher = record.get("publisher", {}) or series.get("publisher", {}) or {}

        # Parse cover date
        cover_date = record.get("cover_date")

        return {
            # Identifiers
            "metron_id": record.get("id"),
            "upc": record.get("upc"),
            "sku": record.get("sku"),
            "isbn": record.get("isbn"),

            # Series info
            "series_id": series.get("id"),
            "series_name": series.get("name"),
            "series_volume": series.get("volume"),

            # Publisher info
            "publisher_id": publisher.get("id"),
            "publisher_name": publisher.get("name"),

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
            "description": record.get("desc"),

            # Variant info
            "is_variant": record.get("is_variant", False),
            "variant_name": record.get("variant_name"),

            # Ratings
            "rating": record.get("rating", {}).get("name") if record.get("rating") else None,

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
