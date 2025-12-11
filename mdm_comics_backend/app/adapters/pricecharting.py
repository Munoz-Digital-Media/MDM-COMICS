"""
PriceCharting Adapter v1.0.0

Adapter for PriceCharting API - real-time pricing data.
https://www.pricecharting.com/api

Per pipeline spec:
- Rate-sensitive API - use conservative limits
- Provides pricing, sales velocity, trend analytics
"""
import os
import logging
from typing import Any, Dict, Optional

from app.core.adapter_registry import (
    DataSourceAdapter,
    AdapterConfig,
    FetchResult,
    PRICECHARTING_CONFIG,
)
from app.core.http_client import ResilientHTTPClient

logger = logging.getLogger(__name__)


class PriceChartingAdapter(DataSourceAdapter):
    """
    Adapter for PriceCharting API.

    Provides real-time pricing data for comics and collectibles.
    """

    BASE_URL = "https://www.pricecharting.com/api"

    def __init__(
        self,
        config: AdapterConfig = PRICECHARTING_CONFIG,
        client: Optional[ResilientHTTPClient] = None
    ):
        if client is None:
            from app.core.http_client import get_pricecharting_client
            client = get_pricecharting_client()

        super().__init__(config, client)

        self._api_token = os.getenv(config.api_key_env_var or "PRICECHARTING_API_TOKEN", "")
        if not self._api_token:
            logger.warning(f"[{self.name}] API token not configured - adapter will not function")

    async def health_check(self) -> bool:
        """Check if PriceCharting API is reachable."""
        if not self._api_token:
            return False

        try:
            # Try fetching a known product
            response = await self.client.get(
                f"{self.BASE_URL}/product",
                params={"t": self._api_token, "id": "1"}  # ID 1 should exist
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
        **filters
    ) -> FetchResult:
        """
        Fetch products from PriceCharting.

        Note: PriceCharting API doesn't support traditional pagination.
        Use search/filter parameters instead.
        """
        if not self._api_token:
            return FetchResult(
                success=False,
                errors=[{"error": "API token not configured"}]
            )

        try:
            params = {"t": self._api_token}

            # Add search query if provided
            if "q" in filters:
                params["q"] = filters["q"]

            # Add console/category filter
            if "console" in filters:
                params["console-name"] = filters["console"]

            response = await self.client.get(
                f"{self.BASE_URL}/products",
                params=params
            )

            if response.status_code != 200:
                return FetchResult(
                    success=False,
                    errors=[{"error": f"API returned {response.status_code}"}]
                )

            data = response.json()
            products = data.get("products", [])

            return FetchResult(
                success=True,
                records=products,
                has_more=False,  # PriceCharting doesn't paginate search results
            )

        except Exception as e:
            logger.error(f"[{self.name}] Fetch failed: {e}")
            return FetchResult(
                success=False,
                errors=[{"error": str(e)}]
            )

    async def fetch_by_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single product by PriceCharting ID."""
        if not self._api_token:
            return None

        try:
            response = await self.client.get(
                f"{self.BASE_URL}/product",
                params={"t": self._api_token, "id": external_id}
            )

            if response.status_code != 200:
                return None

            return response.json()

        except Exception as e:
            logger.error(f"[{self.name}] Fetch by ID {external_id} failed: {e}")
            return None

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize PriceCharting record to canonical schema.

        Maps PriceCharting fields to our internal structure.
        """

        def parse_cents(value) -> Optional[float]:
            """Convert cents to dollars."""
            if value is None:
                return None
            try:
                return round(int(value) / 100, 2)
            except (ValueError, TypeError):
                return None

        return {
            # Identifiers
            "pricecharting_id": record.get("id"),
            "handle": record.get("product-name"),
            "asin": record.get("asin"),
            "upc": record.get("upc"),

            # Names
            "title": record.get("product-name"),
            "console_name": record.get("console-name"),

            # Prices (convert cents to dollars)
            "price_loose": parse_cents(record.get("loose-price")),
            "price_cib": parse_cents(record.get("cib-price")),
            "price_new": parse_cents(record.get("new-price")),
            "price_graded": parse_cents(record.get("graded-price")),
            "price_bgs_10": parse_cents(record.get("bgs-10-price")),
            "price_cgc_98": parse_cents(record.get("cgc-9.8-price")),
            "price_cgc_96": parse_cents(record.get("cgc-9.6-price")),

            # Volume/sales data
            "sales_volume": record.get("sales-volume"),

            # Source tracking
            "_source": self.name,
            "_source_id": str(record.get("id")),
        }
