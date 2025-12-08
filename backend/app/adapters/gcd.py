"""
Grand Comics Database (GCD) Adapter v1.0.0

Adapter for GCD - structured bibliographic metadata.
https://comics.org/

Per pipeline spec:
- Type: Database Dump + Web Scraping (NO API)
- License: CC BY-SA 4.0 - fully usable for data
- DO NOT USE IMAGES - publisher copyright
- Priority: P2 for bibliographic enrichment

Data includes:
- Bibliographic metadata (series, issues, volumes, print runs)
- Creator credits (writers, pencillers, inkers, colorists, letterers, editors)
- Story details and synopses
- Reprint/variant tracking
- Publisher data
- Indicia information
- ISBN/barcode data
"""
import logging
import re
from typing import Any, Dict, Optional, List
from bs4 import BeautifulSoup

from app.core.adapter_registry import (
    DataSourceAdapter,
    AdapterConfig,
    FetchResult,
    GCD_CONFIG,
)
from app.core.http_client import ResilientHTTPClient

logger = logging.getLogger(__name__)


class GCDAdapter(DataSourceAdapter):
    """
    Adapter for Grand Comics Database.

    GCD does not have a public API. This adapter supports:
    1. Web scraping for specific lookups (use sparingly)
    2. Processing database dumps (primary method)

    Per licensing: DATA ONLY - no images.
    """

    BASE_URL = "https://www.comics.org"

    def __init__(
        self,
        config: AdapterConfig = GCD_CONFIG,
        client: Optional[ResilientHTTPClient] = None
    ):
        if client is None:
            from app.core.http_client import get_gcd_client
            client = get_gcd_client()

        super().__init__(config, client)

    async def health_check(self) -> bool:
        """Check if GCD website is reachable."""
        try:
            response = await self.client.get(f"{self.BASE_URL}/")
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
        Fetch is not supported for GCD - use database dumps.

        This method is implemented for interface compliance but
        returns an empty result. Use import_from_dump() instead.
        """
        logger.warning(f"[{self.name}] fetch_page not supported - use database dumps")
        return FetchResult(
            success=True,
            records=[],
            has_more=False,
            errors=[{"warning": "GCD does not support API fetching. Use database dumps."}]
        )

    async def fetch_by_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single issue by GCD ID via web scraping.

        Use sparingly - prefer database dumps for bulk data.
        """
        try:
            url = f"{self.BASE_URL}/issue/{external_id}/"
            response = await self.client.get(url)

            if response.status_code != 200:
                return None

            return self._parse_issue_page(response.text, external_id)

        except Exception as e:
            logger.error(f"[{self.name}] Fetch by ID {external_id} failed: {e}")
            return None

    async def search_series(
        self,
        name: str,
        publisher: Optional[str] = None,
        year: Optional[int] = None,
    ) -> FetchResult:
        """
        Search for series by name via web scraping.

        Use sparingly - prefer database dumps for bulk data.
        """
        try:
            params = {
                "q": name,
                "search_object": "series",
                "order1": "date",
            }
            if publisher:
                params["pub_name"] = publisher
            if year:
                params["start_year"] = year

            response = await self.client.get(
                f"{self.BASE_URL}/searchNew/",
                params=params
            )

            if response.status_code != 200:
                return FetchResult(
                    success=False,
                    errors=[{"error": f"Search returned {response.status_code}"}]
                )

            results = self._parse_search_results(response.text)

            return FetchResult(
                success=True,
                records=results,
                has_more=False,  # Pagination not implemented for scraping
            )

        except Exception as e:
            logger.error(f"[{self.name}] Search failed: {e}")
            return FetchResult(
                success=False,
                errors=[{"error": str(e)}]
            )

    def _parse_issue_page(self, html: str, gcd_id: str) -> Dict[str, Any]:
        """Parse GCD issue page HTML."""
        soup = BeautifulSoup(html, "html.parser")

        # Extract issue data from page
        # Note: GCD page structure may change - this is best-effort
        data = {
            "gcd_id": gcd_id,
            "raw_html_length": len(html),
        }

        # Try to get title
        title_elem = soup.select_one("h1.item_name")
        if title_elem:
            data["title"] = title_elem.get_text(strip=True)

        # Try to get series info
        series_elem = soup.select_one("span.series_name a")
        if series_elem:
            data["series_name"] = series_elem.get_text(strip=True)
            series_href = series_elem.get("href", "")
            match = re.search(r"/series/(\d+)/", series_href)
            if match:
                data["series_gcd_id"] = match.group(1)

        # Try to get publisher
        publisher_elem = soup.select_one("span.publisher a")
        if publisher_elem:
            data["publisher_name"] = publisher_elem.get_text(strip=True)

        # Try to get cover date
        date_elem = soup.select_one("dt:contains('Cover Date') + dd")
        if date_elem:
            data["cover_date"] = date_elem.get_text(strip=True)

        # Try to get indicia
        indicia_elem = soup.select_one("dt:contains('Indicia') + dd")
        if indicia_elem:
            data["indicia"] = indicia_elem.get_text(strip=True)

        # Try to get barcode
        barcode_elem = soup.select_one("dt:contains('Barcode') + dd")
        if barcode_elem:
            data["barcode"] = barcode_elem.get_text(strip=True)

        # Try to get ISBN
        isbn_elem = soup.select_one("dt:contains('ISBN') + dd")
        if isbn_elem:
            data["isbn"] = isbn_elem.get_text(strip=True)

        # Creator credits - parse from credits section
        credits = []
        credits_section = soup.select_one(".credits")
        if credits_section:
            for credit in credits_section.select(".credit"):
                role = credit.select_one(".role")
                names = credit.select(".name")
                if role and names:
                    credits.append({
                        "role": role.get_text(strip=True),
                        "names": [n.get_text(strip=True) for n in names]
                    })
        data["credits"] = credits

        return data

    def _parse_search_results(self, html: str) -> List[Dict[str, Any]]:
        """Parse GCD search results page."""
        soup = BeautifulSoup(html, "html.parser")
        results = []

        for row in soup.select(".results_table tbody tr"):
            cols = row.select("td")
            if len(cols) >= 3:
                link = cols[0].select_one("a")
                if link:
                    href = link.get("href", "")
                    match = re.search(r"/(series|issue)/(\d+)/", href)

                    result = {
                        "name": link.get_text(strip=True),
                        "url": href,
                    }

                    if match:
                        result["type"] = match.group(1)
                        result["gcd_id"] = match.group(2)

                    # Get other columns if present
                    if len(cols) > 1:
                        result["publisher"] = cols[1].get_text(strip=True)
                    if len(cols) > 2:
                        result["year"] = cols[2].get_text(strip=True)

                    results.append(result)

        return results

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize GCD record to canonical schema.
        """
        return {
            # Identifiers
            "gcd_id": record.get("gcd_id"),
            "series_gcd_id": record.get("series_gcd_id"),
            "isbn": record.get("isbn"),
            "barcode": record.get("barcode"),

            # Names
            "title": record.get("title"),
            "series_name": record.get("series_name"),
            "publisher_name": record.get("publisher_name"),

            # Dates
            "cover_date": record.get("cover_date"),

            # Metadata
            "indicia": record.get("indicia"),
            "credits": record.get("credits", []),

            # Source tracking
            "_source": self.name,
            "_source_id": str(record.get("gcd_id")),
            "_license": "CC-BY-SA-4.0",
            "_requires_attribution": True,
            "_attribution": self.config.attribution_text,
        }

    def import_from_dump(self, dump_path: str):
        """
        Import data from GCD MySQL database dump.

        GCD offers MySQL dumps at comics.org/download/ (requires free account).
        This is the preferred method for bulk data ingestion.

        Args:
            dump_path: Path to the extracted dump files

        TODO: Implement MySQL dump parsing and import
        """
        raise NotImplementedError(
            "Database dump import not yet implemented. "
            "Download dumps from https://comics.org/download/"
        )
