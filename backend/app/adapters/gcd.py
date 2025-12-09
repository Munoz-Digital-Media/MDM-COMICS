"""
Grand Comics Database (GCD) Adapter v1.7.0

Adapter for GCD - structured bibliographic metadata.
https://comics.org/

Per pipeline spec:
- Type: SQLite Database Dump (primary), Web Scraping (fallback)
- License: CC BY-SA 4.0 - fully usable for data
- DO NOT USE IMAGES - publisher copyright
- Priority: P1 for catalog data (GCD-Primary architecture)

Data includes:
- Bibliographic metadata (series, issues, volumes, print runs)
- Creator credits (writers, pencillers, inkers, colorists, letterers, editors)
- Story details and synopses
- Reprint/variant tracking
- Publisher data
- Indicia information
- ISBN/barcode data

v1.7.0: Added SQLite dump import (import_from_sqlite)
- Direct SQLite queries via sqlite3 module
- Streaming cursor with fetchmany for memory efficiency
- JOIN support for series/publisher denormalization
"""
import logging
import re
import sqlite3
from typing import Any, Dict, Optional, List, Iterator, Generator
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

    def import_from_sqlite(
        self,
        db_path: str,
        batch_size: int = 1000,
        offset: int = 0,
        limit: int = 0,
    ) -> Generator[List[Dict[str, Any]], None, int]:
        """
        Import data from GCD SQLite database dump.

        GCD offers SQLite dumps at comics.org/download/ (requires free account).
        This is the preferred method for bulk data ingestion.

        Args:
            db_path: Path to the SQLite database file
            batch_size: Number of records per batch (default 1000)
            offset: Starting record offset for resumable imports
            limit: Maximum records to import (0 = unlimited)

        Yields:
            Batches of normalized comic issue records

        Returns:
            Total count of records processed

        Example:
            adapter = GCDAdapter()
            for batch in adapter.import_from_sqlite('/data/gcd/2025-12-01.db'):
                for record in batch:
                    # Process record
                    pass
        """
        logger.info(f"[{self.name}] Opening SQLite database: {db_path}")

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        cursor = conn.cursor()

        # Get total count for progress reporting
        cursor.execute("SELECT COUNT(*) FROM gcd_issue WHERE deleted = 0")
        total_available = cursor.fetchone()[0]
        logger.info(f"[{self.name}] Total non-deleted issues in GCD: {total_available:,}")

        # Build query with JOINs for series and publisher data
        # Explicitly exclude image-related columns
        query = """
            SELECT
                i.id as gcd_id,
                i.number as issue_number,
                i.volume,
                i.title as story_title,
                i.isbn,
                i.valid_isbn,
                i.barcode,
                i.publication_date,
                i.key_date,
                i.on_sale_date,
                i.page_count,
                i.price as cover_price,
                i.variant_of_id,
                i.variant_name,
                i.series_id as gcd_series_id,
                s.name as series_name,
                s.sort_name as series_sort_name,
                s.year_began as series_year_began,
                s.year_ended as series_year_ended,
                s.publisher_id as gcd_publisher_id,
                p.name as publisher_name
            FROM gcd_issue i
            LEFT JOIN gcd_series s ON i.series_id = s.id
            LEFT JOIN gcd_publisher p ON s.publisher_id = p.id
            WHERE i.deleted = 0
            ORDER BY i.id
        """

        # Add LIMIT/OFFSET if specified
        # Note: SQLite requires LIMIT when using OFFSET
        if limit > 0:
            query += f" LIMIT {limit}"
        elif offset > 0:
            # No limit but has offset - use very large number to get all remaining
            query += f" LIMIT -1"  # SQLite: -1 means unlimited
        if offset > 0:
            query += f" OFFSET {offset}"

        logger.info(f"[{self.name}] Executing query with offset={offset}, limit={limit or 'unlimited'}")
        cursor.execute(query)

        records_processed = 0
        batch = []

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                record = self._normalize_sqlite_row(dict(row))
                batch.append(record)
                records_processed += 1

            if batch:
                logger.debug(f"[{self.name}] Yielding batch of {len(batch)} records (total: {records_processed:,})")
                yield batch
                batch = []

        # Yield any remaining records
        if batch:
            yield batch

        cursor.close()
        conn.close()

        logger.info(f"[{self.name}] Import complete. Processed {records_processed:,} records")
        return records_processed

    def _normalize_sqlite_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a SQLite row to canonical schema.

        Handles NULL values and type conversions.
        """
        # Parse cover price - GCD stores as text like "$2.99"
        cover_price = row.get("cover_price")
        if cover_price and isinstance(cover_price, str):
            # Extract numeric value from price string
            price_match = re.search(r'[\d.]+', cover_price)
            if price_match:
                try:
                    cover_price = float(price_match.group())
                except ValueError:
                    cover_price = None

        # Parse key_date to release_date (YYYY-MM-DD format)
        release_date = row.get("key_date")
        if release_date and len(release_date) < 4:
            release_date = None  # Invalid date

        return {
            # GCD identifiers
            "gcd_id": row.get("gcd_id"),
            "gcd_series_id": row.get("gcd_series_id"),
            "gcd_publisher_id": row.get("gcd_publisher_id"),

            # Cross-reference fields for matching
            "isbn": row.get("isbn") or row.get("valid_isbn"),
            "upc": row.get("barcode"),  # Map barcode to upc

            # Issue metadata
            "issue_number": row.get("issue_number"),
            "volume": row.get("volume"),
            "story_title": row.get("story_title"),
            "page_count": row.get("page_count"),
            "cover_price": cover_price,
            "release_date": release_date,

            # Variant tracking
            "variant_of_gcd_id": row.get("variant_of_id"),
            "variant_name": row.get("variant_name"),

            # Series info (denormalized for convenience)
            "series_name": row.get("series_name"),
            "series_sort_name": row.get("series_sort_name"),
            "series_year_began": row.get("series_year_began"),
            "series_year_ended": row.get("series_year_ended"),

            # Publisher info (denormalized)
            "publisher_name": row.get("publisher_name"),

            # Source tracking for FieldProvenance
            "_source": self.name,
            "_source_id": str(row.get("gcd_id")),
            "_license": "CC-BY-SA-4.0",
            "_requires_attribution": True,
            "_attribution": self.config.attribution_text,
        }

    def get_total_count(self, db_path: str) -> int:
        """Get total number of non-deleted issues in the GCD dump."""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM gcd_issue WHERE deleted = 0")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count

    def validate_schema(self, db_path: str) -> Dict[str, Any]:
        """
        Validate that the SQLite dump has expected schema.

        Returns validation results including:
        - Whether required tables exist
        - Required columns present
        - Sample data for verification
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        results = {
            "valid": True,
            "tables": {},
            "errors": [],
        }

        required_tables = ["gcd_issue", "gcd_series", "gcd_publisher"]

        for table in required_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]

                cursor.execute(f"PRAGMA table_info({table})")
                columns = [col[1] for col in cursor.fetchall()]

                results["tables"][table] = {
                    "exists": True,
                    "row_count": count,
                    "columns": columns,
                }
            except sqlite3.OperationalError as e:
                results["valid"] = False
                results["errors"].append(f"Table {table} error: {e}")
                results["tables"][table] = {"exists": False}

        # Verify critical columns in gcd_issue
        if results["tables"].get("gcd_issue", {}).get("exists"):
            required_cols = ["id", "series_id", "number", "deleted"]
            missing = [c for c in required_cols if c not in results["tables"]["gcd_issue"]["columns"]]
            if missing:
                results["valid"] = False
                results["errors"].append(f"Missing columns in gcd_issue: {missing}")

        cursor.close()
        conn.close()

        return results

    def import_from_dump(self, dump_path: str):
        """
        DEPRECATED: Use import_from_sqlite() instead.

        This method exists for backward compatibility.
        """
        logger.warning(f"[{self.name}] import_from_dump is deprecated - use import_from_sqlite")
        # Attempt to use as SQLite path
        return self.import_from_sqlite(dump_path)
