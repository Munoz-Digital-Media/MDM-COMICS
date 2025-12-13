"""
ComicBookRealm Scraper Adapter v1.10.0

Web scraper for ComicBookRealm.com - comic database with pricing and grading data.

Per constitution_cyberSec.json: Mandatory robots.txt compliance.
Per constitution_data_hygiene.json: No PII storage.

Features:
- Cover image URLs (96% coverage)
- CGC grading examples for AI training
- Price data and market values
- Series and issue metadata

Usage:
    adapter = ComicBookRealmAdapter(config, client)
    result = await adapter.search_issues("Amazing Spider-Man", "300")
"""
import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urljoin, urlparse

from bs4 import BeautifulSoup

from app.core.adapter_registry import (
    DataSourceAdapter, AdapterConfig, FetchResult, DataSourceType
)
from app.core.http_client import ResilientHTTPClient, RateLimitConfig, RetryConfig
from app.adapters.robots_checker import robots_checker, USER_AGENT

logger = logging.getLogger(__name__)

# ComicBookRealm URLs
CBR_BASE_URL = "https://comicbookrealm.com"
CBR_SEARCH_URL = f"{CBR_BASE_URL}/search"


class ComicBookRealmAdapter(DataSourceAdapter):
    """
    Scraper adapter for ComicBookRealm.com.

    Provides:
    - Issue search and details
    - Cover images
    - CGC census data
    - Price guide values
    - Grading examples for AI training

    Note: This is a scraper and must respect robots.txt and rate limits.
    """

    def __init__(
        self,
        config: AdapterConfig,
        client: ResilientHTTPClient,
    ):
        super().__init__(config, client)
        self.base_url = CBR_BASE_URL
        self._last_request_time = 0.0
        self._min_delay = 2.0  # Minimum 2 seconds between requests

    async def _enforce_delay(self) -> None:
        """Enforce minimum delay between requests."""
        import time
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_delay:
            await asyncio.sleep(self._min_delay - elapsed)
        self._last_request_time = time.monotonic()

    async def _check_robots(self, url: str) -> bool:
        """Check robots.txt before scraping."""
        try:
            can_fetch = await robots_checker.can_fetch(url)
            if not can_fetch:
                logger.warning(f"[CBR] URL disallowed by robots.txt: {url}")
            return can_fetch
        except Exception as e:
            logger.error(f"[CBR] Robots check failed: {e}")
            return False  # Conservative: deny on error

    async def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page with robots.txt compliance."""
        if not await self._check_robots(url):
            return None

        await self._enforce_delay()

        try:
            response = await self.client.get(url)
            if response.status_code == 200:
                return response.text
            else:
                logger.warning(f"[CBR] HTTP {response.status_code} for {url}")
                return None
        except Exception as e:
            logger.error(f"[CBR] Fetch failed for {url}: {e}")
            return None

    async def health_check(self) -> bool:
        """Check if ComicBookRealm is reachable."""
        try:
            can_fetch = await robots_checker.can_fetch(self.base_url)
            if not can_fetch:
                return False

            response = await self.client.get(self.base_url)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"[CBR] Health check failed: {e}")
            return False

    async def fetch_page(
        self,
        page: int = 1,
        page_size: int = 100,
        cursor: Optional[str] = None,
        **filters
    ) -> FetchResult:
        """
        Fetch a page of search results.

        Note: ComicBookRealm doesn't support bulk listing.
        Use search_issues() for targeted searches.
        """
        return FetchResult(
            success=False,
            errors=[{"message": "ComicBookRealm does not support bulk listing"}],
        )

    async def fetch_by_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single issue by ComicBookRealm ID/URL.

        Args:
            external_id: The CBR issue ID or full URL
        """
        # Handle both ID and URL
        if external_id.startswith("http"):
            url = external_id
        else:
            # Construct URL from ID (format: /series/publisher/title/issue)
            url = f"{self.base_url}/{external_id}"

        html = await self._fetch_page(url)
        if not html:
            return None

        return self._parse_issue_page(html, url)

    async def search_issues(
        self,
        series_name: str,
        issue_number: Optional[str] = None,
        limit: int = 10,
    ) -> FetchResult:
        """
        Search for issues by series name and optionally issue number.

        Args:
            series_name: Series/title name
            issue_number: Optional issue number
            limit: Maximum results

        Returns:
            FetchResult with matching issues
        """
        # Build search query
        query = series_name
        if issue_number:
            query = f"{series_name} {issue_number}"

        # URL encode the query
        search_url = f"{CBR_SEARCH_URL}?q={quote(query)}"

        html = await self._fetch_page(search_url)
        if not html:
            return FetchResult(
                success=False,
                errors=[{"message": "Search failed"}],
            )

        results = self._parse_search_results(html, limit)

        return FetchResult(
            success=True,
            records=results,
            total_count=len(results),
        )

    def _parse_search_results(self, html: str, limit: int) -> List[Dict[str, Any]]:
        """Parse search results page."""
        soup = BeautifulSoup(html, 'html.parser')
        results = []

        # Find search result items
        # Note: Actual selectors depend on site structure
        result_items = soup.select('.search-result, .comic-item, .issue-row, tr.comic')[:limit]

        for item in result_items:
            try:
                result = self._extract_search_result(item)
                if result:
                    results.append(result)
            except Exception as e:
                logger.warning(f"[CBR] Failed to parse search result: {e}")

        # Try alternate structure if no results
        if not results:
            # Look for table rows with comic data
            rows = soup.select('table tr')
            for row in rows[:limit]:
                if row.find('a'):
                    result = self._extract_table_row(row)
                    if result:
                        results.append(result)

        return results

    def _extract_search_result(self, item) -> Optional[Dict[str, Any]]:
        """Extract data from a search result item."""
        # Find the link to the issue page
        link = item.find('a')
        if not link:
            return None

        href = link.get('href', '')
        if href and not href.startswith('http'):
            href = urljoin(self.base_url, href)

        # Extract text content
        title_text = link.get_text(strip=True)

        # Find cover image
        img = item.find('img')
        cover_url = None
        if img:
            cover_url = img.get('src') or img.get('data-src')
            if cover_url and not cover_url.startswith('http'):
                cover_url = urljoin(self.base_url, cover_url)

        # Find price if available
        price_elem = item.select_one('.price, .value, [class*="price"]')
        price = None
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price = self._parse_price(price_text)

        return {
            "title": title_text,
            "url": href,
            "cover_url": cover_url,
            "price": price,
            "_source": "comicbookrealm",
        }

    def _extract_table_row(self, row) -> Optional[Dict[str, Any]]:
        """Extract data from a table row."""
        cells = row.find_all('td')
        if len(cells) < 2:
            return None

        link = row.find('a')
        if not link:
            return None

        href = link.get('href', '')
        if href and not href.startswith('http'):
            href = urljoin(self.base_url, href)

        title = link.get_text(strip=True)

        # Look for image
        img = row.find('img')
        cover_url = None
        if img:
            cover_url = img.get('src') or img.get('data-src')
            if cover_url and not cover_url.startswith('http'):
                cover_url = urljoin(self.base_url, cover_url)

        return {
            "title": title,
            "url": href,
            "cover_url": cover_url,
            "_source": "comicbookrealm",
        }

    def _parse_issue_page(self, html: str, url: str) -> Dict[str, Any]:
        """Parse a single issue detail page."""
        soup = BeautifulSoup(html, 'html.parser')

        data = {
            "url": url,
            "_source": "comicbookrealm",
        }

        # Extract title/series
        title_elem = soup.select_one('h1, .title, .comic-title')
        if title_elem:
            data["title"] = title_elem.get_text(strip=True)

        # Extract issue number
        issue_match = re.search(r'#(\d+[A-Za-z]?)', data.get("title", ""))
        if issue_match:
            data["issue_number"] = issue_match.group(1)

        # Extract cover image
        cover_img = soup.select_one('.cover img, .comic-cover img, img.cover')
        if cover_img:
            cover_url = cover_img.get('src') or cover_img.get('data-src')
            if cover_url:
                if not cover_url.startswith('http'):
                    cover_url = urljoin(self.base_url, cover_url)
                data["cover_url"] = cover_url

        # Extract publisher
        publisher_elem = soup.select_one('.publisher, [class*="publisher"]')
        if publisher_elem:
            data["publisher"] = publisher_elem.get_text(strip=True)

        # Extract price data
        price_data = self._extract_price_data(soup)
        data.update(price_data)

        # Extract CGC census data
        grading_data = self._extract_grading_data(soup)
        data.update(grading_data)

        # Extract description
        desc_elem = soup.select_one('.description, .synopsis, .about')
        if desc_elem:
            data["description"] = desc_elem.get_text(strip=True)[:2000]

        # Extract cover date
        date_elem = soup.select_one('.cover-date, .date, [class*="date"]')
        if date_elem:
            date_text = date_elem.get_text(strip=True)
            data["cover_date_text"] = date_text
            parsed_date = self._parse_date(date_text)
            if parsed_date:
                data["cover_date"] = parsed_date

        return data

    def _extract_price_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract pricing information from page."""
        price_data = {}

        # Look for price guide section
        price_section = soup.select_one('.price-guide, .prices, [class*="price"]')
        if price_section:
            # Near Mint price
            nm_elem = price_section.select_one('[class*="nm"], [class*="near-mint"]')
            if nm_elem:
                price_data["price_nm"] = self._parse_price(nm_elem.get_text())

            # Very Fine price
            vf_elem = price_section.select_one('[class*="vf"], [class*="very-fine"]')
            if vf_elem:
                price_data["price_vf"] = self._parse_price(vf_elem.get_text())

            # Fine price
            f_elem = price_section.select_one('[class*="fine"]:not([class*="very"])')
            if f_elem:
                price_data["price_f"] = self._parse_price(f_elem.get_text())

            # Good price
            g_elem = price_section.select_one('[class*="good"]')
            if g_elem:
                price_data["price_g"] = self._parse_price(g_elem.get_text())

        # Look for generic price display
        price_elems = soup.select('[class*="price"]')
        for elem in price_elems:
            text = elem.get_text(strip=True).lower()
            price = self._parse_price(elem.get_text())
            if price:
                if 'near mint' in text or 'nm' in text.split():
                    price_data.setdefault("price_nm", price)
                elif 'very fine' in text or 'vf' in text.split():
                    price_data.setdefault("price_vf", price)
                elif 'fine' in text or 'fn' in text.split():
                    price_data.setdefault("price_f", price)
                elif 'good' in text or 'gd' in text.split():
                    price_data.setdefault("price_g", price)
                elif 'fair' in text or 'fr' in text.split():
                    price_data.setdefault("price_fr", price)

        return price_data

    def _extract_grading_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract CGC grading data for AI training.

        This data helps train the grading AI model.
        """
        grading_data = {}
        grading_examples = []

        # Look for CGC census section
        census_section = soup.select_one('.cgc-census, .grading, [class*="census"]')
        if census_section:
            # Extract CGC population data
            rows = census_section.select('tr, .grade-row')
            for row in rows:
                grade_data = self._parse_grade_row(row)
                if grade_data:
                    grading_examples.append(grade_data)

        # Look for graded sales data
        sales_section = soup.select_one('.sales, .sold, [class*="sale"]')
        if sales_section:
            sales = sales_section.select('tr, .sale-row')
            for sale in sales:
                sale_data = self._parse_sale_row(sale)
                if sale_data:
                    grading_examples.append(sale_data)

        # Look for condition notes
        condition_elem = soup.select_one('.condition, .defects, [class*="condition"]')
        if condition_elem:
            condition_text = condition_elem.get_text(strip=True)
            grading_data["condition_notes"] = condition_text[:1000]

            # Parse defects from text
            defects = self._extract_defects(condition_text)
            if defects:
                grading_data["defects"] = defects

        if grading_examples:
            grading_data["grading_examples"] = grading_examples

        return grading_data

    def _parse_grade_row(self, row) -> Optional[Dict[str, Any]]:
        """Parse a CGC grade row."""
        text = row.get_text(strip=True)
        if not text:
            return None

        # Look for grade number (e.g., 9.8, 9.6, etc.)
        grade_match = re.search(r'(\d+\.?\d*)\s*(NM|VF|FN|VG|GD|FR|PR)?', text, re.I)
        if not grade_match:
            return None

        grade_num = float(grade_match.group(1))
        grade_label = grade_match.group(2) or self._grade_to_label(grade_num)

        # Look for population count
        pop_match = re.search(r'(\d+)\s*(?:copies|census|pop)', text, re.I)
        population = int(pop_match.group(1)) if pop_match else None

        return {
            "grade_numeric": grade_num,
            "grade_label": grade_label.upper() if grade_label else "",
            "population": population,
            "source": "comicbookrealm",
        }

    def _parse_sale_row(self, row) -> Optional[Dict[str, Any]]:
        """Parse a graded sale row."""
        text = row.get_text(strip=True)
        if not text:
            return None

        # Look for grade
        grade_match = re.search(r'(\d+\.?\d*)', text)
        if not grade_match:
            return None

        grade_num = float(grade_match.group(1))

        # Look for sale price
        price_match = re.search(r'\$[\d,]+(?:\.\d{2})?', text)
        price = None
        if price_match:
            price = self._parse_price(price_match.group())

        return {
            "grade_numeric": grade_num,
            "grade_label": self._grade_to_label(grade_num),
            "sale_price": price,
            "source": "comicbookrealm",
        }

    def _extract_defects(self, text: str) -> List[str]:
        """Extract defect keywords from condition text."""
        defect_keywords = [
            "tear", "spine roll", "spine stress", "foxing", "water damage",
            "water stain", "moisture", "rusty staple", "missing staple",
            "loose staple", "detached cover", "loose cover", "crease",
            "fold", "corner chip", "edge chip", "color break", "fading",
            "sun shadow", "browning", "brittleness", "piece missing",
            "cut", "tape", "tape residue", "writing", "stamp", "label",
            "subscription crease", "bindery defect", "miswrap", "miscut",
            "manufacturing defect", "centerfold loose", "centerfold detached",
        ]

        text_lower = text.lower()
        found_defects = []

        for defect in defect_keywords:
            if defect in text_lower:
                found_defects.append(defect)

        return found_defects

    def _grade_to_label(self, grade: float) -> str:
        """Convert numeric grade to label."""
        if grade >= 9.8:
            return "NM/MT"
        elif grade >= 9.4:
            return "NM"
        elif grade >= 9.0:
            return "VF/NM"
        elif grade >= 8.0:
            return "VF"
        elif grade >= 7.0:
            return "FN/VF"
        elif grade >= 6.0:
            return "FN"
        elif grade >= 5.0:
            return "VG/FN"
        elif grade >= 4.0:
            return "VG"
        elif grade >= 3.0:
            return "GD/VG"
        elif grade >= 2.0:
            return "GD"
        elif grade >= 1.0:
            return "FR/GD"
        else:
            return "PR"

    def _parse_price(self, text: str) -> Optional[float]:
        """Parse price from text."""
        if not text:
            return None

        # Remove currency symbols and commas
        clean = re.sub(r'[^\d.]', '', text)
        try:
            return float(clean) if clean else None
        except ValueError:
            return None

    def _parse_date(self, text: str) -> Optional[str]:
        """Parse date from text to ISO format."""
        # Common date patterns
        patterns = [
            (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),
            (r'(\d{2})/(\d{2})/(\d{4})', '%m/%d/%Y'),
            (r'(\w+)\s+(\d{4})', '%B %Y'),
            (r'(\w{3})\s+(\d{4})', '%b %Y'),
        ]

        for pattern, date_format in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    date_str = match.group()
                    dt = datetime.strptime(date_str, date_format)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue

        return None

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a ComicBookRealm record to canonical schema.
        """
        # Parse series and issue from title
        title = record.get("title", "")
        series_name = title
        issue_number = record.get("issue_number")

        if not issue_number:
            # Try to extract from title
            match = re.search(r'#(\d+[A-Za-z]?)', title)
            if match:
                issue_number = match.group(1)
                series_name = title[:match.start()].strip()

        return {
            # Core fields
            "series_name": series_name,
            "issue_number": issue_number,
            "publisher": record.get("publisher"),

            # Images
            "cover_url": record.get("cover_url"),
            "image": record.get("cover_url"),

            # Pricing
            "price_nm": record.get("price_nm"),
            "price_vf": record.get("price_vf"),
            "price_f": record.get("price_f"),
            "price_g": record.get("price_g"),

            # Dates
            "cover_date": record.get("cover_date"),

            # Grading data
            "grading_examples": record.get("grading_examples", []),
            "condition_notes": record.get("condition_notes"),
            "defects": record.get("defects", []),

            # Description
            "description": record.get("description"),

            # Source tracking
            "cbr_url": record.get("url"),
            "_source": "comicbookrealm",
            "_raw": record,
        }

    async def fetch_grading_examples(
        self,
        series_name: str,
        issue_number: str,
    ) -> List[Dict[str, Any]]:
        """
        Fetch grading examples for AI training.

        Returns list of grading examples with:
        - grade_numeric: Numeric grade (0.5-10.0)
        - grade_label: Grade label (PR, GD, VG, FN, VF, NM, etc.)
        - defects: List of defects
        - source_url: URL of the source
        """
        # Search for the issue
        result = await self.search_issues(series_name, issue_number, limit=5)

        if not result.success or not result.records:
            return []

        examples = []

        # Fetch detail pages to get grading data
        for record in result.records[:3]:  # Limit to 3 pages
            url = record.get("url")
            if url:
                detail = await self.fetch_by_id(url)
                if detail and detail.get("grading_examples"):
                    for example in detail["grading_examples"]:
                        example["source_url"] = url
                        examples.append(example)

        return examples


# Adapter configuration
COMICBOOKREALM_CONFIG = AdapterConfig(
    name="comicbookrealm",
    source_type=DataSourceType.SCRAPER,
    enabled=True,
    priority=30,
    requests_per_second=0.5,  # Conservative: 1 req per 2 seconds
    burst_limit=2,
    auth_type=None,
    license_type="scraper",
    requires_attribution=False,
    images_allowed=True,  # Cover images are allowed
    extra={
        "base_url": CBR_BASE_URL,
        "requires_robots_check": True,
        "min_delay_seconds": 2.0,
    },
)


def get_comicbookrealm_client() -> ResilientHTTPClient:
    """Get HTTP client configured for ComicBookRealm."""
    return ResilientHTTPClient(
        rate_limit_config=RateLimitConfig(
            requests_per_second=0.5,
            burst_limit=2,
            min_request_interval=2.0,
        ),
        retry_config=RetryConfig(
            max_retries=3,
            base_delay=5.0,
            max_delay=60.0,
        ),
        timeout=30.0,
        default_headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        },
    )


async def create_comicbookrealm_adapter() -> ComicBookRealmAdapter:
    """Create and return a ComicBookRealm adapter instance."""
    client = get_comicbookrealm_client()
    adapter = ComicBookRealmAdapter(COMICBOOKREALM_CONFIG, client)

    # Register with global registry
    from app.core.adapter_registry import adapter_registry
    adapter_registry._adapters["comicbookrealm"] = adapter
    adapter_registry._configs["comicbookrealm"] = COMICBOOKREALM_CONFIG

    return adapter
