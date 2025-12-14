"""
MyComicShop Scraper Adapter v1.10.0

Web scraper for MyComicShop.com - comic retailer with inventory and pricing.

Per constitution_cyberSec.json: Mandatory robots.txt compliance.
Per constitution_data_hygiene.json: No PII storage.

Features:
- Cover images
- Retail pricing (actual sale prices)
- Inventory availability
- Condition grading data

Usage:
    adapter = MyComicShopAdapter(config, client)
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

# MyComicShop URLs
MCS_BASE_URL = "https://www.mycomicshop.com"
MCS_SEARCH_URL = f"{MCS_BASE_URL}/search"


class MyComicShopAdapter(DataSourceAdapter):
    """
    Scraper adapter for MyComicShop.com.

    Provides:
    - Issue search and details
    - Cover images
    - Current inventory prices
    - Condition data from listings

    Note: This is a scraper and must respect robots.txt and rate limits.
    """

    def __init__(
        self,
        config: AdapterConfig,
        client: ResilientHTTPClient,
    ):
        super().__init__(config, client)
        self.base_url = MCS_BASE_URL
        self._last_request_time = 0.0
        self._min_delay = 3.0  # Minimum 3 seconds between requests (conservative)

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
                logger.warning(f"[MCS] URL disallowed by robots.txt: {url}")
            return can_fetch
        except Exception as e:
            logger.error(f"[MCS] Robots check failed: {e}")
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
                logger.warning(f"[MCS] HTTP {response.status_code} for {url}")
                return None
        except Exception as e:
            logger.error(f"[MCS] Fetch failed for {url}: {e}")
            return None

    async def health_check(self) -> bool:
        """Check if MyComicShop is reachable."""
        try:
            can_fetch = await robots_checker.can_fetch(self.base_url)
            if not can_fetch:
                return False

            response = await self.client.get(self.base_url)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"[MCS] Health check failed: {e}")
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

        Note: Use search_issues() for targeted searches.
        """
        return FetchResult(
            success=False,
            errors=[{"message": "MyComicShop does not support bulk listing"}],
        )

    async def fetch_by_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single issue by MyComicShop ID/URL.

        Args:
            external_id: The MCS item ID or full URL
        """
        # Handle both ID and URL
        if external_id.startswith("http"):
            url = external_id
        else:
            url = f"{self.base_url}/item/{external_id}"

        html = await self._fetch_page(url)
        if not html:
            return None

        return self._parse_item_page(html, url)

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
            query = f"{series_name} #{issue_number}"

        # URL encode the query
        search_url = f"{MCS_SEARCH_URL}?q={quote(query)}&pubid=&Sort=Date"

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

        # MyComicShop uses table-based layout
        # Look for product rows/items
        items = soup.select('.product-row, .item-row, tr.item, .comic-item')[:limit]

        for item in items:
            try:
                result = self._extract_search_item(item)
                if result:
                    results.append(result)
            except Exception as e:
                logger.warning(f"[MCS] Failed to parse search result: {e}")

        # Try alternate: look for links to item pages
        if not results:
            links = soup.select('a[href*="/item/"]')[:limit]
            for link in links:
                result = self._extract_link_item(link)
                if result:
                    results.append(result)

        return results

    def _extract_search_item(self, item) -> Optional[Dict[str, Any]]:
        """Extract data from a search result item."""
        # Find the link to the item page
        link = item.find('a')
        if not link:
            return None

        href = link.get('href', '')
        if href and not href.startswith('http'):
            href = urljoin(self.base_url, href)

        title_text = link.get_text(strip=True)

        # Find cover image
        img = item.find('img')
        cover_url = None
        if img:
            cover_url = img.get('src') or img.get('data-src')
            if cover_url and not cover_url.startswith('http'):
                cover_url = urljoin(self.base_url, cover_url)

        # Find price
        price_elem = item.select_one('.price, .item-price, [class*="price"]')
        price = None
        if price_elem:
            price = self._parse_price(price_elem.get_text())

        # Find condition/grade
        grade_elem = item.select_one('.grade, .condition, [class*="grade"]')
        grade = None
        if grade_elem:
            grade = grade_elem.get_text(strip=True)

        return {
            "title": title_text,
            "url": href,
            "cover_url": cover_url,
            "price": price,
            "grade": grade,
            "_source": "mycomicshop",
        }

    def _extract_link_item(self, link) -> Optional[Dict[str, Any]]:
        """Extract minimal data from a link."""
        href = link.get('href', '')
        if not href or '/item/' not in href:
            return None

        if not href.startswith('http'):
            href = urljoin(self.base_url, href)

        title = link.get_text(strip=True)
        if not title:
            return None

        # Try to find adjacent image
        parent = link.parent
        img = None
        if parent:
            img = parent.find('img')

        cover_url = None
        if img:
            cover_url = img.get('src') or img.get('data-src')
            if cover_url and not cover_url.startswith('http'):
                cover_url = urljoin(self.base_url, cover_url)

        return {
            "title": title,
            "url": href,
            "cover_url": cover_url,
            "_source": "mycomicshop",
        }

    def _parse_item_page(self, html: str, url: str) -> Dict[str, Any]:
        """Parse a single item detail page."""
        soup = BeautifulSoup(html, 'html.parser')

        data = {
            "url": url,
            "_source": "mycomicshop",
        }

        # Extract title
        title_elem = soup.select_one('h1, .item-title, .product-title')
        if title_elem:
            data["title"] = title_elem.get_text(strip=True)

        # Extract series name
        series_elem = soup.select_one('.series, .title-series, a[href*="/series/"]')
        if series_elem:
            data["series_name"] = series_elem.get_text(strip=True)

        # Extract issue number
        issue_match = re.search(r'#(\d+[A-Za-z]?)', data.get("title", ""))
        if issue_match:
            data["issue_number"] = issue_match.group(1)

        # Extract cover image
        cover_img = soup.select_one('.item-image img, .product-image img, img.cover')
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

        # Extract listings/inventory with prices and conditions
        listings = self._extract_listings(soup)
        if listings:
            data["listings"] = listings

            # Get best price for each grade tier
            prices_by_grade = {}
            for listing in listings:
                grade = listing.get("grade", "").upper()
                price = listing.get("price")
                if grade and price:
                    if grade not in prices_by_grade or price < prices_by_grade[grade]:
                        prices_by_grade[grade] = price

            # Map to standard grade prices
            data.update(self._map_grade_prices(prices_by_grade))

        # Extract description
        desc_elem = soup.select_one('.description, .item-description')
        if desc_elem:
            data["description"] = desc_elem.get_text(strip=True)[:2000]

        # Extract cover date
        date_elem = soup.select_one('.date, .cover-date, [class*="date"]')
        if date_elem:
            date_text = date_elem.get_text(strip=True)
            data["cover_date_text"] = date_text
            parsed_date = self._parse_date(date_text)
            if parsed_date:
                data["cover_date"] = parsed_date

        return data

    def _extract_listings(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract inventory listings with prices and conditions."""
        listings = []

        # Look for inventory table or list
        listing_rows = soup.select('.inventory-row, .listing-row, tr.copy')

        for row in listing_rows:
            listing = {}

            # Extract grade/condition
            grade_elem = row.select_one('.grade, .condition, td:nth-child(1)')
            if grade_elem:
                listing["grade"] = grade_elem.get_text(strip=True)

            # Extract price
            price_elem = row.select_one('.price, td.price, [class*="price"]')
            if price_elem:
                listing["price"] = self._parse_price(price_elem.get_text())

            # Extract condition notes
            notes_elem = row.select_one('.notes, .defects, .details')
            if notes_elem:
                listing["notes"] = notes_elem.get_text(strip=True)[:500]
                # Extract defects from notes
                listing["defects"] = self._extract_defects(listing["notes"])

            # Extract availability
            avail_elem = row.select_one('.stock, .availability, [class*="stock"]')
            if avail_elem:
                listing["in_stock"] = 'out' not in avail_elem.get_text().lower()

            if listing.get("grade") or listing.get("price"):
                listings.append(listing)

        return listings

    def _map_grade_prices(self, prices_by_grade: Dict[str, float]) -> Dict[str, float]:
        """Map grade abbreviations to standard price fields."""
        mapped = {}

        grade_mapping = {
            # Near Mint variants
            "NM": "price_nm", "NM+": "price_nm", "NM-": "price_nm",
            "9.8": "price_nm", "9.6": "price_nm", "9.4": "price_nm",
            # Very Fine variants
            "VF": "price_vf", "VF+": "price_vf", "VF-": "price_vf",
            "VF/NM": "price_vf", "8.0": "price_vf", "8.5": "price_vf",
            # Fine variants
            "FN": "price_f", "FN+": "price_f", "FN-": "price_f",
            "FN/VF": "price_f", "6.0": "price_f", "6.5": "price_f", "7.0": "price_f",
            # Very Good variants
            "VG": "price_vg", "VG+": "price_vg", "VG-": "price_vg",
            "VG/FN": "price_vg", "4.0": "price_vg", "4.5": "price_vg", "5.0": "price_vg",
            # Good variants
            "GD": "price_g", "GD+": "price_g", "GD-": "price_g",
            "GD/VG": "price_g", "2.0": "price_g", "2.5": "price_g", "3.0": "price_g",
            # Fair/Poor
            "FR": "price_fr", "PR": "price_pr",
        }

        for grade, price in prices_by_grade.items():
            field = grade_mapping.get(grade)
            if field and price:
                if field not in mapped or price < mapped[field]:
                    mapped[field] = price

        return mapped

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
        Normalize a MyComicShop record to canonical schema.
        """
        # Parse series and issue from title if needed
        title = record.get("title", "")
        series_name = record.get("series_name", title)
        issue_number = record.get("issue_number")

        if not issue_number:
            match = re.search(r'#(\d+[A-Za-z]?)', title)
            if match:
                issue_number = match.group(1)
                if not record.get("series_name"):
                    series_name = title[:match.start()].strip()

        return {
            # Core fields
            "series_name": series_name,
            "issue_number": issue_number,
            "publisher": record.get("publisher"),

            # Images
            "cover_url": record.get("cover_url"),
            "image": record.get("cover_url"),

            # Pricing (from listings)
            "price_nm": record.get("price_nm"),
            "price_vf": record.get("price_vf"),
            "price_f": record.get("price_f"),
            "price_vg": record.get("price_vg"),
            "price_g": record.get("price_g"),

            # Individual listings for analysis
            "listings": record.get("listings", []),

            # Dates
            "cover_date": record.get("cover_date"),

            # Description
            "description": record.get("description"),

            # Source tracking
            "mcs_url": record.get("url"),
            "_source": "mycomicshop",
            "_raw": record,
        }


# Adapter configuration
MYCOMICSHOP_CONFIG = AdapterConfig(
    name="mycomicshop",
    source_type=DataSourceType.SCRAPER,
    enabled=True,
    priority=40,
    requests_per_second=0.3,  # Conservative: 1 req per 3+ seconds
    burst_limit=2,
    auth_type=None,
    license_type="scraper",
    requires_attribution=False,
    images_allowed=True,
    extra={
        "base_url": MCS_BASE_URL,
        "requires_robots_check": True,
        "min_delay_seconds": 3.0,
    },
)


def get_mycomicshop_client() -> ResilientHTTPClient:
    """Get HTTP client configured for MyComicShop."""
    return ResilientHTTPClient(
        rate_limit_config=RateLimitConfig(
            requests_per_second=0.3,
            burst_limit=2,
            min_request_interval=3.0,
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


async def create_mycomicshop_adapter() -> MyComicShopAdapter:
    """Create and return a MyComicShop adapter instance.
    
    Note: v1.12.2 - Client is now auto-initialized via __aenter__.
    """
    client = get_mycomicshop_client()
    # v1.12.2: Initialize the httpx client
    await client.__aenter__()
    
    adapter = MyComicShopAdapter(MYCOMICSHOP_CONFIG, client)

    # Register with global registry
    from app.core.adapter_registry import adapter_registry
    adapter_registry._adapters["mycomicshop"] = adapter
    adapter_registry._configs["mycomicshop"] = MYCOMICSHOP_CONFIG

    return adapter
