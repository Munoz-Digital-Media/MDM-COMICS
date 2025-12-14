"""
Marvel Fandom (Marvel Database) Adapter v2.0.0

Adapter for Marvel Database on Fandom - editorial story/character depth.
https://marvel.fandom.com/

Per pipeline spec:
- Type: MediaWiki API + HTML scraping for structured data
- License: CC BY-SA 3.0 - TEXT ONLY
- DO NOT USE IMAGES - Fair Use only on Fandom, not redistributable
- Priority: P3 for wiki-powered lore and community insights

v2.0.0: Added story-level credit extraction
- Per-story credits (writer, penciler, inker, colorist, letterer, editor)
- Character appearances per story
- Cover variants
- Editor-in-chief

Allowed content:
- Character bios and descriptions (TEXT ONLY)
- Story arc summaries
- Issue descriptions and synopses
- Creator information with specific roles
- Publisher data
- Team rosters and affiliations
- Event timelines
"""
import logging
import re
from typing import Any, Dict, Optional, List
from urllib.parse import quote, unquote
from bs4 import BeautifulSoup

from app.core.adapter_registry import (
    DataSourceAdapter,
    AdapterConfig,
    FetchResult,
    MARVEL_FANDOM_CONFIG,
)
from app.core.http_client import ResilientHTTPClient

logger = logging.getLogger(__name__)


class MarvelFandomAdapter(DataSourceAdapter):
    """
    Adapter for Marvel Database (Fandom) via MediaWiki API.

    Provides character bios, story arcs, and community-curated lore.
    TEXT ONLY - images are NOT covered by CC BY-SA.
    """

    BASE_URL = "https://marvel.fandom.com/api.php"

    def __init__(
        self,
        config: AdapterConfig = MARVEL_FANDOM_CONFIG,
        client: Optional[ResilientHTTPClient] = None
    ):
        if client is None:
            from app.core.http_client import ResilientHTTPClient, RateLimitConfig
            client = ResilientHTTPClient(
                rate_limit_config=RateLimitConfig(
                    requests_per_second=1.0,
                    burst_limit=3,
                    min_request_interval=1.0,
                )
            )

        super().__init__(config, client)

    async def health_check(self) -> bool:
        """Check if Marvel Fandom API is reachable."""
        try:
            response = await self.client.get(
                self.BASE_URL,
                params={
                    "action": "query",
                    "meta": "siteinfo",
                    "format": "json",
                }
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"[{self.name}] Health check failed: {e}")
            return False

    async def fetch_page(
        self,
        page: int = 1,
        page_size: int = 50,
        cursor: Optional[str] = None,
        category: str = "Characters",
        **filters
    ) -> FetchResult:
        """
        Fetch pages from a category.

        Args:
            page: Not used - MediaWiki uses continuation tokens
            page_size: Number of results (max 500)
            cursor: Continuation token from previous request
            category: Category to fetch from
        """
        try:
            params = {
                "action": "query",
                "list": "categorymembers",
                "cmtitle": f"Category:{category}",
                "cmlimit": min(page_size, 500),
                "format": "json",
            }

            if cursor:
                params["cmcontinue"] = cursor

            response = await self.client.get(self.BASE_URL, params=params)

            if response.status_code != 200:
                return FetchResult(
                    success=False,
                    errors=[{"error": f"API returned {response.status_code}"}]
                )

            data = response.json()

            # Extract results
            query = data.get("query", {})
            members = query.get("categorymembers", [])

            # Check for continuation
            continue_data = data.get("continue", {})
            next_cursor = continue_data.get("cmcontinue")

            return FetchResult(
                success=True,
                records=members,
                has_more=next_cursor is not None,
                next_cursor=next_cursor,
            )

        except Exception as e:
            logger.error(f"[{self.name}] Fetch failed: {e}")
            return FetchResult(
                success=False,
                errors=[{"error": str(e)}]
            )

    async def fetch_by_id(self, page_title: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single page by title.

        Args:
            page_title: The page title (e.g., "Spider-Man_(Peter_Parker)")
        """
        try:
            # Get page content with parsed HTML
            response = await self.client.get(
                self.BASE_URL,
                params={
                    "action": "parse",
                    "page": page_title,
                    "prop": "text|categories|sections",
                    "format": "json",
                }
            )

            if response.status_code != 200:
                return None

            data = response.json()

            if "error" in data:
                logger.warning(f"[{self.name}] API error for {page_title}: {data['error']}")
                return None

            return data.get("parse", {})

        except Exception as e:
            logger.error(f"[{self.name}] Fetch by ID {page_title} failed: {e}")
            return None

    async def get_page_extract(self, page_title: str) -> Optional[str]:
        """
        Get plain text extract of a page (no HTML).

        This is the safest method for getting content without images.
        """
        try:
            response = await self.client.get(
                self.BASE_URL,
                params={
                    "action": "query",
                    "titles": page_title,
                    "prop": "extracts",
                    "exintro": "1",  # Just the intro
                    "explaintext": "1",  # Plain text, no HTML
                    "format": "json",
                }
            )

            if response.status_code != 200:
                return None

            data = response.json()
            pages = data.get("query", {}).get("pages", {})

            for page_id, page_data in pages.items():
                if page_id != "-1":
                    return page_data.get("extract")

            return None

        except Exception as e:
            logger.error(f"[{self.name}] Get extract failed: {e}")
            return None

    async def search(
        self,
        query: str,
        limit: int = 20,
        namespace: int = 0,  # 0 = main namespace
    ) -> FetchResult:
        """
        Search for pages matching a query.
        """
        try:
            response = await self.client.get(
                self.BASE_URL,
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": query,
                    "srlimit": min(limit, 50),
                    "srnamespace": namespace,
                    "format": "json",
                }
            )

            if response.status_code != 200:
                return FetchResult(
                    success=False,
                    errors=[{"error": f"Search returned {response.status_code}"}]
                )

            data = response.json()
            results = data.get("query", {}).get("search", [])

            return FetchResult(
                success=True,
                records=results,
                total_count=data.get("query", {}).get("searchinfo", {}).get("totalhits"),
            )

        except Exception as e:
            logger.error(f"[{self.name}] Search failed: {e}")
            return FetchResult(
                success=False,
                errors=[{"error": str(e)}]
            )

    async def get_character_info(self, character_name: str) -> Optional[Dict[str, Any]]:
        """
        Get character information from their wiki page.

        Returns structured character data extracted from the page.
        """
        # Try to find the character page
        search_result = await self.search(character_name, limit=5)

        if not search_result.success or not search_result.records:
            return None

        # Get the first result that looks like a character page
        page_title = None
        for result in search_result.records:
            title = result.get("title", "")
            # Skip disambiguation pages
            if "(Disambiguation)" not in title:
                page_title = title
                break

        if not page_title:
            return None

        # Get the page content
        page_data = await self.fetch_by_id(page_title)
        if not page_data:
            return None

        # Get plain text extract
        extract = await self.get_page_extract(page_title)

        return {
            "title": page_data.get("title"),
            "page_id": page_data.get("pageid"),
            "extract": extract,
            "categories": [c.get("*") for c in page_data.get("categories", [])],
            "sections": [s.get("line") for s in page_data.get("sections", [])],
        }

    async def fetch_issue_credits(
        self,
        series_name: str,
        volume: int,
        issue_number: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch full issue data with story-level credits.

        Args:
            series_name: e.g., "Amazing Fantasy"
            volume: e.g., 1
            issue_number: e.g., "15"

        Returns:
            Dict with stories, credits, characters, cover variants
        """
        # Build the wiki page title
        # Format: "Amazing_Fantasy_Vol_1_15"
        page_title = f"{series_name.replace(' ', '_')}_Vol_{volume}_{issue_number}"

        try:
            # Get the raw HTML
            response = await self.client.get(
                self.BASE_URL,
                params={
                    "action": "parse",
                    "page": page_title,
                    "prop": "text|categories",
                    "format": "json",
                }
            )

            if response.status_code != 200:
                logger.warning(f"[{self.name}] Failed to fetch {page_title}: {response.status_code}")
                return None

            data = response.json()
            if "error" in data:
                logger.debug(f"[{self.name}] Page not found: {page_title}")
                return None

            parse_data = data.get("parse", {})
            html = parse_data.get("text", {}).get("*", "")

            if not html:
                return None

            # Parse the HTML
            soup = BeautifulSoup(html, "html.parser")

            # Extract issue metadata
            result = {
                "page_title": page_title,
                "fandom_url": f"https://marvel.fandom.com/wiki/{page_title}",
                "stories": [],
                "cover_variants": [],
                "editor_in_chief": None,
                "release_date": None,
                "cover_date": None,
                "marvel_unlimited": False,
            }

            # Find the issue info box
            info_box = soup.find("aside", class_="portable-infobox")
            if info_box:
                result.update(self._parse_info_box(info_box))

            # Find all story sections
            result["stories"] = self._parse_stories(soup)

            # Find cover variants
            result["cover_variants"] = self._parse_cover_variants(soup)

            return result

        except Exception as e:
            logger.error(f"[{self.name}] Error fetching {page_title}: {e}")
            return None

    def _parse_info_box(self, info_box: BeautifulSoup) -> Dict[str, Any]:
        """Parse the issue info box for metadata."""
        result = {}

        for row in info_box.find_all("div", class_="pi-item"):
            label_elem = row.find("h3", class_="pi-data-label")
            value_elem = row.find("div", class_="pi-data-value")

            if not label_elem or not value_elem:
                continue

            label = label_elem.get_text(strip=True).lower()

            if "editor-in-chief" in label or "editor in chief" in label:
                # Get the first link's text
                link = value_elem.find("a")
                if link:
                    result["editor_in_chief"] = link.get_text(strip=True)

            elif "release date" in label:
                result["release_date"] = value_elem.get_text(strip=True)

            elif "cover date" in label:
                result["cover_date"] = value_elem.get_text(strip=True)

            elif "marvel unlimited" in label:
                text = value_elem.get_text(strip=True).lower()
                result["marvel_unlimited"] = "available" in text or "yes" in text

        return result

    def _parse_stories(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse individual stories with their credits."""
        stories = []
        current_story = None

        # Iterate through all h2 and h3 elements
        for elem in soup.find_all(["h2", "h3"]):
            text = elem.get_text(strip=True)

            # Story header pattern: '1. "Spider-Man!"' or '1. Spider-Man!'
            match = re.match(r'^(\d+)\.\s*"?(.+?)"?\s*$', text)
            if match and elem.name == "h2":
                # Save previous story if exists
                if current_story:
                    stories.append(current_story)

                current_story = {
                    "story_number": int(match.group(1)),
                    "title": match.group(2).strip('"'),
                    "credits": {},
                    "characters": [],
                    "synopsis": None,
                }
                continue

            # Credit roles are h3 elements under each story
            if current_story and elem.name == "h3":
                for role in ["Writer", "Penciler", "Inker", "Colorist", "Letterer", "Editor"]:
                    if role in text:
                        # The creator names are in links in the next sibling element
                        next_elem = elem.find_next_sibling()
                        if next_elem:
                            links = next_elem.find_all("a") if hasattr(next_elem, "find_all") else []
                            names = [a.get_text(strip=True) for a in links if a.get_text(strip=True)]
                            if names:
                                current_story["credits"][role.lower()] = names
                        break

                # Check for Appearances section
                if "Appearances" in text:
                    next_elem = elem.find_next_sibling()
                    while next_elem and next_elem.name not in ["h2", "h3"]:
                        if hasattr(next_elem, "find_all"):
                            for link in next_elem.find_all("a"):
                                char_name = link.get_text(strip=True)
                                # Skip category links and empty names
                                if char_name and not char_name.startswith("Category:"):
                                    current_story["characters"].append(char_name)
                        next_elem = next_elem.find_next_sibling()

        # Don't forget the last story
        if current_story:
            stories.append(current_story)

        # If no numbered stories found, try alternative parsing
        if not stories:
            stories = self._parse_stories_table(soup)

        return stories

    def _parse_stories_table(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse stories from table format (some pages use tables)."""
        stories = []

        # Look for tables with story credits
        for table in soup.find_all("table", class_="wikitable"):
            rows = table.find_all("tr")

            current_story = None
            for row in rows:
                cells = row.find_all(["th", "td"])
                if len(cells) < 2:
                    continue

                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)

                # New story starts with a story title cell
                if "story" in label or label.isdigit():
                    if current_story:
                        stories.append(current_story)
                    current_story = {
                        "story_number": len(stories) + 1,
                        "title": value,
                        "credits": {},
                        "characters": [],
                    }

                elif current_story:
                    # Credit roles
                    for role in ["writer", "penciler", "inker", "colorist", "letterer", "editor"]:
                        if role in label:
                            creators = [c.strip() for c in value.split(",")]
                            current_story["credits"][role] = creators
                            break

            if current_story:
                stories.append(current_story)

        return stories

    def _parse_cover_variants(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse cover variant information."""
        variants = []

        # Look for "Alternate Covers" or "Cover Gallery" section
        for heading in soup.find_all(["h2", "h3"]):
            if "cover" in heading.get_text(strip=True).lower():
                # Find gallery or thumbnail links after this heading
                sibling = heading.find_next_sibling()
                variant_num = 0

                while sibling and sibling.name not in ["h2", "h3"]:
                    # Look for thumbnail captions
                    captions = sibling.find_all("div", class_="thumbcaption") if hasattr(sibling, 'find_all') else []
                    for caption in captions:
                        variant_num += 1
                        text = caption.get_text(strip=True)
                        variants.append({
                            "variant_number": variant_num,
                            "variant_name": text,
                            "variant_type": self._classify_variant(text),
                        })

                    sibling = sibling.find_next_sibling()

                break

        return variants

    def _classify_variant(self, text: str) -> str:
        """Classify variant type from description."""
        text_lower = text.lower()

        if "virgin" in text_lower:
            return "virgin"
        elif "incentive" in text_lower or "1:" in text_lower:
            return "incentive"
        elif "newsstand" in text_lower:
            return "newsstand"
        elif "direct" in text_lower:
            return "direct"
        elif "variant" in text_lower:
            return "variant"
        else:
            return "standard"

    async def search_issue(
        self,
        series_name: str,
        issue_number: str,
        year: Optional[int] = None
    ) -> Optional[str]:
        """
        Search for an issue and return the best matching page title.

        Useful when volume number is unknown.
        """
        query = f"{series_name} {issue_number}"
        if year:
            query += f" {year}"

        result = await self.search(query, limit=10)

        if not result.success or not result.records:
            return None

        # Look for a page that matches the issue pattern
        for record in result.records:
            title = record.get("title", "")
            # Match pattern like "Series_Name_Vol_X_Issue"
            if re.search(rf'{re.escape(series_name)}.*Vol.*{issue_number}', title, re.IGNORECASE):
                return title

        return None

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize Marvel Fandom record to canonical schema.
        """
        # Handle different record types (category member vs full page)
        if "pageid" in record:
            # Category member
            return {
                "fandom_page_id": record.get("pageid"),
                "title": record.get("title"),

                # Source tracking
                "_source": self.name,
                "_source_id": str(record.get("pageid")),
                "_license": "CC-BY-SA-3.0",
                "_requires_attribution": True,
                "_attribution": self.config.attribution_text,
                "_text_only": True,  # Reminder: no images allowed
            }
        else:
            # Full page data
            return {
                "fandom_page_id": record.get("page_id"),
                "title": record.get("title"),
                "extract": record.get("extract"),
                "categories": record.get("categories", []),
                "sections": record.get("sections", []),

                # Source tracking
                "_source": self.name,
                "_source_id": str(record.get("page_id")),
                "_license": "CC-BY-SA-3.0",
                "_requires_attribution": True,
                "_attribution": self.config.attribution_text,
                "_text_only": True,
            }
