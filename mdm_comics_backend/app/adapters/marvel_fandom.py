"""
Marvel Fandom (Marvel Database) Adapter v1.0.0

Adapter for Marvel Database on Fandom - editorial story/character depth.
https://marvel.fandom.com/

Per pipeline spec:
- Type: MediaWiki API
- License: CC BY-SA 3.0 - TEXT ONLY
- DO NOT USE IMAGES - Fair Use only on Fandom, not redistributable
- Priority: P3 for wiki-powered lore and community insights

Allowed content:
- Character bios and descriptions (TEXT ONLY)
- Story arc summaries
- Issue descriptions and synopses
- Creator information
- Publisher data
- Team rosters and affiliations
- Event timelines
"""
import logging
from typing import Any, Dict, Optional, List
from urllib.parse import quote

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
