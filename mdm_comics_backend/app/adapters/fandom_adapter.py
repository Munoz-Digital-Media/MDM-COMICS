"""
Generic Fandom Wiki Adapter v1.1.0

Multi-wiki adapter for Fandom (MediaWiki-based) comic databases.
Supports: DC Comics, Image Comics, IDW Publishing, Dark Horse, Dynamite.

Per pipeline spec:
- Type: MediaWiki API
- License: CC BY-SA 3.0
- Images: Harvested for perceptual hash matching database (mobile image search)
- Priority: P3 for wiki-powered lore and community insights

Supported Wikis:
- DC Database: https://dc.fandom.com/
- Image Comics Database: https://imagecomics.fandom.com/
- IDW Comics Database: https://comics.fandom.com/wiki/IDW
- Dark Horse Database: https://darkhorse.fandom.com/
- Dynamite Database: https://dynamiteentertainment.fandom.com/

Allowed content:
- Character bios and descriptions
- Story arc summaries
- Issue descriptions and synopses
- Creator information with specific roles
- Publisher data
- Cover images (for perceptual hash matching)
"""
import logging
import re
from typing import Any, Dict, Optional, List, Tuple
from bs4 import BeautifulSoup

from app.core.http_client import ResilientHTTPClient, RateLimitConfig

logger = logging.getLogger(__name__)


# Wiki configurations
FANDOM_WIKIS = {
    "dc_fandom": {
        "base_url": "https://dc.fandom.com/api.php",
        "display_name": "DC Database",
        "publisher_names": ["DC", "DC Comics", "Vertigo", "WildStorm", "DC/Vertigo"],
        "page_format": "{series}_Vol_{volume}_{issue}",
    },
    "image_fandom": {
        "base_url": "https://imagecomics.fandom.com/api.php",
        "display_name": "Image Comics Database",
        "publisher_names": ["Image", "Image Comics"],
        "page_format": "{series}_Vol_{volume}_{issue}",
    },
    "idw_fandom": {
        "base_url": "https://comics.fandom.com/api.php",  # IDW is on generic comics wiki
        "display_name": "IDW Publishing Database",
        "publisher_names": ["IDW", "IDW Publishing"],
        "page_format": "{series}_Vol_{volume}_{issue}",
    },
    "darkhorse_fandom": {
        "base_url": "https://darkhorse.fandom.com/api.php",
        "display_name": "Dark Horse Comics Database",
        "publisher_names": ["Dark Horse", "Dark Horse Comics"],
        "page_format": "{series}_Vol_{volume}_{issue}",
    },
    "dynamite_fandom": {
        "base_url": "https://dynamiteentertainment.fandom.com/api.php",
        "display_name": "Dynamite Entertainment Database",
        "publisher_names": ["Dynamite", "Dynamite Entertainment"],
        "page_format": "{series}_Vol_{volume}_{issue}",
    },
}


class FandomAdapter:
    """
    Generic adapter for Fandom (MediaWiki) comic databases.

    Extracts metadata and cover images for perceptual hash matching.
    """

    def __init__(
        self,
        wiki_key: str,
        client: Optional[ResilientHTTPClient] = None
    ):
        """
        Initialize Fandom adapter for a specific wiki.

        Args:
            wiki_key: Key from FANDOM_WIKIS (dc_fandom, image_fandom, idw_fandom)
            client: Optional HTTP client (creates one if not provided)
        """
        if wiki_key not in FANDOM_WIKIS:
            raise ValueError(f"Unknown wiki: {wiki_key}. Valid: {list(FANDOM_WIKIS.keys())}")

        self.wiki_key = wiki_key
        self.config = FANDOM_WIKIS[wiki_key]
        self.base_url = self.config["base_url"]
        self.display_name = self.config["display_name"]
        self.publisher_names = self.config["publisher_names"]

        if client is None:
            self.client = ResilientHTTPClient(
                rate_limit_config=RateLimitConfig(
                    requests_per_second=1.0,
                    burst_limit=3,
                    min_request_interval=1.0,
                )
            )
        else:
            self.client = client

        self._owns_client = client is None

    async def close(self):
        """Close the HTTP client if we own it."""
        if self._owns_client and self.client:
            await self.client.close()

    def matches_publisher(self, publisher_name: Optional[str]) -> bool:
        """Check if a publisher name matches this wiki's coverage."""
        if not publisher_name:
            return False
        pub_lower = publisher_name.lower()
        return any(p.lower() in pub_lower for p in self.publisher_names)

    async def health_check(self) -> bool:
        """Check if the wiki API is reachable."""
        try:
            response = await self.client.get(
                self.base_url,
                params={
                    "action": "query",
                    "meta": "siteinfo",
                    "format": "json",
                }
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"[{self.wiki_key}] Health check failed: {e}")
            return False

    async def search(
        self,
        query: str,
        limit: int = 20,
        namespace: int = 0,  # 0 = main namespace
    ) -> List[Dict[str, Any]]:
        """Search for pages matching a query."""
        try:
            response = await self.client.get(
                self.base_url,
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
                return []

            data = response.json()
            return data.get("query", {}).get("search", [])

        except Exception as e:
            logger.error(f"[{self.wiki_key}] Search failed: {e}")
            return []

    async def get_page_extract(self, page_title: str) -> Optional[str]:
        """
        Get plain text extract of a page (no HTML).
        This is the safest method for getting content without images.
        """
        try:
            response = await self.client.get(
                self.base_url,
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
            logger.error(f"[{self.wiki_key}] Get extract failed: {e}")
            return None

    async def fetch_issue_data(
        self,
        series_name: str,
        volume: int,
        issue_number: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch issue data including metadata and synopsis.

        Args:
            series_name: e.g., "Batman"
            volume: e.g., 1
            issue_number: e.g., "1"

        Returns:
            Dict with description, cover_date, release_date, page_count
        """
        # Build the wiki page title
        # Format varies by wiki but typically: "Series_Name_Vol_X_Issue"
        page_title = self.config["page_format"].format(
            series=series_name.replace(' ', '_'),
            volume=volume,
            issue=issue_number
        )

        try:
            # Get the raw HTML
            response = await self.client.get(
                self.base_url,
                params={
                    "action": "parse",
                    "page": page_title,
                    "prop": "text|categories",
                    "format": "json",
                }
            )

            if response.status_code != 200:
                logger.debug(f"[{self.wiki_key}] Failed to fetch {page_title}: {response.status_code}")
                return None

            data = response.json()
            if "error" in data:
                # Try alternative page format without Vol
                alt_title = f"{series_name.replace(' ', '_')}_{issue_number}"
                return await self._fetch_page_data(alt_title)

            parse_data = data.get("parse", {})
            html = parse_data.get("text", {}).get("*", "")

            if not html:
                return None

            # Parse the HTML
            soup = BeautifulSoup(html, "html.parser")

            result = {
                "page_title": page_title,
                "wiki_url": f"{self.base_url.replace('/api.php', '/wiki/')}{page_title}",
                "description": None,
                "cover_date": None,
                "release_date": None,
                "page_count": None,
                "image": None,
                "_source": self.wiki_key,
                "_license": "CC-BY-SA-3.0",
            }

            # Find the infobox and extract data including image
            info_box = soup.find("aside", class_="portable-infobox")
            if info_box:
                result.update(self._parse_info_box(info_box, soup))

            # Get description from first paragraph or extract
            first_p = soup.find("p")
            if first_p:
                text = first_p.get_text(strip=True)
                if text and len(text) > 50:
                    result["description"] = text

            return result

        except Exception as e:
            logger.error(f"[{self.wiki_key}] Error fetching {page_title}: {e}")
            return None

    async def _fetch_page_data(self, page_title: str) -> Optional[Dict[str, Any]]:
        """Fetch data from an alternative page title."""
        try:
            response = await self.client.get(
                self.base_url,
                params={
                    "action": "parse",
                    "page": page_title,
                    "prop": "text",
                    "format": "json",
                }
            )

            if response.status_code != 200:
                return None

            data = response.json()
            if "error" in data:
                return None

            parse_data = data.get("parse", {})
            html = parse_data.get("text", {}).get("*", "")

            if not html:
                return None

            soup = BeautifulSoup(html, "html.parser")

            result = {
                "page_title": page_title,
                "wiki_url": f"{self.base_url.replace('/api.php', '/wiki/')}{page_title}",
                "description": None,
                "cover_date": None,
                "release_date": None,
                "page_count": None,
                "image": None,
                "_source": self.wiki_key,
                "_license": "CC-BY-SA-3.0",
            }

            # Find the infobox and extract data including image
            info_box = soup.find("aside", class_="portable-infobox")
            if info_box:
                result.update(self._parse_info_box(info_box, soup))

            first_p = soup.find("p")
            if first_p:
                text = first_p.get_text(strip=True)
                if text and len(text) > 50:
                    result["description"] = text

            return result

        except Exception as e:
            logger.debug(f"[{self.wiki_key}] Alt fetch failed: {e}")
            return None

    def _parse_info_box(self, info_box: BeautifulSoup, soup: BeautifulSoup = None) -> Dict[str, Any]:
        """Parse the issue info box for metadata and cover image."""
        result = {}

        # Extract cover image from infobox
        # Fandom infoboxes usually have the cover in a figure or pi-image element
        image_elem = info_box.find("figure", class_="pi-image")
        if image_elem:
            img_tag = image_elem.find("img")
            if img_tag:
                # Get the highest resolution image URL available
                image_url = img_tag.get("data-src") or img_tag.get("src")
                if image_url:
                    # Clean up Fandom image URL to get full resolution
                    # Fandom uses /revision/latest/scale-to-width-down/XXX format
                    # Remove the scale-to-width-down part to get full resolution
                    if "/scale-to-width-down/" in image_url:
                        image_url = re.sub(r'/scale-to-width-down/\d+', '', image_url)
                    if "/scale-to-height-down/" in image_url:
                        image_url = re.sub(r'/scale-to-height-down/\d+', '', image_url)
                    result["image"] = image_url

        # If no image in infobox, try finding cover image elsewhere
        if not result.get("image") and soup:
            # Try the main page image
            main_img = soup.find("img", class_="pi-image-thumbnail")
            if main_img:
                image_url = main_img.get("data-src") or main_img.get("src")
                if image_url:
                    if "/scale-to-width-down/" in image_url:
                        image_url = re.sub(r'/scale-to-width-down/\d+', '', image_url)
                    result["image"] = image_url

        for row in info_box.find_all("div", class_="pi-item"):
            label_elem = row.find("h3", class_="pi-data-label")
            value_elem = row.find("div", class_="pi-data-value")

            if not label_elem or not value_elem:
                continue

            label = label_elem.get_text(strip=True).lower()
            value = value_elem.get_text(strip=True)

            if "release date" in label or "released" in label:
                result["release_date"] = value
            elif "cover date" in label:
                result["cover_date"] = value
            elif "page" in label and "count" in label:
                # Extract numeric page count
                match = re.search(r'(\d+)', value)
                if match:
                    result["page_count"] = int(match.group(1))
            elif "pages" in label:
                match = re.search(r'(\d+)', value)
                if match:
                    result["page_count"] = int(match.group(1))

        return result

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

        results = await self.search(query, limit=10)

        if not results:
            return None

        # Look for a page that matches the issue pattern
        for record in results:
            title = record.get("title", "")
            # Match pattern like "Series_Name_Vol_X_Issue"
            if re.search(rf'{re.escape(series_name)}.*{issue_number}', title, re.IGNORECASE):
                return title

        return None


# Factory functions for each wiki
def create_dc_fandom_adapter(client: Optional[ResilientHTTPClient] = None) -> FandomAdapter:
    """Create adapter for DC Database (Fandom)."""
    return FandomAdapter("dc_fandom", client)


def create_image_fandom_adapter(client: Optional[ResilientHTTPClient] = None) -> FandomAdapter:
    """Create adapter for Image Comics Database (Fandom)."""
    return FandomAdapter("image_fandom", client)


def create_idw_fandom_adapter(client: Optional[ResilientHTTPClient] = None) -> FandomAdapter:
    """Create adapter for IDW Publishing Database (Fandom)."""
    return FandomAdapter("idw_fandom", client)


def create_darkhorse_fandom_adapter(client: Optional[ResilientHTTPClient] = None) -> FandomAdapter:
    """Create adapter for Dark Horse Comics Database (Fandom)."""
    return FandomAdapter("darkhorse_fandom", client)


def create_dynamite_fandom_adapter(client: Optional[ResilientHTTPClient] = None) -> FandomAdapter:
    """Create adapter for Dynamite Entertainment Database (Fandom)."""
    return FandomAdapter("dynamite_fandom", client)
