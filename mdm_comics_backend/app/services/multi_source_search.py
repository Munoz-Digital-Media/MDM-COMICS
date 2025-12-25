"""
Multi-Source Comic Search Service v1.2.0

Provides resilient comic search with automatic failover across multiple sources.
Per constitution_cyberSec.json: No single point of failure.

v1.2.0: Smart series-based wiki routing for Fandom searches
        - "spawn" now correctly routes to Image Fandom
        - Series name pattern matching for 100+ known titles
v1.1.0: Added ComicVine fallback, fixed error propagation for rate limits

Search Priority:
1. Local cache (GCD data) - always checked first
2. Metron API - primary external source
3. ComicVine API - secondary API fallback
4. Fandom wikis - smart publisher-aware routing (DC, Marvel, Image)

Features:
- Automatic failover on source failure/rate limit
- Smart series-to-publisher inference for Fandom routing
- Result aggregation from multiple sources
- Source attribution in results
"""
import asyncio
import logging
import os
import re
from typing import Any, Dict, List, Optional, Set

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.models.comic_data import ComicIssue, ComicSeries
from app.services.source_rotator import source_rotator, SourceCapability
from app.services.metron import metron_service
from app.adapters.fandom_adapter import (
    FandomAdapter, FANDOM_WIKIS,
    create_dc_fandom_adapter, create_image_fandom_adapter
)
from app.adapters.comicvine_adapter import ComicVineAdapter
from app.adapters.mycomicshop_adapter import MyComicShopAdapter
from app.core.adapter_registry import AdapterConfig, DataSourceType
from app.core.http_client import ResilientHTTPClient, RateLimitConfig, RetryConfig

# Pre-configured adapters for fallback sources
COMICVINE_CONFIG = AdapterConfig(
    name="comicvine",
    source_type=DataSourceType.API,
    requests_per_second=0.05,  # 200/hour = ~3/min
    burst_limit=2,
)

MYCOMICSHOP_CONFIG = AdapterConfig(
    name="mycomicshop",
    source_type=DataSourceType.SCRAPER,
    requests_per_second=0.15,  # ~10/min for scraping
    burst_limit=2,
)

logger = logging.getLogger(__name__)

# Publisher to Fandom wiki mapping
PUBLISHER_WIKI_MAP = {
    "dc": "dc_fandom",
    "dc comics": "dc_fandom",
    "vertigo": "dc_fandom",
    "wildstorm": "dc_fandom",
    "marvel": "marvel_fandom",
    "marvel comics": "marvel_fandom",
    "image": "image_fandom",
    "image comics": "image_fandom",
    "dark horse": "darkhorse_fandom",
    "dark horse comics": "darkhorse_fandom",
    "idw": "idw_fandom",
    "idw publishing": "idw_fandom",
    "dynamite": "dynamite_fandom",
    "dynamite entertainment": "dynamite_fandom",
}

# Well-known series to publisher mapping for smart wiki routing
# Enables automatic wiki selection when publisher is not provided
SERIES_PUBLISHER_MAP = {
    # Image Comics - Major titles
    "spawn": "image",
    "the walking dead": "image",
    "walking dead": "image",
    "invincible": "image",
    "saga": "image",
    "savage dragon": "image",
    "witchblade": "image",
    "the darkness": "image",
    "darkness": "image",
    "wildcats": "image",
    "cyberforce": "image",
    "youngblood": "image",
    "haunt": "image",
    "curse of spawn": "image",
    "spawn the dark ages": "image",
    "violator": "image",
    "angela": "image",
    "medieval spawn": "image",
    "sam and twitch": "image",
    "sam & twitch": "image",
    "deadly class": "image",
    "wytches": "image",
    "east of west": "image",
    "jupiter's legacy": "image",
    "kick-ass": "image",
    "chew": "image",
    "black science": "image",
    "descender": "image",
    "monstress": "image",
    "paper girls": "image",
    "sex criminals": "image",
    "the wicked + the divine": "image",
    "criminal": "image",
    "fatale": "image",
    "lazarus": "image",
    "rad black": "image",
    "rat queens": "image",
    "southern bastards": "image",
    "ice cream man": "image",

    # DC Comics - Major titles
    "batman": "dc",
    "superman": "dc",
    "wonder woman": "dc",
    "justice league": "dc",
    "flash": "dc",
    "the flash": "dc",
    "green lantern": "dc",
    "aquaman": "dc",
    "green arrow": "dc",
    "nightwing": "dc",
    "teen titans": "dc",
    "suicide squad": "dc",
    "harley quinn": "dc",
    "batgirl": "dc",
    "supergirl": "dc",
    "robin": "dc",
    "detective comics": "dc",
    "action comics": "dc",
    "swamp thing": "dc",
    "sandman": "dc",
    "the sandman": "dc",
    "doom patrol": "dc",
    "hellblazer": "dc",
    "john constantine hellblazer": "dc",
    "constantine": "dc",
    "preacher": "dc",
    "y the last man": "dc",
    "fables": "dc",
    "100 bullets": "dc",
    "transmetropolitan": "dc",
    "watchmen": "dc",
    "v for vendetta": "dc",
    "crisis on infinite earths": "dc",
    "final crisis": "dc",
    "infinite crisis": "dc",
    "dceased": "dc",
    "dark nights metal": "dc",
    "dark knights death metal": "dc",
    "the batman who laughs": "dc",
    "shazam": "dc",
    "cyborg": "dc",
    "hawkman": "dc",
    "black adam": "dc",
    "blue beetle": "dc",
    "booster gold": "dc",
    "catwoman": "dc",
    "poison ivy": "dc",
    "red hood": "dc",
    "joker": "dc",
    "the joker": "dc",
    "birds of prey": "dc",
    "batwoman": "dc",
    "batwing": "dc",

    # Marvel Comics - Major titles
    "amazing spider-man": "marvel",
    "the amazing spider-man": "marvel",
    "spider-man": "marvel",
    "spectacular spider-man": "marvel",
    "x-men": "marvel",
    "uncanny x-men": "marvel",
    "avengers": "marvel",
    "fantastic four": "marvel",
    "iron man": "marvel",
    "invincible iron man": "marvel",
    "captain america": "marvel",
    "thor": "marvel",
    "mighty thor": "marvel",
    "hulk": "marvel",
    "incredible hulk": "marvel",
    "daredevil": "marvel",
    "wolverine": "marvel",
    "deadpool": "marvel",
    "punisher": "marvel",
    "the punisher": "marvel",
    "venom": "marvel",
    "carnage": "marvel",
    "ghost rider": "marvel",
    "moon knight": "marvel",
    "black panther": "marvel",
    "doctor strange": "marvel",
    "captain marvel": "marvel",
    "ms marvel": "marvel",
    "ms. marvel": "marvel",
    "scarlet witch": "marvel",
    "vision": "marvel",
    "hawkeye": "marvel",
    "black widow": "marvel",
    "guardians of the galaxy": "marvel",
    "silver surfer": "marvel",
    "new mutants": "marvel",
    "x-force": "marvel",
    "excalibur": "marvel",
    "x-factor": "marvel",
    "cable": "marvel",
    "gambit": "marvel",
    "rogue": "marvel",
    "magneto": "marvel",
    "secret wars": "marvel",
    "civil war": "marvel",
    "house of m": "marvel",
    "house of x": "marvel",
    "powers of x": "marvel",
    "infinity gauntlet": "marvel",
    "infinity war": "marvel",
    "age of apocalypse": "marvel",
    "ultimate spider-man": "marvel",
    "miles morales": "marvel",
    "miles morales spider-man": "marvel",
    "spider-gwen": "marvel",
    "spider-verse": "marvel",
    "secret invasion": "marvel",
    "eternals": "marvel",
    "blade": "marvel",
    "ant-man": "marvel",
    "wasp": "marvel",
    "she-hulk": "marvel",
    "elektra": "marvel",
    "iron fist": "marvel",
    "luke cage": "marvel",
    "jessica jones": "marvel",
    "star wars": "marvel",  # Marvel currently publishes Star Wars
}


class MultiSourceSearchService:
    """
    Orchestrates comic search across multiple data sources with fallback.

    Usage:
        results = await multi_source_search.search_issues(
            db=db,
            series_name="Batman",
            number="1",
            publisher_name="DC"
        )
    """

    def __init__(self):
        self._fandom_adapters: Dict[str, FandomAdapter] = {}
        self._comicvine_adapter: Optional[ComicVineAdapter] = None
        self._comicvine_client: Optional[ResilientHTTPClient] = None
        self._mycomicshop_adapter: Optional[MyComicShopAdapter] = None
        self._mycomicshop_client: Optional[ResilientHTTPClient] = None

    async def _get_comicvine_adapter(self) -> Optional[ComicVineAdapter]:
        """Get or create ComicVine adapter with HTTP client."""
        if self._comicvine_adapter is None:
            api_key = os.getenv("COMIC_VINE_API_KEY", "")
            if not api_key:
                logger.debug("[MULTI-SEARCH] ComicVine API key not configured")
                return None

            rate_limit = RateLimitConfig(requests_per_second=0.05)
            retry = RetryConfig(max_retries=2, base_delay=1.0)
            self._comicvine_client = ResilientHTTPClient(rate_limit, retry)
            self._comicvine_adapter = ComicVineAdapter(COMICVINE_CONFIG, self._comicvine_client, api_key)

        return self._comicvine_adapter

    async def _get_mycomicshop_adapter(self) -> Optional[MyComicShopAdapter]:
        """Get or create MyComicShop adapter with HTTP client."""
        if self._mycomicshop_adapter is None:
            rate_limit = RateLimitConfig(requests_per_second=0.15)
            retry = RetryConfig(max_retries=2, base_delay=2.0)
            self._mycomicshop_client = ResilientHTTPClient(rate_limit, retry)
            self._mycomicshop_adapter = MyComicShopAdapter(MYCOMICSHOP_CONFIG, self._mycomicshop_client)

        return self._mycomicshop_adapter

    def _get_fandom_adapter(self, wiki_key: str) -> Optional[FandomAdapter]:
        """Get or create a Fandom adapter for a wiki."""
        if wiki_key not in FANDOM_WIKIS:
            return None
        if wiki_key not in self._fandom_adapters:
            self._fandom_adapters[wiki_key] = FandomAdapter(wiki_key)
        return self._fandom_adapters[wiki_key]

    def _get_wiki_for_publisher(self, publisher_name: Optional[str]) -> Optional[str]:
        """Get the appropriate Fandom wiki key for a publisher."""
        if not publisher_name:
            return None
        pub_lower = publisher_name.lower().strip()
        return PUBLISHER_WIKI_MAP.get(pub_lower)

    def _infer_publisher_from_series(self, series_name: str) -> Optional[str]:
        """
        Infer publisher from well-known series names.
        Returns publisher shorthand (dc, marvel, image) or None if unknown.
        """
        if not series_name:
            return None

        series_lower = series_name.lower().strip()

        # Direct match first
        if series_lower in SERIES_PUBLISHER_MAP:
            return SERIES_PUBLISHER_MAP[series_lower]

        # Try partial matching for series that contain the keyword
        # e.g., "spawn #1" should match "spawn"
        for known_series, publisher in SERIES_PUBLISHER_MAP.items():
            if series_lower.startswith(known_series + " ") or series_lower == known_series:
                return publisher

        return None

    async def search_local_cache(
        self,
        db: AsyncSession,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
        publisher_name: Optional[str] = None,
        upc: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Search local database cache first (includes GCD data).
        This is always fast and doesn't hit external APIs.
        """
        try:
            query = select(ComicIssue).options(selectinload(ComicIssue.series))

            # UPC is exact match - highest priority
            if upc:
                query = query.where(ComicIssue.upc == upc)
            else:
                # Series name search
                if series_name:
                    query = query.join(ComicSeries).where(
                        ComicSeries.name.ilike(f"%{series_name}%")
                    )

                # Issue number
                if number:
                    query = query.where(ComicIssue.number == number)

                # Publisher filter
                if publisher_name:
                    if not series_name:
                        query = query.join(ComicSeries)
                    query = query.where(
                        ComicSeries.publisher_name.ilike(f"%{publisher_name}%")
                    )

            query = query.limit(limit)
            result = await db.execute(query)
            issues = result.scalars().all()

            return [
                {
                    "id": issue.metron_id or issue.id,
                    "issue": f"{issue.series.name} #{issue.number}" if issue.series else f"#{issue.number}",
                    "series": {"name": issue.series.name if issue.series else ""},
                    "number": issue.number,
                    "volume": issue.volume,  # Added volume field
                    "image": issue.image,
                    "cover_date": str(issue.cover_date) if issue.cover_date else None,
                    "_source": "local_cache",
                    "_cached": True,
                }
                for issue in issues
            ]
        except Exception as e:
            logger.warning(f"[MULTI-SEARCH] Local cache search failed: {e}")
            return []

    async def search_metron(
        self,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
        publisher_name: Optional[str] = None,
        upc: Optional[str] = None,
        isbn: Optional[str] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """
        Search Metron API with timeout handling.

        Search priority (exact identifiers first):
        1. UPC - exact barcode match
        2. ISBN - exact match
        3. Series name + filters
        """
        try:
            result = await asyncio.wait_for(
                metron_service.search_issues(
                    series_name=series_name,
                    number=number,
                    publisher_name=publisher_name,
                    upc=upc,
                    isbn=isbn,
                    page=page
                ),
                timeout=10.0
            )

            # Add source attribution
            if result and "results" in result:
                for item in result["results"]:
                    item["_source"] = "metron"

            return result
        except asyncio.TimeoutError:
            logger.warning("[MULTI-SEARCH] Metron timeout")
            return {"results": [], "error": "timeout"}
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "rate limit" in error_str:
                logger.warning("[MULTI-SEARCH] Metron rate limited")
                return {"results": [], "error": "rate_limited"}
            logger.warning(f"[MULTI-SEARCH] Metron error: {e}")
            return {"results": [], "error": str(e)}

    async def search_comicvine(
        self,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Search ComicVine API as fallback source.

        ComicVine has good coverage but lower rate limit (200/hour).
        """
        adapter = await self._get_comicvine_adapter()
        if adapter is None:
            return {"results": [], "error": "not_configured"}

        try:
            # Build search query
            query = series_name or ""
            if number:
                query = f"{query} {number}".strip()

            if not query:
                return {"results": [], "error": "no_query"}

            result = await asyncio.wait_for(
                adapter.search_issues(query, limit=10),
                timeout=15.0  # Increased from 8s - ComicVine can be slow
            )

            # Convert FetchResult to dict format
            if result.success:
                records = []
                for item in result.records:
                    records.append({
                        "id": f"cv_{item.get('id', '')}",
                        "issue": item.get("name") or f"{item.get('volume', {}).get('name', '')} #{item.get('issue_number', '')}",
                        "series": {"name": item.get("volume", {}).get("name", "")},
                        "number": item.get("issue_number", ""),
                        "image": item.get("image", {}).get("medium_url") if isinstance(item.get("image"), dict) else None,
                        "cover_date": item.get("cover_date"),
                        "_source": "comicvine",
                    })
                logger.info(f"[MULTI-SEARCH] ComicVine returned {len(records)} results")
                return {"results": records}
            else:
                error = result.errors[0].get("error", "unknown") if result.errors else "unknown"
                return {"results": [], "error": error}

        except asyncio.TimeoutError:
            logger.warning("[MULTI-SEARCH] ComicVine timeout")
            return {"results": [], "error": "timeout"}
        except Exception as e:
            error_str = str(e).lower()
            if "420" in error_str or "rate" in error_str:
                logger.warning("[MULTI-SEARCH] ComicVine rate limited")
                return {"results": [], "error": "rate_limited"}
            logger.warning(f"[MULTI-SEARCH] ComicVine error: {e}")
            return {"results": [], "error": str(e)}

    async def search_mycomicshop(
        self,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Search MyComicShop as final fallback (scraper - no API rate limits).

        This is a web scraper, so it's slower but doesn't have API quotas.
        """
        adapter = await self._get_mycomicshop_adapter()
        if adapter is None:
            return {"results": [], "error": "not_configured"}

        try:
            if not series_name:
                return {"results": [], "error": "no_query"}

            result = await asyncio.wait_for(
                adapter.search_issues(series_name, issue_number=number, limit=10),
                timeout=15.0  # Scrapers are slower
            )

            if result.success:
                records = []
                for item in result.records:
                    records.append({
                        "id": f"mcs_{item.get('id', '')}",
                        "issue": item.get("title", ""),
                        "series": {"name": item.get("series_name", series_name)},
                        "number": item.get("issue_number", number or ""),
                        "image": item.get("image"),
                        "cover_date": item.get("cover_date"),
                        "_source": "mycomicshop",
                    })
                logger.info(f"[MULTI-SEARCH] MyComicShop returned {len(records)} results")
                return {"results": records}
            else:
                error = result.errors[0].get("error", "unknown") if result.errors else "unknown"
                return {"results": [], "error": error}

        except asyncio.TimeoutError:
            logger.warning("[MULTI-SEARCH] MyComicShop timeout")
            return {"results": [], "error": "timeout"}
        except Exception as e:
            logger.warning(f"[MULTI-SEARCH] MyComicShop error: {e}")
            return {"results": [], "error": str(e)}

    async def search_fandom(
        self,
        series_name: str,
        number: Optional[str] = None,
        publisher_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search Fandom wikis with smart publisher-aware routing.

        Routing logic:
        1. If publisher explicitly provided, use that wiki only
        2. If series name matches known series (e.g., "spawn" -> Image), prioritize that wiki
        3. Otherwise, search all major wikis in parallel
        """
        results = []

        # Determine which wikis to search
        wiki_key = self._get_wiki_for_publisher(publisher_name)

        if wiki_key:
            # Publisher explicitly provided - search that wiki only
            wikis_to_search = [wiki_key]
            logger.debug(f"[FANDOM] Using explicit publisher wiki: {wiki_key}")
        else:
            # Try to infer publisher from series name
            inferred_publisher = self._infer_publisher_from_series(series_name)

            if inferred_publisher:
                # Series matched - prioritize that publisher's wiki, but also search others
                primary_wiki = PUBLISHER_WIKI_MAP.get(inferred_publisher)
                if primary_wiki:
                    # Search inferred wiki first (higher priority), then others
                    wikis_to_search = [primary_wiki]
                    logger.info(f"[FANDOM] Inferred '{series_name}' -> {inferred_publisher} -> {primary_wiki}")
                else:
                    wikis_to_search = ["dc_fandom", "marvel_fandom", "image_fandom"]
            else:
                # Publisher unknown - search major wikis in parallel
                wikis_to_search = ["dc_fandom", "marvel_fandom", "image_fandom"]
                logger.debug(f"[FANDOM] No publisher inferred for '{series_name}', searching all wikis")

        async def search_wiki(wiki_key: str) -> List[Dict[str, Any]]:
            try:
                adapter = self._get_fandom_adapter(wiki_key)
                if not adapter:
                    return []

                wiki_results = []

                # Strategy 1: If we have an issue number, try direct fetch first
                if number:
                    # Try fetching specific issue page (e.g., Spawn_Vol_1_1)
                    for vol in [1, 2, 3]:  # Try first 3 volumes
                        issue_data = await asyncio.wait_for(
                            adapter.fetch_issue_data(series_name, vol, number),
                            timeout=5.0
                        )
                        if issue_data and issue_data.get("page_title"):
                            title = issue_data["page_title"].replace("_", " ")
                            wiki_results.append({
                                "id": f"fandom_{wiki_key}_{title}",
                                "issue": f"{series_name} #{number}",
                                "series": {"name": series_name.title()},
                                "number": number,
                                "image": issue_data.get("image"),
                                "cover_date": issue_data.get("cover_date"),
                                "_source": wiki_key,
                                "_wiki_title": title,
                                "_description": issue_data.get("description"),
                            })
                            break  # Found it, stop trying volumes

                # Strategy 2: Search for issue pages with "Vol" in query
                if len(wiki_results) < 5:
                    # Search for issue pages specifically
                    query = f"{series_name} Vol 1"
                    if number:
                        query = f"{series_name} Vol 1 {number}"

                    search_results = await asyncio.wait_for(
                        adapter.search(query, limit=10),
                        timeout=8.0
                    )

                    # Collect items for parallel image fetching
                    items_to_fetch = []
                    for item in search_results[:5]:
                        title = item.get("title", "")
                        # Skip if we already have this result
                        if any(r.get("_wiki_title") == title for r in wiki_results):
                            continue
                        # Prefer titles that look like issue pages (contain Vol or #)
                        is_issue_page = "Vol" in title or "_" in title
                        # Extract issue number from title if present
                        # Match number at end after space, underscore, or "Vol X "
                        issue_match = re.search(r'(?:Vol\s*\d+[\s_]+)?(\d+)$', title)
                        extracted_number = issue_match.group(1) if issue_match else (number or "")

                        items_to_fetch.append({
                            "title": title,
                            "is_issue_page": is_issue_page,
                            "extracted_number": extracted_number,
                            "pageid": item.get("pageid", ""),
                        })

                    # Fetch images in parallel for first 3 items (with short timeout)
                    async def fetch_image_for_item(item_data):
                        try:
                            # Try to parse Vol and issue from title for fetch_issue_data
                            vol_match = re.search(r'Vol[\s_]*(\d+)', item_data["title"])
                            vol = int(vol_match.group(1)) if vol_match else 1
                            issue_num = item_data["extracted_number"]
                            if issue_num:
                                issue_data = await asyncio.wait_for(
                                    adapter.fetch_issue_data(series_name, vol, issue_num),
                                    timeout=3.0
                                )
                                return issue_data.get("image") if issue_data else None
                        except Exception:
                            pass
                        return None

                    # Parallel image fetch for top 3 items
                    image_tasks = [fetch_image_for_item(item) for item in items_to_fetch[:3]]
                    images = await asyncio.gather(*image_tasks, return_exceptions=True)

                    # Build results with fetched images
                    for idx, item_data in enumerate(items_to_fetch):
                        image_url = None
                        if idx < len(images) and not isinstance(images[idx], Exception):
                            image_url = images[idx]

                        issue_display = item_data["title"].replace("_", " ")
                        wiki_results.append({
                            "id": f"fandom_{wiki_key}_{item_data['pageid']}",
                            "issue": issue_display,
                            "series": {"name": series_name.title()},
                            "number": item_data["extracted_number"],
                            "image": image_url,
                            "cover_date": None,
                            "_source": wiki_key,
                            "_wiki_title": item_data["title"],
                            "_is_issue_page": item_data["is_issue_page"],
                        })

                return wiki_results[:5]
            except asyncio.TimeoutError:
                logger.debug(f"[MULTI-SEARCH] {wiki_key} timeout")
                return []
            except Exception as e:
                logger.debug(f"[MULTI-SEARCH] {wiki_key} error: {e}")
                return []

        # Search wikis in parallel
        tasks = [search_wiki(wiki) for wiki in wikis_to_search]
        wiki_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in wiki_results:
            if isinstance(result, list):
                results.extend(result)

        return results

    async def search_issues(
        self,
        db: AsyncSession,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
        publisher_name: Optional[str] = None,
        cover_year: Optional[int] = None,
        upc: Optional[str] = None,
        isbn: Optional[str] = None,
        page: int = 1,
        ip_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Multi-source comic search with automatic failover.

        Search priority (exact identifiers first):
        1. UPC - exact barcode match (fastest, most reliable)
        2. ISBN - exact match
        3. Series name + filters

        Search order:
        1. Local cache (always, fast)
        2. Metron API (primary)
        3. ComicVine API (if Metron fails/rate limited)
        4. Fandom wikis (if ComicVine also fails)
        5. MyComicShop scraper (final fallback - no API limits)

        Returns aggregated results with source attribution.
        """
        all_results = []
        sources_tried = []
        sources_failed = []
        message = None

        # 1. Always check local cache first
        cache_results = await self.search_local_cache(
            db=db,
            series_name=series_name,
            number=number,
            publisher_name=publisher_name,
            upc=upc,
            limit=20
        )

        if cache_results:
            all_results.extend(cache_results)
            sources_tried.append("local_cache")
            logger.info(f"[MULTI-SEARCH] Found {len(cache_results)} in local cache")

        # 2. Try Metron API (UPC/ISBN prioritized in adapter)
        metron_result = await self.search_metron(
            series_name=series_name,
            number=number,
            publisher_name=publisher_name,
            upc=upc,
            isbn=isbn,
            page=page
        )

        sources_tried.append("metron")

        metron_records = metron_result.get("results", [])
        metron_failed = metron_result.get("error") is not None
        metron_empty = len(metron_records) == 0

        if metron_failed:
            sources_failed.append(f"metron:{metron_result['error']}")

        # Add Metron results if any
        if metron_records:
            # Normalize Metron records to have top-level fields needed by frontend
            for rec in metron_records:
                if "series" in rec and isinstance(rec["series"], dict):
                    # Flatten volume if present in series but not top-level
                    if "volume" in rec["series"] and "volume" not in rec:
                        rec["volume"] = rec["series"]["volume"]

            all_results.extend(metron_records)
            logger.info(f"[MULTI-SEARCH] Found {len(metron_records)} from Metron")

        # Try fallbacks if Metron failed OR returned empty results
        if (metron_failed or metron_empty) and series_name:
            # 3. Try ComicVine as first fallback
            if not all_results:
                logger.info("[MULTI-SEARCH] Metron failed, trying ComicVine")
                cv_result = await self.search_comicvine(
                    series_name=series_name,
                    number=number
                )
                sources_tried.append("comicvine")

                if cv_result.get("error"):
                    sources_failed.append(f"comicvine:{cv_result['error']}")
                else:
                    cv_records = cv_result.get("results", [])
                    if cv_records:
                        all_results.extend(cv_records)
                        logger.info(f"[MULTI-SEARCH] Found {len(cv_records)} from ComicVine")

            # 4. Try Fandom wikis (if no results OR fewer than 5)
            if len(all_results) < 5 and series_name:
                logger.info(f"[MULTI-SEARCH] Trying Fandom wikis (have {len(all_results)} results)")
                fandom_results = await self.search_fandom(
                    series_name=series_name,
                    number=number,
                    publisher_name=publisher_name
                )

                if fandom_results:
                    all_results.extend(fandom_results)
                    sources_tried.append("fandom")
                    logger.info(f"[MULTI-SEARCH] Found {len(fandom_results)} from Fandom")

            # 5. FINAL FALLBACK: MyComicShop scraper (if still fewer than 5 results)
            if len(all_results) < 5 and series_name:
                logger.info(f"[MULTI-SEARCH] Trying MyComicShop scraper (have {len(all_results)} results)")
                mcs_result = await self.search_mycomicshop(
                    series_name=series_name,
                    number=number
                )
                sources_tried.append("mycomicshop")

                if mcs_result.get("error"):
                    sources_failed.append(f"mycomicshop:{mcs_result['error']}")
                else:
                    mcs_records = mcs_result.get("results", [])
                    if mcs_records:
                        all_results.extend(mcs_records)
                        logger.info(f"[MULTI-SEARCH] Found {len(mcs_records)} from MyComicShop")

            if metron_failed:
                if metron_result.get("error") == "rate_limited":
                    message = "Primary API rate limited. Showing results from alternative sources."
                elif metron_result.get("error") == "timeout":
                    message = "Primary API timed out. Showing results from alternative sources."

        # Deduplicate by issue identifier (prefer Metron results)
        seen_ids = set()
        unique_results = []

        # Sort by source priority
        source_priority = {"metron": 0, "comicvine": 1, "local_cache": 2, "fandom": 3, "mycomicshop": 4}
        all_results.sort(key=lambda x: source_priority.get(x.get("_source", ""), 5))

        for result in all_results:
            # Create a dedup key
            series_name_key = result.get("series", {}).get("name", "").lower() if isinstance(result.get("series"), dict) else ""
            issue_key = f"{series_name_key}:{result.get('number', '')}".lower()

            if issue_key not in seen_ids:
                seen_ids.add(issue_key)
                unique_results.append(result)

        # Build response
        response = {
            "results": unique_results[:20],  # Limit to 20
            "count": len(unique_results),
            "next": None,
            "previous": None,
            "_sources_tried": sources_tried,
        }

        if sources_failed:
            response["_sources_failed"] = sources_failed

        if message:
            response["message"] = message

        return response

    async def close(self):
        """Close all Fandom adapters."""
        for adapter in self._fandom_adapters.values():
            await adapter.close()
        self._fandom_adapters.clear()


# Global instance
multi_source_search = MultiSourceSearchService()
