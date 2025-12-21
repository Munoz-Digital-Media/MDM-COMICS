"""
Multi-Source Comic Search Service v1.1.0

Provides resilient comic search with automatic failover across multiple sources.
Per constitution_cyberSec.json: No single point of failure.

v1.1.0: Added ComicVine fallback, fixed error propagation for rate limits

Search Priority:
1. Local cache (GCD data) - always checked first
2. Metron API - primary external source
3. ComicVine API - secondary API fallback
4. Fandom wikis - publisher-aware routing (DC, Marvel, Image)

Features:
- Automatic failover on source failure/rate limit
- Publisher-aware routing to relevant Fandom wikis
- Result aggregation from multiple sources
- Source attribution in results
"""
import asyncio
import logging
import os
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

            config = AdapterConfig(
                name="comicvine",
                source_type=DataSourceType.API,
                base_url="https://comicvine.gamespot.com/api",
                rate_limit=RateLimitConfig(requests_per_minute=3),  # 200/hour = 3.3/min
                retry=RetryConfig(max_retries=2, base_delay=1.0),
            )
            self._comicvine_client = ResilientHTTPClient(config.rate_limit, config.retry)
            self._comicvine_adapter = ComicVineAdapter(config, self._comicvine_client, api_key)

        return self._comicvine_adapter

    async def _get_mycomicshop_adapter(self) -> Optional[MyComicShopAdapter]:
        """Get or create MyComicShop adapter with HTTP client."""
        if self._mycomicshop_adapter is None:
            config = AdapterConfig(
                name="mycomicshop",
                source_type=DataSourceType.SCRAPER,
                base_url="https://www.mycomicshop.com",
                rate_limit=RateLimitConfig(requests_per_minute=10),  # Conservative for scraping
                retry=RetryConfig(max_retries=2, base_delay=2.0),
            )
            self._mycomicshop_client = ResilientHTTPClient(config.rate_limit, config.retry)
            self._mycomicshop_adapter = MyComicShopAdapter(config, self._mycomicshop_client)

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
                timeout=8.0
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
        Search Fandom wikis with publisher-aware routing.
        If publisher known, search that wiki. Otherwise, search major wikis in parallel.
        """
        results = []

        # Determine which wikis to search
        wiki_key = self._get_wiki_for_publisher(publisher_name)

        if wiki_key:
            # Publisher known - search specific wiki
            wikis_to_search = [wiki_key]
        else:
            # Publisher unknown - search major wikis in parallel
            wikis_to_search = ["dc_fandom", "marvel_fandom", "image_fandom"]

        async def search_wiki(wiki_key: str) -> List[Dict[str, Any]]:
            try:
                adapter = self._get_fandom_adapter(wiki_key)
                if not adapter:
                    return []

                query = series_name
                if number:
                    query += f" {number}"

                search_results = await asyncio.wait_for(
                    adapter.search(query, limit=10),
                    timeout=8.0
                )

                wiki_results = []
                for item in search_results[:5]:  # Limit per wiki
                    title = item.get("title", "")
                    wiki_results.append({
                        "id": f"fandom_{wiki_key}_{item.get('pageid', '')}",
                        "issue": title,
                        "series": {"name": series_name},
                        "number": number or "",
                        "image": None,  # Would need additional fetch
                        "cover_date": None,
                        "_source": wiki_key,
                        "_wiki_title": title,
                    })

                return wiki_results
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

            # 4. Still no results? Try Fandom wikis
            if not all_results and series_name:
                logger.info("[MULTI-SEARCH] Trying Fandom wikis")
                fandom_results = await self.search_fandom(
                    series_name=series_name,
                    number=number,
                    publisher_name=publisher_name
                )

                if fandom_results:
                    all_results.extend(fandom_results)
                    sources_tried.append("fandom")
                    logger.info(f"[MULTI-SEARCH] Found {len(fandom_results)} from Fandom")

            # 5. FINAL FALLBACK: MyComicShop scraper (no API rate limits)
            if not all_results and series_name:
                logger.info("[MULTI-SEARCH] All APIs failed, trying MyComicShop scraper")
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
