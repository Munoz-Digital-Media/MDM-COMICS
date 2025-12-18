"""
Multi-Source Comic Search Service v1.0.0

Provides resilient comic search with automatic failover across multiple sources.
Per constitution_cyberSec.json: No single point of failure.

Search Priority:
1. Local cache (GCD data) - always checked first
2. Metron API - primary external source
3. ComicVine API - secondary API fallback
4. Fandom wikis - publisher-aware routing (DC, Marvel, Image)
5. MyComicShop - scraper fallback

Features:
- Automatic failover on source failure/rate limit
- Publisher-aware routing to relevant Fandom wikis
- Result aggregation from multiple sources
- Source attribution in results
"""
import asyncio
import logging
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
        page: int = 1
    ) -> Dict[str, Any]:
        """Search Metron API with timeout handling."""
        try:
            result = await asyncio.wait_for(
                metron_service.search_issues(
                    series_name=series_name,
                    number=number,
                    publisher_name=publisher_name,
                    upc=upc,
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
        page: int = 1,
        ip_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Multi-source comic search with automatic failover.

        Search order:
        1. Local cache (always, fast)
        2. Metron API (primary)
        3. Fandom wikis (if Metron fails/rate limited)

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

        # 2. Try Metron API
        metron_result = await self.search_metron(
            series_name=series_name,
            number=number,
            publisher_name=publisher_name,
            upc=upc,
            page=page
        )

        sources_tried.append("metron")

        if metron_result.get("error"):
            sources_failed.append(f"metron:{metron_result['error']}")

            # 3. Metron failed - try Fandom wikis as fallback
            if series_name:
                logger.info("[MULTI-SEARCH] Metron failed, trying Fandom wikis")
                fandom_results = await self.search_fandom(
                    series_name=series_name,
                    number=number,
                    publisher_name=publisher_name
                )

                if fandom_results:
                    all_results.extend(fandom_results)
                    sources_tried.append("fandom")
                    logger.info(f"[MULTI-SEARCH] Found {len(fandom_results)} from Fandom")

                if metron_result["error"] == "rate_limited":
                    message = "Primary API rate limited. Showing results from alternative sources."
                elif metron_result["error"] == "timeout":
                    message = "Primary API timed out. Showing results from alternative sources."
        else:
            # Metron succeeded
            metron_issues = metron_result.get("results", [])
            all_results.extend(metron_issues)
            logger.info(f"[MULTI-SEARCH] Found {len(metron_issues)} from Metron")

        # Deduplicate by issue identifier (prefer Metron results)
        seen_ids = set()
        unique_results = []

        # Sort by source priority (metron first, then cache, then fandom)
        source_priority = {"metron": 0, "local_cache": 1}
        all_results.sort(key=lambda x: source_priority.get(x.get("_source", ""), 2))

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
