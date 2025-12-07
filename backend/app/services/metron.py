"""
Metron Comic Database API Service
https://metron.cloud/

v1.1.0: Refactored to use ResilientHTTPClient for retry logic,
        exponential backoff, and rate limiting per pipeline spec.
"""
import logging
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

from ..core.config import settings
from ..core.http_client import get_metron_client, ResilientHTTPClient

logger = logging.getLogger(__name__)


class MetronService:
    """
    Service for interacting with the Metron comic database API.
    
    Uses ResilientHTTPClient for:
    - Automatic retries with exponential backoff
    - Rate limiting to prevent bans
    - Circuit breaker for repeated failures
    - Jitter to prevent thundering herd
    """

    def __init__(self):
        self.base_url = settings.METRON_API_BASE
        self._username = settings.METRON_USERNAME
        self._password = settings.METRON_PASSWORD
        self._client: Optional[ResilientHTTPClient] = None

    @asynccontextmanager
    async def _get_client(self):
        """Get or create a resilient HTTP client."""
        if self._client is None:
            self._client = get_metron_client()
            # Enter the context manager
            await self._client.__aenter__()
            
        yield self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.__aexit__(None, None, None)
            self._client = None

    async def _request(
        self, 
        endpoint: str, 
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Make authenticated request to Metron API.
        
        All retry logic, rate limiting, and error handling is delegated
        to ResilientHTTPClient. This method just handles Metron-specific
        auth and response parsing.
        """
        async with self._get_client() as client:
            url = f"{self.base_url}/{endpoint}/"
            
            # Metron uses Basic Auth
            auth_header = {
                "Authorization": self._build_basic_auth()
            }
            
            logger.debug(f"[METRON] Requesting {endpoint} with params: {params}")
            
            response = await client.get(
                url,
                params=params,
                headers=auth_header,
            )
            
            # ResilientHTTPClient already handles retries and raises on fatal errors
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"[METRON] Got response with {len(data.get('results', []))} results")
            
            return data
            
    def _build_basic_auth(self) -> str:
        """Build Basic Auth header value."""
        import base64
        credentials = f"{self._username}:{self._password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    async def search_issues(
        self,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
        publisher_name: Optional[str] = None,
        cover_year: Optional[int] = None,
        upc: Optional[str] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """
        Search for comic issues.

        Args:
            series_name: Name of the series (e.g., "amazing spider-man")
            number: Issue number (e.g., "300")
            publisher_name: Publisher name (e.g., "marvel")
            cover_year: Year of cover date
            upc: UPC barcode number
            page: Page number for pagination
        """
        params = {"page": page}
        if series_name:
            params["series_name"] = series_name
        if number:
            params["number"] = number
        if publisher_name:
            params["publisher_name"] = publisher_name
        if cover_year:
            params["cover_year"] = cover_year
        if upc:
            params["upc"] = upc

        return await self._request("issue", params)

    async def get_issue(self, issue_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific issue."""
        async with self._get_client() as client:
            url = f"{self.base_url}/issue/{issue_id}/"
            
            response = await client.get(
                url,
                headers={"Authorization": self._build_basic_auth()},
            )
            response.raise_for_status()
            return response.json()

    async def search_series(
        self,
        name: Optional[str] = None,
        publisher_name: Optional[str] = None,
        year_began: Optional[int] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """
        Search for comic series.

        Args:
            name: Series name
            publisher_name: Publisher name
            year_began: Year the series started
            page: Page number for pagination
        """
        params = {"page": page}
        if name:
            params["name"] = name
        if publisher_name:
            params["publisher_name"] = publisher_name
        if year_began:
            params["year_began"] = year_began

        return await self._request("series", params)

    async def get_series(self, series_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific series."""
        async with self._get_client() as client:
            url = f"{self.base_url}/series/{series_id}/"
            
            response = await client.get(
                url,
                headers={"Authorization": self._build_basic_auth()},
            )
            response.raise_for_status()
            return response.json()

    async def get_publishers(self, page: int = 1) -> Dict[str, Any]:
        """Get list of publishers."""
        return await self._request("publisher", {"page": page})

    async def search_characters(
        self,
        name: Optional[str] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """Search for characters."""
        params = {"page": page}
        if name:
            params["name"] = name
        return await self._request("character", params)

    async def search_creators(
        self,
        name: Optional[str] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """Search for creators (writers, artists, etc.)."""
        params = {"page": page}
        if name:
            params["name"] = name
        return await self._request("creator", params)

    async def fetch_all_pages(
        self,
        method: str,
        max_pages: int = 100,
        **kwargs
    ) -> list:
        """
        Fetch all pages of results from a paginated endpoint.
        
        Respects rate limits automatically via ResilientHTTPClient.
        
        Args:
            method: Method name to call (e.g., 'search_issues')
            max_pages: Maximum pages to fetch (safety limit)
            **kwargs: Arguments to pass to the method
            
        Returns:
            List of all results combined
        """
        all_results = []
        page = 1
        
        while page <= max_pages:
            logger.info(f"[METRON] Fetching page {page}...")
            
            # Call the appropriate method
            func = getattr(self, method)
            response = await func(page=page, **kwargs)
            
            results = response.get("results", [])
            all_results.extend(results)
            
            # Check if more pages exist
            next_url = response.get("next")
            if not next_url or not results:
                logger.info(f"[METRON] Completed after {page} pages, {len(all_results)} total results")
                break
                
            page += 1
            
        return all_results


# Singleton instance
metron_service = MetronService()
