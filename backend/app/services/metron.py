"""
Metron Comic Database API Service
https://metron.cloud/
"""
import httpx
from typing import Optional, List, Dict, Any
from ..core.config import settings


class MetronService:
    """Service for interacting with the Metron comic database API."""

    def __init__(self):
        self.base_url = settings.METRON_API_BASE
        self.auth = (settings.METRON_USERNAME, settings.METRON_PASSWORD)

    async def _request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated request to Metron API."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/{endpoint}/",
                auth=self.auth,
                params=params,
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()

    async def search_issues(
        self,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
        publisher_name: Optional[str] = None,
        cover_year: Optional[int] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """
        Search for comic issues.

        Args:
            series_name: Name of the series (e.g., "amazing spider-man")
            number: Issue number (e.g., "300")
            publisher_name: Publisher name (e.g., "marvel")
            cover_year: Year of cover date
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

        return await self._request("issue", params)

    async def get_issue(self, issue_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific issue."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/issue/{issue_id}/",
                auth=self.auth,
                timeout=30.0
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
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/series/{series_id}/",
                auth=self.auth,
                timeout=30.0
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


# Singleton instance
metron_service = MetronService()
