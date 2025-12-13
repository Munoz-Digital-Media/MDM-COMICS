"""
Robots.txt Compliance Checker v1.10.0

Per constitution_cyberSec.json: Mandatory robots.txt compliance for scraping.

Features:
- Fetches and parses robots.txt for any domain
- Caches directives for 24 hours
- Respects Crawl-delay if specified
- Honors Disallow paths
- Identifies as MDMComicsBot/1.0

Usage:
    checker = RobotsTxtChecker()

    # Check if URL is allowed
    can_fetch = await checker.can_fetch("https://comicbookrealm.com/some/path")

    # Get crawl delay
    delay = await checker.get_crawl_delay("https://comicbookrealm.com")
"""
import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx

logger = logging.getLogger(__name__)

# Our bot user agent
USER_AGENT = "MDMComicsBot/1.0 (+https://mdmcomics.com/bot)"


@dataclass
class RobotsDirectives:
    """Parsed robots.txt directives for a domain."""
    domain: str
    allow_patterns: List[str] = field(default_factory=list)
    disallow_patterns: List[str] = field(default_factory=list)
    crawl_delay: Optional[float] = None
    sitemap_urls: List[str] = field(default_factory=list)
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    raw_content: str = ""
    is_allowed_default: bool = True  # Default if no matching rules


@dataclass
class CacheEntry:
    """Cache entry for robots.txt directives."""
    directives: RobotsDirectives
    expires_at: datetime


class RobotsTxtChecker:
    """
    Checks robots.txt compliance before scraping.

    Caches parsed directives to avoid repeated fetches.
    """

    def __init__(
        self,
        cache_ttl_hours: float = 24.0,
        request_timeout: float = 10.0,
        user_agent: str = USER_AGENT,
    ):
        self.cache_ttl = timedelta(hours=cache_ttl_hours)
        self.request_timeout = request_timeout
        self.user_agent = user_agent
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _get_robots_url(self, url: str) -> str:
        """Get robots.txt URL for a given URL."""
        domain = self._get_domain(url)
        return f"{domain}/robots.txt"

    def _get_path(self, url: str) -> str:
        """Extract path from URL."""
        parsed = urlparse(url)
        return parsed.path or "/"

    async def _fetch_robots_txt(self, domain: str) -> Optional[str]:
        """Fetch robots.txt content from a domain."""
        robots_url = f"{domain}/robots.txt"

        try:
            async with httpx.AsyncClient(timeout=self.request_timeout) as client:
                response = await client.get(
                    robots_url,
                    headers={"User-Agent": self.user_agent},
                    follow_redirects=True,
                )

                if response.status_code == 200:
                    return response.text
                elif response.status_code == 404:
                    # No robots.txt = everything allowed
                    logger.info(f"[ROBOTS] No robots.txt found for {domain} (404)")
                    return ""
                else:
                    logger.warning(
                        f"[ROBOTS] Unexpected status {response.status_code} for {robots_url}"
                    )
                    return None

        except httpx.TimeoutException:
            logger.warning(f"[ROBOTS] Timeout fetching {robots_url}")
            return None
        except httpx.RequestError as e:
            logger.warning(f"[ROBOTS] Error fetching {robots_url}: {e}")
            return None

    def _parse_robots_txt(self, domain: str, content: str) -> RobotsDirectives:
        """Parse robots.txt content into directives."""
        directives = RobotsDirectives(domain=domain, raw_content=content)

        if not content:
            return directives

        # Track if we're in a section that applies to us
        in_our_section = False
        in_wildcard_section = False

        our_user_agent = self.user_agent.split("/")[0].lower()  # "mdmcomicsbot"

        for line in content.split("\n"):
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue

            # Parse directive
            if ":" not in line:
                continue

            key, value = line.split(":", 1)
            key = key.strip().lower()
            value = value.strip()

            if key == "user-agent":
                ua = value.lower()
                if ua == "*":
                    in_wildcard_section = True
                    in_our_section = False
                elif our_user_agent in ua or ua in our_user_agent:
                    in_our_section = True
                    in_wildcard_section = False
                else:
                    in_our_section = False
                    in_wildcard_section = False

            elif in_our_section or in_wildcard_section:
                if key == "disallow":
                    if value:
                        directives.disallow_patterns.append(value)
                elif key == "allow":
                    if value:
                        directives.allow_patterns.append(value)
                elif key == "crawl-delay":
                    try:
                        directives.crawl_delay = float(value)
                    except ValueError:
                        pass
                elif key == "sitemap":
                    directives.sitemap_urls.append(value)

        # Specific rules for our bot override wildcard rules
        if in_our_section:
            logger.info(f"[ROBOTS] Found specific rules for {our_user_agent} in {domain}")

        return directives

    async def get_directives(self, url: str) -> RobotsDirectives:
        """
        Get robots.txt directives for a URL's domain.

        Fetches and caches the directives.
        """
        domain = self._get_domain(url)

        async with self._lock:
            # Check cache
            if domain in self._cache:
                entry = self._cache[domain]
                if datetime.now(timezone.utc) < entry.expires_at:
                    return entry.directives

            # Fetch and parse
            content = await self._fetch_robots_txt(domain)

            if content is None:
                # Fetch failed - be conservative, assume disallowed
                directives = RobotsDirectives(
                    domain=domain,
                    is_allowed_default=False,  # Conservative: deny on error
                )
            else:
                directives = self._parse_robots_txt(domain, content)

            # Cache
            self._cache[domain] = CacheEntry(
                directives=directives,
                expires_at=datetime.now(timezone.utc) + self.cache_ttl,
            )

            return directives

    def _matches_pattern(self, path: str, pattern: str) -> bool:
        """Check if a path matches a robots.txt pattern."""
        if not pattern:
            return False

        # Convert robots.txt pattern to regex
        # * matches any sequence of characters
        # $ matches end of URL
        regex_pattern = ""
        for char in pattern:
            if char == "*":
                regex_pattern += ".*"
            elif char == "$":
                regex_pattern += "$"
            else:
                regex_pattern += re.escape(char)

        try:
            return bool(re.match(regex_pattern, path))
        except re.error:
            return False

    async def can_fetch(self, url: str) -> bool:
        """
        Check if the URL is allowed by robots.txt.

        Returns:
            True if allowed, False if disallowed.
        """
        directives = await self.get_directives(url)
        path = self._get_path(url)

        # Check Allow rules first (they take precedence for matching paths)
        for pattern in directives.allow_patterns:
            if self._matches_pattern(path, pattern):
                logger.debug(f"[ROBOTS] {url} allowed by pattern: {pattern}")
                return True

        # Check Disallow rules
        for pattern in directives.disallow_patterns:
            if self._matches_pattern(path, pattern):
                logger.debug(f"[ROBOTS] {url} disallowed by pattern: {pattern}")
                return False

        # No matching rules - use default
        return directives.is_allowed_default

    async def get_crawl_delay(self, url: str) -> float:
        """
        Get the crawl delay for a domain.

        Returns:
            Delay in seconds, or 0.0 if not specified.
        """
        directives = await self.get_directives(url)
        return directives.crawl_delay or 0.0

    async def check_and_wait(
        self,
        url: str,
        min_delay: float = 1.0
    ) -> Tuple[bool, float]:
        """
        Check if URL is allowed and return appropriate delay.

        Returns:
            Tuple of (is_allowed, delay_seconds)
        """
        directives = await self.get_directives(url)
        is_allowed = await self.can_fetch(url)

        # Use the greater of crawl-delay or min_delay
        delay = max(directives.crawl_delay or 0.0, min_delay)

        return is_allowed, delay

    def clear_cache(self) -> None:
        """Clear the directives cache."""
        self._cache.clear()
        logger.info("[ROBOTS] Cache cleared")

    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        now = datetime.now(timezone.utc)
        valid = sum(1 for e in self._cache.values() if now < e.expires_at)
        expired = len(self._cache) - valid

        return {
            "total_entries": len(self._cache),
            "valid_entries": valid,
            "expired_entries": expired,
        }


# Global instance
robots_checker = RobotsTxtChecker()
