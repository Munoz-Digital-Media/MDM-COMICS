"""
Grading Tool Scraper Adapter v1.10.0

Web scraper for comic grading reference sites - AI training data harvester.

Per constitution_cyberSec.json: Mandatory robots.txt compliance.
Per constitution_data_hygiene.json: No PII storage.

Purpose:
This adapter scrapes grading reference materials to build training data
for the AI grading prediction model. It collects:
- Grade examples with detailed defect descriptions
- Photo references of various conditions
- CGC census population data
- Grade definitions and criteria

Usage:
    adapter = GradingToolAdapter(config, client)
    examples = await adapter.fetch_grading_examples("spine roll")
"""
import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urljoin

from bs4 import BeautifulSoup

from app.core.adapter_registry import (
    DataSourceAdapter, AdapterConfig, FetchResult, DataSourceType
)
from app.core.http_client import ResilientHTTPClient, RateLimitConfig, RetryConfig
from app.adapters.robots_checker import robots_checker, USER_AGENT

logger = logging.getLogger(__name__)

# Grading reference site URLs
GRADING_TOOL_BASE = "https://www.comicbookgradingtool.com"


# Standard CGC grading scale with definitions
CGC_GRADES = {
    10.0: {"label": "GM", "name": "Gem Mint", "description": "Perfect in every way"},
    9.9: {"label": "MT", "name": "Mint", "description": "Nearly perfect with only minuscule defects"},
    9.8: {"label": "NM/MT", "name": "Near Mint/Mint", "description": "Nearly perfect with minor imperfection"},
    9.6: {"label": "NM+", "name": "Near Mint+", "description": "Nearly perfect with one or two small defects"},
    9.4: {"label": "NM", "name": "Near Mint", "description": "Nearly perfect with a few minor defects"},
    9.2: {"label": "NM-", "name": "Near Mint-", "description": "Nearly perfect but not quite"},
    9.0: {"label": "VF/NM", "name": "Very Fine/Near Mint", "description": "Outstanding with minor wear"},
    8.5: {"label": "VF+", "name": "Very Fine+", "description": "Excellent with light wear"},
    8.0: {"label": "VF", "name": "Very Fine", "description": "Excellent but shows signs of wear"},
    7.5: {"label": "VF-", "name": "Very Fine-", "description": "Above average with moderate wear"},
    7.0: {"label": "FN/VF", "name": "Fine/Very Fine", "description": "Above average with noticeable wear"},
    6.5: {"label": "FN+", "name": "Fine+", "description": "Above average with moderate defects"},
    6.0: {"label": "FN", "name": "Fine", "description": "Above average, shows moderate wear"},
    5.5: {"label": "FN-", "name": "Fine-", "description": "Slightly above average"},
    5.0: {"label": "VG/FN", "name": "Very Good/Fine", "description": "Average with noticeable wear"},
    4.5: {"label": "VG+", "name": "Very Good+", "description": "Average with multiple defects"},
    4.0: {"label": "VG", "name": "Very Good", "description": "Average, shows significant wear"},
    3.5: {"label": "VG-", "name": "Very Good-", "description": "Below average"},
    3.0: {"label": "GD/VG", "name": "Good/Very Good", "description": "Below average with heavy wear"},
    2.5: {"label": "GD+", "name": "Good+", "description": "Below average, well-read"},
    2.0: {"label": "GD", "name": "Good", "description": "Shows heavy wear but complete"},
    1.8: {"label": "GD-", "name": "Good-", "description": "Heavily worn"},
    1.5: {"label": "FR/GD", "name": "Fair/Good", "description": "Very heavily worn"},
    1.0: {"label": "FR", "name": "Fair", "description": "Heavily worn, soiled, or damaged"},
    0.5: {"label": "PR", "name": "Poor", "description": "Heavily damaged but complete"},
}

# Common defect types and their severity weights
DEFECT_CATALOG = {
    # Cover defects (most impactful)
    "missing cover": {"severity": 0.9, "category": "cover", "grade_impact": -4.0},
    "detached cover": {"severity": 0.7, "category": "cover", "grade_impact": -3.0},
    "loose cover": {"severity": 0.5, "category": "cover", "grade_impact": -2.0},
    "cover tear": {"severity": 0.6, "category": "cover", "grade_impact": -2.5},
    "cover crease": {"severity": 0.4, "category": "cover", "grade_impact": -1.5},
    "color break": {"severity": 0.3, "category": "cover", "grade_impact": -1.0},
    "cover scuff": {"severity": 0.2, "category": "cover", "grade_impact": -0.5},

    # Spine defects
    "spine roll": {"severity": 0.5, "category": "spine", "grade_impact": -2.0},
    "spine stress": {"severity": 0.3, "category": "spine", "grade_impact": -1.0},
    "spine tear": {"severity": 0.5, "category": "spine", "grade_impact": -2.0},
    "spine split": {"severity": 0.6, "category": "spine", "grade_impact": -2.5},
    "spine creases": {"severity": 0.3, "category": "spine", "grade_impact": -1.0},

    # Staple defects
    "rusty staples": {"severity": 0.4, "category": "staple", "grade_impact": -1.5},
    "missing staple": {"severity": 0.5, "category": "staple", "grade_impact": -2.0},
    "loose staple": {"severity": 0.3, "category": "staple", "grade_impact": -1.0},
    "popped staple": {"severity": 0.4, "category": "staple", "grade_impact": -1.5},

    # Page defects
    "page tear": {"severity": 0.4, "category": "pages", "grade_impact": -1.5},
    "missing page": {"severity": 0.9, "category": "pages", "grade_impact": -4.0},
    "loose page": {"severity": 0.3, "category": "pages", "grade_impact": -1.0},
    "browning": {"severity": 0.3, "category": "pages", "grade_impact": -1.0},
    "brittleness": {"severity": 0.5, "category": "pages", "grade_impact": -2.0},
    "foxing": {"severity": 0.3, "category": "pages", "grade_impact": -1.0},
    "tanning": {"severity": 0.2, "category": "pages", "grade_impact": -0.5},

    # Environmental damage
    "water damage": {"severity": 0.6, "category": "environmental", "grade_impact": -2.5},
    "water stain": {"severity": 0.4, "category": "environmental", "grade_impact": -1.5},
    "sun fading": {"severity": 0.3, "category": "environmental", "grade_impact": -1.0},
    "mildew": {"severity": 0.5, "category": "environmental", "grade_impact": -2.0},
    "smoke damage": {"severity": 0.4, "category": "environmental", "grade_impact": -1.5},

    # User damage
    "writing": {"severity": 0.4, "category": "user", "grade_impact": -1.5},
    "stamp": {"severity": 0.3, "category": "user", "grade_impact": -1.0},
    "tape": {"severity": 0.4, "category": "user", "grade_impact": -1.5},
    "tape residue": {"severity": 0.3, "category": "user", "grade_impact": -1.0},
    "cut coupon": {"severity": 0.7, "category": "user", "grade_impact": -3.0},
    "subscription crease": {"severity": 0.4, "category": "user", "grade_impact": -1.5},

    # Manufacturing defects
    "miscut": {"severity": 0.2, "category": "manufacturing", "grade_impact": -0.5},
    "miswrap": {"severity": 0.2, "category": "manufacturing", "grade_impact": -0.5},
    "bindery defect": {"severity": 0.2, "category": "manufacturing", "grade_impact": -0.5},

    # Corner/edge defects
    "corner chip": {"severity": 0.3, "category": "corner", "grade_impact": -1.0},
    "corner crease": {"severity": 0.3, "category": "corner", "grade_impact": -1.0},
    "edge chip": {"severity": 0.3, "category": "corner", "grade_impact": -1.0},
    "edge tear": {"severity": 0.4, "category": "corner", "grade_impact": -1.5},
}


class GradingToolAdapter(DataSourceAdapter):
    """
    Scraper adapter for grading reference materials.

    Provides:
    - Grading examples with defect descriptions
    - Grade definitions and criteria
    - Defect severity information
    - Training data for AI grading model

    Note: This is a scraper and must respect robots.txt and rate limits.
    """

    def __init__(
        self,
        config: AdapterConfig,
        client: ResilientHTTPClient,
    ):
        super().__init__(config, client)
        self.base_url = GRADING_TOOL_BASE
        self._last_request_time = 0.0
        self._min_delay = 10.0  # Very conservative: 10 seconds between requests

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
                logger.warning(f"[GRADING] URL disallowed by robots.txt: {url}")
            return can_fetch
        except Exception as e:
            logger.error(f"[GRADING] Robots check failed: {e}")
            return False

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
                logger.warning(f"[GRADING] HTTP {response.status_code} for {url}")
                return None
        except Exception as e:
            logger.error(f"[GRADING] Fetch failed for {url}: {e}")
            return None

    async def health_check(self) -> bool:
        """Check if grading reference site is reachable."""
        try:
            can_fetch = await robots_checker.can_fetch(self.base_url)
            if not can_fetch:
                return False

            response = await self.client.get(self.base_url)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"[GRADING] Health check failed: {e}")
            return False

    async def fetch_page(
        self,
        page: int = 1,
        page_size: int = 100,
        cursor: Optional[str] = None,
        **filters
    ) -> FetchResult:
        """Not supported - use fetch_grading_examples instead."""
        return FetchResult(
            success=False,
            errors=[{"message": "Use fetch_grading_examples() instead"}],
        )

    async def fetch_by_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a specific grading guide page.

        Args:
            external_id: The page ID or URL path
        """
        if external_id.startswith("http"):
            url = external_id
        else:
            url = f"{self.base_url}/{external_id}"

        html = await self._fetch_page(url)
        if not html:
            return None

        return self._parse_grading_page(html, url)

    async def fetch_grading_examples(
        self,
        defect_type: Optional[str] = None,
        grade_range: Optional[tuple] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Fetch grading examples for AI training.

        Args:
            defect_type: Filter by defect type (e.g., "spine roll")
            grade_range: Filter by grade range (e.g., (6.0, 8.0))
            limit: Maximum examples to return

        Returns:
            List of grading examples with defects and grades
        """
        examples = []

        # Start with built-in defect catalog
        if defect_type:
            defect_info = DEFECT_CATALOG.get(defect_type.lower())
            if defect_info:
                examples.append({
                    "defects": [defect_type],
                    "defect_severity": {defect_type: defect_info["severity"]},
                    "grade_impact": defect_info["grade_impact"],
                    "category": defect_info["category"],
                    "source": "catalog",
                })

        # Try to scrape grading reference pages
        search_url = f"{self.base_url}/grading-guide"
        html = await self._fetch_page(search_url)

        if html:
            scraped_examples = self._parse_grading_examples(html, defect_type)
            examples.extend(scraped_examples[:limit - len(examples)])

        # Generate synthetic examples based on grade definitions
        if len(examples) < limit:
            synthetic = self._generate_synthetic_examples(
                defect_type, grade_range, limit - len(examples)
            )
            examples.extend(synthetic)

        return examples[:limit]

    def _parse_grading_page(self, html: str, url: str) -> Dict[str, Any]:
        """Parse a grading guide page."""
        soup = BeautifulSoup(html, 'html.parser')

        data = {
            "url": url,
            "_source": "gradingtool",
        }

        # Extract title
        title_elem = soup.select_one('h1, .title')
        if title_elem:
            data["title"] = title_elem.get_text(strip=True)

        # Extract grade definitions
        grade_sections = soup.select('.grade-section, .grade-definition, [class*="grade"]')
        definitions = []
        for section in grade_sections:
            definition = self._parse_grade_definition(section)
            if definition:
                definitions.append(definition)

        if definitions:
            data["grade_definitions"] = definitions

        # Extract defect examples
        defect_sections = soup.select('.defect, .example, [class*="defect"]')
        examples = []
        for section in defect_sections:
            example = self._parse_defect_example(section)
            if example:
                examples.append(example)

        if examples:
            data["defect_examples"] = examples

        # Extract images
        images = soup.select('img[src*="grade"], img[src*="defect"], img.example')
        image_urls = []
        for img in images:
            src = img.get('src') or img.get('data-src')
            if src:
                if not src.startswith('http'):
                    src = urljoin(self.base_url, src)
                image_urls.append({
                    "url": src,
                    "alt": img.get('alt', ''),
                })

        if image_urls:
            data["images"] = image_urls

        return data

    def _parse_grade_definition(self, section) -> Optional[Dict[str, Any]]:
        """Parse a grade definition section."""
        text = section.get_text(strip=True)
        if not text:
            return None

        # Look for grade number
        grade_match = re.search(r'(\d+\.?\d*)', text)
        if not grade_match:
            return None

        grade_num = float(grade_match.group(1))

        # Get label from CGC_GRADES if available
        grade_info = CGC_GRADES.get(grade_num, {})

        return {
            "grade_numeric": grade_num,
            "grade_label": grade_info.get("label", ""),
            "grade_name": grade_info.get("name", ""),
            "description": text[:500],
        }

    def _parse_defect_example(self, section) -> Optional[Dict[str, Any]]:
        """Parse a defect example section."""
        text = section.get_text(strip=True).lower()
        if not text:
            return None

        # Find matching defects
        found_defects = []
        for defect in DEFECT_CATALOG:
            if defect in text:
                found_defects.append(defect)

        if not found_defects:
            return None

        # Calculate severity
        severities = {}
        total_impact = 0.0
        for defect in found_defects:
            info = DEFECT_CATALOG[defect]
            severities[defect] = info["severity"]
            total_impact += info["grade_impact"]

        # Estimate grade based on defects
        estimated_grade = max(0.5, 10.0 + total_impact)

        return {
            "defects": found_defects,
            "defect_severity": severities,
            "estimated_grade": round(estimated_grade, 1),
            "raw_description": text[:500],
            "source": "gradingtool",
        }

    def _parse_grading_examples(
        self,
        html: str,
        defect_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Parse grading examples from HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        examples = []

        # Look for example sections
        sections = soup.select(
            '.example, .grade-example, [class*="example"], '
            '.defect-example, tr.grade'
        )

        for section in sections:
            example = self._parse_defect_example(section)
            if example:
                # Apply defect filter if specified
                if defect_filter:
                    if defect_filter.lower() not in str(example.get("defects", [])).lower():
                        continue
                examples.append(example)

        return examples

    def _generate_synthetic_examples(
        self,
        defect_type: Optional[str],
        grade_range: Optional[tuple],
        count: int
    ) -> List[Dict[str, Any]]:
        """
        Generate synthetic training examples based on defect catalog.

        These examples encode known relationships between defects and grades.
        """
        import random
        examples = []

        # Get defects to use
        if defect_type and defect_type.lower() in DEFECT_CATALOG:
            defects_to_use = [(defect_type.lower(), DEFECT_CATALOG[defect_type.lower()])]
        else:
            defects_to_use = list(DEFECT_CATALOG.items())

        for _ in range(count):
            # Pick 1-3 random defects
            num_defects = random.randint(1, min(3, len(defects_to_use)))
            selected = random.sample(defects_to_use, num_defects)

            defect_names = [d[0] for d in selected]
            severities = {d[0]: d[1]["severity"] for d in selected}

            # Calculate grade
            total_impact = sum(d[1]["grade_impact"] for d in selected)
            base_grade = 9.4  # Start from NM
            calculated_grade = max(0.5, base_grade + total_impact + random.uniform(-0.2, 0.2))
            calculated_grade = round(calculated_grade * 2) / 2  # Round to nearest 0.5

            # Apply grade range filter
            if grade_range:
                if not (grade_range[0] <= calculated_grade <= grade_range[1]):
                    continue

            # Get label
            closest_grade = min(CGC_GRADES.keys(), key=lambda g: abs(g - calculated_grade))
            grade_info = CGC_GRADES[closest_grade]

            examples.append({
                "defects": defect_names,
                "defect_severity": severities,
                "grade_numeric": calculated_grade,
                "grade_label": grade_info["label"],
                "confidence": 0.7,  # Synthetic examples have lower confidence
                "source": "synthetic",
            })

        return examples

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a grading record to training data schema.
        """
        return {
            # Core grading data
            "defects": record.get("defects", []),
            "defect_severity": record.get("defect_severity", {}),
            "grade_numeric": record.get("grade_numeric") or record.get("estimated_grade"),
            "grade_label": record.get("grade_label", ""),

            # Additional data
            "raw_description": record.get("raw_description"),
            "images": record.get("images", []),
            "confidence": record.get("confidence", 0.5),

            # Source tracking
            "source": record.get("source", "gradingtool"),
            "source_url": record.get("url"),
            "_source": "gradingtool",
            "_raw": record,
        }

    def get_defect_catalog(self) -> Dict[str, Any]:
        """Return the full defect catalog for reference."""
        return DEFECT_CATALOG

    def get_grade_scale(self) -> Dict[float, Dict[str, str]]:
        """Return the CGC grade scale for reference."""
        return CGC_GRADES

    def estimate_grade(
        self,
        defects: List[str],
        base_grade: float = 9.4
    ) -> Dict[str, Any]:
        """
        Estimate a grade based on defects.

        Args:
            defects: List of defect names
            base_grade: Starting grade (default: NM 9.4)

        Returns:
            Dict with estimated grade and breakdown
        """
        total_impact = 0.0
        breakdown = {}

        for defect in defects:
            defect_lower = defect.lower()
            if defect_lower in DEFECT_CATALOG:
                info = DEFECT_CATALOG[defect_lower]
                impact = info["grade_impact"]
                total_impact += impact
                breakdown[defect] = {
                    "impact": impact,
                    "severity": info["severity"],
                    "category": info["category"],
                }

        final_grade = max(0.5, base_grade + total_impact)
        final_grade = round(final_grade * 2) / 2  # Round to nearest 0.5

        # Get label
        closest_grade = min(CGC_GRADES.keys(), key=lambda g: abs(g - final_grade))
        grade_info = CGC_GRADES[closest_grade]

        return {
            "estimated_grade": final_grade,
            "grade_label": grade_info["label"],
            "grade_name": grade_info["name"],
            "base_grade": base_grade,
            "total_impact": total_impact,
            "defect_breakdown": breakdown,
        }


# Adapter configuration
GRADINGTOOL_CONFIG = AdapterConfig(
    name="gradingtool",
    source_type=DataSourceType.SCRAPER,
    enabled=True,
    priority=50,
    requests_per_second=0.1,  # Very conservative: 1 req per 10 seconds
    burst_limit=1,
    auth_type=None,
    license_type="scraper",
    requires_attribution=False,
    images_allowed=True,
    extra={
        "base_url": GRADING_TOOL_BASE,
        "requires_robots_check": True,
        "min_delay_seconds": 10.0,
    },
)


def get_gradingtool_client() -> ResilientHTTPClient:
    """Get HTTP client configured for grading tool."""
    return ResilientHTTPClient(
        rate_limit_config=RateLimitConfig(
            requests_per_second=0.1,
            burst_limit=1,
            min_request_interval=10.0,
        ),
        retry_config=RetryConfig(
            max_retries=2,
            base_delay=10.0,
            max_delay=60.0,
        ),
        timeout=30.0,
        default_headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        },
    )


async def create_gradingtool_adapter() -> GradingToolAdapter:
    """Create and return a GradingTool adapter instance."""
    client = get_gradingtool_client()
    adapter = GradingToolAdapter(GRADINGTOOL_CONFIG, client)

    # Register with global registry
    from app.core.adapter_registry import adapter_registry
    adapter_registry._adapters["gradingtool"] = adapter
    adapter_registry._configs["gradingtool"] = GRADINGTOOL_CONFIG

    return adapter
