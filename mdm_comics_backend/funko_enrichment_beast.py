#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║                     FUNKO ENRICHMENT BEAST v1.0                               ║
║                                                                               ║
║  An unstoppable, self-healing, multi-source data enrichment juggernaut.      ║
║                                                                               ║
║  Features:                                                                    ║
║  • Multi-source enrichment (Series parsing → eBay → Funko.com)               ║
║  • Exponential backoff with jitter                                           ║
║  • Circuit breaker pattern                                                   ║
║  • Checkpoint/resume (survives crashes)                                      ║
║  • Parallel processing with smart rate limiting                              ║
║  • Graceful degradation                                                      ║
║  • Comprehensive error logging                                               ║
║  • Real-time progress with ETA                                               ║
║                                                                               ║
║  Usage:                                                                       ║
║    python funko_enrichment_beast.py                                          ║
║    python funko_enrichment_beast.py --resume                                 ║
║    python funko_enrichment_beast.py --reset                                  ║
║                                                                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import logging
import os
import random
import re
import signal
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

import httpx
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

load_dotenv()

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Paths
CHECKPOINT_FILE = Path(__file__).parent / "enrichment_checkpoint.json"
ERROR_LOG_FILE = Path(__file__).parent / "enrichment_errors.log"

# Rate limiting
REQUESTS_PER_SECOND = 0.5  # 1 request every 2 seconds
BATCH_SIZE = 25
MAX_CONCURRENT_REQUESTS = 3

# Retry settings
MAX_RETRIES = 5
BASE_RETRY_DELAY = 2.0
MAX_RETRY_DELAY = 60.0

# Circuit breaker
CIRCUIT_BREAKER_THRESHOLD = 10  # failures before circuit opens
CIRCUIT_BREAKER_RESET_TIME = 300  # seconds before trying again

# Scraper headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════════

class ColorFormatter(logging.Formatter):
    """Colorized log formatter for terminal output."""

    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m',
        'BOLD': '\033[1m',
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']

        # Add emoji based on level
        emoji = {
            'DEBUG': '🔍',
            'INFO': '✅',
            'WARNING': '⚠️',
            'ERROR': '❌',
            'CRITICAL': '💀',
        }.get(record.levelname, '')

        record.msg = f"{color}{emoji} {record.msg}{reset}"
        return super().format(record)

# Setup logging
logger = logging.getLogger("BEAST")
logger.setLevel(logging.DEBUG)

# Console handler with colors
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(ColorFormatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(console_handler)

# File handler for errors
file_handler = logging.FileHandler(ERROR_LOG_FILE, mode='a')
file_handler.setLevel(logging.WARNING)
file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
logger.addHandler(file_handler)

# Suppress httpx logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════

class EnrichmentSource(Enum):
    SERIES_PARSER = "series_parser"
    FUNKO_COM = "funko_com"
    EBAY = "ebay"
    MANUAL = "manual"

@dataclass
class EnrichmentResult:
    category: Optional[str] = None
    license: Optional[str] = None
    product_type: Optional[str] = None
    box_number: Optional[str] = None
    funko_url: Optional[str] = None
    source: Optional[str] = None
    confidence: float = 0.0

    def has_data(self) -> bool:
        return any([self.category, self.license, self.product_type, self.box_number])

    def merge_with(self, other: 'EnrichmentResult') -> 'EnrichmentResult':
        """Merge with another result, preferring non-None values with higher confidence."""
        return EnrichmentResult(
            category=self.category or other.category,
            license=self.license or other.license,
            product_type=self.product_type or other.product_type,
            box_number=self.box_number or other.box_number,
            funko_url=self.funko_url or other.funko_url,
            source=f"{self.source or ''},{other.source or ''}".strip(','),
            confidence=max(self.confidence, other.confidence),
        )

@dataclass
class Checkpoint:
    """Checkpoint for resumable enrichment."""
    last_processed_id: int = 0
    total_processed: int = 0
    total_enriched: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    start_time: str = ""
    last_update: str = ""
    errors: list = field(default_factory=list)

    def save(self):
        self.last_update = datetime.now().isoformat()
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls) -> 'Checkpoint':
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                return cls(**data)
        return cls(start_time=datetime.now().isoformat())

@dataclass
class CircuitBreaker:
    """Circuit breaker to prevent cascading failures."""
    failures: int = 0
    last_failure_time: float = 0
    is_open: bool = False

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= CIRCUIT_BREAKER_THRESHOLD:
            self.is_open = True
            logger.warning(f"Circuit breaker OPEN after {self.failures} failures")

    def record_success(self):
        self.failures = max(0, self.failures - 1)
        if self.failures == 0:
            self.is_open = False

    def can_proceed(self) -> bool:
        if not self.is_open:
            return True
        # Check if enough time has passed to try again
        if time.time() - self.last_failure_time > CIRCUIT_BREAKER_RESET_TIME:
            logger.info("Circuit breaker attempting reset...")
            self.is_open = False
            self.failures = CIRCUIT_BREAKER_THRESHOLD - 1  # One more failure will re-open
            return True
        return False

# ═══════════════════════════════════════════════════════════════════════════════
# SERIES PARSER (Source 1: Instant, 100% reliable for available data)
# ═══════════════════════════════════════════════════════════════════════════════

# Mapping of series names to enrichment data
PRODUCT_TYPE_PATTERNS = {
    r'^Pop!': 'Pop!',
    r'Pop! Vinyl': 'Pop!',
    r'Pocket Pop': 'Pocket Pop!',
    r'Mystery Minis': 'Mystery Minis',
    r'Wacky Wobbler': 'Wacky Wobbler',
    r'Dorbz': 'Dorbz',
    r'Hikari': 'Hikari',
    r'Pint Size Heroes': 'Pint Size Heroes',
    r'Rock Candy': 'Rock Candy',
    r'Vynl': 'Vynl',
    r'5 Star': '5 Star',
    r'Action Figures': 'Action Figure',
    r'Plushies': 'Plush',
    r'Soda': 'Soda',
    r'Bitty Pop': 'Bitty Pop!',
    r'Pop! Pez': 'Pop! Pez',
    r'Pop! Pins': 'Pop! Pin',
    r'Pop! Tees': 'Apparel',
    r'ReAction': 'ReAction',
}

LICENSE_PATTERNS = {
    # Direct mappings
    r'Disney': 'Disney',
    r'Marvel': 'Marvel',
    r'Star Wars': 'Star Wars',
    r'DC ': 'DC Comics',
    r'DC$': 'DC Comics',
    r'Harry Potter': 'Harry Potter',
    r'Pokemon|Pokémon': 'Pokémon',
    r'Dragon Ball': 'Dragon Ball',
    r'Naruto': 'Naruto',
    r'One Piece': 'One Piece',
    r'My Hero Academia': 'My Hero Academia',
    r'Demon Slayer': 'Demon Slayer',
    r'Attack on Titan': 'Attack on Titan',
    r'Stranger Things': 'Stranger Things',
    r'The Office': 'The Office',
    r'Friends': 'Friends',
    r'Seinfeld': 'Seinfeld',
    r'Game of Thrones': 'Game of Thrones',
    r'Lord of the Rings|LOTR': 'The Lord of the Rings',
    r'The Witcher': 'The Witcher',
    r'Transformers': 'Transformers',
    r'G\.I\. Joe|GI Joe': 'G.I. Joe',
    r'Teenage Mutant Ninja Turtles|TMNT': 'TMNT',
    r'Power Rangers': 'Power Rangers',
    r'WWE': 'WWE',
    r'NBA': 'NBA',
    r'NFL': 'NFL',
    r'MLB': 'MLB',
    r'NHL': 'NHL',
    r'Simpsons': 'The Simpsons',
    r'Family Guy': 'Family Guy',
    r'Rick and Morty': 'Rick and Morty',
    r'South Park': 'South Park',
    r'Futurama': 'Futurama',
    r'SpongeBob': 'SpongeBob SquarePants',
    r'Sesame Street': 'Sesame Street',
    r'Peanuts': 'Peanuts',
    r'Looney Tunes': 'Looney Tunes',
    r'Hanna[ -]Barbera': 'Hanna-Barbera',
    r'Scooby[ -]Doo': 'Scooby-Doo',
    r'Batman': 'DC Comics',
    r'Superman': 'DC Comics',
    r'Wonder Woman': 'DC Comics',
    r'Justice League': 'DC Comics',
    r'Avengers': 'Marvel',
    r'X-Men': 'Marvel',
    r'Spider-Man': 'Marvel',
    r'Iron Man': 'Marvel',
    r'Captain America': 'Marvel',
    r'Deadpool': 'Marvel',
    r'Guardians of the Galaxy': 'Marvel',
    r'Thor': 'Marvel',
    r'Hulk': 'Marvel',
    r'Black Panther': 'Marvel',
}

CATEGORY_PATTERNS = {
    r'Pop! Television|TV': 'Television',
    r'Pop! Movies': 'Movies',
    r'Pop! Animation': 'Animation',
    r'Pop! Games': 'Games',
    r'Pop! Heroes': 'Comics & Superheroes',
    r'Pop! Rocks': 'Music',
    r'Pop! Sports': 'Sports',
    r'Pop! Icons': 'Icons',
    r'Pop! Ad Icons': 'Ad Icons',
    r'Pop! Disney': 'Disney',
    r'Pop! Marvel': 'Comics & Superheroes',
    r'Pop! Star Wars': 'Movies',
    r'Pop! Anime': 'Animation',
    r'Pop! WWE': 'Sports',
    r'Pop! NBA|Pop! NFL|Pop! MLB|Pop! NHL': 'Sports',
    r'Horror': 'Horror',
    r'Sci-Fi': 'Sci-Fi',
    r'Fantasy': 'Fantasy',
}

def parse_series_data(series_list: list[str], title: str) -> EnrichmentResult:
    """Parse series names to extract enrichment data. Fast and reliable."""
    result = EnrichmentResult(source=EnrichmentSource.SERIES_PARSER.value)

    combined_text = ' '.join(series_list) + ' ' + title

    # Extract product type
    for pattern, product_type in PRODUCT_TYPE_PATTERNS.items():
        if re.search(pattern, combined_text, re.IGNORECASE):
            result.product_type = product_type
            break

    # Extract license
    for pattern, license_name in LICENSE_PATTERNS.items():
        if re.search(pattern, combined_text, re.IGNORECASE):
            result.license = license_name
            break

    # Extract category
    for pattern, category in CATEGORY_PATTERNS.items():
        if re.search(pattern, combined_text, re.IGNORECASE):
            result.category = category
            break

    # Try to extract box number from title
    box_match = re.search(r'#(\d+)', title)
    if box_match:
        result.box_number = box_match.group(1)

    # Calculate confidence based on how much data we found
    fields_found = sum([
        bool(result.category),
        bool(result.license),
        bool(result.product_type),
        bool(result.box_number),
    ])
    result.confidence = fields_found / 4.0

    return result

# ═══════════════════════════════════════════════════════════════════════════════
# FUNKO.COM SCRAPER (Source 2: Best data but may fail)
# ═══════════════════════════════════════════════════════════════════════════════

class FunkoScraper:
    """Scraper for Funko.com with retry logic."""

    def __init__(self, client: httpx.AsyncClient, circuit_breaker: CircuitBreaker):
        self.client = client
        self.circuit_breaker = circuit_breaker

    async def search_product_url(self, title: str) -> Optional[str]:
        """Search Funko.com for a product URL."""
        if not self.circuit_breaker.can_proceed():
            return None

        search_url = f"https://funko.com/search?q={quote_plus(title)}"

        for attempt in range(MAX_RETRIES):
            try:
                response = await self.client.get(
                    search_url,
                    headers=HEADERS,
                    follow_redirects=True,
                    timeout=15.0
                )
                response.raise_for_status()

                # Find product URLs
                patterns = [
                    r'href="(https://funko\.com/[a-zA-Z0-9%_-]+/[A-Za-z0-9]+\.html)"',
                    r'href="(/[a-zA-Z0-9%_-]+/[A-Za-z0-9]+\.html)"',
                ]

                for pattern in patterns:
                    matches = re.findall(pattern, response.text, re.IGNORECASE)
                    for url in matches:
                        if url.startswith("/"):
                            url = f"https://funko.com{url}"
                        if not any(skip in url for skip in ["/search", "/collections", "/fandoms", "/all-funko", "/account", "/login"]):
                            self.circuit_breaker.record_success()
                            return url

                return None

            except (httpx.TimeoutException, httpx.HTTPStatusError) as e:
                delay = min(BASE_RETRY_DELAY * (2 ** attempt) + random.uniform(0, 1), MAX_RETRY_DELAY)
                logger.debug(f"Funko search retry {attempt + 1}/{MAX_RETRIES} for '{title[:30]}' after {delay:.1f}s: {e}")
                await asyncio.sleep(delay)
            except Exception as e:
                self.circuit_breaker.record_failure()
                logger.debug(f"Funko search failed for '{title[:30]}': {e}")
                return None

        self.circuit_breaker.record_failure()
        return None

    async def scrape_product_details(self, url: str) -> EnrichmentResult:
        """Scrape product details from a Funko.com product page."""
        result = EnrichmentResult(
            source=EnrichmentSource.FUNKO_COM.value,
            funko_url=url
        )

        if not self.circuit_breaker.can_proceed():
            return result

        for attempt in range(MAX_RETRIES):
            try:
                response = await self.client.get(
                    url,
                    headers=HEADERS,
                    follow_redirects=True,
                    timeout=15.0
                )
                response.raise_for_status()
                text = response.text

                # Extract data using patterns
                patterns = [
                    (r'Category:\s*</span>\s*<a[^>]+href="/fandoms/[^"]*"[^>]*>\s*([^<]+?)\s*</a>', 'category'),
                    (r'License:\s*</span>\s*<a[^>]*>\s*([^<]+?)\s*</a>', 'license'),
                    (r'Product Type:\s*</span>\s*<a[^>]*>\s*([^<]+?)\s*</a>', 'product_type'),
                    (r'Box Number:\s*</span>\s*(\d+)', 'box_number'),
                    (r'Item Number:\s*</span>\s*(\d+)', 'item_number'),
                ]

                for pattern, field_name in patterns:
                    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                    if match:
                        value = match.group(1).strip()
                        value = value.replace('&amp;', '&').replace('&#39;', "'").replace('&eacute;', 'é')

                        if field_name == 'item_number' and not result.box_number:
                            result.box_number = value
                        elif field_name != 'item_number':
                            setattr(result, field_name, value)

                result.confidence = 0.9 if result.has_data() else 0.0
                self.circuit_breaker.record_success()
                return result

            except (httpx.TimeoutException, httpx.HTTPStatusError) as e:
                delay = min(BASE_RETRY_DELAY * (2 ** attempt) + random.uniform(0, 1), MAX_RETRY_DELAY)
                logger.debug(f"Funko scrape retry {attempt + 1}/{MAX_RETRIES} after {delay:.1f}s: {e}")
                await asyncio.sleep(delay)
            except Exception as e:
                self.circuit_breaker.record_failure()
                logger.debug(f"Funko scrape failed for {url}: {e}")
                return result

        self.circuit_breaker.record_failure()
        return result

    async def enrich(self, title: str) -> EnrichmentResult:
        """Full enrichment pipeline for a single Funko."""
        url = await self.search_product_url(title)
        if url:
            return await self.scrape_product_details(url)
        return EnrichmentResult()

# ═══════════════════════════════════════════════════════════════════════════════
# EBAY SCRAPER (Source 3: Fallback with better coverage)
# ═══════════════════════════════════════════════════════════════════════════════

class EbayScraper:
    """Scraper for eBay listings to extract Funko data."""

    def __init__(self, client: httpx.AsyncClient, circuit_breaker: CircuitBreaker):
        self.client = client
        self.circuit_breaker = circuit_breaker

    async def search_and_extract(self, title: str) -> EnrichmentResult:
        """Search eBay and extract data from listing titles."""
        result = EnrichmentResult(source=EnrichmentSource.EBAY.value)

        if not self.circuit_breaker.can_proceed():
            return result

        # Clean up title for search
        clean_title = re.sub(r'[^\w\s]', ' ', title)
        search_url = f"https://www.ebay.com/sch/i.html?_nkw=funko+pop+{quote_plus(clean_title)}&_sacat=0"

        for attempt in range(MAX_RETRIES):
            try:
                response = await self.client.get(
                    search_url,
                    headers=HEADERS,
                    follow_redirects=True,
                    timeout=20.0
                )
                response.raise_for_status()
                text = response.text

                # Extract box numbers from listing titles
                # Pattern: #123 or Number 123 or No. 123
                box_matches = re.findall(r'(?:#|No\.?|Number)\s*(\d{1,4})\b', text)
                if box_matches:
                    # Take the most common box number
                    from collections import Counter
                    common_box = Counter(box_matches).most_common(1)
                    if common_box:
                        result.box_number = common_box[0][0]

                # Look for franchise/license patterns in titles
                for pattern, license_name in LICENSE_PATTERNS.items():
                    if re.search(pattern, text, re.IGNORECASE):
                        result.license = license_name
                        break

                result.confidence = 0.6 if result.has_data() else 0.0
                self.circuit_breaker.record_success()
                return result

            except (httpx.TimeoutException, httpx.HTTPStatusError) as e:
                delay = min(BASE_RETRY_DELAY * (2 ** attempt) + random.uniform(0, 1), MAX_RETRY_DELAY)
                logger.debug(f"eBay search retry {attempt + 1}/{MAX_RETRIES} after {delay:.1f}s: {e}")
                await asyncio.sleep(delay)
            except Exception as e:
                self.circuit_breaker.record_failure()
                logger.debug(f"eBay search failed for '{title[:30]}': {e}")
                return result

        self.circuit_breaker.record_failure()
        return result

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class DatabaseManager:
    """Async database manager with connection pooling and retry logic."""

    def __init__(self):
        self.engine = create_async_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False,
        )
        self.SessionLocal = sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    async def get_unenriched_funkos(self, limit: int, after_id: int = 0) -> list[tuple]:
        """Get Funkos that need enrichment."""
        async with self.SessionLocal() as db:
            result = await db.execute(text("""
                SELECT f.id, f.title, f.handle,
                       COALESCE(
                           (SELECT json_agg(fs.name)
                            FROM funko_series ffs
                            JOIN funko_series_names fs ON ffs.series_id = fs.id
                            WHERE ffs.funko_id = f.id),
                           '[]'
                       ) as series
                FROM funkos f
                WHERE f.category IS NULL
                  AND f.id > :after_id
                ORDER BY f.id
                LIMIT :limit
            """), {"limit": limit, "after_id": after_id})
            rows = result.fetchall()
            # row[3] is series - asyncpg already decodes json_agg to a Python list
            def parse_series(s):
                if s is None:
                    return []
                if isinstance(s, list):
                    return s
                if isinstance(s, str):
                    return json.loads(s) if s else []
                return []
            return [(row[0], row[1], row[2], parse_series(row[3])) for row in rows]

    async def update_funko(self, funko_id: int, result: EnrichmentResult) -> bool:
        """Update a Funko with enrichment data."""
        for attempt in range(MAX_RETRIES):
            try:
                async with self.SessionLocal() as db:
                    await db.execute(text("""
                        UPDATE funkos SET
                            category = COALESCE(:category, category, ''),
                            license = COALESCE(:license, license),
                            product_type = COALESCE(:product_type, product_type),
                            box_number = COALESCE(:box_number, box_number),
                            funko_url = COALESCE(:funko_url, funko_url),
                            updated_at = NOW()
                        WHERE id = :id
                    """), {
                        "category": result.category or '',
                        "license": result.license,
                        "product_type": result.product_type,
                        "box_number": result.box_number,
                        "funko_url": result.funko_url,
                        "id": funko_id
                    })
                    await db.commit()
                    return True
            except Exception as e:
                logger.debug(f"DB update retry {attempt + 1}/{MAX_RETRIES} for ID {funko_id}: {e}")
                await asyncio.sleep(BASE_RETRY_DELAY * (attempt + 1))
        return False

    async def mark_as_failed(self, funko_id: int) -> bool:
        """Mark a Funko as processed but failed (empty category)."""
        try:
            async with self.SessionLocal() as db:
                await db.execute(text("""
                    UPDATE funkos SET category = '', updated_at = NOW() WHERE id = :id
                """), {"id": funko_id})
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to mark Funko {funko_id} as failed: {e}")
            return False

    async def get_stats(self) -> dict:
        """Get current enrichment statistics."""
        async with self.SessionLocal() as db:
            result = await db.execute(text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN category IS NOT NULL AND category != '' THEN 1 END) as enriched,
                    COUNT(CASE WHEN category IS NULL THEN 1 END) as pending,
                    COUNT(CASE WHEN category = '' THEN 1 END) as failed
                FROM funkos
            """))
            row = result.fetchone()
            return {
                "total": row[0],
                "enriched": row[1],
                "pending": row[2],
                "failed": row[3],
            }

# ═══════════════════════════════════════════════════════════════════════════════
# THE BEAST - MAIN ENRICHMENT ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class EnrichmentBeast:
    """The unstoppable enrichment engine."""

    def __init__(self):
        self.db = DatabaseManager()
        self.checkpoint = Checkpoint.load()
        self.funko_circuit = CircuitBreaker()
        self.ebay_circuit = CircuitBreaker()
        self.running = True
        self.rate_limiter = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.last_request_time = 0

        # Stats
        self.session_enriched = 0
        self.session_failed = 0
        self.session_start = time.time()

    async def rate_limit(self):
        """Enforce rate limiting."""
        async with self.rate_limiter:
            now = time.time()
            time_since_last = now - self.last_request_time
            if time_since_last < 1.0 / REQUESTS_PER_SECOND:
                await asyncio.sleep((1.0 / REQUESTS_PER_SECOND) - time_since_last)
            self.last_request_time = time.time()

    async def enrich_single(
        self,
        client: httpx.AsyncClient,
        funko_id: int,
        title: str,
        handle: str,
        series: list[str]
    ) -> bool:
        """Enrich a single Funko using all available sources."""
        await self.rate_limit()

        # Source 1: Parse series data (instant, always works)
        series_result = parse_series_data(series, title)

        # Source 2: Try Funko.com (if circuit is closed)
        funko_result = EnrichmentResult()
        if self.funko_circuit.can_proceed():
            funko_scraper = FunkoScraper(client, self.funko_circuit)
            funko_result = await funko_scraper.enrich(title)

        # Source 3: Try eBay as fallback (if Funko.com didn't get box number)
        ebay_result = EnrichmentResult()
        if not funko_result.box_number and self.ebay_circuit.can_proceed():
            ebay_scraper = EbayScraper(client, self.ebay_circuit)
            ebay_result = await ebay_scraper.search_and_extract(title)

        # Merge results (prefer Funko.com > eBay > Series parser)
        final_result = series_result.merge_with(funko_result).merge_with(ebay_result)

        # Update database
        if final_result.has_data():
            success = await self.db.update_funko(funko_id, final_result)
            if success:
                self.session_enriched += 1
                self.checkpoint.total_enriched += 1
                sources = final_result.source.split(',')
                logger.info(
                    f"[{self.checkpoint.total_processed}/{self.checkpoint.total_enriched}] "
                    f"{title[:40]} → "
                    f"#{final_result.box_number or 'N/A'} | {final_result.license or 'N/A'} | {final_result.product_type or 'N/A'} "
                    f"[{', '.join(sources)}]"
                )
                return True

        # Mark as failed
        await self.db.mark_as_failed(funko_id)
        self.session_failed += 1
        self.checkpoint.total_failed += 1
        logger.warning(f"[{self.checkpoint.total_processed}] {title[:40]} - No data found")
        return False

    def print_banner(self):
        """Print the startup banner."""
        banner = """
╔═══════════════════════════════════════════════════════════════════════════════╗
║                                                                               ║
║   ███████╗██╗   ██╗███╗   ██╗██╗  ██╗ ██████╗     ██████╗ ███████╗ █████╗    ║
║   ██╔════╝██║   ██║████╗  ██║██║ ██╔╝██╔═══██╗    ██╔══██╗██╔════╝██╔══██╗   ║
║   █████╗  ██║   ██║██╔██╗ ██║█████╔╝ ██║   ██║    ██████╔╝█████╗  ███████║   ║
║   ██╔══╝  ██║   ██║██║╚██╗██║██╔═██╗ ██║   ██║    ██╔══██╗██╔══╝  ██╔══██║   ║
║   ██║     ╚██████╔╝██║ ╚████║██║  ██╗╚██████╔╝    ██████╔╝███████╗██║  ██║   ║
║   ╚═╝      ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═╝ ╚═════╝     ╚═════╝ ╚══════╝╚═╝  ╚═╝   ║
║                                                                               ║
║                    ███████╗███╗   ██╗██████╗ ██╗ ██████╗██╗  ██╗             ║
║                    ██╔════╝████╗  ██║██╔══██╗██║██╔════╝██║  ██║             ║
║                    █████╗  ██╔██╗ ██║██████╔╝██║██║     ███████║             ║
║                    ██╔══╝  ██║╚██╗██║██╔══██╗██║██║     ██╔══██║             ║
║                    ███████╗██║ ╚████║██║  ██║██║╚██████╗██║  ██║             ║
║                    ╚══════╝╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝ ╚═════╝╚═╝  ╚═╝             ║
║                                                                               ║
║                          B   E   A   S   T                                    ║
║                                                                               ║
║                    Multi-Source Data Enrichment Engine                        ║
║                                                                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
        """
        print(banner)

    def print_stats(self):
        """Print current statistics."""
        elapsed = time.time() - self.session_start
        rate = self.checkpoint.total_processed / elapsed if elapsed > 0 else 0
        remaining = 17735 - self.checkpoint.total_processed  # Approximate
        eta_seconds = remaining / rate if rate > 0 else 0
        eta = str(timedelta(seconds=int(eta_seconds)))

        print(f"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║  PROGRESS                                                                     ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  Processed:  {self.checkpoint.total_processed:>6}  │  Enriched:  {self.checkpoint.total_enriched:>6}  │  Failed:  {self.checkpoint.total_failed:>6}     ║
║  Rate:       {rate:>6.2f}/s │  Elapsed:   {str(timedelta(seconds=int(elapsed))):>8}  │  ETA:     {eta:>8}     ║
║                                                                               ║
║  Funko.com Circuit: {'🔴 OPEN' if self.funko_circuit.is_open else '🟢 CLOSED':12}  │  eBay Circuit: {'🔴 OPEN' if self.ebay_circuit.is_open else '🟢 CLOSED':12}       ║
╚═══════════════════════════════════════════════════════════════════════════════╝
        """)

    async def run(self):
        """Main run loop."""
        self.print_banner()

        # Get initial stats
        stats = await self.db.get_stats()
        print(f"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║  DATABASE STATUS                                                              ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  Total Funkos:     {stats['total']:>6}                                                    ║
║  Already Enriched: {stats['enriched']:>6}                                                    ║
║  Pending:          {stats['pending']:>6}                                                    ║
║  Previously Failed:{stats['failed']:>6}                                                    ║
╚═══════════════════════════════════════════════════════════════════════════════╝

Starting enrichment... Press Ctrl+C to pause (progress will be saved).
        """)

        # Setup signal handler for graceful shutdown
        def signal_handler(sig, frame):
            logger.warning("Shutdown requested, saving checkpoint...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Main processing loop
        async with httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
        ) as client:

            last_stats_time = time.time()

            while self.running:
                # Get batch of Funkos to process
                funkos = await self.db.get_unenriched_funkos(
                    BATCH_SIZE,
                    self.checkpoint.last_processed_id
                )

                if not funkos:
                    logger.info("🎉 All Funkos have been processed!")
                    break

                # Process batch
                for funko_id, title, handle, series in funkos:
                    if not self.running:
                        break

                    self.checkpoint.total_processed += 1
                    self.checkpoint.last_processed_id = funko_id

                    try:
                        await self.enrich_single(client, funko_id, title, handle, series)
                    except Exception as e:
                        logger.error(f"Unexpected error processing {title[:30]}: {e}")
                        self.checkpoint.errors.append({
                            "id": funko_id,
                            "title": title,
                            "error": str(e),
                            "time": datetime.now().isoformat()
                        })
                        # Keep only last 100 errors
                        self.checkpoint.errors = self.checkpoint.errors[-100:]

                    # Print stats every 60 seconds
                    if time.time() - last_stats_time > 60:
                        self.print_stats()
                        last_stats_time = time.time()

                # Save checkpoint after each batch
                self.checkpoint.save()

        # Final save and stats
        self.checkpoint.save()
        self.print_stats()
        logger.info(f"Checkpoint saved to {CHECKPOINT_FILE}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    """Main entry point."""
    # Parse command line args
    if "--reset" in sys.argv:
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            print("Checkpoint reset!")
        return

    if not DATABASE_URL:
        print("❌ DATABASE_URL not set! Add it to .env file.")
        print("   Example: DATABASE_URL=postgresql+asyncpg://user:pass@host:port/db")
        return

    # Create and run the beast
    beast = EnrichmentBeast()
    await beast.run()


if __name__ == "__main__":
    asyncio.run(main())
