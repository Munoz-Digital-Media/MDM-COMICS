"""
Local Funko Enrichment Script
Run this from your local machine to enrich Funko data from Funko.com

Usage:
    cd F:\apps\mdm_comics\backend
    pip install httpx asyncpg sqlalchemy python-dotenv
    python enrich_funkos_local.py

This connects directly to the Railway Postgres database and enriches Funkos
with Category, License, Product Type, and Box Number from Funko.com
"""
import asyncio
import re
import os
import logging
from urllib.parse import quote_plus
from datetime import datetime

import httpx
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database URL from Railway
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set. Add it to .env file or set as environment variable")

# Convert postgres:// to postgresql+asyncpg://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Create async engine
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Scraper settings
SCRAPER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


async def search_funko_url(client: httpx.AsyncClient, title: str) -> str | None:
    """Search Funko.com and return the best matching product URL."""
    search_url = f"https://funko.com/search?q={quote_plus(title)}"
    try:
        response = await client.get(search_url, headers=SCRAPER_HEADERS, follow_redirects=True)
        response.raise_for_status()

        # Pattern: /product-slug/ID.html where ID can be numeric or alphanumeric
        patterns = [
            r'href="(https://funko\.com/[a-zA-Z0-9%_-]+/[A-Za-z0-9]+\.html)"',
            r'href="(/[a-zA-Z0-9%_-]+/[A-Za-z0-9]+\.html)"',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, response.text, re.IGNORECASE)
            if matches:
                for url in matches:
                    if url.startswith("/"):
                        url = f"https://funko.com{url}"
                    # Skip non-product pages
                    if "/search" in url or "/collections" in url or "/fandoms" in url or "/all-funko" in url:
                        continue
                    return url

        return None
    except Exception as e:
        logger.debug(f"Search failed for '{title}': {e}")
        return None


async def scrape_funko_details(client: httpx.AsyncClient, url: str) -> dict:
    """Scrape product details from a Funko.com product page."""
    details = {"category": None, "license": None, "product_type": None, "box_number": None, "funko_url": url}

    try:
        response = await client.get(url, headers=SCRAPER_HEADERS, follow_redirects=True)
        response.raise_for_status()
        text = response.text

        # Updated patterns for Funko.com's current HTML structure
        # Format: "Category: </span>...<a href="/fandoms/...">Value</a>"
        patterns = [
            (r'Category:\s*</span>\s*<a[^>]+href="/fandoms/[^"]*"[^>]*>\s*([^<]+?)\s*</a>', 'category'),
            (r'License:\s*</span>\s*<a[^>]*>\s*([^<]+?)\s*</a>', 'license'),
            (r'Product Type:\s*</span>\s*<a[^>]*>\s*([^<]+?)\s*</a>', 'product_type'),
            (r'Box Number:\s*</span>\s*(\d+)', 'box_number'),
            (r'Item Number:\s*</span>\s*(\d+)', 'item_number'),  # Fallback for box_number
        ]

        for pattern, field in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                value = match.group(1).strip()
                # Decode HTML entities
                value = value.replace('&amp;', '&').replace('&#39;', "'")
                if value:
                    # Use item_number as box_number if box_number not found
                    if field == 'item_number' and not details['box_number']:
                        details['box_number'] = value
                    elif field != 'item_number':
                        details[field] = value

        return details
    except Exception as e:
        logger.debug(f"Failed to scrape {url}: {e}")
        return details


async def get_unenriched_count() -> int:
    """Get count of Funkos that need enrichment."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(text("SELECT COUNT(*) FROM funkos WHERE category IS NULL"))
        return result.scalar() or 0


async def enrich_batch(batch_size: int = 50, delay: float = 1.5) -> tuple[int, int]:
    """Enrich a batch of Funkos. Returns (enriched_count, failed_count)."""
    enriched = 0
    failed = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        async with AsyncSessionLocal() as db:
            # Get Funkos that need enrichment
            result = await db.execute(text("""
                SELECT id, title FROM funkos
                WHERE category IS NULL
                ORDER BY id
                LIMIT :limit
            """), {"limit": batch_size})
            rows = result.fetchall()

            if not rows:
                return 0, 0

            for funko_id, title in rows:
                try:
                    # Search for product URL
                    product_url = await search_funko_url(client, title)

                    if product_url:
                        # Scrape details
                        details = await scrape_funko_details(client, product_url)

                        if any([details["category"], details["license"], details["product_type"], details["box_number"]]):
                            await db.execute(text("""
                                UPDATE funkos SET
                                    category = :category,
                                    license = :license,
                                    product_type = :product_type,
                                    box_number = :box_number,
                                    funko_url = :funko_url,
                                    updated_at = :updated_at
                                WHERE id = :id
                            """), {
                                "category": details["category"],
                                "license": details["license"],
                                "product_type": details["product_type"],
                                "box_number": details["box_number"],
                                "funko_url": details["funko_url"],
                                "updated_at": datetime.utcnow(),
                                "id": funko_id
                            })
                            enriched += 1
                            logger.info(f"✓ {title[:50]} -> #{details['box_number'] or 'N/A'}, {details['license'] or 'N/A'}")
                        else:
                            # Mark as attempted (empty string to avoid re-processing)
                            await db.execute(text("""
                                UPDATE funkos SET category = '', updated_at = :updated_at WHERE id = :id
                            """), {"updated_at": datetime.utcnow(), "id": funko_id})
                            failed += 1
                            logger.warning(f"✗ {title[:50]} - No data found on page")
                    else:
                        # No URL found
                        await db.execute(text("""
                            UPDATE funkos SET category = '', updated_at = :updated_at WHERE id = :id
                        """), {"updated_at": datetime.utcnow(), "id": funko_id})
                        failed += 1
                        logger.warning(f"✗ {title[:50]} - No product URL found")

                    # Rate limiting
                    await asyncio.sleep(delay)

                except Exception as e:
                    logger.error(f"Error enriching {title}: {e}")
                    failed += 1

            await db.commit()

    return enriched, failed


async def main():
    """Main enrichment loop."""
    print("=" * 60)
    print("FUNKO ENRICHMENT SCRIPT")
    print("=" * 60)

    # Check connection
    try:
        unenriched = await get_unenriched_count()
        print(f"\nFunkos needing enrichment: {unenriched}")
    except Exception as e:
        print(f"\n❌ Database connection failed: {e}")
        print("\nMake sure DATABASE_URL is set in .env file")
        print("You can find it in Railway dashboard -> Variables")
        return

    if unenriched == 0:
        print("\n✓ All Funkos already enriched!")
        return

    print(f"\nStarting enrichment with 1.5 second delay between requests...")
    print("Press Ctrl+C to stop at any time\n")

    total_enriched = 0
    total_failed = 0
    batch_num = 0

    try:
        while True:
            batch_num += 1
            remaining = await get_unenriched_count()

            if remaining == 0:
                break

            print(f"\n--- Batch {batch_num} ({remaining} remaining) ---")
            enriched, failed = await enrich_batch(batch_size=50, delay=1.5)

            if enriched == 0 and failed == 0:
                break

            total_enriched += enriched
            total_failed += failed

            print(f"Batch complete: {enriched} enriched, {failed} failed")
            print(f"Total progress: {total_enriched} enriched, {total_failed} failed")

    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")

    print("\n" + "=" * 60)
    print("ENRICHMENT COMPLETE")
    print(f"Total enriched: {total_enriched}")
    print(f"Total failed: {total_failed}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
