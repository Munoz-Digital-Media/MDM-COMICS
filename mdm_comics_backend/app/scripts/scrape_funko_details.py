"""
Funko.com Scraper
Enriches Funko database entries with Category, License, Product Type, and Box Number.

Usage:
    python -m app.scripts.scrape_funko_details [--limit N] [--delay SECONDS]
"""
import asyncio
import argparse
import logging
import re
import json
from typing import Optional, Dict, Any
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.models.funko import Funko

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Headers to mimic browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


async def search_funko(client: httpx.AsyncClient, title: str) -> Optional[str]:
    """Search Funko.com and return the best matching product URL."""
    search_url = f"https://funko.com/search?q={quote_plus(title)}"

    try:
        response = await client.get(search_url, headers=HEADERS, follow_redirects=True)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Look for product links in search results
        # Funko.com uses various structures, try common patterns
        product_links = soup.select('a[href*=".html"]')

        for link in product_links:
            href = link.get("href", "")
            # Match product URLs like /pop-black-panther/12345.html
            if re.match(r"^/[\w-]+/\d+\.html$", href):
                return f"https://funko.com{href}"
            elif re.match(r"^https://funko\.com/[\w-]+/\d+\.html$", href):
                return href

        return None
    except Exception as e:
        logger.error(f"Search failed for '{title}': {e}")
        return None


async def scrape_product_details(client: httpx.AsyncClient, url: str) -> Dict[str, Any]:
    """Scrape product details from a Funko.com product page."""
    details = {
        "category": None,
        "license": None,
        "product_type": None,
        "box_number": None,
        "funko_url": url,
    }

    try:
        response = await client.get(url, headers=HEADERS, follow_redirects=True)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Look for definition list with product details
        dl_elements = soup.find_all("dl")

        for dl in dl_elements:
            dt_elements = dl.find_all("dt")
            dd_elements = dl.find_all("dd")

            for dt, dd in zip(dt_elements, dd_elements):
                label = dt.get_text(strip=True).lower().replace(":", "")
                value = dd.get_text(strip=True)

                if "category" in label:
                    details["category"] = value
                elif "license" in label:
                    details["license"] = value
                elif "product type" in label:
                    details["product_type"] = value
                elif "box number" in label:
                    details["box_number"] = value

        # Also try looking for specific data attributes or JSON-LD
        script_tags = soup.find_all("script", type="application/ld+json")
        for script in script_tags:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    if "category" in data and not details["category"]:
                        details["category"] = data["category"]
                    if "brand" in data and not details["license"]:
                        brand = data.get("brand", {})
                        if isinstance(brand, dict):
                            details["license"] = brand.get("name")
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass

        return details
    except Exception as e:
        logger.error(f"Failed to scrape {url}: {e}")
        return details


async def enrich_funko(
    client: httpx.AsyncClient,
    db: AsyncSession,
    funko: Funko
) -> bool:
    """Search and enrich a single Funko entry."""
    logger.info(f"Processing: {funko.title} (ID: {funko.id})")

    # Search for the product
    product_url = await search_funko(client, funko.title)

    if not product_url:
        logger.warning(f"No match found for: {funko.title}")
        return False

    # Scrape the details
    details = await scrape_product_details(client, product_url)

    if not any([details["category"], details["license"], details["product_type"], details["box_number"]]):
        logger.warning(f"No details extracted for: {funko.title}")
        return False

    # Update the database
    await db.execute(
        update(Funko)
        .where(Funko.id == funko.id)
        .values(
            category=details["category"],
            license=details["license"],
            product_type=details["product_type"],
            box_number=details["box_number"],
            funko_url=details["funko_url"],
        )
    )

    logger.info(f"Enriched: {funko.title} -> Category: {details['category']}, License: {details['license']}, Type: {details['product_type']}, Box#: {details['box_number']}")
    return True


async def run_scraper(limit: int = 100, delay: float = 1.0, skip_enriched: bool = True):
    """Run the scraper to enrich Funko entries."""
    async with AsyncSessionLocal() as db:
        # Get Funkos that need enrichment
        query = select(Funko)
        if skip_enriched:
            query = query.where(Funko.category.is_(None))
        query = query.limit(limit)

        result = await db.execute(query)
        funkos = result.scalars().all()

        logger.info(f"Found {len(funkos)} Funkos to process")

        if not funkos:
            logger.info("No Funkos need enrichment")
            return

        enriched = 0
        failed = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            for i, funko in enumerate(funkos):
                try:
                    success = await enrich_funko(client, db, funko)
                    if success:
                        enriched += 1
                    else:
                        failed += 1

                    # Commit periodically
                    if (i + 1) % 10 == 0:
                        await db.commit()
                        logger.info(f"Progress: {i + 1}/{len(funkos)} - Enriched: {enriched}, Failed: {failed}")

                    # Rate limiting
                    await asyncio.sleep(delay)

                except Exception as e:
                    logger.error(f"Error processing {funko.title}: {e}")
                    failed += 1

        await db.commit()
        logger.info(f"Complete! Enriched: {enriched}, Failed: {failed}")


def main():
    parser = argparse.ArgumentParser(description="Scrape Funko.com for product details")
    parser.add_argument("--limit", type=int, default=100, help="Max number of Funkos to process")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    parser.add_argument("--include-enriched", action="store_true", help="Re-process already enriched entries")

    args = parser.parse_args()

    asyncio.run(run_scraper(
        limit=args.limit,
        delay=args.delay,
        skip_enriched=not args.include_enriched
    ))


if __name__ == "__main__":
    main()
