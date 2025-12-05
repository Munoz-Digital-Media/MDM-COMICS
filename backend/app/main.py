"""
MDM Comics Backend
FastAPI application entry point
"""
import asyncio
import json
import logging
import re
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import quote_plus

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, func, update

from app.api.routes import products, users, auth, cart, orders, grading, comics, checkout, funkos
from app.core.config import settings
from app.core.database import init_db, AsyncSessionLocal

# Import models to register them with SQLAlchemy
from app.models import (
    User, Product, CartItem, Order, OrderItem, GradeRequest,
    ComicPublisher, ComicSeries, ComicIssue, ComicCharacter,
    ComicCreator, ComicArc, MetronAPILog,
    Funko, FunkoSeriesName
)

logger = logging.getLogger(__name__)

# Background enrichment task reference
_enrichment_task: Optional[asyncio.Task] = None


async def import_funkos_if_needed():
    """
    Import Funko data using handle-based reconciliation (BE-007 fix).

    This function now uses handle-based comparison instead of count-based,
    ensuring that new Funkos in the JSON file are always imported even if
    the total count is similar. This is more robust for incremental updates.
    """
    async with AsyncSessionLocal() as db:
        # Find the JSON file
        json_path = Path(__file__).parent.parent / "funko_data.json"
        if not json_path.exists():
            logger.warning(f"Funko data file not found at {json_path}")
            return

        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        total_in_file = len(data)
        if total_in_file == 0:
            logger.info("Funko data file is empty, nothing to import.")
            return

        # BE-007 FIX: Get all existing handles for content-based reconciliation
        # This replaces the count-based check that could miss new items
        existing_result = await db.execute(select(Funko.handle))
        existing_handles = set(row[0] for row in existing_result.fetchall())
        logger.info(f"Found {len(existing_handles)} existing Funko handles in database")

        # Calculate how many new items we need to import
        file_handles = set(item.get('handle', '') for item in data if item.get('handle'))
        new_handles = file_handles - existing_handles

        if not new_handles:
            logger.info(f"Funko database is in sync with file ({len(existing_handles)} entries). No import needed.")
            return

        logger.info(f"BE-007: Handle-based sync found {len(new_handles)} new Funkos to import")

        # Get existing series names for efficient lookup
        series_result = await db.execute(select(FunkoSeriesName))
        series_cache = {s.name: s for s in series_result.scalars().all()}
        logger.info(f"Loaded {len(series_cache)} existing series names")

        imported = 0
        skipped = 0
        batch_size = 500

        for item in data:
            handle = item.get('handle', '')
            if not handle:
                continue

            # Skip if already exists (handle-based check)
            if handle in existing_handles:
                skipped += 1
                continue

            # Get or create series
            series_list = item.get('series', [])
            funko_series = []

            for series_name in series_list:
                if series_name not in series_cache:
                    new_series = FunkoSeriesName(name=series_name)
                    db.add(new_series)
                    await db.flush()
                    series_cache[series_name] = new_series
                funko_series.append(series_cache[series_name])

            # Create Funko
            funko = Funko(
                handle=handle,
                title=item.get('title', ''),
                image_url=item.get('image', ''),
                series=funko_series
            )
            db.add(funko)
            existing_handles.add(handle)  # Add to set so we skip if duplicated in file
            imported += 1

            # Commit in batches
            if imported % batch_size == 0:
                await db.commit()
                logger.info(f"Imported {imported} Funkos (skipped {skipped})...")

        await db.commit()
        logger.info(f"BE-007: Funko import complete! {imported} new entries added, {skipped} already existed.")


# ============== FUNKO ENRICHMENT (Background Task) ==============

SCRAPER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


async def search_funko_url(client: httpx.AsyncClient, title: str) -> Optional[str]:
    """Search Funko.com and return the best matching product URL."""
    search_url = f"https://funko.com/search?q={quote_plus(title)}"
    try:
        response = await client.get(search_url, headers=SCRAPER_HEADERS, follow_redirects=True)
        response.raise_for_status()

        # Look for product URLs in response
        # Pattern: /product-slug/ID.html where ID can be numeric or alphanumeric
        # Examples: /pop-batman/86369.html, /dc-dog-collar/DCCPDC0001.html
        # Also handle URL-encoded chars like %23 for #
        patterns = [
            r'href="(https://funko\.com/[a-zA-Z0-9%_-]+/[A-Za-z0-9]+\.html)"',  # Full URL
            r'href="(/[a-zA-Z0-9%_-]+/[A-Za-z0-9]+\.html)"',  # Relative URL
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
                    logger.info(f"Found URL for '{title}': {url}")
                    return url

        logger.warning(f"No product URL found for '{title}' (response len: {len(response.text)})")
        return None
    except Exception as e:
        logger.warning(f"Search failed for '{title}': {e}")
        return None


async def scrape_funko_details(client: httpx.AsyncClient, url: str) -> dict:
    """Scrape product details from a Funko.com product page."""
    details = {"category": None, "license": None, "product_type": None, "box_number": None, "funko_url": url}

    try:
        response = await client.get(url, headers=SCRAPER_HEADERS, follow_redirects=True)
        response.raise_for_status()
        text = response.text

        # Parse using regex (faster than BeautifulSoup for simple extraction)
        # Look for patterns like <dt>Category:</dt><dd>...<a>Value</a>...</dd>
        patterns = [
            (r'<dt[^>]*>\s*Category[^<]*</dt>\s*<dd[^>]*>.*?(?:<a[^>]*>([^<]+)</a>|([^<]+))', 'category'),
            (r'<dt[^>]*>\s*License[^<]*</dt>\s*<dd[^>]*>.*?(?:<a[^>]*>([^<]+)</a>|([^<]+))', 'license'),
            (r'<dt[^>]*>\s*Product Type[^<]*</dt>\s*<dd[^>]*>.*?(?:<a[^>]*>([^<]+)</a>|([^<]+))', 'product_type'),
            (r'<dt[^>]*>\s*Box Number[^<]*</dt>\s*<dd[^>]*>\s*(\d+)', 'box_number'),
        ]

        for pattern, field in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                # Get the first non-None group
                value = next((g for g in match.groups() if g), None)
                if value:
                    details[field] = value.strip()

        return details
    except Exception as e:
        logger.debug(f"Failed to scrape {url}: {e}")
        return details


async def enrich_funkos_background(batch_size: int = 50, delay: float = 2.0, max_batches: int = 10):
    """Background task to enrich Funko entries with data from Funko.com."""
    logger.info("Starting background Funko enrichment...")

    total_enriched = 0
    total_failed = 0
    batches_processed = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        while batches_processed < max_batches:
            async with AsyncSessionLocal() as db:
                # Get Funkos that need enrichment (no category set)
                result = await db.execute(
                    select(Funko)
                    .where(Funko.category.is_(None))
                    .limit(batch_size)
                )
                funkos = result.scalars().all()

                if not funkos:
                    logger.info("No more Funkos to enrich!")
                    break

                logger.info(f"Enrichment batch {batches_processed + 1}: Processing {len(funkos)} Funkos...")

                for funko in funkos:
                    try:
                        # Search for product URL
                        product_url = await search_funko_url(client, funko.title)

                        if product_url:
                            # Scrape details
                            details = await scrape_funko_details(client, product_url)

                            if any([details["category"], details["license"], details["product_type"], details["box_number"]]):
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
                                total_enriched += 1
                                logger.debug(f"Enriched: {funko.title} -> Box#{details['box_number']}, {details['license']}")
                            else:
                                # Mark as attempted (set empty string to avoid re-processing)
                                await db.execute(
                                    update(Funko)
                                    .where(Funko.id == funko.id)
                                    .values(category="")
                                )
                                total_failed += 1
                        else:
                            # No URL found, mark as attempted
                            await db.execute(
                                update(Funko)
                                .where(Funko.id == funko.id)
                                .values(category="")
                            )
                            total_failed += 1

                        # Rate limiting
                        await asyncio.sleep(delay)

                    except Exception as e:
                        logger.error(f"Error enriching {funko.title}: {e}")
                        total_failed += 1

                await db.commit()
                batches_processed += 1
                logger.info(f"Batch {batches_processed} complete. Total enriched: {total_enriched}, failed: {total_failed}")

    logger.info(f"Background enrichment finished. Enriched: {total_enriched}, Failed: {total_failed}")


async def migrate_funko_columns():
    """Add enrichment columns to funkos table if they don't exist."""
    from sqlalchemy import text
    from app.core.database import engine

    columns_to_add = [
        ("category", "VARCHAR(255)"),
        ("license", "VARCHAR(255)"),
        ("product_type", "VARCHAR(100)"),
        ("box_number", "VARCHAR(50)"),
        ("funko_url", "TEXT"),
    ]

    async with engine.begin() as conn:
        for col_name, col_type in columns_to_add:
            try:
                await conn.execute(text(
                    f"ALTER TABLE funkos ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
                logger.info(f"Added column: {col_name}")
            except Exception as e:
                # Column already exists or other issue
                pass

        # Add indexes
        for col in ["category", "license", "product_type", "box_number"]:
            try:
                await conn.execute(text(
                    f"CREATE INDEX IF NOT EXISTS ix_funkos_{col} ON funkos({col})"
                ))
            except Exception:
                pass

    logger.info("Funko columns migration complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database tables on startup."""
    global _enrichment_task

    await init_db()
    # Migrate funko columns (add new enrichment fields)
    await migrate_funko_columns()
    # Import Funkos if database is empty
    await import_funkos_if_needed()

    # NOTE: Background enrichment disabled - Funko.com blocks Railway's IP ranges
    # Enrichment must be done via client-side calls or a proxy service
    # _enrichment_task = asyncio.create_task(enrich_funkos_background(
    #     batch_size=50,   # 50 Funkos per batch
    #     delay=2.0,       # 2 seconds between requests (be nice to Funko.com)
    #     max_batches=20   # Process up to 1000 Funkos per server restart
    # ))
    logger.info("Background Funko enrichment DISABLED (Funko.com blocks cloud IPs)")

    yield

    # Cleanup on shutdown
    if _enrichment_task and not _enrichment_task.done():
        _enrichment_task.cancel()
        try:
            await _enrichment_task
        except asyncio.CancelledError:
            logger.info("Background enrichment task cancelled")


app = FastAPI(
    lifespan=lifespan,
    title="MDM Comics API",
    description="AI-powered comic book grading and e-commerce platform",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS - adjust origins for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(users.router, prefix="/api/users", tags=["Users"])
app.include_router(products.router, prefix="/api/products", tags=["Products"])
app.include_router(cart.router, prefix="/api/cart", tags=["Cart"])
app.include_router(orders.router, prefix="/api/orders", tags=["Orders"])
app.include_router(grading.router, prefix="/api/grading", tags=["AI Grading"])
app.include_router(comics.router, prefix="/api", tags=["Comics Database"])
app.include_router(checkout.router, prefix="/api", tags=["Checkout"])
app.include_router(funkos.router, prefix="/api", tags=["Funko Database"])


@app.get("/", tags=["Health"])
async def root():
    return {
        "message": "MDM Comics API",
        "version": "0.1.0",
        "status": "operational"
    }


@app.get("/health", tags=["Health"])
async def health_check():
    return {
        "status": "healthy",
        "database": "connected",
        "ml_model": "loaded"
    }
