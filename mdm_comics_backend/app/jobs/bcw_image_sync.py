"""
BCW Image Sync Job

Fetches product images from BCW product pages and uploads to S3.
Reads product catalog from Excel file and scrapes images for each product.

Per 20251216_mdm_comics_bcw_catalog.xlsx
"""
import asyncio
import logging
import re
import os
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.services.storage import StorageService

logger = logging.getLogger(__name__)

# Path to BCW catalog Excel file (relative to project root)
CATALOG_PATH = Path(__file__).parent.parent.parent / "data" / "bcw_catalog.xlsx"

# Image size preference - larger cache ID = higher quality
PREFERRED_CACHE_ID = "e421e1a1fb9352138824a73060698151"  # Large images
FALLBACK_CACHE_ID = "018cb6c939f4384972ea386d6d6280e0"   # Thumbnails


@dataclass
class ImageResult:
    """Result of fetching images for a single product."""
    bcw_sku: str
    mdm_sku: str
    images_found: int
    images_uploaded: int
    primary_image_url: Optional[str]
    error: Optional[str] = None


@dataclass
class SyncResult:
    """Result of the full image sync job."""
    total_products: int
    products_processed: int
    products_with_images: int
    total_images_uploaded: int
    errors: List[str]
    duration_ms: int


async def fetch_product_page(client: httpx.AsyncClient, url: str) -> Optional[str]:
    """Fetch BCW product page HTML."""
    try:
        response = await client.get(url, follow_redirects=True)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


def extract_image_urls(html: str, bcw_sku: str) -> List[str]:
    """Extract product image URLs from BCW product page HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    image_urls = set()

    # Find all image URLs in the page
    for img in soup.find_all('img'):
        src = img.get('src', '') or img.get('data-src', '')
        if src and '/media/catalog/product/' in src:
            image_urls.add(src)

    # Also check for gallery data in JavaScript
    for script in soup.find_all('script'):
        script_text = script.string or ''
        # Look for image URLs in JSON data
        urls = re.findall(r'https://www\.bcwsupplies\.com/media/catalog/product/[^"\']+', script_text)
        image_urls.update(urls)

    # Filter out video thumbnails and non-product images
    VIDEO_PATTERNS = ['hqdefault', 'maxresdefault', 'mqdefault', 'sddefault', 'youtube', 'video', 'play-button']

    filtered_urls = []
    for url in image_urls:
        filename = url.split('/')[-1].lower()
        # Skip video thumbnails
        if any(pattern in filename for pattern in VIDEO_PATTERNS):
            continue
        # Skip very small images (likely icons)
        if 'icon' in filename or 'logo' in filename:
            continue
        filtered_urls.append(url)

    # Filter and prioritize images
    prioritized = []
    seen_filenames = set()

    for url in filtered_urls:
        # Extract filename
        filename = url.split('/')[-1]
        base_filename = re.sub(r'_\d+\.jpg$', '.jpg', filename)  # Normalize numbered variants

        # Skip duplicates (different cache sizes of same image)
        if base_filename in seen_filenames:
            # But prefer larger cache versions
            if PREFERRED_CACHE_ID in url:
                # Replace with higher quality version
                prioritized = [u for u in prioritized if base_filename not in u]
                prioritized.append(url)
            continue

        seen_filenames.add(base_filename)
        prioritized.append(url)

    # Sort to put main product images first (usually contain SKU)
    sku_lower = bcw_sku.lower().replace('-', '')

    def sort_key(url):
        filename = url.split('/')[-1].lower()
        # Prioritize images with SKU in filename
        has_sku = sku_lower in filename.replace('-', '')
        # Prioritize larger cache
        is_large = PREFERRED_CACHE_ID in url
        return (not has_sku, not is_large, filename)

    prioritized.sort(key=sort_key)

    return prioritized[:10]  # Limit to 10 images per product


async def download_image(client: httpx.AsyncClient, url: str) -> Optional[bytes]:
    """Download image from URL."""
    try:
        response = await client.get(url, follow_redirects=True)
        response.raise_for_status()
        return response.content
    except Exception as e:
        logger.error(f"Failed to download image {url}: {e}")
        return None


async def process_product(
    client: httpx.AsyncClient,
    storage: StorageService,
    db: AsyncSession,
    bcw_sku: str,
    mdm_sku: str,
    product_url: str,
    product_name: str,
) -> ImageResult:
    """Process a single product - fetch page, extract images, upload to S3."""
    result = ImageResult(
        bcw_sku=bcw_sku,
        mdm_sku=mdm_sku,
        images_found=0,
        images_uploaded=0,
        primary_image_url=None,
    )

    # Fetch product page
    html = await fetch_product_page(client, product_url)
    if not html:
        result.error = "Failed to fetch product page"
        return result

    # Extract image URLs
    image_urls = extract_image_urls(html, bcw_sku)
    result.images_found = len(image_urls)

    if not image_urls:
        result.error = "No images found on page"
        return result

    logger.info(f"[{bcw_sku}] Found {len(image_urls)} images")

    # Download and upload each image
    uploaded_urls = []
    for i, img_url in enumerate(image_urls):
        image_data = await download_image(client, img_url)
        if not image_data:
            continue

        # Generate S3 key
        ext = img_url.split('.')[-1].split('?')[0]  # Get extension, strip query params
        if ext not in ('jpg', 'jpeg', 'png', 'gif', 'webp'):
            ext = 'jpg'

        s3_key = f"bcw-products/{mdm_sku}/{i:02d}_{bcw_sku.lower()}.{ext}"

        # Upload to S3
        try:
            s3_url = await storage.upload_bytes(
                image_data,
                s3_key,
                content_type=f"image/{ext}",
            )
            if s3_url:
                uploaded_urls.append(s3_url)
                result.images_uploaded += 1

                if i == 0:
                    result.primary_image_url = s3_url

                logger.info(f"[{bcw_sku}] Uploaded image {i+1}: {s3_key}")
        except Exception as e:
            logger.error(f"[{bcw_sku}] Failed to upload image {i}: {e}")

    # Update product record with primary image
    if result.primary_image_url:
        try:
            await db.execute(text("""
                UPDATE products
                SET cover_image_url = :image_url,
                    updated_at = NOW()
                WHERE sku = :mdm_sku
            """), {
                "image_url": result.primary_image_url,
                "mdm_sku": mdm_sku,
            })

            # Also store all images in product_images table if it exists
            for i, s3_url in enumerate(uploaded_urls):
                await db.execute(text("""
                    INSERT INTO product_images (product_sku, image_url, position, source, created_at)
                    VALUES (:sku, :url, :position, 'bcw', NOW())
                    ON CONFLICT (product_sku, image_url) DO UPDATE SET
                        position = :position,
                        updated_at = NOW()
                """), {
                    "sku": mdm_sku,
                    "url": s3_url,
                    "position": i,
                })
        except Exception as e:
            # Table might not exist, that's ok
            logger.debug(f"[{bcw_sku}] Could not update product record: {e}")

    return result


async def run_bcw_image_sync_job(
    catalog_path: Path = CATALOG_PATH,
    batch_size: int = 5,
    delay_between_requests: float = 1.0,
) -> SyncResult:
    """
    Main job to sync BCW product images.

    Args:
        catalog_path: Path to the BCW catalog Excel file
        batch_size: Number of concurrent requests
        delay_between_requests: Delay in seconds between batches

    Returns:
        SyncResult with statistics
    """
    job_name = "bcw_image_sync"
    start_time = datetime.now(timezone.utc)
    logger.info(f"[{job_name}] Starting BCW image sync")

    errors = []

    # Read catalog
    try:
        df = pd.read_excel(catalog_path, header=1)
        df = df.dropna(subset=['BCW-SKU', 'MDM-SKU', 'URL'])
        products = df[['BCW-SKU', 'BCW-NAME', 'MDM-SKU', 'URL']].to_dict('records')
        logger.info(f"[{job_name}] Loaded {len(products)} products from catalog")
    except Exception as e:
        logger.error(f"[{job_name}] Failed to read catalog: {e}")
        return SyncResult(
            total_products=0,
            products_processed=0,
            products_with_images=0,
            total_images_uploaded=0,
            errors=[str(e)],
            duration_ms=0,
        )

    total_products = len(products)
    products_processed = 0
    products_with_images = 0
    total_images = 0

    async with AsyncSessionLocal() as db:
        storage = StorageService()

        if not storage.is_configured():
            logger.error(f"[{job_name}] S3 storage not configured")
            return SyncResult(
                total_products=total_products,
                products_processed=0,
                products_with_images=0,
                total_images_uploaded=0,
                errors=["S3 storage not configured"],
                duration_ms=0,
            )

        # Process products in batches
        async with httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        ) as client:

            for i in range(0, len(products), batch_size):
                batch = products[i:i + batch_size]

                tasks = [
                    process_product(
                        client=client,
                        storage=storage,
                        db=db,
                        bcw_sku=p['BCW-SKU'],
                        mdm_sku=p['MDM-SKU'],
                        product_url=p['URL'],
                        product_name=p['BCW-NAME'],
                    )
                    for p in batch
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        errors.append(str(result))
                        continue

                    products_processed += 1
                    if result.images_uploaded > 0:
                        products_with_images += 1
                        total_images += result.images_uploaded

                    if result.error:
                        errors.append(f"{result.bcw_sku}: {result.error}")

                logger.info(
                    f"[{job_name}] Progress: {products_processed}/{total_products} "
                    f"({products_with_images} with images, {total_images} total images)"
                )

                # Commit after each batch
                await db.commit()

                # Rate limiting
                if i + batch_size < len(products):
                    await asyncio.sleep(delay_between_requests)

    duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)

    logger.info(
        f"[{job_name}] Complete: {products_processed}/{total_products} products, "
        f"{products_with_images} with images, {total_images} images uploaded, "
        f"{len(errors)} errors, {duration_ms}ms"
    )

    return SyncResult(
        total_products=total_products,
        products_processed=products_processed,
        products_with_images=products_with_images,
        total_images_uploaded=total_images,
        errors=errors[:20],  # Limit error list
        duration_ms=duration_ms,
    )


# CLI entry point
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    result = asyncio.run(run_bcw_image_sync_job())

    print("\n" + "=" * 50)
    print("BCW IMAGE SYNC COMPLETE")
    print("=" * 50)
    print(f"Total products:      {result.total_products}")
    print(f"Products processed:  {result.products_processed}")
    print(f"Products with images: {result.products_with_images}")
    print(f"Total images uploaded: {result.total_images_uploaded}")
    print(f"Errors:              {len(result.errors)}")
    print(f"Duration:            {result.duration_ms}ms")

    if result.errors:
        print("\nErrors:")
        for err in result.errors[:10]:
            print(f"  - {err}")

    sys.exit(0 if len(result.errors) == 0 else 1)
