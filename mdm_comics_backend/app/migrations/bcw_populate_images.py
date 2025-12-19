"""
Migration: Populate BCW product images from S3

Scans the S3 bucket for all BCW product images and:
1. Finds all images in each product's folder
2. Sets the largest image as primary image_url (better quality, less whitespace)
3. Populates the images array with all additional images
"""
import asyncio
import json
import os
import sys
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# S3 configuration
S3_BUCKET = "mdm-comics-assets"
S3_PREFIX = "bcw-products/"
S3_BASE_URL = f"https://{S3_BUCKET}.s3.us-east-2.amazonaws.com"


def get_s3_images_for_product(s3_client, sku_folder):
    """Get all images for a product from S3, sorted by filename"""
    try:
        prefix = f"{S3_PREFIX}{sku_folder}/"
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

        if 'Contents' not in response:
            return []

        images = []
        for obj in response['Contents']:
            key = obj['Key']
            size = obj['Size']
            if key.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                filename = key.split('/')[-1]
                url = f"{S3_BASE_URL}/{key}"
                images.append({
                    'filename': filename,
                    'url': url,
                    'size': size
                })

        # Sort by filename (00_, 01_, 02_, etc.)
        images.sort(key=lambda x: x['filename'])
        return images
    except ClientError as e:
        print(f"  Error listing S3 for {sku_folder}: {e}")
        return []


def select_best_primary_image(images):
    """Select the best image for primary display (largest file, usually best quality)"""
    if not images:
        return None, []

    # Find the largest image (usually has less whitespace and better quality)
    # But skip tiny thumbnails (under 10KB are likely BCW icons)
    valid_images = [img for img in images if img['size'] > 10000]

    if not valid_images:
        # Fall back to first image if all are small
        return images[0]['url'], [img['url'] for img in images[1:]]

    # Sort by size descending to find largest
    sorted_by_size = sorted(valid_images, key=lambda x: x['size'], reverse=True)
    best_image = sorted_by_size[0]

    # All other images go in the gallery
    other_images = [img['url'] for img in images if img['url'] != best_image['url']]

    return best_image['url'], other_images


async def run_migration():
    """Populate BCW product images from S3"""

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        return False

    # Convert to async URL if needed
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    # Initialize S3 client
    s3_client = boto3.client('s3')

    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        # Get all products with BCW images
        result = await session.execute(text("""
            SELECT id, sku, image_url
            FROM products
            WHERE image_url LIKE '%bcw-products%'
        """))
        products = result.fetchall()

        print(f"Found {len(products)} BCW products to process")
        print("-" * 60)

        updated_count = 0
        skipped_count = 0

        for product in products:
            product_id, sku, current_image_url = product

            # Extract folder name from current image URL
            # URL format: .../bcw-products/MDM-SKU/00_filename.jpg
            try:
                parts = current_image_url.split('/bcw-products/')
                if len(parts) < 2:
                    print(f"  {sku}: Could not parse folder from URL (SKIP)")
                    skipped_count += 1
                    continue
                folder_and_file = parts[1]
                folder = folder_and_file.split('/')[0]
            except Exception as e:
                print(f"  {sku}: Error parsing URL: {e} (SKIP)")
                skipped_count += 1
                continue

            # Get all images from S3
            images = get_s3_images_for_product(s3_client, folder)

            if not images:
                print(f"  {sku}: No images found in S3 (SKIP)")
                skipped_count += 1
                continue

            # Select best primary image and gallery images
            primary_url, gallery_urls = select_best_primary_image(images)

            # Update the product (convert gallery_urls to JSON string for PostgreSQL)
            await session.execute(
                text("""
                    UPDATE products
                    SET image_url = :primary_url, images = CAST(:gallery_urls AS jsonb)
                    WHERE id = :product_id
                """),
                {
                    "primary_url": primary_url,
                    "gallery_urls": json.dumps(gallery_urls),
                    "product_id": product_id
                }
            )

            print(f"  {sku}: {len(images)} images found, primary updated, {len(gallery_urls)} in gallery (OK)")
            updated_count += 1

        await session.commit()

        print("-" * 60)
        print(f"Migration complete: {updated_count} products updated, {skipped_count} skipped")

    await engine.dispose()
    return True


if __name__ == "__main__":
    success = asyncio.run(run_migration())
    sys.exit(0 if success else 1)
