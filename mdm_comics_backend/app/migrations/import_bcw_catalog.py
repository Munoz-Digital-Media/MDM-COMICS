"""
BCW Catalog Import Migration

Imports BCW products from Excel catalog into bcw_product_mappings table
and links S3 images to each product.

Run: python -m app.migrations.import_bcw_catalog
"""
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

# Paths
CATALOG_PATH = Path(__file__).parent.parent.parent / "data" / "bcw_catalog.xlsx"

# S3 configuration
S3_BUCKET = "mdm-comics-assets"
S3_REGION = "us-east-2"
S3_BASE_URL = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com"


async def import_bcw_catalog():
    """
    Import BCW catalog from Excel into database.

    Creates bcw_product_mappings entries and links S3 images.
    """
    print("=" * 60)
    print("BCW CATALOG IMPORT")
    print("=" * 60)

    # Read catalog
    try:
        df = pd.read_excel(CATALOG_PATH, header=1)
        df = df.dropna(subset=['BCW-SKU', 'MDM-SKU'])
        print(f"Loaded {len(df)} products from catalog")
    except Exception as e:
        print(f"ERROR: Failed to read catalog: {e}")
        return

    async with AsyncSessionLocal() as db:
        imported = 0
        updated = 0
        errors = []

        for _, row in df.iterrows():
            bcw_sku = str(row['BCW-SKU']).strip()
            mdm_sku = str(row['MDM-SKU']).strip()
            bcw_name = str(row.get('BCW-NAME', '')).strip() if pd.notna(row.get('BCW-NAME')) else None
            bcw_url = str(row.get('URL', '')).strip() if pd.notna(row.get('URL')) else None
            bcw_category = str(row.get('CATEGORY', '')).strip() if pd.notna(row.get('CATEGORY')) else None

            # Build S3 image URL (primary image is 00_)
            s3_image_url = f"{S3_BASE_URL}/bcw-products/{mdm_sku}/00_{bcw_sku.lower()}.jpg"

            try:
                # Check if exists
                result = await db.execute(text("""
                    SELECT id FROM bcw_product_mappings WHERE mdm_sku = :mdm_sku
                """), {"mdm_sku": mdm_sku})
                existing = result.fetchone()

                if existing:
                    # Update existing
                    await db.execute(text("""
                        UPDATE bcw_product_mappings
                        SET bcw_sku = :bcw_sku,
                            product_name = :product_name,
                            bcw_category = :bcw_category,
                            updated_at = NOW()
                        WHERE mdm_sku = :mdm_sku
                    """), {
                        "mdm_sku": mdm_sku,
                        "bcw_sku": bcw_sku,
                        "product_name": bcw_name,
                        "bcw_category": bcw_category,
                    })
                    updated += 1
                    print(f"  Updated: {mdm_sku}")
                else:
                    # Insert new
                    await db.execute(text("""
                        INSERT INTO bcw_product_mappings (
                            mdm_sku, bcw_sku, product_name, bcw_category,
                            is_active, is_dropship_only, sync_inventory,
                            imported_at, imported_from, created_at, updated_at
                        ) VALUES (
                            :mdm_sku, :bcw_sku, :product_name, :bcw_category,
                            true, true, true,
                            NOW(), 'bcw_catalog_excel', NOW(), NOW()
                        )
                    """), {
                        "mdm_sku": mdm_sku,
                        "bcw_sku": bcw_sku,
                        "product_name": bcw_name,
                        "bcw_category": bcw_category,
                    })
                    imported += 1
                    print(f"  Imported: {mdm_sku} -> {bcw_name}")

            except Exception as e:
                errors.append(f"{mdm_sku}: {e}")
                print(f"  ERROR: {mdm_sku}: {e}")

        await db.commit()

        print("\n" + "=" * 60)
        print("IMPORT SUMMARY")
        print("=" * 60)
        print(f"New imports:  {imported}")
        print(f"Updated:      {updated}")
        print(f"Errors:       {len(errors)}")

        # Now create/update products table entries
        print("\n" + "=" * 60)
        print("CREATING PRODUCTS WITH IMAGES")
        print("=" * 60)

        products_created = 0
        products_updated = 0

        for _, row in df.iterrows():
            bcw_sku = str(row['BCW-SKU']).strip()
            mdm_sku = str(row['MDM-SKU']).strip()
            bcw_name = str(row.get('BCW-NAME', '')).strip() if pd.notna(row.get('BCW-NAME')) else mdm_sku

            # S3 image URL
            s3_image_url = f"{S3_BASE_URL}/bcw-products/{mdm_sku}/00_{bcw_sku.lower()}.jpg"

            try:
                # Check if product exists
                result = await db.execute(text("""
                    SELECT id FROM products WHERE sku = :sku
                """), {"sku": mdm_sku})
                existing = result.fetchone()

                if existing:
                    # Update image URL
                    await db.execute(text("""
                        UPDATE products
                        SET image_url = :image_url,
                            updated_at = NOW()
                        WHERE sku = :sku
                    """), {
                        "sku": mdm_sku,
                        "image_url": s3_image_url,
                    })
                    products_updated += 1
                    print(f"  Updated product image: {mdm_sku}")
                else:
                    # Create new product
                    await db.execute(text("""
                        INSERT INTO products (
                            sku, name, description, image_url,
                            is_active, created_at, updated_at
                        ) VALUES (
                            :sku, :name, :description, :image_url,
                            true, NOW(), NOW()
                        )
                    """), {
                        "sku": mdm_sku,
                        "name": bcw_name,
                        "description": f"BCW {bcw_name}",
                        "image_url": s3_image_url,
                    })
                    products_created += 1
                    print(f"  Created product: {mdm_sku} -> {bcw_name}")

            except Exception as e:
                print(f"  ERROR creating product {mdm_sku}: {e}")

        await db.commit()

        print("\n" + "=" * 60)
        print("PRODUCTS SUMMARY")
        print("=" * 60)
        print(f"Products created: {products_created}")
        print(f"Products updated: {products_updated}")

        # Final verification
        result = await db.execute(text("SELECT COUNT(*) FROM bcw_product_mappings"))
        mapping_count = result.scalar()

        result = await db.execute(text("SELECT COUNT(*) FROM products WHERE sku LIKE 'MDM-%'"))
        product_count = result.scalar()

        print("\n" + "=" * 60)
        print("FINAL VERIFICATION")
        print("=" * 60)
        print(f"bcw_product_mappings: {mapping_count} records")
        print(f"products (MDM-*):     {product_count} records")
        print("\nImport complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(import_bcw_catalog())
