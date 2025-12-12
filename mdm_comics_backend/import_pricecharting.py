#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PriceCharting Funko Import - THE ULTIMATE DATA SOURCE

Imports from PriceCharting CSV:
- Pricing data (loose, CIB, new)
- Genre (license)
- Product type
- Box numbers (extracted from product name)
- UPC codes
- Release dates
"""

import asyncio
import csv
import os
import re
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

CSV_PATH = r"F:\apps\mdm_comics\backend\pricecharting_funkos.csv"


def parse_price(price_str: str) -> float | None:
    """Parse price string like '$6.00' to float."""
    if not price_str or price_str.strip() == '':
        return None
    try:
        return float(price_str.replace('$', '').replace(',', '').strip())
    except (ValueError, TypeError):
        return None


def extract_box_number(product_name: str) -> str | None:
    """Extract box number from product name like 'Barb #28' -> '28'."""
    match = re.search(r'#(\d+)', product_name)
    if match:
        return match.group(1)
    return None


def normalize_title(title: str) -> str:
    """Normalize title for matching."""
    # Remove box number for matching
    title = re.sub(r'\s*#\d+\s*', ' ', title)
    # Remove special chars, lowercase, strip
    title = title.lower().strip()
    title = re.sub(r'[^\w\s]', '', title)
    title = re.sub(r'\s+', ' ', title)
    return title.strip()


def extract_product_type(console_name: str) -> str | None:
    """Extract product type from console-name field."""
    if not console_name:
        return None
    # Map PriceCharting console names to our product types
    cn = console_name.lower()
    if 'pop!' in cn or 'pop ' in cn or cn.startswith('funko pop'):
        return 'Pop!'
    if 'soda' in cn:
        return 'Soda'
    if 'mystery mini' in cn:
        return 'Mystery Minis'
    if 'dorbz' in cn:
        return 'Dorbz'
    if 'rock candy' in cn:
        return 'Rock Candy'
    if 'vynl' in cn:
        return 'Vynl'
    if 'hikari' in cn:
        return 'Hikari'
    if 'pint size' in cn:
        return 'Pint Size Heroes'
    if 'pocket pop' in cn:
        return 'Pocket Pop!'
    if 'bitty' in cn:
        return 'Bitty Pop!'
    # Default: use the console name as-is
    return console_name


async def add_price_columns():
    """Add price columns to funkos table if they don't exist."""
    columns = [
        ("price_loose", "DECIMAL(10,2)"),
        ("price_cib", "DECIMAL(10,2)"),
        ("price_new", "DECIMAL(10,2)"),
        ("upc", "VARCHAR(50)"),
        ("release_date", "DATE"),
        ("pricecharting_id", "INTEGER"),
    ]

    async with engine.begin() as conn:
        for col_name, col_type in columns:
            try:
                await conn.execute(text(
                    f"ALTER TABLE funkos ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
                print(f"Added column: {col_name}")
            except Exception as e:
                pass  # Column exists

        # Add index on pricecharting_id
        try:
            await conn.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_funkos_pricecharting_id ON funkos(pricecharting_id)"
            ))
        except Exception as e:
            # Index may already exist
            print(f"Note: Could not create index: {e}")
            pass

    print("Price columns ready!")


async def import_pricecharting():
    """Import PriceCharting data into funkos table."""
    print("=" * 70)
    print("PRICECHARTING FUNKO IMPORT")
    print("=" * 70)

    # Add columns first
    await add_price_columns()

    # Read CSV
    records = []
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip non-Funko entries
            console = row.get('console-name', '').lower()
            if 'funko' not in console and 'pop' not in console:
                continue
            records.append(row)

    print(f"Loaded {len(records)} Funko records from PriceCharting CSV")

    # Build lookup by normalized title
    pc_by_title = {}
    for row in records:
        title = row.get('product-name', '')
        if title:
            norm = normalize_title(title)
            if norm not in pc_by_title:  # Keep first match
                pc_by_title[norm] = row

    print(f"Built {len(pc_by_title)} normalized title lookups")

    async with AsyncSessionLocal() as db:
        # Get all funkos from DB
        result = await db.execute(text("""
            SELECT id, title FROM funkos
        """))
        db_records = [(row[0], row[1]) for row in result.fetchall()]
        print(f"Found {len(db_records)} Funkos in database")

        # Match and update
        updated = 0
        matched = 0
        price_updates = 0

        for funko_id, db_title in db_records:
            # Try to match by normalized title
            norm_title = normalize_title(db_title)
            pc_row = pc_by_title.get(norm_title)

            if not pc_row:
                continue

            matched += 1

            # Extract data
            price_loose = parse_price(pc_row.get('loose-price', ''))
            price_cib = parse_price(pc_row.get('cib-price', ''))
            price_new = parse_price(pc_row.get('new-price', ''))
            upc = pc_row.get('upc', '').strip() or None
            genre = pc_row.get('genre', '').strip() or None
            console_name = pc_row.get('console-name', '')
            product_type = extract_product_type(console_name)
            box_number = extract_box_number(pc_row.get('product-name', ''))
            pricecharting_id = int(pc_row.get('id', 0)) or None

            # Parse release date
            release_date = None
            rd_str = pc_row.get('release-date', '')
            if rd_str:
                try:
                    release_date = datetime.strptime(rd_str, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    pass

            # Update the record
            await db.execute(text("""
                UPDATE funkos SET
                    category = COALESCE(category, :genre),
                    license = COALESCE(license, :genre),
                    product_type = COALESCE(product_type, :product_type),
                    box_number = COALESCE(box_number, :box_number),
                    price_loose = :price_loose,
                    price_cib = :price_cib,
                    price_new = :price_new,
                    upc = COALESCE(upc, :upc),
                    release_date = COALESCE(release_date, :release_date),
                    pricecharting_id = :pricecharting_id,
                    updated_at = NOW()
                WHERE id = :id
            """), {
                "genre": genre,
                "product_type": product_type,
                "box_number": box_number,
                "price_loose": price_loose,
                "price_cib": price_cib,
                "price_new": price_new,
                "upc": upc,
                "release_date": release_date,
                "pricecharting_id": pricecharting_id,
                "id": funko_id
            })
            updated += 1

            if price_loose or price_cib or price_new:
                price_updates += 1

            if updated % 500 == 0:
                await db.commit()
                print(f"Updated {updated} records ({price_updates} with prices)...")

        await db.commit()

    print(f"\n" + "=" * 70)
    print("IMPORT COMPLETE!")
    print("=" * 70)
    print(f"Database Records:     {len(db_records)}")
    print(f"PriceCharting Matched:{matched}")
    print(f"Updated:              {updated}")
    print(f"With Price Data:      {price_updates}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(import_pricecharting())
