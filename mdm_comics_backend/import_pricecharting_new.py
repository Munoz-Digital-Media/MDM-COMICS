#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PriceCharting Funko Import - ADD NEW RECORDS

Adds PriceCharting Funkos that don't exist in our database as NEW records.
This complements import_pricecharting.py which only updates existing records.
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
    title = re.sub(r'\s*#\d+\s*', ' ', title)
    title = title.lower().strip()
    title = re.sub(r'[^\w\s]', '', title)
    title = re.sub(r'\s+', ' ', title)
    return title.strip()


def create_handle(title: str, pc_id: int = None) -> str:
    """Create a URL-friendly handle from title, with PC ID suffix for uniqueness."""
    handle = title.lower().strip()
    handle = re.sub(r'[^\w\s-]', '', handle)
    handle = re.sub(r'\s+', '-', handle)
    handle = re.sub(r'-+', '-', handle)
    # Add pricecharting ID to ensure uniqueness
    if pc_id:
        handle = f"{handle[:180]}-pc{pc_id}"
    return handle[:200]  # Limit length


def extract_product_type(console_name: str) -> str | None:
    """Extract product type from console-name field."""
    if not console_name:
        return None
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
    return console_name


async def import_new_funkos():
    """Import NEW Funkos from PriceCharting that don't exist in DB."""
    print("=" * 70)
    print("PRICECHARTING - ADD NEW FUNKOS")
    print("=" * 70)

    # Read CSV
    records = []
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            console = row.get('console-name', '').lower()
            if 'funko' not in console and 'pop' not in console:
                continue
            records.append(row)

    print(f"Loaded {len(records)} Funko records from PriceCharting CSV")

    async with AsyncSessionLocal() as db:
        # Get all existing normalized titles from DB
        result = await db.execute(text("SELECT title FROM funkos"))
        existing_titles = {normalize_title(row[0]) for row in result.fetchall()}
        print(f"Found {len(existing_titles)} existing Funkos in database")

        # Also get existing pricecharting_ids to avoid duplicates
        result = await db.execute(text(
            "SELECT pricecharting_id FROM funkos WHERE pricecharting_id IS NOT NULL"
        ))
        existing_pc_ids = {row[0] for row in result.fetchall()}
        print(f"Found {len(existing_pc_ids)} existing PriceCharting IDs")

        # Find records to add
        to_add = []
        for row in records:
            title = row.get('product-name', '')
            if not title:
                continue

            norm_title = normalize_title(title)
            pc_id = int(row.get('id', 0)) if row.get('id') else None

            # Skip if title already exists or PC ID already exists
            if norm_title in existing_titles:
                continue
            if pc_id and pc_id in existing_pc_ids:
                continue

            to_add.append(row)

        print(f"Found {len(to_add)} NEW Funkos to add")

        if not to_add:
            print("No new Funkos to add!")
            return

        # Insert new records
        added = 0
        errors = 0

        for row in to_add:
            try:
                title = row.get('product-name', '')
                pc_id = int(row.get('id', 0)) if row.get('id') else None
                handle = create_handle(title, pc_id)
                console_name = row.get('console-name', '')
                product_type = extract_product_type(console_name)
                box_number = extract_box_number(title)
                genre = row.get('genre', '').strip() or None
                upc = row.get('upc', '').strip() or None

                price_loose = parse_price(row.get('loose-price', ''))
                price_cib = parse_price(row.get('cib-price', ''))
                price_new = parse_price(row.get('new-price', ''))

                # Parse release date
                release_date = None
                rd_str = row.get('release-date', '')
                if rd_str:
                    try:
                        release_date = datetime.strptime(rd_str, '%Y-%m-%d').date()
                    except (ValueError, TypeError):
                        pass

                # Insert the record
                await db.execute(text("""
                    INSERT INTO funkos (
                        title, handle, product_type, box_number,
                        category, license,
                        price_loose, price_cib, price_new,
                        upc, release_date, pricecharting_id,
                        created_at, updated_at
                    ) VALUES (
                        :title, :handle, :product_type, :box_number,
                        :category, :license,
                        :price_loose, :price_cib, :price_new,
                        :upc, :release_date, :pricecharting_id,
                        NOW(), NOW()
                    )
                """), {
                    "title": title,
                    "handle": handle,
                    "product_type": product_type,
                    "box_number": box_number,
                    "category": genre,
                    "license": genre,
                    "price_loose": price_loose,
                    "price_cib": price_cib,
                    "price_new": price_new,
                    "upc": upc,
                    "release_date": release_date,
                    "pricecharting_id": pc_id,
                })
                added += 1

                if added % 500 == 0:
                    await db.commit()
                    print(f"Added {added} records...")

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"Error adding '{title[:50]}': {e}")

        await db.commit()

        # Get final count
        result = await db.execute(text("SELECT COUNT(*) FROM funkos"))
        total_count = result.scalar()

    print(f"\n" + "=" * 70)
    print("IMPORT COMPLETE!")
    print("=" * 70)
    print(f"PriceCharting Records:  {len(records)}")
    print(f"Already in DB:          {len(existing_titles)}")
    print(f"New Records Added:      {added}")
    print(f"Errors:                 {errors}")
    print(f"Total Funkos in DB:     {total_count}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(import_new_funkos())
