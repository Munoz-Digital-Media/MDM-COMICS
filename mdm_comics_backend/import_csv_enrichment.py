#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Import Funko enrichment data from CSV archive.
Matches records by handle and updates missing fields.
"""

import asyncio
import csv
import os
import ast
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

CSV_PATH = r"E:\01_filez\syslogs\archive_extracted\data.csv"


def parse_list_field(value: str) -> list:
    """Parse a string representation of a list."""
    if not value or value == "[]":
        return []
    try:
        return ast.literal_eval(value)
    except:
        return []


def extract_category(interest_list: list) -> str:
    """Extract category from interest field."""
    if not interest_list:
        return None
    # First item is usually the main category
    return interest_list[0] if interest_list else None


def extract_license(license_list: list) -> str:
    """Extract license from license field."""
    if not license_list:
        return None
    return license_list[0] if license_list else None


def normalize_title(title: str) -> str:
    """Normalize title for matching."""
    import re
    # Remove special chars, lowercase, strip
    title = title.lower().strip()
    title = re.sub(r'[^\w\s]', '', title)
    title = re.sub(r'\s+', ' ', title)
    return title


async def import_csv_data():
    """Import enrichment data from CSV."""
    print("=" * 60)
    print("CSV ENRICHMENT IMPORT")
    print("=" * 60)

    # Read CSV
    records = []
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)

    print(f"Loaded {len(records)} records from CSV")

    # Build lookup by normalized title
    csv_by_title = {}
    for row in records:
        title = row.get('title', '')
        if title:
            norm = normalize_title(title)
            csv_by_title[norm] = row

    print(f"Built {len(csv_by_title)} normalized title lookups")

    async with AsyncSessionLocal() as db:
        # Get all funkos that need enrichment (by title)
        result = await db.execute(text("""
            SELECT id, title, handle FROM funkos
            WHERE category IS NULL OR category = ''
        """))
        db_records = [(row[0], row[1], row[2]) for row in result.fetchall()]
        print(f"Found {len(db_records)} Funkos needing enrichment in DB")

        # Match and update
        updated = 0
        matched = 0

        for funko_id, db_title, db_handle in db_records:
            # Try to match by normalized title
            norm_title = normalize_title(db_title)
            csv_row = csv_by_title.get(norm_title)

            if not csv_row:
                continue

            matched += 1

            # Parse fields
            interest = parse_list_field(csv_row.get('interest', '[]'))
            license_list = parse_list_field(csv_row.get('license', '[]'))

            category = extract_category(interest)
            license_val = extract_license(license_list)
            product_type = csv_row.get('product_type') or csv_row.get('form_factor')

            # Only update if we have data
            if category or license_val or product_type:
                await db.execute(text("""
                    UPDATE funkos SET
                        category = COALESCE(:category, category),
                        license = COALESCE(:license, license),
                        product_type = COALESCE(:product_type, product_type),
                        updated_at = NOW()
                    WHERE id = :id AND (category IS NULL OR category = '')
                """), {
                    "category": category,
                    "license": license_val,
                    "product_type": product_type,
                    "id": funko_id
                })
                updated += 1

                if updated % 100 == 0:
                    await db.commit()
                    print(f"Updated {updated} records...")

        await db.commit()

    print(f"\n" + "=" * 60)
    print(f"IMPORT COMPLETE")
    print(f"CSV Records: {len(records)}")
    print(f"Matched: {matched}")
    print(f"Updated: {updated}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(import_csv_data())
