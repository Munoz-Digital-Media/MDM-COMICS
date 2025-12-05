#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Import PriceCharting Comics Data into Database

Creates the comics table and imports all comic book data from PriceCharting.
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

CSV_PATH = r"F:\apps\mdm_comics\backend\pricecharting_comics.csv"


def parse_cents_to_dollars(value: str) -> float | None:
    """Parse price in cents to dollars."""
    if not value or value.strip() == '':
        return None
    try:
        cents = int(value)
        return round(cents / 100, 2)
    except:
        return None


def extract_issue_number(product_name: str) -> str | None:
    """Extract issue number from product name like 'Amazing Spider-Man #300 (1988)' -> '300'."""
    match = re.search(r'#(\d+)', product_name)
    if match:
        return match.group(1)
    return None


def extract_year(product_name: str) -> int | None:
    """Extract year from product name like 'Amazing Spider-Man #300 (1988)' -> 1988."""
    match = re.search(r'\((\d{4})\)', product_name)
    if match:
        return int(match.group(1))
    return None


def extract_series_name(console_name: str) -> str | None:
    """Extract series name from console-name like 'Comic Books Amazing Spider-Man' -> 'Amazing Spider-Man'."""
    if not console_name:
        return None
    # Remove "Comic Books " prefix
    if console_name.startswith("Comic Books "):
        return console_name[12:].strip()
    return console_name


def create_handle(title: str, pc_id: str = None) -> str:
    """Create a URL-friendly handle from title, with PC ID suffix for uniqueness."""
    handle = title.lower().strip()
    handle = re.sub(r'[^\w\s-]', '', handle)
    handle = re.sub(r'\s+', '-', handle)
    handle = re.sub(r'-+', '-', handle)
    # Add pricecharting ID to ensure uniqueness
    if pc_id:
        handle = f"{handle[:180]}-pc{pc_id}"
    return handle[:200]


async def ensure_comic_issues_columns():
    """Ensure comic_issues table has needed columns."""
    columns_to_add = [
        ("pricecharting_id", "INTEGER UNIQUE"),
        ("price_loose", "DECIMAL(12,2)"),
        ("price_cib", "DECIMAL(12,2)"),
        ("price_new", "DECIMAL(12,2)"),
        ("price_graded", "DECIMAL(12,2)"),
        ("price_bgs_10", "DECIMAL(12,2)"),
        ("price_cgc_98", "DECIMAL(12,2)"),
        ("price_cgc_96", "DECIMAL(12,2)"),
        ("asin", "VARCHAR(20)"),
        ("sales_volume", "INTEGER"),
        ("handle", "VARCHAR(255)"),
        ("year", "INTEGER"),
        ("series_name", "VARCHAR(255)"),
    ]

    async with engine.begin() as conn:
        for col_name, col_type in columns_to_add:
            try:
                await conn.execute(text(
                    f"ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
            except:
                pass

    print("comic_issues table ready!")


async def import_comics():
    """Import comics from CSV into comic_issues table."""
    print("=" * 70)
    print("PRICECHARTING COMICS IMPORT -> comic_issues")
    print("=" * 70)

    # Ensure columns exist
    await ensure_comic_issues_columns()

    # Read CSV
    records = []
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Only include Comic Book genre
            if row.get('genre') == 'Comic Book' or 'Comic Books' in row.get('console-name', ''):
                records.append(row)

    print(f"Loaded {len(records)} comic records from CSV")

    async with AsyncSessionLocal() as db:
        # Get existing pricecharting IDs to avoid duplicates
        result = await db.execute(text(
            "SELECT pricecharting_id FROM comic_issues WHERE pricecharting_id IS NOT NULL"
        ))
        existing_ids = {row[0] for row in result.fetchall()}
        print(f"Found {len(existing_ids)} existing comics in database")

        # Insert records
        added = 0
        updated = 0
        errors = 0

        for row in records:
            try:
                pc_id = int(row.get('id', 0)) if row.get('id') else None
                if not pc_id:
                    continue

                title = row.get('product-name', '')
                if not title:
                    continue

                handle = create_handle(title, pc_id)
                console_name = row.get('console-name', '')
                series = extract_series_name(console_name)
                issue_number = extract_issue_number(title)
                year = extract_year(title)

                # Parse prices (in cents -> dollars)
                price_loose = parse_cents_to_dollars(row.get('loose-price', ''))
                price_cib = parse_cents_to_dollars(row.get('cib-price', ''))
                price_new = parse_cents_to_dollars(row.get('new-price', ''))
                price_graded = parse_cents_to_dollars(row.get('graded-price', ''))
                price_bgs_10 = parse_cents_to_dollars(row.get('bgs-10-price', ''))
                price_cgc_98 = parse_cents_to_dollars(row.get('condition-18-price', ''))  # 9.8
                price_cgc_96 = parse_cents_to_dollars(row.get('condition-17-price', ''))  # 9.6

                # Other fields
                upc = row.get('upc', '').strip() or None
                asin = row.get('asin', '').strip() or None
                sales_volume = int(row.get('sales-volume', 0)) if row.get('sales-volume') else None

                # Parse release date
                release_date = None
                rd_str = row.get('release-date', '')
                if rd_str:
                    try:
                        release_date = datetime.strptime(rd_str, '%Y-%m-%d').date()
                    except:
                        pass

                if pc_id in existing_ids:
                    # Update existing record
                    await db.execute(text("""
                        UPDATE comic_issues SET
                            issue_name = :title,
                            series_name = :series,
                            number = :issue_number,
                            year = :year,
                            price_loose = :price_loose,
                            price_cib = :price_cib,
                            price_new = :price_new,
                            price_graded = :price_graded,
                            price_bgs_10 = :price_bgs_10,
                            price_cgc_98 = :price_cgc_98,
                            price_cgc_96 = :price_cgc_96,
                            upc = COALESCE(:upc, upc),
                            asin = COALESCE(:asin, asin),
                            store_date = COALESCE(:release_date, store_date),
                            sales_volume = :sales_volume,
                            updated_at = NOW()
                        WHERE pricecharting_id = :pc_id
                    """), {
                        "title": title,
                        "series": series,
                        "issue_number": issue_number,
                        "year": year,
                        "price_loose": price_loose,
                        "price_cib": price_cib,
                        "price_new": price_new,
                        "price_graded": price_graded,
                        "price_bgs_10": price_bgs_10,
                        "price_cgc_98": price_cgc_98,
                        "price_cgc_96": price_cgc_96,
                        "upc": upc,
                        "asin": asin,
                        "release_date": release_date,
                        "sales_volume": sales_volume,
                        "pc_id": pc_id,
                    })
                    updated += 1
                else:
                    # Insert new record into comic_issues
                    await db.execute(text("""
                        INSERT INTO comic_issues (
                            issue_name, handle, series_name, number, year,
                            price_loose, price_cib, price_new, price_graded,
                            price_bgs_10, price_cgc_98, price_cgc_96,
                            upc, asin, store_date, sales_volume,
                            pricecharting_id, created_at, updated_at
                        ) VALUES (
                            :title, :handle, :series, :issue_number, :year,
                            :price_loose, :price_cib, :price_new, :price_graded,
                            :price_bgs_10, :price_cgc_98, :price_cgc_96,
                            :upc, :asin, :release_date, :sales_volume,
                            :pc_id, NOW(), NOW()
                        )
                    """), {
                        "title": title,
                        "handle": handle,
                        "series": series,
                        "issue_number": issue_number,
                        "year": year,
                        "price_loose": price_loose,
                        "price_cib": price_cib,
                        "price_new": price_new,
                        "price_graded": price_graded,
                        "price_bgs_10": price_bgs_10,
                        "price_cgc_98": price_cgc_98,
                        "price_cgc_96": price_cgc_96,
                        "upc": upc,
                        "asin": asin,
                        "release_date": release_date,
                        "sales_volume": sales_volume,
                        "pc_id": pc_id,
                    })
                    added += 1

                if (added + updated) % 500 == 0:
                    await db.commit()
                    print(f"Progress: {added} added, {updated} updated...")

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"Error: {e}")

        await db.commit()

        # Get final count
        result = await db.execute(text("SELECT COUNT(*) FROM comic_issues"))
        total_count = result.scalar()

    print(f"\n{'=' * 70}")
    print("IMPORT COMPLETE!")
    print("=" * 70)
    print(f"CSV Records:        {len(records)}")
    print(f"Added:              {added}")
    print(f"Updated:            {updated}")
    print(f"Errors:             {errors}")
    print(f"Total in Database:  {total_count}")
    print("=" * 70)

    # Show sample high-value comics
    async with AsyncSessionLocal() as db:
        result = await db.execute(text("""
            SELECT issue_name, series_name, price_loose, price_graded
            FROM comic_issues
            WHERE price_loose IS NOT NULL
            ORDER BY price_loose DESC
            LIMIT 10
        """))
        print("\nTop 10 Most Valuable Comics (Loose Price):")
        for row in result.fetchall():
            title, series, loose, graded = row
            title_str = (title[:50] if title else "")
            loose_str = f"${loose:,.2f}" if loose else "N/A"
            graded_str = f"${graded:,.2f}" if graded else "N/A"
            print(f"  {title_str:50s} | Loose: {loose_str:>12} | Graded: {graded_str:>12}")


if __name__ == "__main__":
    asyncio.run(import_comics())
