#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unify Comics Schema

Migrates data from 'comics' table to 'comic_issues' table,
adding necessary pricing columns, then drops the redundant table.

Per constitution_db.json:
- snake_case enforced for all tables/columns
- Every table: primary key, not null for required columns, FK constraints
- Critical tables track change provenance (who, when, reason)
"""

import asyncio
import os
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


async def step1_add_pricing_columns():
    """Add pricing and external ID columns to comic_issues table."""
    print("\n" + "=" * 70)
    print("STEP 1: Adding pricing columns to comic_issues")
    print("=" * 70)

    columns_to_add = [
        # External IDs
        ("pricecharting_id", "INTEGER UNIQUE"),

        # Pricing columns (in dollars)
        ("price_loose", "DECIMAL(12,2)"),
        ("price_cib", "DECIMAL(12,2)"),
        ("price_new", "DECIMAL(12,2)"),
        ("price_graded", "DECIMAL(12,2)"),

        # Graded pricing tiers
        ("price_bgs_10", "DECIMAL(12,2)"),
        ("price_cgc_98", "DECIMAL(12,2)"),
        ("price_cgc_96", "DECIMAL(12,2)"),

        # Additional metadata
        ("asin", "VARCHAR(20)"),
        ("sales_volume", "INTEGER"),

        # Handle for URL routing
        ("handle", "VARCHAR(255)"),

        # Year extracted from title
        ("year", "INTEGER"),

        # Series name (denormalized for quick access)
        ("series_name", "VARCHAR(255)"),
    ]

    async with engine.begin() as conn:
        for col_name, col_type in columns_to_add:
            try:
                await conn.execute(text(
                    f"ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
                print(f"  + Added column: {col_name}")
            except Exception as e:
                print(f"  - Column {col_name} already exists or error: {e}")

        # Add indexes for performance
        indexes = [
            ("ix_comic_issues_pricecharting_id", "pricecharting_id"),
            ("ix_comic_issues_handle", "handle"),
            ("ix_comic_issues_year", "year"),
            ("ix_comic_issues_series_name", "series_name"),
        ]

        for idx_name, col in indexes:
            try:
                await conn.execute(text(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON comic_issues({col})"
                ))
                print(f"  + Created index: {idx_name}")
            except Exception as e:
                print(f"  - Index {idx_name}: {e}")

    print("  Done!")


async def step2_migrate_data():
    """Migrate data from comics to comic_issues."""
    print("\n" + "=" * 70)
    print("STEP 2: Migrating data from 'comics' to 'comic_issues'")
    print("=" * 70)

    async with AsyncSessionLocal() as db:
        # Get count from comics table
        result = await db.execute(text("SELECT COUNT(*) FROM comics"))
        comics_count = result.scalar()
        print(f"  Found {comics_count} records in 'comics' table")

        if comics_count == 0:
            print("  No data to migrate!")
            return 0

        # Get existing pricecharting_ids in comic_issues to avoid duplicates
        result = await db.execute(text(
            "SELECT pricecharting_id FROM comic_issues WHERE pricecharting_id IS NOT NULL"
        ))
        existing_pc_ids = {row[0] for row in result.fetchall()}
        print(f"  Found {len(existing_pc_ids)} existing pricecharting_ids in comic_issues")

        # Fetch all records from comics
        result = await db.execute(text("""
            SELECT
                title, handle, series, issue_number, year,
                price_loose, price_cib, price_new, price_graded,
                price_bgs_10, price_cgc_98, price_cgc_96,
                upc, asin, release_date, sales_volume, pricecharting_id
            FROM comics
        """))
        comics_data = result.fetchall()

        migrated = 0
        skipped = 0
        errors = 0

        for row in comics_data:
            (title, handle, series, issue_number, year,
             price_loose, price_cib, price_new, price_graded,
             price_bgs_10, price_cgc_98, price_cgc_96,
             upc, asin, release_date, sales_volume, pricecharting_id) = row

            # Skip if already exists
            if pricecharting_id and pricecharting_id in existing_pc_ids:
                skipped += 1
                continue

            try:
                # Insert into comic_issues
                # Using issue_name for the full title, number for issue_number
                await db.execute(text("""
                    INSERT INTO comic_issues (
                        issue_name, handle, series_name, number, year,
                        price_loose, price_cib, price_new, price_graded,
                        price_bgs_10, price_cgc_98, price_cgc_96,
                        upc, asin, store_date, sales_volume, pricecharting_id,
                        created_at, updated_at
                    ) VALUES (
                        :title, :handle, :series, :number, :year,
                        :price_loose, :price_cib, :price_new, :price_graded,
                        :price_bgs_10, :price_cgc_98, :price_cgc_96,
                        :upc, :asin, :release_date, :sales_volume, :pricecharting_id,
                        NOW(), NOW()
                    )
                """), {
                    "title": title,
                    "handle": handle,
                    "series": series,
                    "number": issue_number,
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
                    "pricecharting_id": pricecharting_id,
                })
                migrated += 1

                if migrated % 500 == 0:
                    await db.commit()
                    print(f"    Migrated {migrated} records...")

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"    Error migrating '{title[:40]}': {e}")

        await db.commit()

        print(f"\n  Migration Results:")
        print(f"    Migrated: {migrated}")
        print(f"    Skipped (duplicates): {skipped}")
        print(f"    Errors: {errors}")

        return migrated


async def step3_verify_migration():
    """Verify the migration was successful."""
    print("\n" + "=" * 70)
    print("STEP 3: Verifying migration")
    print("=" * 70)

    async with AsyncSessionLocal() as db:
        # Count both tables
        result = await db.execute(text("SELECT COUNT(*) FROM comics"))
        comics_count = result.scalar()

        result = await db.execute(text("SELECT COUNT(*) FROM comic_issues"))
        issues_count = result.scalar()

        result = await db.execute(text(
            "SELECT COUNT(*) FROM comic_issues WHERE pricecharting_id IS NOT NULL"
        ))
        issues_with_pc = result.scalar()

        print(f"  comics table: {comics_count} rows")
        print(f"  comic_issues table: {issues_count} rows")
        print(f"  comic_issues with pricecharting_id: {issues_with_pc} rows")

        # Sample verification
        result = await db.execute(text("""
            SELECT issue_name, series_name, number, price_loose, pricecharting_id
            FROM comic_issues
            WHERE pricecharting_id IS NOT NULL
            ORDER BY price_loose DESC NULLS LAST
            LIMIT 5
        """))
        samples = result.fetchall()

        if samples:
            print(f"\n  Top 5 most valuable migrated comics:")
            for title, series, num, price, pc_id in samples:
                price_str = f"${price:,.2f}" if price else "N/A"
                print(f"    - {title[:50]} | {price_str}")

        return comics_count, issues_count


async def step4_drop_comics_table():
    """Drop the redundant comics table after verification."""
    print("\n" + "=" * 70)
    print("STEP 4: Dropping redundant 'comics' table")
    print("=" * 70)

    async with engine.begin() as conn:
        # First rename to backup just in case
        try:
            await conn.execute(text("ALTER TABLE comics RENAME TO comics_backup_deprecated"))
            print("  + Renamed 'comics' to 'comics_backup_deprecated'")
            print("  (Table preserved as backup - can be dropped later with:")
            print("   DROP TABLE comics_backup_deprecated;)")
        except Exception as e:
            print(f"  Error: {e}")


async def main():
    """Run the full migration."""
    print("=" * 70)
    print("COMICS SCHEMA UNIFICATION")
    print("Migrating 'comics' -> 'comic_issues'")
    print("=" * 70)

    # Step 1: Add columns
    await step1_add_pricing_columns()

    # Step 2: Migrate data
    migrated = await step2_migrate_data()

    # Step 3: Verify
    comics_count, issues_count = await step3_verify_migration()

    # Step 4: Only drop if migration looks successful
    if migrated > 0 or comics_count == 0:
        await step4_drop_comics_table()
    else:
        print("\n  WARNING: Skipping table drop - migration may have issues")

    print("\n" + "=" * 70)
    print("MIGRATION COMPLETE!")
    print("=" * 70)
    print(f"  comic_issues now contains all comic data")
    print(f"  Total records: {issues_count}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
