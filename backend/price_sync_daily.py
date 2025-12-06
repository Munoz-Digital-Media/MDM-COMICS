#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Daily Price Sync System

Syncs pricing data from PriceCharting for Funkos and Comics.
Per constitution_db.json Section 5: "Critical tables track change provenance (who, when, reason)."

Creates price_changelog table to track:
- What changed (entity, field, old_value, new_value)
- When (changed_at)
- Why (reason: "daily_sync", "api_update", etc.)
- Source (data_source: "pricecharting")
"""

import asyncio
import os
import re
import requests
from datetime import datetime, date
from decimal import Decimal
from typing import Optional
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

# PriceCharting API - BLOCK-001: Token must be set via environment variable
# IMPORTANT: The old hardcoded token has been rotated. Get new token from PriceCharting dashboard.
PC_API_TOKEN = os.getenv("PRICECHARTING_API_TOKEN", "")
PC_BASE_URL = "https://www.pricecharting.com/api/product"

if not PC_API_TOKEN:
    print("WARNING: PRICECHARTING_API_TOKEN not set. Price sync will fail.")


async def ensure_changelog_table():
    """Create price_changelog table if not exists.

    Per constitution_db.json:
    - Primary key, not null for required columns
    - Track change provenance (who, when, reason)
    - snake_case enforced
    """
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS price_changelog (
                id SERIAL PRIMARY KEY,
                entity_type VARCHAR(50) NOT NULL,
                entity_id INTEGER NOT NULL,
                entity_name VARCHAR(500),
                field_name VARCHAR(100) NOT NULL,
                old_value DECIMAL(12,2),
                new_value DECIMAL(12,2),
                change_pct DECIMAL(8,2),
                data_source VARCHAR(50) NOT NULL DEFAULT 'pricecharting',
                reason VARCHAR(100) NOT NULL DEFAULT 'daily_sync',
                changed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                sync_batch_id UUID
            )
        """))

        # Indexes for efficient querying
        indexes = [
            ("ix_price_changelog_entity", "entity_type, entity_id"),
            ("ix_price_changelog_changed_at", "changed_at DESC"),
            ("ix_price_changelog_batch", "sync_batch_id"),
        ]
        for idx_name, cols in indexes:
            try:
                await conn.execute(text(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON price_changelog({cols})"
                ))
            except:
                pass

    print("price_changelog table ready!")


def parse_cents_to_dollars(value) -> Optional[float]:
    """Parse price in cents to dollars."""
    if value is None:
        return None
    try:
        cents = int(value)
        return round(cents / 100, 2)
    except:
        return None


async def fetch_pricecharting_product(pc_id: int) -> Optional[dict]:
    """Fetch single product from PriceCharting API."""
    try:
        response = requests.get(
            PC_BASE_URL,
            params={"t": PC_API_TOKEN, "id": pc_id},
            timeout=15
        )
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        pass  # Rate limiting or network issues
    return None


async def sync_funko_prices(db: AsyncSession, batch_id: str) -> dict:
    """Sync Funko prices from PriceCharting."""
    stats = {"checked": 0, "updated": 0, "changes": 0, "errors": 0}

    # Get Funkos with pricecharting_id
    result = await db.execute(text("""
        SELECT id, title, pricecharting_id, price_loose, price_cib, price_new
        FROM funkos
        WHERE pricecharting_id IS NOT NULL
        ORDER BY updated_at ASC
        LIMIT 500
    """))
    funkos = result.fetchall()

    print(f"Checking {len(funkos)} Funkos for price updates...")

    for funko_id, title, pc_id, old_loose, old_cib, old_new in funkos:
        stats["checked"] += 1

        try:
            data = await fetch_pricecharting_product(pc_id)
            if not data:
                continue

            # Parse new prices
            new_loose = parse_cents_to_dollars(data.get("loose-price"))
            new_cib = parse_cents_to_dollars(data.get("cib-price"))
            new_new = parse_cents_to_dollars(data.get("new-price"))

            changes = []

            # Check for changes
            price_fields = [
                ("price_loose", old_loose, new_loose),
                ("price_cib", old_cib, new_cib),
                ("price_new", old_new, new_new),
            ]

            for field_name, old_val, new_val in price_fields:
                if new_val is None:
                    continue

                old_float = float(old_val) if old_val else 0
                new_float = float(new_val)

                # Only log if there's an actual change
                if abs(old_float - new_float) > 0.01:
                    change_pct = None
                    if old_float > 0:
                        change_pct = round(((new_float - old_float) / old_float) * 100, 2)

                    changes.append({
                        "field_name": field_name,
                        "old_value": old_val,
                        "new_value": new_val,
                        "change_pct": change_pct,
                    })

            if changes:
                # Log changes to changelog
                for change in changes:
                    await db.execute(text("""
                        INSERT INTO price_changelog (
                            entity_type, entity_id, entity_name, field_name,
                            old_value, new_value, change_pct,
                            data_source, reason, sync_batch_id
                        ) VALUES (
                            'funko', :entity_id, :entity_name, :field_name,
                            :old_value, :new_value, :change_pct,
                            'pricecharting', 'daily_sync', CAST(:batch_id AS UUID)
                        )
                    """), {
                        "entity_id": funko_id,
                        "entity_name": title[:500] if title else None,
                        "field_name": change["field_name"],
                        "old_value": change["old_value"],
                        "new_value": change["new_value"],
                        "change_pct": change["change_pct"],
                        "batch_id": batch_id,
                    })
                    stats["changes"] += 1

                # Update the funko record
                await db.execute(text("""
                    UPDATE funkos SET
                        price_loose = COALESCE(:new_loose, price_loose),
                        price_cib = COALESCE(:new_cib, price_cib),
                        price_new = COALESCE(:new_new, price_new),
                        updated_at = NOW()
                    WHERE id = :id
                """), {
                    "new_loose": new_loose,
                    "new_cib": new_cib,
                    "new_new": new_new,
                    "id": funko_id,
                })
                stats["updated"] += 1

            # Rate limiting
            if stats["checked"] % 10 == 0:
                await asyncio.sleep(0.5)

        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                print(f"  Error syncing Funko {pc_id}: {e}")

        if stats["checked"] % 100 == 0:
            await db.commit()
            print(f"  Progress: {stats['checked']} checked, {stats['updated']} updated, {stats['changes']} changes logged")

    await db.commit()
    return stats


async def sync_comic_prices(db: AsyncSession, batch_id: str) -> dict:
    """Sync Comic prices from PriceCharting."""
    stats = {"checked": 0, "updated": 0, "changes": 0, "errors": 0}

    # Get Comics with pricecharting_id
    result = await db.execute(text("""
        SELECT id, issue_name, pricecharting_id,
               price_loose, price_cib, price_new, price_graded
        FROM comic_issues
        WHERE pricecharting_id IS NOT NULL
        ORDER BY updated_at ASC
        LIMIT 500
    """))
    comics = result.fetchall()

    print(f"Checking {len(comics)} Comics for price updates...")

    for comic_id, title, pc_id, old_loose, old_cib, old_new, old_graded in comics:
        stats["checked"] += 1

        try:
            data = await fetch_pricecharting_product(pc_id)
            if not data:
                continue

            # Parse new prices
            new_loose = parse_cents_to_dollars(data.get("loose-price"))
            new_cib = parse_cents_to_dollars(data.get("cib-price"))
            new_new = parse_cents_to_dollars(data.get("new-price"))
            new_graded = parse_cents_to_dollars(data.get("graded-price"))

            changes = []

            # Check for changes
            price_fields = [
                ("price_loose", old_loose, new_loose),
                ("price_cib", old_cib, new_cib),
                ("price_new", old_new, new_new),
                ("price_graded", old_graded, new_graded),
            ]

            for field_name, old_val, new_val in price_fields:
                if new_val is None:
                    continue

                old_float = float(old_val) if old_val else 0
                new_float = float(new_val)

                if abs(old_float - new_float) > 0.01:
                    change_pct = None
                    if old_float > 0:
                        change_pct = round(((new_float - old_float) / old_float) * 100, 2)

                    changes.append({
                        "field_name": field_name,
                        "old_value": old_val,
                        "new_value": new_val,
                        "change_pct": change_pct,
                    })

            if changes:
                # Log changes to changelog
                for change in changes:
                    await db.execute(text("""
                        INSERT INTO price_changelog (
                            entity_type, entity_id, entity_name, field_name,
                            old_value, new_value, change_pct,
                            data_source, reason, sync_batch_id
                        ) VALUES (
                            'comic', :entity_id, :entity_name, :field_name,
                            :old_value, :new_value, :change_pct,
                            'pricecharting', 'daily_sync', CAST(:batch_id AS UUID)
                        )
                    """), {
                        "entity_id": comic_id,
                        "entity_name": title[:500] if title else None,
                        "field_name": change["field_name"],
                        "old_value": change["old_value"],
                        "new_value": change["new_value"],
                        "change_pct": change["change_pct"],
                        "batch_id": batch_id,
                    })
                    stats["changes"] += 1

                # Update the comic record
                await db.execute(text("""
                    UPDATE comic_issues SET
                        price_loose = COALESCE(:new_loose, price_loose),
                        price_cib = COALESCE(:new_cib, price_cib),
                        price_new = COALESCE(:new_new, price_new),
                        price_graded = COALESCE(:new_graded, price_graded),
                        updated_at = NOW()
                    WHERE id = :id
                """), {
                    "new_loose": new_loose,
                    "new_cib": new_cib,
                    "new_new": new_new,
                    "new_graded": new_graded,
                    "id": comic_id,
                })
                stats["updated"] += 1

            # Rate limiting
            if stats["checked"] % 10 == 0:
                await asyncio.sleep(0.5)

        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                print(f"  Error syncing Comic {pc_id}: {e}")

        if stats["checked"] % 100 == 0:
            await db.commit()
            print(f"  Progress: {stats['checked']} checked, {stats['updated']} updated, {stats['changes']} changes logged")

    await db.commit()
    return stats


async def get_sync_summary(db: AsyncSession, batch_id: str) -> dict:
    """Get summary of price changes from this sync batch."""
    result = await db.execute(text("""
        SELECT
            entity_type,
            COUNT(DISTINCT entity_id) as entities_changed,
            COUNT(*) as total_changes,
            AVG(ABS(change_pct)) as avg_change_pct,
            MAX(change_pct) as max_increase_pct,
            MIN(change_pct) as max_decrease_pct
        FROM price_changelog
        WHERE sync_batch_id = CAST(:batch_id AS UUID)
        GROUP BY entity_type
    """), {"batch_id": batch_id})

    summary = {}
    for row in result.fetchall():
        entity_type, entities, changes, avg_pct, max_inc, max_dec = row
        summary[entity_type] = {
            "entities_changed": entities,
            "total_changes": changes,
            "avg_change_pct": float(avg_pct) if avg_pct else 0,
            "max_increase_pct": float(max_inc) if max_inc else 0,
            "max_decrease_pct": float(max_dec) if max_dec else 0,
        }
    return summary


async def get_significant_changes(db: AsyncSession, batch_id: str, threshold_pct: float = 10.0) -> list:
    """Get significant price changes (>threshold%) for alerting."""
    result = await db.execute(text("""
        SELECT entity_type, entity_name, field_name, old_value, new_value, change_pct
        FROM price_changelog
        WHERE sync_batch_id = CAST(:batch_id AS UUID)
          AND ABS(change_pct) >= :threshold
        ORDER BY ABS(change_pct) DESC
        LIMIT 20
    """), {"batch_id": batch_id, "threshold": threshold_pct})

    return [
        {
            "type": row[0],
            "name": row[1],
            "field": row[2],
            "old": float(row[3]) if row[3] else 0,
            "new": float(row[4]) if row[4] else 0,
            "change_pct": float(row[5]) if row[5] else 0,
        }
        for row in result.fetchall()
    ]


async def main():
    """Run daily price sync."""
    import uuid

    print("=" * 70)
    print("DAILY PRICE SYNC")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 70)

    # Create changelog table
    await ensure_changelog_table()

    # Generate batch ID for this sync run
    batch_id = str(uuid.uuid4())
    print(f"Sync Batch ID: {batch_id}")

    async with AsyncSessionLocal() as db:
        # Sync Funkos
        print("\n" + "-" * 70)
        print("SYNCING FUNKO PRICES")
        print("-" * 70)
        funko_stats = await sync_funko_prices(db, batch_id)

        # Sync Comics
        print("\n" + "-" * 70)
        print("SYNCING COMIC PRICES")
        print("-" * 70)
        comic_stats = await sync_comic_prices(db, batch_id)

        # Get summary
        print("\n" + "=" * 70)
        print("SYNC SUMMARY")
        print("=" * 70)

        summary = await get_sync_summary(db, batch_id)

        print(f"\nFunkos:")
        print(f"  Checked: {funko_stats['checked']}")
        print(f"  Updated: {funko_stats['updated']}")
        print(f"  Changes Logged: {funko_stats['changes']}")
        print(f"  Errors: {funko_stats['errors']}")

        print(f"\nComics:")
        print(f"  Checked: {comic_stats['checked']}")
        print(f"  Updated: {comic_stats['updated']}")
        print(f"  Changes Logged: {comic_stats['changes']}")
        print(f"  Errors: {comic_stats['errors']}")

        # Show significant changes
        significant = await get_significant_changes(db, batch_id, threshold_pct=10.0)
        if significant:
            print(f"\n{'=' * 70}")
            print("SIGNIFICANT PRICE CHANGES (>10%)")
            print("=" * 70)
            for item in significant:
                direction = "+" if item["change_pct"] > 0 else "-"
                print(f"  [{direction}] {item['name'][:50] if item['name'] else 'Unknown'}")
                print(f"    {item['field']}: ${item['old']:.2f} -> ${item['new']:.2f} ({item['change_pct']:+.1f}%)")

        # Final counts
        result = await db.execute(text("SELECT COUNT(*) FROM price_changelog WHERE sync_batch_id = CAST(:batch_id AS UUID)"), {"batch_id": batch_id})
        total_changes = result.scalar()

        print(f"\n{'=' * 70}")
        print("SYNC COMPLETE!")
        print(f"Total changes logged: {total_changes}")
        print(f"Batch ID: {batch_id}")
        print(f"Finished: {datetime.now().isoformat()}")
        print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
