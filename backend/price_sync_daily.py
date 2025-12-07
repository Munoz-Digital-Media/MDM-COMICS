#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Daily Price Sync System v1.1.0

Syncs pricing data from PriceCharting for Funkos and Comics.
Per constitution_db.json Section 5: "Critical tables track change provenance (who, when, reason)."

v1.1.0 CRITICAL CHANGES:
- Replaced blocking requests.get() with async httpx via ResilientHTTPClient
- Proper exponential backoff with jitter to PREVENT BANS
- 429 Retry-After header respect
- Circuit breaker for repeated failures
- Idempotent changelog inserts via upsert

Creates price_changelog table to track:
- What changed (entity, field, old_value, new_value)
- When (changed_at)
- Why (reason: "daily_sync", "api_update", etc.)
- Source (data_source: "pricecharting")
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from decimal import Decimal
from typing import Optional
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# Add app to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.http_client import get_pricecharting_client, ResilientHTTPClient

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# PriceCharting API - Token from environment only
PC_API_TOKEN = os.getenv("PRICECHARTING_API_TOKEN", "")
PC_BASE_URL = "https://www.pricecharting.com/api/product"

if not PC_API_TOKEN:
    logger.error("PRICECHARTING_API_TOKEN not set. Price sync will fail.")


async def ensure_changelog_table():
    """Create price_changelog table if not exists.

    Per constitution_db.json:
    - Primary key, not null for required columns
    - Track change provenance (who, when, reason)
    - snake_case enforced
    
    v1.1.0: Added unique constraint for idempotency
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
            # v1.1.0: Index for weekly movers query (outreach)
            ("ix_price_changelog_weekly_movers", "entity_type, changed_at, change_pct"),
        ]
        for idx_name, cols in indexes:
            try:
                await conn.execute(text(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON price_changelog({cols})"
                ))
            except Exception:
                pass
                
        # v1.1.0: Unique constraint for idempotency - prevent duplicate entries on restart
        try:
            await conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ix_price_changelog_idempotent 
                ON price_changelog(entity_type, entity_id, field_name, sync_batch_id)
                WHERE sync_batch_id IS NOT NULL
            """))
        except Exception:
            pass

    logger.info("price_changelog table ready!")


def parse_cents_to_dollars(value) -> Optional[float]:
    """Parse price in cents to dollars."""
    if value is None:
        return None
    try:
        cents = int(value)
        return round(cents / 100, 2)
    except (ValueError, TypeError):
        return None


async def fetch_pricecharting_product(
    client: ResilientHTTPClient,
    pc_id: int
) -> Optional[dict]:
    """
    Fetch single product from PriceCharting API.
    
    Uses ResilientHTTPClient for:
    - Automatic retries with exponential backoff
    - Rate limiting (1 req/sec, 3 burst max)
    - 429 Retry-After respect
    - Circuit breaker
    """
    try:
        response = await client.get(
            PC_BASE_URL,
            params={"t": PC_API_TOKEN, "id": pc_id},
        )
        return response.json()
    except Exception as e:
        logger.warning(f"Failed to fetch PC product {pc_id}: {e}")
        return None


async def upsert_changelog_entry(
    db: AsyncSession,
    entity_type: str,
    entity_id: int,
    entity_name: str,
    field_name: str,
    old_value: Optional[Decimal],
    new_value: Optional[Decimal],
    change_pct: Optional[float],
    batch_id: str,
) -> None:
    """
    Insert changelog entry with idempotency.
    
    v1.1.0: Uses ON CONFLICT DO NOTHING to prevent duplicate entries
    if the sync is restarted mid-run.
    """
    await db.execute(text("""
        INSERT INTO price_changelog (
            entity_type, entity_id, entity_name, field_name,
            old_value, new_value, change_pct,
            data_source, reason, sync_batch_id, changed_at
        ) VALUES (
            :entity_type, :entity_id, :entity_name, :field_name,
            :old_value, :new_value, :change_pct,
            'pricecharting', 'daily_sync', CAST(:batch_id AS UUID), NOW()
        )
        ON CONFLICT (entity_type, entity_id, field_name, sync_batch_id) 
        WHERE sync_batch_id IS NOT NULL
        DO NOTHING
    """), {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "entity_name": entity_name[:500] if entity_name else None,
        "field_name": field_name,
        "old_value": old_value,
        "new_value": new_value,
        "change_pct": change_pct,
        "batch_id": batch_id,
    })


async def sync_funko_prices(
    db: AsyncSession, 
    client: ResilientHTTPClient,
    batch_id: str
) -> dict:
    """
    Sync Funko prices from PriceCharting.
    
    Rate limiting is handled by ResilientHTTPClient - no manual sleeps needed.
    """
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

    logger.info(f"Checking {len(funkos)} Funkos for price updates...")

    for funko_id, title, pc_id, old_loose, old_cib, old_new in funkos:
        stats["checked"] += 1

        try:
            # ResilientHTTPClient handles all rate limiting and retries
            data = await fetch_pricecharting_product(client, pc_id)
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
                # Log changes to changelog (idempotent)
                for change in changes:
                    await upsert_changelog_entry(
                        db=db,
                        entity_type="funko",
                        entity_id=funko_id,
                        entity_name=title,
                        field_name=change["field_name"],
                        old_value=change["old_value"],
                        new_value=change["new_value"],
                        change_pct=change["change_pct"],
                        batch_id=batch_id,
                    )
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

        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                logger.error(f"Error syncing Funko {pc_id}: {e}")

        # Commit every 100 records for progress visibility
        # Note: This creates partial update risk on crash - acceptable tradeoff
        # for long-running syncs. Future: use savepoints.
        if stats["checked"] % 100 == 0:
            await db.commit()
            logger.info(f"Progress: {stats['checked']} checked, {stats['updated']} updated, {stats['changes']} changes logged")

    await db.commit()
    return stats


async def sync_comic_prices(
    db: AsyncSession,
    client: ResilientHTTPClient,
    batch_id: str
) -> dict:
    """
    Sync Comic prices from PriceCharting.
    
    Rate limiting is handled by ResilientHTTPClient.
    """
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

    logger.info(f"Checking {len(comics)} Comics for price updates...")

    for comic_id, title, pc_id, old_loose, old_cib, old_new, old_graded in comics:
        stats["checked"] += 1

        try:
            data = await fetch_pricecharting_product(client, pc_id)
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
                # Log changes to changelog (idempotent)
                for change in changes:
                    await upsert_changelog_entry(
                        db=db,
                        entity_type="comic",
                        entity_id=comic_id,
                        entity_name=title,
                        field_name=change["field_name"],
                        old_value=change["old_value"],
                        new_value=change["new_value"],
                        change_pct=change["change_pct"],
                        batch_id=batch_id,
                    )
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

        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                logger.error(f"Error syncing Comic {pc_id}: {e}")

        if stats["checked"] % 100 == 0:
            await db.commit()
            logger.info(f"Progress: {stats['checked']} checked, {stats['updated']} updated, {stats['changes']} changes logged")

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


async def get_significant_changes(
    db: AsyncSession, 
    batch_id: str, 
    threshold_pct: float = 10.0
) -> list:
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
    """Run daily price sync with resilient HTTP client."""
    import uuid

    logger.info("=" * 70)
    logger.info("DAILY PRICE SYNC v1.1.0")
    logger.info(f"Started: {datetime.now().isoformat()}")
    logger.info("=" * 70)

    # Create changelog table
    await ensure_changelog_table()

    # Generate batch ID for this sync run
    batch_id = str(uuid.uuid4())
    logger.info(f"Sync Batch ID: {batch_id}")

    # Create resilient HTTP client for PriceCharting
    # This client handles ALL rate limiting, retries, and backoff
    async with get_pricecharting_client() as client:
        async with AsyncSessionLocal() as db:
            # Sync Funkos
            logger.info("-" * 70)
            logger.info("SYNCING FUNKO PRICES")
            logger.info("-" * 70)
            funko_stats = await sync_funko_prices(db, client, batch_id)

            # Sync Comics
            logger.info("-" * 70)
            logger.info("SYNCING COMIC PRICES")
            logger.info("-" * 70)
            comic_stats = await sync_comic_prices(db, client, batch_id)

            # Get summary
            logger.info("=" * 70)
            logger.info("SYNC SUMMARY")
            logger.info("=" * 70)

            summary = await get_sync_summary(db, batch_id)

            logger.info(f"Funkos:")
            logger.info(f"  Checked: {funko_stats['checked']}")
            logger.info(f"  Updated: {funko_stats['updated']}")
            logger.info(f"  Changes Logged: {funko_stats['changes']}")
            logger.info(f"  Errors: {funko_stats['errors']}")

            logger.info(f"Comics:")
            logger.info(f"  Checked: {comic_stats['checked']}")
            logger.info(f"  Updated: {comic_stats['updated']}")
            logger.info(f"  Changes Logged: {comic_stats['changes']}")
            logger.info(f"  Errors: {comic_stats['errors']}")

            # Show significant changes
            significant = await get_significant_changes(db, batch_id, threshold_pct=10.0)
            if significant:
                logger.info("=" * 70)
                logger.info("SIGNIFICANT PRICE CHANGES (>10%)")
                logger.info("=" * 70)
                for item in significant:
                    direction = "↑" if item["change_pct"] > 0 else "↓"
                    name = (item['name'][:50] if item['name'] else 'Unknown')
                    logger.info(f"  [{direction}] {name}")
                    logger.info(f"      {item['field']}: ${item['old']:.2f} -> ${item['new']:.2f} ({item['change_pct']:+.1f}%)")

            # Final counts
            result = await db.execute(text(
                "SELECT COUNT(*) FROM price_changelog WHERE sync_batch_id = CAST(:batch_id AS UUID)"
            ), {"batch_id": batch_id})
            total_changes = result.scalar()

            logger.info("=" * 70)
            logger.info("SYNC COMPLETE!")
            logger.info(f"Total changes logged: {total_changes}")
            logger.info(f"Batch ID: {batch_id}")
            logger.info(f"Finished: {datetime.now().isoformat()}")
            logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
