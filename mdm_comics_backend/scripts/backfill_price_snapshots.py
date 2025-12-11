"""
Historical Price Snapshots Backfill Script v1.7.0

Generates historical snapshots from existing price_changelog data.

This script:
1. Finds the earliest price_changelog entry
2. For each day from that date to yesterday:
   a. Reconstructs entity prices as of that date (walking changelog)
   b. Creates snapshot rows
3. Calculates ML features retrospectively (after snapshots exist)
4. Supports resume capability with progress checkpoints

Run: python scripts/backfill_price_snapshots.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--entity-type funko|comic]

Per constitution_db.json §7: Batch operations with progress logging.
"""
import asyncio
import argparse
import logging
import sys
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.core.config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BackfillProgress:
    """Track backfill progress for resume capability."""

    def __init__(self, checkpoint_file: str = ".backfill_checkpoint.json"):
        self.checkpoint_file = Path(__file__).parent / checkpoint_file
        self.data: Dict[str, Any] = {}
        self._load()

    def _load(self):
        import json
        if self.checkpoint_file.exists():
            try:
                self.data = json.loads(self.checkpoint_file.read_text())
                logger.info(f"Loaded checkpoint: last_date={self.data.get('last_date')}")
            except Exception as e:
                logger.warning(f"Could not load checkpoint: {e}")
                self.data = {}

    def save(self, last_date: date, stats: Dict[str, int]):
        import json
        self.data = {
            "last_date": last_date.isoformat(),
            "total_snapshots": stats.get("total", 0),
            "updated_at": datetime.utcnow().isoformat()
        }
        self.checkpoint_file.write_text(json.dumps(self.data, indent=2))

    def get_last_date(self) -> Optional[date]:
        if "last_date" in self.data:
            return date.fromisoformat(self.data["last_date"])
        return None

    def clear(self):
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        self.data = {}


async def get_changelog_date_range(db: AsyncSession) -> tuple[Optional[date], Optional[date]]:
    """Get the earliest and latest changelog dates."""
    result = await db.execute(text("""
        SELECT
            MIN(changed_at::date) as earliest,
            MAX(changed_at::date) as latest
        FROM price_changelog
    """))
    row = result.fetchone()

    if not row or not row.earliest:
        return None, None

    return row.earliest, row.latest


async def get_entities_with_prices(db: AsyncSession, entity_type: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get all entities that have had price data."""
    type_filter = "AND entity_type = :type" if entity_type else ""
    params = {"type": entity_type} if entity_type else {}

    # Get entities from current tables
    entities = []

    # Funkos with pricecharting_id
    if not entity_type or entity_type == "funko":
        result = await db.execute(text("""
            SELECT id, pricecharting_id, price_loose, price_cib, price_new
            FROM funkos
            WHERE pricecharting_id IS NOT NULL
        """))
        for row in result.fetchall():
            entities.append({
                "entity_type": "funko",
                "entity_id": row.id,
                "pricecharting_id": row.pricecharting_id,
                "current_prices": {
                    "price_loose": float(row.price_loose) if row.price_loose else None,
                    "price_cib": float(row.price_cib) if row.price_cib else None,
                    "price_new": float(row.price_new) if row.price_new else None,
                }
            })

    # Comics with pricecharting_id
    if not entity_type or entity_type == "comic":
        result = await db.execute(text("""
            SELECT id, pricecharting_id, price_loose, price_cib, price_new,
                   price_graded, price_bgs_10, price_cgc_98, price_cgc_96
            FROM comic_issues
            WHERE pricecharting_id IS NOT NULL
        """))
        for row in result.fetchall():
            entities.append({
                "entity_type": "comic",
                "entity_id": row.id,
                "pricecharting_id": row.pricecharting_id,
                "current_prices": {
                    "price_loose": float(row.price_loose) if row.price_loose else None,
                    "price_cib": float(row.price_cib) if row.price_cib else None,
                    "price_new": float(row.price_new) if row.price_new else None,
                    "price_graded": float(row.price_graded) if row.price_graded else None,
                    "price_bgs_10": float(row.price_bgs_10) if row.price_bgs_10 else None,
                    "price_cgc_98": float(row.price_cgc_98) if row.price_cgc_98 else None,
                    "price_cgc_96": float(row.price_cgc_96) if row.price_cgc_96 else None,
                }
            })

    logger.info(f"Found {len(entities)} entities with pricecharting_id")
    return entities


async def reconstruct_price_at_date(
    db: AsyncSession,
    entity_type: str,
    entity_id: int,
    target_date: date,
    current_prices: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Reconstruct prices as of a specific date by walking the changelog backwards.

    Algorithm:
    1. Start with current prices
    2. Walk changelog from today backwards to target_date
    3. For each change, revert it (set price to old_value)
    4. Result is prices as they were on target_date
    """
    # Start with current prices
    prices = dict(current_prices)

    # Get all changes AFTER target_date (we'll revert these)
    result = await db.execute(text("""
        SELECT field_name, old_value, new_value, changed_at
        FROM price_changelog
        WHERE entity_type = :type
        AND entity_id = :id
        AND changed_at::date > :target_date
        ORDER BY changed_at DESC
    """), {
        "type": entity_type,
        "id": entity_id,
        "target_date": target_date
    })

    changes = result.fetchall()

    # Revert each change (walking backwards in time)
    for change in changes:
        field = change.field_name
        old_val = float(change.old_value) if change.old_value else None

        if field in prices:
            prices[field] = old_val

    return prices


async def check_price_changed(
    db: AsyncSession,
    entity_type: str,
    entity_id: int,
    check_date: date
) -> bool:
    """Check if any price changed on the given date."""
    result = await db.execute(text("""
        SELECT COUNT(*) as changes
        FROM price_changelog
        WHERE entity_type = :type
        AND entity_id = :id
        AND changed_at::date = :date
    """), {
        "type": entity_type,
        "id": entity_id,
        "date": check_date
    })
    row = result.fetchone()
    return (row.changes or 0) > 0


async def get_days_since_change(
    db: AsyncSession,
    entity_type: str,
    entity_id: int,
    as_of_date: date
) -> Optional[int]:
    """Get days since last price change as of a date."""
    result = await db.execute(text("""
        SELECT MAX(changed_at::date) as last_change
        FROM price_changelog
        WHERE entity_type = :type
        AND entity_id = :id
        AND changed_at::date <= :date
    """), {
        "type": entity_type,
        "id": entity_id,
        "date": as_of_date
    })
    row = result.fetchone()

    if not row or not row.last_change:
        return None

    return (as_of_date - row.last_change).days


async def create_snapshot_for_date(
    db: AsyncSession,
    entity: Dict[str, Any],
    snapshot_date: date
) -> bool:
    """Create a snapshot for an entity on a specific date."""
    entity_type = entity["entity_type"]
    entity_id = entity["entity_id"]
    pricecharting_id = entity["pricecharting_id"]

    # Reconstruct prices as of this date
    prices = await reconstruct_price_at_date(
        db, entity_type, entity_id, snapshot_date, entity["current_prices"]
    )

    # Check if price changed on this date
    price_changed = await check_price_changed(db, entity_type, entity_id, snapshot_date)

    # Get days since last change
    days_since_change = await get_days_since_change(db, entity_type, entity_id, snapshot_date)

    try:
        if entity_type == "funko":
            await db.execute(text("""
                INSERT INTO price_snapshots (
                    snapshot_date, entity_type, entity_id, pricecharting_id,
                    price_loose, price_cib, price_new,
                    price_changed, days_since_change,
                    data_source, is_stale, created_at
                ) VALUES (
                    :date, 'funko', :id, :pc_id,
                    :loose, :cib, :new,
                    :changed, :days,
                    'pricecharting_backfill', FALSE, NOW()
                )
                ON CONFLICT (entity_type, entity_id, snapshot_date) DO NOTHING
            """), {
                "date": snapshot_date,
                "id": entity_id,
                "pc_id": pricecharting_id,
                "loose": prices.get("price_loose"),
                "cib": prices.get("price_cib"),
                "new": prices.get("price_new"),
                "changed": price_changed,
                "days": days_since_change,
            })
        else:  # comic
            await db.execute(text("""
                INSERT INTO price_snapshots (
                    snapshot_date, entity_type, entity_id, pricecharting_id,
                    price_loose, price_cib, price_new,
                    price_graded, price_bgs_10, price_cgc_98, price_cgc_96,
                    price_changed, days_since_change,
                    data_source, is_stale, created_at
                ) VALUES (
                    :date, 'comic', :id, :pc_id,
                    :loose, :cib, :new,
                    :graded, :bgs10, :cgc98, :cgc96,
                    :changed, :days,
                    'pricecharting_backfill', FALSE, NOW()
                )
                ON CONFLICT (entity_type, entity_id, snapshot_date) DO NOTHING
            """), {
                "date": snapshot_date,
                "id": entity_id,
                "pc_id": pricecharting_id,
                "loose": prices.get("price_loose"),
                "cib": prices.get("price_cib"),
                "new": prices.get("price_new"),
                "graded": prices.get("price_graded"),
                "bgs10": prices.get("price_bgs_10"),
                "cgc98": prices.get("price_cgc_98"),
                "cgc96": prices.get("price_cgc_96"),
                "changed": price_changed,
                "days": days_since_change,
            })

        return True

    except Exception as e:
        logger.error(f"Error creating snapshot for {entity_type}:{entity_id} @ {snapshot_date}: {e}")
        return False


async def backfill(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    entity_type: Optional[str] = None,
    batch_size: int = 100,
    resume: bool = True
):
    """
    Main backfill function.

    Args:
        start_date: First date to backfill (default: earliest changelog date)
        end_date: Last date to backfill (default: yesterday)
        entity_type: 'funko' or 'comic' or None for both
        batch_size: Commit every N entities
        resume: Resume from checkpoint if available
    """
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    progress = BackfillProgress()
    stats = {"total": 0, "days": 0, "errors": 0}

    async with async_session() as db:
        # Determine date range
        if not start_date or not end_date:
            earliest, latest = await get_changelog_date_range(db)
            if not earliest:
                logger.error("No price_changelog data found!")
                return

            if not start_date:
                start_date = earliest
            if not end_date:
                end_date = date.today() - timedelta(days=1)  # Yesterday

        # Resume from checkpoint if available
        if resume:
            checkpoint_date = progress.get_last_date()
            if checkpoint_date and checkpoint_date > start_date:
                logger.info(f"Resuming from checkpoint: {checkpoint_date}")
                start_date = checkpoint_date + timedelta(days=1)

        total_days = (end_date - start_date).days + 1
        logger.info(f"Backfilling {total_days} days: {start_date} to {end_date}")

        # Get all entities
        entities = await get_entities_with_prices(db, entity_type)
        if not entities:
            logger.error("No entities found with pricecharting_id!")
            return

        logger.info(f"Processing {len(entities)} entities")

        # Process each day
        current_date = start_date
        while current_date <= end_date:
            day_start = datetime.now()
            day_count = 0

            logger.info(f"Processing {current_date} ({stats['days']+1}/{total_days})")

            for i, entity in enumerate(entities):
                success = await create_snapshot_for_date(db, entity, current_date)
                if success:
                    day_count += 1
                    stats["total"] += 1
                else:
                    stats["errors"] += 1

                # Commit in batches
                if (i + 1) % batch_size == 0:
                    await db.commit()

            # Commit remaining
            await db.commit()

            # Update progress
            stats["days"] += 1
            progress.save(current_date, stats)

            elapsed = (datetime.now() - day_start).total_seconds()
            logger.info(f"  Created {day_count} snapshots in {elapsed:.1f}s")

            current_date += timedelta(days=1)

    await engine.dispose()

    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total days: {stats['days']}")
    logger.info(f"Total snapshots: {stats['total']}")
    logger.info(f"Errors: {stats['errors']}")

    # Clear checkpoint on successful completion
    if stats["errors"] == 0:
        progress.clear()
        logger.info("Checkpoint cleared (successful completion)")


async def update_ml_features(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    entity_type: Optional[str] = None
):
    """
    Update ML features for backfilled snapshots.

    This should be run AFTER backfill completes.
    """
    # Import here to avoid circular imports
    from app.services.price_ml_features import PriceMLFeaturesService

    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    if not end_date:
        end_date = date.today() - timedelta(days=1)
    if not start_date:
        start_date = end_date - timedelta(days=365)

    logger.info(f"Updating ML features: {start_date} to {end_date}")

    async with async_session() as db:
        service = PriceMLFeaturesService(db)

        current_date = start_date
        total_days = (end_date - start_date).days + 1
        day_num = 0

        while current_date <= end_date:
            day_num += 1
            logger.info(f"Updating features for {current_date} ({day_num}/{total_days})")

            stats = await service.update_ml_features_for_date(current_date, entity_type)
            logger.info(f"  Updated: {stats['updated']}, Errors: {stats['errors']}")

            current_date += timedelta(days=1)

    await engine.dispose()
    logger.info("ML feature update complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill historical price snapshots")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--entity-type", choices=["funko", "comic"], help="Entity type to backfill")
    parser.add_argument("--batch-size", type=int, default=100, help="Commit batch size")
    parser.add_argument("--no-resume", action="store_true", help="Don't resume from checkpoint")
    parser.add_argument("--update-features", action="store_true", help="Update ML features after backfill")
    parser.add_argument("--features-only", action="store_true", help="Only update ML features (skip backfill)")

    args = parser.parse_args()

    start = date.fromisoformat(args.start_date) if args.start_date else None
    end = date.fromisoformat(args.end_date) if args.end_date else None

    if args.features_only:
        asyncio.run(update_ml_features(start, end, args.entity_type))
    else:
        asyncio.run(backfill(
            start_date=start,
            end_date=end,
            entity_type=args.entity_type,
            batch_size=args.batch_size,
            resume=not args.no_resume
        ))

        if args.update_features:
            asyncio.run(update_ml_features(start, end, args.entity_type))
