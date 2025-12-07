"""
Analytics Jobs

v1.5.0: Outreach System - Price changelog sync and reporting
"""
import logging
from datetime import timedelta
from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.core.utils import utcnow
from app.models.price_changelog import PriceChangelog
from app.services.price_analytics import PriceAnalyticsService

logger = logging.getLogger(__name__)


async def sync_price_changelog(ctx: dict) -> dict:
    """
    Sync price changes from funko/comic tables to changelog.

    Compares current prices against last recorded values.
    Records changes to price_changelog for analytics.
    """
    async with get_db_session() as db:
        changes_recorded = 0

        # Sync funko prices
        funko_changes = await _sync_funko_prices(db)
        changes_recorded += funko_changes

        # Sync comic prices
        comic_changes = await _sync_comic_prices(db)
        changes_recorded += comic_changes

        logger.info(
            f"Price sync complete: {funko_changes} funko, "
            f"{comic_changes} comic changes"
        )

        return {
            "status": "complete",
            "funko_changes": funko_changes,
            "comic_changes": comic_changes,
            "total": changes_recorded,
        }


async def _sync_funko_prices(db: AsyncSession) -> int:
    """Sync funko price changes."""
    changes = 0

    # Get funkos with price changes since last sync
    # Compare against most recent changelog entry
    query = text("""
        WITH last_prices AS (
            SELECT DISTINCT ON (entity_id)
                entity_id,
                new_value as last_price
            FROM price_changelog
            WHERE entity_type = 'funko'
            ORDER BY entity_id, changed_at DESC
        )
        SELECT
            f.id,
            f.title,
            f.price_new,
            lp.last_price
        FROM funkos f
        LEFT JOIN last_prices lp ON lp.entity_id = f.id
        WHERE f.price_new IS NOT NULL
          AND (lp.last_price IS NULL OR f.price_new != lp.last_price)
    """)

    result = await db.execute(query)

    for row in result.fetchall():
        funko_id, title, current_price, last_price = row

        if last_price is None:
            # First record - skip, we only want changes
            continue

        changelog = PriceChangelog(
            entity_type="funko",
            entity_id=funko_id,
            entity_name=title,
            field_name="price_new",
            old_value=float(last_price),
            new_value=float(current_price),
            changed_at=utcnow(),
        )
        db.add(changelog)
        changes += 1

    return changes


async def _sync_comic_prices(db: AsyncSession) -> int:
    """Sync comic issue price changes."""
    changes = 0

    query = text("""
        WITH last_prices AS (
            SELECT DISTINCT ON (entity_id)
                entity_id,
                new_value as last_price
            FROM price_changelog
            WHERE entity_type = 'comic'
            ORDER BY entity_id, changed_at DESC
        )
        SELECT
            ci.id,
            ci.issue_name,
            ci.price_new,
            lp.last_price
        FROM comic_issues ci
        LEFT JOIN last_prices lp ON lp.entity_id = ci.id
        WHERE ci.price_new IS NOT NULL
          AND (lp.last_price IS NULL OR ci.price_new != lp.last_price)
    """)

    result = await db.execute(query)

    for row in result.fetchall():
        issue_id, issue_name, current_price, last_price = row

        if last_price is None:
            continue

        changelog = PriceChangelog(
            entity_type="comic",
            entity_id=issue_id,
            entity_name=issue_name,
            field_name="price_new",
            old_value=float(last_price),
            new_value=float(current_price),
            changed_at=utcnow(),
        )
        db.add(changelog)
        changes += 1

    return changes


async def generate_weekly_report(ctx: dict) -> dict:
    """
    Generate weekly price movement report.

    Used for internal analytics and newsletter content.
    """
    async with get_db_session() as db:
        service = PriceAnalyticsService(db)

        # Get all entity types
        movers = await service.get_weekly_movers(entity_type="all", limit=10)

        # Generate blurbs for top movers
        winner_blurbs = []
        for mover in movers.winners[:3]:
            blurb = await service.generate_blurb(mover)
            winner_blurbs.append({
                "name": mover.name,
                "change_percent": mover.change_percent,
                "blurb": blurb,
            })

        loser_blurbs = []
        for mover in movers.losers[:3]:
            blurb = await service.generate_blurb(mover)
            loser_blurbs.append({
                "name": mover.name,
                "change_percent": mover.change_percent,
                "blurb": blurb,
            })

        report = {
            "generated_at": utcnow().isoformat(),
            "period_start": movers.period_start.isoformat(),
            "period_end": movers.period_end.isoformat(),
            "summary": {
                "total_winners": len(movers.winners),
                "total_losers": len(movers.losers),
            },
            "top_winners": winner_blurbs,
            "top_losers": loser_blurbs,
        }

        logger.info(
            f"Weekly report generated: {len(movers.winners)} winners, "
            f"{len(movers.losers)} losers"
        )

        return {
            "status": "complete",
            "report": report,
        }
