"""
Price Analytics Service

v1.5.0: Outreach System - Price movement reports from changelog data
"""
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Optional
from dataclasses import dataclass
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.utils import utcnow

logger = logging.getLogger(__name__)


@dataclass
class PriceMover:
    """Single price movement record."""
    entity_type: str  # funko, comic
    entity_id: int
    name: str
    image_url: Optional[str]
    price_old: Decimal
    price_new: Decimal
    change_dollars: Decimal
    change_percent: float
    context: Optional[str] = None


@dataclass
class PriceMoversReport:
    """Weekly price movers report."""
    generated_at: datetime
    period_start: datetime
    period_end: datetime
    winners: List[PriceMover]
    losers: List[PriceMover]
    notable: List[PriceMover]


class PriceAnalyticsService:
    """Generate price movement reports from changelog data."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_weekly_movers(
        self,
        entity_type: str = "all",
        limit: int = 5,
    ) -> PriceMoversReport:
        """
        Get top winners and losers for the week.

        Aggregates multiple price changes per entity to get NET change.
        """
        now = utcnow()
        week_ago = now - timedelta(days=7)

        # Build entity filter
        entity_filter = ""
        if entity_type != "all":
            entity_filter = "AND entity_type = :entity_type"

        # Query for net changes per entity
        query = f"""
            WITH entity_changes AS (
                SELECT
                    entity_type,
                    entity_id,
                    entity_name,
                    field_name,
                    FIRST_VALUE(old_value) OVER (
                        PARTITION BY entity_type, entity_id, field_name
                        ORDER BY changed_at ASC
                    ) as first_old,
                    LAST_VALUE(new_value) OVER (
                        PARTITION BY entity_type, entity_id, field_name
                        ORDER BY changed_at ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as last_new
                FROM price_changelog
                WHERE changed_at >= :week_ago
                {entity_filter}
            ),
            net_changes AS (
                SELECT DISTINCT
                    entity_type,
                    entity_id,
                    entity_name,
                    MAX(first_old) as price_old,
                    MAX(last_new) as price_new,
                    MAX(last_new) - MAX(first_old) as change_dollars,
                    CASE
                        WHEN MAX(first_old) > 0
                        THEN ((MAX(last_new) - MAX(first_old)) / MAX(first_old)) * 100
                        ELSE 0
                    END as change_percent
                FROM entity_changes
                WHERE first_old IS NOT NULL AND last_new IS NOT NULL
                GROUP BY entity_type, entity_id, entity_name
            )
            SELECT * FROM net_changes
            WHERE ABS(change_percent) > 1
            ORDER BY change_percent {{order}}
            LIMIT :limit
        """

        params = {"week_ago": week_ago, "limit": limit}
        if entity_type != "all":
            params["entity_type"] = entity_type

        # Get winners
        winners_query = query.format(order="DESC")
        winners_result = await self.db.execute(text(winners_query), params)

        winners = []
        for row in winners_result.fetchall():
            if row.change_percent and row.change_percent > 0:
                winners.append(await self._row_to_price_mover(row))

        # Get losers
        losers_query = query.format(order="ASC")
        losers_result = await self.db.execute(text(losers_query), params)

        losers = []
        for row in losers_result.fetchall():
            if row.change_percent and row.change_percent < 0:
                losers.append(await self._row_to_price_mover(row))

        return PriceMoversReport(
            generated_at=now,
            period_start=week_ago,
            period_end=now,
            winners=winners,
            losers=losers,
            notable=[],  # TODO: Implement notable movers
        )

    async def get_daily_movers(
        self,
        entity_type: str = "all",
        limit: int = 5,
    ) -> PriceMoversReport:
        """
        Get top winners and losers for today (24 hours).

        Used for daily social media posts (4:30 PM EST schedule).
        """
        now = utcnow()
        day_ago = now - timedelta(days=1)

        # Build entity filter
        entity_filter = ""
        if entity_type != "all":
            entity_filter = "AND entity_type = :entity_type"

        # Query for net changes per entity (last 24 hours)
        query = f"""
            WITH entity_changes AS (
                SELECT
                    entity_type,
                    entity_id,
                    entity_name,
                    field_name,
                    FIRST_VALUE(old_value) OVER (
                        PARTITION BY entity_type, entity_id, field_name
                        ORDER BY changed_at ASC
                    ) as first_old,
                    LAST_VALUE(new_value) OVER (
                        PARTITION BY entity_type, entity_id, field_name
                        ORDER BY changed_at ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as last_new
                FROM price_changelog
                WHERE changed_at >= :day_ago
                {entity_filter}
            ),
            net_changes AS (
                SELECT DISTINCT
                    entity_type,
                    entity_id,
                    entity_name,
                    MAX(first_old) as price_old,
                    MAX(last_new) as price_new,
                    MAX(last_new) - MAX(first_old) as change_dollars,
                    CASE
                        WHEN MAX(first_old) > 0
                        THEN ((MAX(last_new) - MAX(first_old)) / MAX(first_old)) * 100
                        ELSE 0
                    END as change_percent
                FROM entity_changes
                WHERE first_old IS NOT NULL AND last_new IS NOT NULL
                GROUP BY entity_type, entity_id, entity_name
            )
            SELECT * FROM net_changes
            WHERE ABS(change_percent) > 0.5
            ORDER BY change_percent {{order}}
            LIMIT :limit
        """

        params = {"day_ago": day_ago, "limit": limit}
        if entity_type != "all":
            params["entity_type"] = entity_type

        # Get winners
        winners_query = query.format(order="DESC")
        winners_result = await self.db.execute(text(winners_query), params)

        winners = []
        for row in winners_result.fetchall():
            if row.change_percent and row.change_percent > 0:
                winners.append(await self._row_to_price_mover(row))

        # Get losers
        losers_query = query.format(order="ASC")
        losers_result = await self.db.execute(text(losers_query), params)

        losers = []
        for row in losers_result.fetchall():
            if row.change_percent and row.change_percent < 0:
                losers.append(await self._row_to_price_mover(row))

        return PriceMoversReport(
            generated_at=now,
            period_start=day_ago,
            period_end=now,
            winners=winners,
            losers=losers,
            notable=[],
        )

    async def _row_to_price_mover(self, row) -> PriceMover:
        """Convert DB row to PriceMover with image lookup."""
        image_url = await self._get_entity_image(row.entity_type, row.entity_id)

        return PriceMover(
            entity_type=row.entity_type,
            entity_id=row.entity_id,
            name=row.entity_name or "Unknown",
            image_url=image_url,
            price_old=Decimal(str(row.price_old)) if row.price_old else Decimal("0"),
            price_new=Decimal(str(row.price_new)) if row.price_new else Decimal("0"),
            change_dollars=Decimal(str(row.change_dollars)) if row.change_dollars else Decimal("0"),
            change_percent=float(row.change_percent) if row.change_percent else 0.0,
        )

    async def _get_entity_image(self, entity_type: str, entity_id: int) -> Optional[str]:
        """Look up image URL for entity."""
        try:
            if entity_type == "funko":
                result = await self.db.execute(
                    text("SELECT image_url FROM funkos WHERE id = :id"),
                    {"id": entity_id}
                )
            elif entity_type == "comic":
                result = await self.db.execute(
                    text("SELECT image FROM comic_issues WHERE id = :id"),
                    {"id": entity_id}
                )
            else:
                return None

            row = result.fetchone()
            return row[0] if row else None
        except Exception as e:
            logger.warning(f"Failed to get entity image: {e}")
            return None

    async def generate_blurb(
        self,
        mover: PriceMover,
        context: Optional[str] = None,
    ) -> str:
        """Generate human-readable blurb for price mover."""
        direction = "up" if mover.change_percent > 0 else "down"
        percent = abs(mover.change_percent)

        base_blurb = (
            f"{mover.name} is {direction} {percent:.1f}% this week, "
            f"from ${mover.price_old:.2f} to ${mover.price_new:.2f}."
        )

        if context:
            base_blurb += f" {context}"

        return base_blurb
