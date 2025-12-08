"""
Collection Valuation Service

v1.5.2: The Rack Report - Database-wide valuation for newsletter and analytics

Provides:
- Total database value (comics + funkos + inventory)
- Weekly change tracking
- Category breakdowns
- Portfolio summary for The Rack Report newsletter
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.utils import utcnow

logger = logging.getLogger(__name__)


@dataclass
class CategoryValuation:
    """Valuation for a single category."""
    category: str
    item_count: int
    total_value: Decimal
    priced_count: int  # Items with price data
    avg_value: Decimal
    min_value: Decimal
    max_value: Decimal


@dataclass
class ValuationChange:
    """Week-over-week change tracking."""
    current_value: Decimal
    previous_value: Decimal
    change_dollars: Decimal
    change_percent: float


@dataclass
class CollectionSummary:
    """Complete collection valuation summary."""
    generated_at: datetime

    # Grand totals
    total_value: Decimal
    total_items: int
    total_priced_items: int

    # By category
    comics: CategoryValuation
    funkos: CategoryValuation
    products: CategoryValuation  # Active inventory

    # Week-over-week change
    weekly_change: Optional[ValuationChange]

    # Top items
    most_valuable_comic: Optional[Dict[str, Any]]
    most_valuable_funko: Optional[Dict[str, Any]]


class CollectionValuationService:
    """
    Calculate total database value for The Rack Report.

    Uses price_new as primary valuation (best available market value).
    Falls back to price_cib, then price_loose for items without price_new.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_collection_summary(self) -> CollectionSummary:
        """Generate complete collection valuation summary."""
        now = utcnow()

        # Get current valuations
        comics = await self._get_comics_valuation()
        funkos = await self._get_funkos_valuation()
        products = await self._get_products_valuation()

        # Calculate totals
        total_value = comics.total_value + funkos.total_value + products.total_value
        total_items = comics.item_count + funkos.item_count + products.item_count
        total_priced = comics.priced_count + funkos.priced_count + products.priced_count

        # Get week-over-week change from snapshots
        weekly_change = await self._get_weekly_change()

        # Get most valuable items
        most_valuable_comic = await self._get_most_valuable("comic")
        most_valuable_funko = await self._get_most_valuable("funko")

        return CollectionSummary(
            generated_at=now,
            total_value=total_value,
            total_items=total_items,
            total_priced_items=total_priced,
            comics=comics,
            funkos=funkos,
            products=products,
            weekly_change=weekly_change,
            most_valuable_comic=most_valuable_comic,
            most_valuable_funko=most_valuable_funko,
        )

    async def _get_comics_valuation(self) -> CategoryValuation:
        """Calculate total comic collection value."""
        try:
            result = await self.db.execute(text("""
                SELECT
                    COUNT(*) as total_count,
                    COUNT(COALESCE(price_new, price_cib, price_loose)) as priced_count,
                    COALESCE(SUM(COALESCE(price_new, price_cib, price_loose)), 0) as total_value,
                    COALESCE(AVG(COALESCE(price_new, price_cib, price_loose)), 0) as avg_value,
                    COALESCE(MIN(COALESCE(price_new, price_cib, price_loose)), 0) as min_value,
                    COALESCE(MAX(COALESCE(price_new, price_cib, price_loose)), 0) as max_value
                FROM comic_issues
                WHERE COALESCE(price_new, price_cib, price_loose) IS NOT NULL
            """))
            row = result.fetchone()

            return CategoryValuation(
                category="comics",
                item_count=row[0] if row else 0,
                priced_count=row[1] if row else 0,
                total_value=Decimal(str(row[2])) if row and row[2] else Decimal("0"),
                avg_value=Decimal(str(row[3])) if row and row[3] else Decimal("0"),
                min_value=Decimal(str(row[4])) if row and row[4] else Decimal("0"),
                max_value=Decimal(str(row[5])) if row and row[5] else Decimal("0"),
            )
        except Exception as e:
            logger.error(f"Failed to get comics valuation: {e}")
            return CategoryValuation(
                category="comics",
                item_count=0,
                priced_count=0,
                total_value=Decimal("0"),
                avg_value=Decimal("0"),
                min_value=Decimal("0"),
                max_value=Decimal("0"),
            )

    async def _get_funkos_valuation(self) -> CategoryValuation:
        """Calculate total Funko collection value."""
        try:
            result = await self.db.execute(text("""
                SELECT
                    COUNT(*) as total_count,
                    COUNT(COALESCE(price_new, price_cib, price_loose)) as priced_count,
                    COALESCE(SUM(COALESCE(price_new, price_cib, price_loose)), 0) as total_value,
                    COALESCE(AVG(COALESCE(price_new, price_cib, price_loose)), 0) as avg_value,
                    COALESCE(MIN(COALESCE(price_new, price_cib, price_loose)), 0) as min_value,
                    COALESCE(MAX(COALESCE(price_new, price_cib, price_loose)), 0) as max_value
                FROM funkos
                WHERE COALESCE(price_new, price_cib, price_loose) IS NOT NULL
            """))
            row = result.fetchone()

            return CategoryValuation(
                category="funkos",
                item_count=row[0] if row else 0,
                priced_count=row[1] if row else 0,
                total_value=Decimal(str(row[2])) if row and row[2] else Decimal("0"),
                avg_value=Decimal(str(row[3])) if row and row[3] else Decimal("0"),
                min_value=Decimal(str(row[4])) if row and row[4] else Decimal("0"),
                max_value=Decimal(str(row[5])) if row and row[5] else Decimal("0"),
            )
        except Exception as e:
            logger.error(f"Failed to get funkos valuation: {e}")
            return CategoryValuation(
                category="funkos",
                item_count=0,
                priced_count=0,
                total_value=Decimal("0"),
                avg_value=Decimal("0"),
                min_value=Decimal("0"),
                max_value=Decimal("0"),
            )

    async def _get_products_valuation(self) -> CategoryValuation:
        """Calculate active inventory value."""
        try:
            result = await self.db.execute(text("""
                SELECT
                    COUNT(*) as total_count,
                    COUNT(price) as priced_count,
                    COALESCE(SUM(price * stock), 0) as total_value,
                    COALESCE(AVG(price), 0) as avg_value,
                    COALESCE(MIN(price), 0) as min_value,
                    COALESCE(MAX(price), 0) as max_value
                FROM products
                WHERE deleted_at IS NULL
                  AND stock > 0
            """))
            row = result.fetchone()

            return CategoryValuation(
                category="products",
                item_count=row[0] if row else 0,
                priced_count=row[1] if row else 0,
                total_value=Decimal(str(row[2])) if row and row[2] else Decimal("0"),
                avg_value=Decimal(str(row[3])) if row and row[3] else Decimal("0"),
                min_value=Decimal(str(row[4])) if row and row[4] else Decimal("0"),
                max_value=Decimal(str(row[5])) if row and row[5] else Decimal("0"),
            )
        except Exception as e:
            logger.error(f"Failed to get products valuation: {e}")
            return CategoryValuation(
                category="products",
                item_count=0,
                priced_count=0,
                total_value=Decimal("0"),
                avg_value=Decimal("0"),
                min_value=Decimal("0"),
                max_value=Decimal("0"),
            )

    async def _get_weekly_change(self) -> Optional[ValuationChange]:
        """Calculate week-over-week portfolio change from price snapshots."""
        try:
            week_ago = utcnow() - timedelta(days=7)
            today = utcnow().date()
            week_ago_date = week_ago.date()

            # Get current total from snapshots
            current_result = await self.db.execute(text("""
                SELECT COALESCE(SUM(COALESCE(price_new, price_cib, price_loose)), 0)
                FROM price_snapshots
                WHERE snapshot_date = (
                    SELECT MAX(snapshot_date) FROM price_snapshots
                )
            """))
            current_value = current_result.scalar() or Decimal("0")

            # Get week-ago total
            previous_result = await self.db.execute(text("""
                SELECT COALESCE(SUM(COALESCE(price_new, price_cib, price_loose)), 0)
                FROM price_snapshots
                WHERE snapshot_date = :week_ago
            """), {"week_ago": week_ago_date})
            previous_value = previous_result.scalar() or Decimal("0")

            if previous_value == 0:
                return None

            change_dollars = Decimal(str(current_value)) - Decimal(str(previous_value))
            change_percent = float(change_dollars / Decimal(str(previous_value)) * 100)

            return ValuationChange(
                current_value=Decimal(str(current_value)),
                previous_value=Decimal(str(previous_value)),
                change_dollars=change_dollars,
                change_percent=change_percent,
            )
        except Exception as e:
            logger.warning(f"Failed to get weekly change: {e}")
            return None

    async def _get_most_valuable(self, entity_type: str) -> Optional[Dict[str, Any]]:
        """Get most valuable item of a type."""
        try:
            if entity_type == "comic":
                result = await self.db.execute(text("""
                    SELECT
                        id,
                        issue_name as name,
                        image as image_url,
                        COALESCE(price_new, price_cib, price_loose) as value
                    FROM comic_issues
                    WHERE COALESCE(price_new, price_cib, price_loose) IS NOT NULL
                    ORDER BY COALESCE(price_new, price_cib, price_loose) DESC
                    LIMIT 1
                """))
            else:  # funko
                result = await self.db.execute(text("""
                    SELECT
                        id,
                        title as name,
                        image_url,
                        COALESCE(price_new, price_cib, price_loose) as value
                    FROM funkos
                    WHERE COALESCE(price_new, price_cib, price_loose) IS NOT NULL
                    ORDER BY COALESCE(price_new, price_cib, price_loose) DESC
                    LIMIT 1
                """))

            row = result.fetchone()
            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "image_url": row[2],
                    "value": float(row[3]) if row[3] else 0,
                    "entity_type": entity_type,
                }
            return None
        except Exception as e:
            logger.error(f"Failed to get most valuable {entity_type}: {e}")
            return None
