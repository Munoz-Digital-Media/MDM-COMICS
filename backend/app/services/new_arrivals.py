"""
New Arrivals Service

v1.5.0: Outreach System - Detect and report new inventory additions
"""
from datetime import datetime, timedelta
from typing import List, Optional
from dataclasses import dataclass
import random
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.utils import utcnow

logger = logging.getLogger(__name__)


@dataclass
class NewArrival:
    """New arrival item."""
    entity_type: str  # funko, comic, product
    entity_id: int
    name: str
    image_url: Optional[str]
    price: Optional[float]
    added_at: datetime
    category: Optional[str] = None


@dataclass
class NewArrivalsReport:
    """New arrivals report."""
    generated_at: datetime
    period_start: datetime
    items: List[NewArrival]
    total_count: int


class NewArrivalsService:
    """Detect and report new inventory additions."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_new_arrivals(
        self,
        since: datetime = None,
        entity_types: List[str] = None,
        limit: int = 20,
    ) -> NewArrivalsReport:
        """
        Get items added since specified date.

        Detection criteria:
        - created_at > since
        - Not deleted (for products: deleted_at IS NULL)
        - Has image (for display)
        """
        if since is None:
            since = utcnow() - timedelta(days=7)

        if entity_types is None:
            entity_types = ["funko", "comic", "product"]

        arrivals = []

        if "funko" in entity_types:
            funkos = await self._get_new_funkos(since, limit)
            arrivals.extend(funkos)

        if "comic" in entity_types:
            comics = await self._get_new_comics(since, limit)
            arrivals.extend(comics)

        if "product" in entity_types:
            products = await self._get_new_products(since, limit)
            arrivals.extend(products)

        # Sort by added_at descending, limit total
        arrivals.sort(key=lambda x: x.added_at, reverse=True)
        arrivals = arrivals[:limit]

        return NewArrivalsReport(
            generated_at=utcnow(),
            period_start=since,
            items=arrivals,
            total_count=len(arrivals),
        )

    async def _get_new_funkos(self, since: datetime, limit: int) -> List[NewArrival]:
        """Get new Funko POPs."""
        try:
            result = await self.db.execute(
                text("""
                    SELECT id, title, image_url, price_new, created_at, category
                    FROM funkos
                    WHERE created_at >= :since
                      AND image_url IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT :limit
                """),
                {"since": since, "limit": limit}
            )

            return [
                NewArrival(
                    entity_type="funko",
                    entity_id=row[0],
                    name=row[1],
                    image_url=row[2],
                    price=float(row[3]) if row[3] else None,
                    added_at=row[4],
                    category=row[5],
                )
                for row in result.fetchall()
            ]
        except Exception as e:
            logger.error(f"Failed to get new funkos: {e}")
            return []

    async def _get_new_comics(self, since: datetime, limit: int) -> List[NewArrival]:
        """Get new comic issues."""
        try:
            result = await self.db.execute(
                text("""
                    SELECT ci.id, ci.issue_name, ci.image, ci.price_new, ci.created_at,
                           cs.name as series_name
                    FROM comic_issues ci
                    LEFT JOIN comic_series cs ON ci.series_id = cs.id
                    WHERE ci.created_at >= :since
                      AND ci.image IS NOT NULL
                    ORDER BY ci.created_at DESC
                    LIMIT :limit
                """),
                {"since": since, "limit": limit}
            )

            return [
                NewArrival(
                    entity_type="comic",
                    entity_id=row[0],
                    name=row[1] or f"{row[5]} Issue",
                    image_url=row[2],
                    price=float(row[3]) if row[3] else None,
                    added_at=row[4],
                    category=row[5],
                )
                for row in result.fetchall()
            ]
        except Exception as e:
            logger.error(f"Failed to get new comics: {e}")
            return []

    async def _get_new_products(self, since: datetime, limit: int) -> List[NewArrival]:
        """Get new shop products."""
        try:
            result = await self.db.execute(
                text("""
                    SELECT id, name, image_url, price, created_at, category
                    FROM products
                    WHERE created_at >= :since
                      AND deleted_at IS NULL
                      AND image_url IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT :limit
                """),
                {"since": since, "limit": limit}
            )

            return [
                NewArrival(
                    entity_type="product",
                    entity_id=row[0],
                    name=row[1],
                    image_url=row[2],
                    price=float(row[3]) if row[3] else None,
                    added_at=row[4],
                    category=row[5],
                )
                for row in result.fetchall()
            ]
        except Exception as e:
            logger.error(f"Failed to get new products: {e}")
            return []

    async def pick_editors_choice(
        self,
        arrivals: List[NewArrival],
        strategy: str = "hybrid",
    ) -> Optional[NewArrival]:
        """
        Select featured item for Editor's Choice spotlight.

        Strategies:
        - random: Pure random selection
        - value_based: Highest price
        - hybrid: Weighted towards higher value with randomness
        """
        if not arrivals:
            return None

        if strategy == "random":
            return random.choice(arrivals)

        elif strategy == "value_based":
            priced = [a for a in arrivals if a.price is not None]
            if priced:
                return max(priced, key=lambda x: x.price)
            return random.choice(arrivals)

        elif strategy == "hybrid":
            priced = [a for a in arrivals if a.price is not None]
            if not priced:
                return random.choice(arrivals)

            total_price = sum(a.price for a in priced)
            if total_price == 0:
                return random.choice(priced)

            weights = [a.price / total_price for a in priced]
            return random.choices(priced, weights=weights, k=1)[0]

        return random.choice(arrivals)
