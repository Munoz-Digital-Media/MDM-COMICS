"""
Price ML Features Service v1.7.0

AI/ML feature engineering for price prediction models.

This service provides:
1. Volatility calculation (standard deviation over time windows)
2. Trend calculation (linear regression slope normalized to -1 to +1)
3. Momentum indicator (price vs moving average)
4. Feature vector generation for ML training
5. Training dataset export

Per constitution_db.json §7: EXPLAIN ANALYZE required for complex queries.
Per constitution_data_hygiene.json §1: All price data uses HTTPS sources.
"""
import logging
import math
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)


@dataclass
class PriceFeatures:
    """ML feature vector for a price snapshot."""
    entity_type: str
    entity_id: int
    snapshot_date: date
    price_primary: Optional[float]  # price_loose for funko, price_cib for comic
    volatility_7d: Optional[float]
    volatility_30d: Optional[float]
    trend_7d: Optional[float]
    trend_30d: Optional[float]
    momentum: Optional[float]
    days_since_change: Optional[int]
    price_changed: bool
    sales_volume: Optional[int]
    is_stale: bool
    confidence_score: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for ML frameworks."""
        return {
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'snapshot_date': self.snapshot_date.isoformat(),
            'price_primary': self.price_primary,
            'volatility_7d': self.volatility_7d,
            'volatility_30d': self.volatility_30d,
            'trend_7d': self.trend_7d,
            'trend_30d': self.trend_30d,
            'momentum': self.momentum,
            'days_since_change': self.days_since_change,
            'price_changed': self.price_changed,
            'sales_volume': self.sales_volume,
            'is_stale': self.is_stale,
            'confidence_score': self.confidence_score,
        }

    def to_vector(self) -> List[float]:
        """Convert to numeric vector for ML models (NaN for None)."""
        nan = float('nan')
        return [
            1.0 if self.entity_type == 'funko' else 0.0,
            float(self.entity_id),
            self.price_primary if self.price_primary else nan,
            self.volatility_7d if self.volatility_7d else nan,
            self.volatility_30d if self.volatility_30d else nan,
            self.trend_7d if self.trend_7d else nan,
            self.trend_30d if self.trend_30d else nan,
            self.momentum if self.momentum else nan,
            float(self.days_since_change) if self.days_since_change else nan,
            1.0 if self.price_changed else 0.0,
            float(self.sales_volume) if self.sales_volume else nan,
            1.0 if self.is_stale else 0.0,
            self.confidence_score if self.confidence_score else nan,
        ]


class PriceMLFeaturesService:
    """
    Service for calculating and managing ML features for price prediction.

    All features are designed to be:
    1. Predictive - correlate with future price movements
    2. Stationary - normalized to be comparable across entities
    3. Robust - handle missing data gracefully
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def calculate_volatility(
        self,
        entity_type: str,
        entity_id: int,
        window_days: int = 30,
        as_of_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate price volatility (standard deviation) over a time window.

        Volatility is a key feature for:
        - Risk assessment
        - Price prediction confidence intervals
        - Identifying trending vs stable items

        Returns volatility normalized by mean price (coefficient of variation).
        """
        if as_of_date is None:
            as_of_date = date.today()

        start_date = as_of_date - timedelta(days=window_days)

        result = await self.db.execute(text("""
            SELECT
                STDDEV(price_loose) as std_dev,
                AVG(price_loose) as avg_price,
                COUNT(*) as sample_count
            FROM price_snapshots
            WHERE entity_type = :type
            AND entity_id = :id
            AND snapshot_date BETWEEN :start AND :end
            AND price_loose IS NOT NULL
        """), {
            "type": entity_type,
            "id": entity_id,
            "start": start_date,
            "end": as_of_date
        })

        row = result.fetchone()

        if not row or not row.std_dev or not row.avg_price or row.sample_count < 3:
            return None

        # Return coefficient of variation (normalized volatility)
        avg = float(row.avg_price)
        if avg <= 0:
            return None

        return float(row.std_dev) / avg

    async def calculate_trend(
        self,
        entity_type: str,
        entity_id: int,
        window_days: int = 30,
        as_of_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate price trend using linear regression slope.

        Returns slope normalized to -1 to +1 range:
        - +1 = strong upward trend (price doubled over window)
        - 0 = no trend (flat)
        - -1 = strong downward trend (price halved over window)

        Uses simple linear regression: y = mx + b
        """
        if as_of_date is None:
            as_of_date = date.today()

        start_date = as_of_date - timedelta(days=window_days)

        # Get price series
        result = await self.db.execute(text("""
            SELECT
                snapshot_date,
                price_loose
            FROM price_snapshots
            WHERE entity_type = :type
            AND entity_id = :id
            AND snapshot_date BETWEEN :start AND :end
            AND price_loose IS NOT NULL
            ORDER BY snapshot_date
        """), {
            "type": entity_type,
            "id": entity_id,
            "start": start_date,
            "end": as_of_date
        })

        rows = result.fetchall()

        if len(rows) < 3:
            return None

        # Simple linear regression
        n = len(rows)
        x_values = list(range(n))  # Days from start
        y_values = [float(r.price_loose) for r in rows]

        # Calculate means
        x_mean = sum(x_values) / n
        y_mean = sum(y_values) / n

        # Calculate slope: sum((x-x_mean)(y-y_mean)) / sum((x-x_mean)^2)
        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values))
        denominator = sum((x - x_mean) ** 2 for x in x_values)

        if denominator == 0:
            return 0.0

        slope = numerator / denominator

        # Normalize slope to -1 to +1 based on percentage change per day
        # If slope * window_days = y_mean, that's a 100% increase = +1
        if y_mean <= 0:
            return 0.0

        daily_pct_change = slope / y_mean
        normalized = daily_pct_change * window_days

        # Clamp to -1 to +1
        return max(-1.0, min(1.0, normalized))

    async def calculate_momentum(
        self,
        entity_type: str,
        entity_id: int,
        as_of_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate price momentum indicator.

        Momentum = (current_price - avg_30d) / std_30d

        Positive momentum indicates price above recent average.
        Similar to z-score - measures how many std devs from mean.
        """
        if as_of_date is None:
            as_of_date = date.today()

        result = await self.db.execute(text("""
            WITH stats AS (
                SELECT
                    AVG(price_loose) as avg_price,
                    STDDEV(price_loose) as std_price
                FROM price_snapshots
                WHERE entity_type = :type
                AND entity_id = :id
                AND snapshot_date BETWEEN :start AND :end
                AND price_loose IS NOT NULL
            ),
            current AS (
                SELECT price_loose
                FROM price_snapshots
                WHERE entity_type = :type
                AND entity_id = :id
                AND snapshot_date = :end
            )
            SELECT
                current.price_loose as current_price,
                stats.avg_price,
                stats.std_price
            FROM stats, current
        """), {
            "type": entity_type,
            "id": entity_id,
            "start": as_of_date - timedelta(days=30),
            "end": as_of_date
        })

        row = result.fetchone()

        if not row or not row.current_price or not row.std_price or float(row.std_price) == 0:
            return None

        momentum = (float(row.current_price) - float(row.avg_price)) / float(row.std_price)

        # Clamp to reasonable range (-5 to +5)
        return max(-5.0, min(5.0, momentum))

    async def calculate_confidence_score(
        self,
        entity_type: str,
        entity_id: int,
        as_of_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate data quality confidence score (0.0 to 1.0).

        Factors:
        - Data freshness (updated recently = higher)
        - Sample count (more data = higher)
        - Price consistency (fewer wild swings = higher)
        """
        if as_of_date is None:
            as_of_date = date.today()

        result = await self.db.execute(text("""
            SELECT
                COUNT(*) as sample_count,
                MAX(snapshot_date) as latest_date,
                STDDEV(price_loose) / NULLIF(AVG(price_loose), 0) as cv
            FROM price_snapshots
            WHERE entity_type = :type
            AND entity_id = :id
            AND snapshot_date BETWEEN :start AND :end
            AND price_loose IS NOT NULL
        """), {
            "type": entity_type,
            "id": entity_id,
            "start": as_of_date - timedelta(days=30),
            "end": as_of_date
        })

        row = result.fetchone()

        if not row or row.sample_count == 0:
            return 0.1  # Minimum confidence for no data

        # Freshness score (0-0.4): days since last update
        freshness = 0.4
        if row.latest_date:
            days_old = (as_of_date - row.latest_date).days
            freshness = max(0.0, 0.4 - (days_old * 0.05))

        # Sample score (0-0.3): how many data points
        sample_score = min(0.3, row.sample_count * 0.01)

        # Stability score (0-0.3): lower CV = more stable = higher score
        stability = 0.3
        if row.cv and float(row.cv) > 0:
            stability = max(0.0, 0.3 - float(row.cv))

        return round(freshness + sample_score + stability, 2)

    async def get_feature_vector(
        self,
        entity_type: str,
        entity_id: int,
        as_of_date: Optional[date] = None
    ) -> Optional[PriceFeatures]:
        """
        Get complete feature vector for an entity as of a date.

        This is the main method for generating ML training/inference data.
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Get the snapshot for this date
        result = await self.db.execute(text("""
            SELECT
                price_loose,
                price_cib,
                price_changed,
                days_since_change,
                sales_volume,
                is_stale,
                volatility_7d,
                volatility_30d,
                trend_7d,
                trend_30d,
                momentum,
                confidence_score
            FROM price_snapshots
            WHERE entity_type = :type
            AND entity_id = :id
            AND snapshot_date = :date
        """), {
            "type": entity_type,
            "id": entity_id,
            "date": as_of_date
        })

        row = result.fetchone()

        if not row:
            # No snapshot exists, try to calculate features from raw data
            volatility_7d = await self.calculate_volatility(entity_type, entity_id, 7, as_of_date)
            volatility_30d = await self.calculate_volatility(entity_type, entity_id, 30, as_of_date)
            trend_7d = await self.calculate_trend(entity_type, entity_id, 7, as_of_date)
            trend_30d = await self.calculate_trend(entity_type, entity_id, 30, as_of_date)
            momentum = await self.calculate_momentum(entity_type, entity_id, as_of_date)
            confidence = await self.calculate_confidence_score(entity_type, entity_id, as_of_date)

            return PriceFeatures(
                entity_type=entity_type,
                entity_id=entity_id,
                snapshot_date=as_of_date,
                price_primary=None,
                volatility_7d=volatility_7d,
                volatility_30d=volatility_30d,
                trend_7d=trend_7d,
                trend_30d=trend_30d,
                momentum=momentum,
                days_since_change=None,
                price_changed=False,
                sales_volume=None,
                is_stale=True,
                confidence_score=confidence
            )

        # Use snapshot data, calculating missing features if needed
        price_primary = float(row.price_loose) if row.price_loose else (
            float(row.price_cib) if row.price_cib else None
        )

        # Calculate features if not stored
        volatility_7d = float(row.volatility_7d) if row.volatility_7d else await self.calculate_volatility(entity_type, entity_id, 7, as_of_date)
        volatility_30d = float(row.volatility_30d) if row.volatility_30d else await self.calculate_volatility(entity_type, entity_id, 30, as_of_date)
        trend_7d = float(row.trend_7d) if row.trend_7d else await self.calculate_trend(entity_type, entity_id, 7, as_of_date)
        trend_30d = float(row.trend_30d) if row.trend_30d else await self.calculate_trend(entity_type, entity_id, 30, as_of_date)
        momentum = float(row.momentum) if row.momentum else await self.calculate_momentum(entity_type, entity_id, as_of_date)
        confidence = float(row.confidence_score) if row.confidence_score else await self.calculate_confidence_score(entity_type, entity_id, as_of_date)

        return PriceFeatures(
            entity_type=entity_type,
            entity_id=entity_id,
            snapshot_date=as_of_date,
            price_primary=price_primary,
            volatility_7d=volatility_7d,
            volatility_30d=volatility_30d,
            trend_7d=trend_7d,
            trend_30d=trend_30d,
            momentum=momentum,
            days_since_change=row.days_since_change,
            price_changed=row.price_changed,
            sales_volume=row.sales_volume,
            is_stale=row.is_stale,
            confidence_score=confidence
        )

    async def update_ml_features_for_date(
        self,
        snapshot_date: date,
        entity_type: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Update ML features for all snapshots on a given date.

        This should be called after the initial snapshot is created
        to populate the volatility, trend, and momentum fields.
        """
        stats = {"updated": 0, "errors": 0}

        # Get all snapshots for this date
        type_filter = "AND entity_type = :type" if entity_type else ""
        params: Dict[str, Any] = {"date": snapshot_date}
        if entity_type:
            params["type"] = entity_type

        result = await self.db.execute(text(f"""
            SELECT entity_type, entity_id
            FROM price_snapshots
            WHERE snapshot_date = :date
            {type_filter}
            AND volatility_30d IS NULL
        """), params)

        rows = result.fetchall()
        logger.info(f"Updating ML features for {len(rows)} snapshots on {snapshot_date}")

        for row in rows:
            try:
                # Calculate features
                vol_7d = await self.calculate_volatility(row.entity_type, row.entity_id, 7, snapshot_date)
                vol_30d = await self.calculate_volatility(row.entity_type, row.entity_id, 30, snapshot_date)
                trend_7d = await self.calculate_trend(row.entity_type, row.entity_id, 7, snapshot_date)
                trend_30d = await self.calculate_trend(row.entity_type, row.entity_id, 30, snapshot_date)
                momentum = await self.calculate_momentum(row.entity_type, row.entity_id, snapshot_date)
                confidence = await self.calculate_confidence_score(row.entity_type, row.entity_id, snapshot_date)

                # Update snapshot
                await self.db.execute(text("""
                    UPDATE price_snapshots
                    SET
                        volatility_7d = :v7,
                        volatility_30d = :v30,
                        trend_7d = :t7,
                        trend_30d = :t30,
                        momentum = :mom,
                        confidence_score = :conf
                    WHERE entity_type = :type
                    AND entity_id = :id
                    AND snapshot_date = :date
                """), {
                    "v7": vol_7d,
                    "v30": vol_30d,
                    "t7": trend_7d,
                    "t30": trend_30d,
                    "mom": momentum,
                    "conf": confidence,
                    "type": row.entity_type,
                    "id": row.entity_id,
                    "date": snapshot_date
                })

                stats["updated"] += 1

            except Exception as e:
                logger.error(f"Error updating features for {row.entity_type}:{row.entity_id}: {e}")
                stats["errors"] += 1

        await self.db.commit()
        return stats

    async def get_training_dataset(
        self,
        entity_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        min_confidence: float = 0.3,
        limit: int = 100000
    ) -> List[Dict[str, Any]]:
        """
        Export training dataset for ML models.

        Returns list of feature dictionaries suitable for pandas/sklearn/pytorch.

        Filters:
        - entity_type: 'funko' or 'comic' or None for both
        - start_date/end_date: date range
        - min_confidence: minimum data quality score
        - limit: max rows to return
        """
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=365)

        type_filter = "AND entity_type = :type" if entity_type else ""
        params: Dict[str, Any] = {
            "start": start_date,
            "end": end_date,
            "min_conf": min_confidence,
            "limit": limit
        }
        if entity_type:
            params["type"] = entity_type

        result = await self.db.execute(text(f"""
            SELECT
                entity_type,
                entity_id,
                snapshot_date,
                price_loose,
                price_cib,
                volatility_7d,
                volatility_30d,
                trend_7d,
                trend_30d,
                momentum,
                days_since_change,
                price_changed,
                sales_volume,
                is_stale,
                confidence_score
            FROM price_snapshots
            WHERE snapshot_date BETWEEN :start AND :end
            {type_filter}
            AND (confidence_score IS NULL OR confidence_score >= :min_conf)
            ORDER BY snapshot_date, entity_type, entity_id
            LIMIT :limit
        """), params)

        rows = result.fetchall()

        dataset = []
        for row in rows:
            price_primary = float(row.price_loose) if row.price_loose else (
                float(row.price_cib) if row.price_cib else None
            )

            dataset.append({
                'entity_type': row.entity_type,
                'entity_id': row.entity_id,
                'snapshot_date': row.snapshot_date.isoformat(),
                'price_primary': price_primary,
                'volatility_7d': float(row.volatility_7d) if row.volatility_7d else None,
                'volatility_30d': float(row.volatility_30d) if row.volatility_30d else None,
                'trend_7d': float(row.trend_7d) if row.trend_7d else None,
                'trend_30d': float(row.trend_30d) if row.trend_30d else None,
                'momentum': float(row.momentum) if row.momentum else None,
                'days_since_change': row.days_since_change,
                'price_changed': row.price_changed,
                'sales_volume': row.sales_volume,
                'is_stale': row.is_stale,
                'confidence_score': float(row.confidence_score) if row.confidence_score else None,
            })

        return dataset

    async def get_volatility_leaders(
        self,
        entity_type: Optional[str] = None,
        snapshot_date: Optional[date] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get items with highest volatility (most price movement)."""
        if snapshot_date is None:
            snapshot_date = date.today()

        type_filter = "AND entity_type = :type" if entity_type else ""
        params: Dict[str, Any] = {"date": snapshot_date, "limit": limit}
        if entity_type:
            params["type"] = entity_type

        result = await self.db.execute(text(f"""
            SELECT
                entity_type,
                entity_id,
                pricecharting_id,
                price_loose,
                volatility_30d,
                trend_30d
            FROM price_snapshots
            WHERE snapshot_date = :date
            {type_filter}
            AND volatility_30d IS NOT NULL
            ORDER BY volatility_30d DESC
            LIMIT :limit
        """), params)

        return [
            {
                "entity_type": r.entity_type,
                "entity_id": r.entity_id,
                "pricecharting_id": r.pricecharting_id,
                "price_loose": float(r.price_loose) if r.price_loose else None,
                "volatility_30d": float(r.volatility_30d),
                "trend_30d": float(r.trend_30d) if r.trend_30d else None,
            }
            for r in result.fetchall()
        ]

    async def get_stable_leaders(
        self,
        entity_type: Optional[str] = None,
        snapshot_date: Optional[date] = None,
        min_price: float = 10.0,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get items with lowest volatility (most stable prices)."""
        if snapshot_date is None:
            snapshot_date = date.today()

        type_filter = "AND entity_type = :type" if entity_type else ""
        params: Dict[str, Any] = {
            "date": snapshot_date,
            "min_price": min_price,
            "limit": limit
        }
        if entity_type:
            params["type"] = entity_type

        result = await self.db.execute(text(f"""
            SELECT
                entity_type,
                entity_id,
                pricecharting_id,
                price_loose,
                volatility_30d,
                days_since_change
            FROM price_snapshots
            WHERE snapshot_date = :date
            {type_filter}
            AND volatility_30d IS NOT NULL
            AND price_loose >= :min_price
            ORDER BY volatility_30d ASC
            LIMIT :limit
        """), params)

        return [
            {
                "entity_type": r.entity_type,
                "entity_id": r.entity_id,
                "pricecharting_id": r.pricecharting_id,
                "price_loose": float(r.price_loose) if r.price_loose else None,
                "volatility_30d": float(r.volatility_30d),
                "days_since_change": r.days_since_change,
            }
            for r in result.fetchall()
        ]


# Convenience function for standalone usage
async def get_ml_features_service() -> PriceMLFeaturesService:
    """Get ML features service with database session."""
    async with AsyncSessionLocal() as db:
        return PriceMLFeaturesService(db)
