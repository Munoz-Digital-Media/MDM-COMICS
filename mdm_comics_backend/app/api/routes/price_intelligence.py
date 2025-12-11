"""
Price Intelligence API Routes v1.7.0

AI/ML-ready endpoints for price analysis, snapshots, and training data export.

Endpoints:
- GET /api/prices/snapshot/{entity_type}/{entity_id} - Get latest snapshot
- GET /api/prices/history/{entity_type}/{entity_id} - Get snapshot history
- GET /api/prices/on-date/{date} - Get all prices as of date
- GET /api/prices/volatility-leaders - Top volatile items
- GET /api/prices/stable-leaders - Most stable items
- GET /api/ml/training-data - Export ML training dataset
- GET /api/ml/features/{entity_type}/{entity_id} - Get feature vector

Per constitution_db.json §7: Rate limiting applied, EXPLAIN ANALYZE for complex queries.
"""
import logging
from datetime import date, timedelta
from typing import Optional, List, Any, Dict
from fastapi import APIRouter, Depends, Query, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.api.deps import get_current_user, get_optional_user
from app.models.user import User
from app.services.price_ml_features import PriceMLFeaturesService

logger = logging.getLogger(__name__)
router = APIRouter()


# ============================================================================
# RESPONSE MODELS
# ============================================================================

class PriceSnapshotResponse(BaseModel):
    """Single price snapshot response."""
    id: int
    snapshot_date: str
    entity_type: str
    entity_id: int
    pricecharting_id: Optional[int] = None
    price_loose: Optional[float] = None
    price_cib: Optional[float] = None
    price_new: Optional[float] = None
    price_graded: Optional[float] = None
    sales_volume: Optional[int] = None
    price_changed: bool = False
    days_since_change: Optional[int] = None
    volatility_7d: Optional[float] = None
    volatility_30d: Optional[float] = None
    trend_7d: Optional[float] = None
    trend_30d: Optional[float] = None
    momentum: Optional[float] = None
    confidence_score: Optional[float] = None
    is_stale: bool = False


class SnapshotHistoryResponse(BaseModel):
    """Snapshot history with pagination."""
    entity_type: str
    entity_id: int
    total_snapshots: int
    snapshots: List[PriceSnapshotResponse]


class VolatilityLeaderResponse(BaseModel):
    """Volatility leader item."""
    entity_type: str
    entity_id: int
    pricecharting_id: Optional[int] = None
    price_loose: Optional[float] = None
    volatility_30d: float
    trend_30d: Optional[float] = None


class StableLeaderResponse(BaseModel):
    """Stability leader item."""
    entity_type: str
    entity_id: int
    pricecharting_id: Optional[int] = None
    price_loose: Optional[float] = None
    volatility_30d: float
    days_since_change: Optional[int] = None


class MLFeatureVectorResponse(BaseModel):
    """ML feature vector for an entity."""
    entity_type: str
    entity_id: int
    snapshot_date: str
    price_primary: Optional[float] = None
    volatility_7d: Optional[float] = None
    volatility_30d: Optional[float] = None
    trend_7d: Optional[float] = None
    trend_30d: Optional[float] = None
    momentum: Optional[float] = None
    days_since_change: Optional[int] = None
    price_changed: bool = False
    sales_volume: Optional[int] = None
    is_stale: bool = False
    confidence_score: Optional[float] = None


class TrainingDatasetResponse(BaseModel):
    """ML training dataset export."""
    total_rows: int
    date_range: Dict[str, str]
    columns: List[str]
    data: List[Dict[str, Any]]


class PriceOnDateResponse(BaseModel):
    """All prices as of a specific date."""
    snapshot_date: str
    total_count: int
    funkos: List[Dict[str, Any]]
    comics: List[Dict[str, Any]]


# ============================================================================
# SNAPSHOT ENDPOINTS
# ============================================================================

@router.get("/snapshot/{entity_type}/{entity_id}", response_model=PriceSnapshotResponse)
async def get_latest_snapshot(
    entity_type: str,
    entity_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Get the latest price snapshot for an entity.

    - **entity_type**: 'funko' or 'comic'
    - **entity_id**: ID of the entity
    """
    if entity_type not in ("funko", "comic"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="entity_type must be 'funko' or 'comic'"
        )

    result = await db.execute(text("""
        SELECT
            id, snapshot_date, entity_type, entity_id, pricecharting_id,
            price_loose, price_cib, price_new, price_graded,
            sales_volume, price_changed, days_since_change,
            volatility_7d, volatility_30d, trend_7d, trend_30d, momentum,
            confidence_score, is_stale
        FROM price_snapshots
        WHERE entity_type = :type AND entity_id = :id
        ORDER BY snapshot_date DESC
        LIMIT 1
    """), {"type": entity_type, "id": entity_id})

    row = result.fetchone()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No snapshot found for {entity_type}:{entity_id}"
        )

    return PriceSnapshotResponse(
        id=row.id,
        snapshot_date=row.snapshot_date.isoformat(),
        entity_type=row.entity_type,
        entity_id=row.entity_id,
        pricecharting_id=row.pricecharting_id,
        price_loose=float(row.price_loose) if row.price_loose else None,
        price_cib=float(row.price_cib) if row.price_cib else None,
        price_new=float(row.price_new) if row.price_new else None,
        price_graded=float(row.price_graded) if row.price_graded else None,
        sales_volume=row.sales_volume,
        price_changed=row.price_changed,
        days_since_change=row.days_since_change,
        volatility_7d=float(row.volatility_7d) if row.volatility_7d else None,
        volatility_30d=float(row.volatility_30d) if row.volatility_30d else None,
        trend_7d=float(row.trend_7d) if row.trend_7d else None,
        trend_30d=float(row.trend_30d) if row.trend_30d else None,
        momentum=float(row.momentum) if row.momentum else None,
        confidence_score=float(row.confidence_score) if row.confidence_score else None,
        is_stale=row.is_stale
    )


@router.get("/history/{entity_type}/{entity_id}", response_model=SnapshotHistoryResponse)
async def get_snapshot_history(
    entity_type: str,
    entity_id: int,
    days: int = Query(default=30, ge=1, le=365, description="Number of days of history"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get price snapshot history for an entity.

    - **entity_type**: 'funko' or 'comic'
    - **entity_id**: ID of the entity
    - **days**: Number of days of history (default 30, max 365)
    """
    if entity_type not in ("funko", "comic"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="entity_type must be 'funko' or 'comic'"
        )

    start_date = date.today() - timedelta(days=days)

    result = await db.execute(text("""
        SELECT
            id, snapshot_date, entity_type, entity_id, pricecharting_id,
            price_loose, price_cib, price_new, price_graded,
            sales_volume, price_changed, days_since_change,
            volatility_7d, volatility_30d, trend_7d, trend_30d, momentum,
            confidence_score, is_stale
        FROM price_snapshots
        WHERE entity_type = :type
        AND entity_id = :id
        AND snapshot_date >= :start
        ORDER BY snapshot_date DESC
    """), {"type": entity_type, "id": entity_id, "start": start_date})

    rows = result.fetchall()

    snapshots = [
        PriceSnapshotResponse(
            id=r.id,
            snapshot_date=r.snapshot_date.isoformat(),
            entity_type=r.entity_type,
            entity_id=r.entity_id,
            pricecharting_id=r.pricecharting_id,
            price_loose=float(r.price_loose) if r.price_loose else None,
            price_cib=float(r.price_cib) if r.price_cib else None,
            price_new=float(r.price_new) if r.price_new else None,
            price_graded=float(r.price_graded) if r.price_graded else None,
            sales_volume=r.sales_volume,
            price_changed=r.price_changed,
            days_since_change=r.days_since_change,
            volatility_7d=float(r.volatility_7d) if r.volatility_7d else None,
            volatility_30d=float(r.volatility_30d) if r.volatility_30d else None,
            trend_7d=float(r.trend_7d) if r.trend_7d else None,
            trend_30d=float(r.trend_30d) if r.trend_30d else None,
            momentum=float(r.momentum) if r.momentum else None,
            confidence_score=float(r.confidence_score) if r.confidence_score else None,
            is_stale=r.is_stale
        )
        for r in rows
    ]

    return SnapshotHistoryResponse(
        entity_type=entity_type,
        entity_id=entity_id,
        total_snapshots=len(snapshots),
        snapshots=snapshots
    )


@router.get("/on-date/{snapshot_date}", response_model=PriceOnDateResponse)
async def get_prices_on_date(
    snapshot_date: str,
    entity_type: Optional[str] = Query(default=None, description="Filter by entity type"),
    limit: int = Query(default=100, ge=1, le=1000, description="Max items per type"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all prices as of a specific date.

    - **snapshot_date**: Date in YYYY-MM-DD format
    - **entity_type**: Optional filter ('funko' or 'comic')
    - **limit**: Max items to return per entity type
    """
    try:
        target_date = date.fromisoformat(snapshot_date)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD"
        )

    funkos = []
    comics = []

    if not entity_type or entity_type == "funko":
        result = await db.execute(text("""
            SELECT entity_id, pricecharting_id, price_loose, price_cib, price_new
            FROM price_snapshots
            WHERE entity_type = 'funko'
            AND snapshot_date = :date
            LIMIT :limit
        """), {"date": target_date, "limit": limit})

        funkos = [
            {
                "entity_id": r.entity_id,
                "pricecharting_id": r.pricecharting_id,
                "price_loose": float(r.price_loose) if r.price_loose else None,
                "price_cib": float(r.price_cib) if r.price_cib else None,
                "price_new": float(r.price_new) if r.price_new else None,
            }
            for r in result.fetchall()
        ]

    if not entity_type or entity_type == "comic":
        result = await db.execute(text("""
            SELECT entity_id, pricecharting_id, price_loose, price_cib, price_new, price_graded
            FROM price_snapshots
            WHERE entity_type = 'comic'
            AND snapshot_date = :date
            LIMIT :limit
        """), {"date": target_date, "limit": limit})

        comics = [
            {
                "entity_id": r.entity_id,
                "pricecharting_id": r.pricecharting_id,
                "price_loose": float(r.price_loose) if r.price_loose else None,
                "price_cib": float(r.price_cib) if r.price_cib else None,
                "price_new": float(r.price_new) if r.price_new else None,
                "price_graded": float(r.price_graded) if r.price_graded else None,
            }
            for r in result.fetchall()
        ]

    return PriceOnDateResponse(
        snapshot_date=target_date.isoformat(),
        total_count=len(funkos) + len(comics),
        funkos=funkos,
        comics=comics
    )


# ============================================================================
# ANALYTICS ENDPOINTS
# ============================================================================

@router.get("/volatility-leaders", response_model=List[VolatilityLeaderResponse])
async def get_volatility_leaders(
    entity_type: Optional[str] = Query(default=None, description="Filter by entity type"),
    limit: int = Query(default=20, ge=1, le=100, description="Number of results"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get items with highest price volatility (most price movement).

    Useful for identifying speculative or trending items.
    """
    service = PriceMLFeaturesService(db)
    leaders = await service.get_volatility_leaders(entity_type, None, limit)

    return [
        VolatilityLeaderResponse(
            entity_type=item["entity_type"],
            entity_id=item["entity_id"],
            pricecharting_id=item.get("pricecharting_id"),
            price_loose=item.get("price_loose"),
            volatility_30d=item["volatility_30d"],
            trend_30d=item.get("trend_30d")
        )
        for item in leaders
    ]


@router.get("/stable-leaders", response_model=List[StableLeaderResponse])
async def get_stable_leaders(
    entity_type: Optional[str] = Query(default=None, description="Filter by entity type"),
    min_price: float = Query(default=10.0, ge=0, description="Minimum price to consider"),
    limit: int = Query(default=20, ge=1, le=100, description="Number of results"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get items with lowest price volatility (most stable prices).

    Useful for identifying reliable investment targets.
    """
    service = PriceMLFeaturesService(db)
    leaders = await service.get_stable_leaders(entity_type, None, min_price, limit)

    return [
        StableLeaderResponse(
            entity_type=item["entity_type"],
            entity_id=item["entity_id"],
            pricecharting_id=item.get("pricecharting_id"),
            price_loose=item.get("price_loose"),
            volatility_30d=item["volatility_30d"],
            days_since_change=item.get("days_since_change")
        )
        for item in leaders
    ]


# ============================================================================
# ML ENDPOINTS
# ============================================================================

@router.get("/ml/features/{entity_type}/{entity_id}", response_model=MLFeatureVectorResponse)
async def get_ml_features(
    entity_type: str,
    entity_id: int,
    as_of_date: Optional[str] = Query(default=None, description="Date for features (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get ML feature vector for an entity.

    Returns computed features suitable for ML model input.
    """
    if entity_type not in ("funko", "comic"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="entity_type must be 'funko' or 'comic'"
        )

    target_date = None
    if as_of_date:
        try:
            target_date = date.fromisoformat(as_of_date)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use YYYY-MM-DD"
            )

    service = PriceMLFeaturesService(db)
    features = await service.get_feature_vector(entity_type, entity_id, target_date)

    if not features:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No data found for {entity_type}:{entity_id}"
        )

    return MLFeatureVectorResponse(
        entity_type=features.entity_type,
        entity_id=features.entity_id,
        snapshot_date=features.snapshot_date.isoformat(),
        price_primary=features.price_primary,
        volatility_7d=features.volatility_7d,
        volatility_30d=features.volatility_30d,
        trend_7d=features.trend_7d,
        trend_30d=features.trend_30d,
        momentum=features.momentum,
        days_since_change=features.days_since_change,
        price_changed=features.price_changed,
        sales_volume=features.sales_volume,
        is_stale=features.is_stale,
        confidence_score=features.confidence_score
    )


@router.get("/ml/training-data", response_model=TrainingDatasetResponse)
async def get_training_dataset(
    entity_type: Optional[str] = Query(default=None, description="Filter by entity type"),
    start_date: Optional[str] = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(default=None, description="End date (YYYY-MM-DD)"),
    min_confidence: float = Query(default=0.3, ge=0, le=1, description="Minimum confidence score"),
    limit: int = Query(default=10000, ge=1, le=100000, description="Max rows to return"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)  # Require auth for ML data export
):
    """
    Export ML training dataset.

    Returns feature data suitable for pandas/sklearn/pytorch.
    Requires authentication.

    - **entity_type**: Filter by 'funko' or 'comic'
    - **start_date**: Start of date range
    - **end_date**: End of date range
    - **min_confidence**: Minimum data quality score
    - **limit**: Maximum rows to return
    """
    # Parse dates
    start = None
    end = None

    if start_date:
        try:
            start = date.fromisoformat(start_date)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid start_date format. Use YYYY-MM-DD"
            )

    if end_date:
        try:
            end = date.fromisoformat(end_date)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid end_date format. Use YYYY-MM-DD"
            )

    service = PriceMLFeaturesService(db)
    dataset = await service.get_training_dataset(
        entity_type=entity_type,
        start_date=start,
        end_date=end,
        min_confidence=min_confidence,
        limit=limit
    )

    # Determine actual date range from data
    dates = [d["snapshot_date"] for d in dataset if d.get("snapshot_date")]
    date_range = {
        "start": min(dates) if dates else "",
        "end": max(dates) if dates else ""
    }

    # Get column names
    columns = list(dataset[0].keys()) if dataset else []

    return TrainingDatasetResponse(
        total_rows=len(dataset),
        date_range=date_range,
        columns=columns,
        data=dataset
    )


# ============================================================================
# STATS ENDPOINT
# ============================================================================

@router.get("/stats")
async def get_snapshot_stats(
    db: AsyncSession = Depends(get_db)
):
    """
    Get overall price snapshot statistics.

    Returns counts and date ranges for monitoring.
    """
    result = await db.execute(text("""
        SELECT
            entity_type,
            COUNT(*) as snapshot_count,
            COUNT(DISTINCT entity_id) as unique_entities,
            MIN(snapshot_date) as earliest_date,
            MAX(snapshot_date) as latest_date,
            COUNT(CASE WHEN price_changed THEN 1 END) as changed_count,
            AVG(confidence_score) as avg_confidence
        FROM price_snapshots
        GROUP BY entity_type
    """))

    stats = {}
    for row in result.fetchall():
        stats[row.entity_type] = {
            "snapshot_count": row.snapshot_count,
            "unique_entities": row.unique_entities,
            "earliest_date": row.earliest_date.isoformat() if row.earliest_date else None,
            "latest_date": row.latest_date.isoformat() if row.latest_date else None,
            "changed_count": row.changed_count,
            "avg_confidence": round(float(row.avg_confidence), 3) if row.avg_confidence else None
        }

    # Get total
    total_result = await db.execute(text("""
        SELECT COUNT(*) as total FROM price_snapshots
    """))
    total = total_result.scalar()

    return {
        "total_snapshots": total,
        "by_entity_type": stats
    }
