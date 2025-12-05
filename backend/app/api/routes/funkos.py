"""
Funko POP API routes
Search and retrieve Funko data from local database.
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
from sqlalchemy.orm import selectinload
from pydantic import BaseModel

from app.core.database import get_db
from app.models.funko import Funko, FunkoSeriesName

router = APIRouter(prefix="/funkos", tags=["funkos"])


# Response schemas
class FunkoSeriesResponse(BaseModel):
    id: int
    name: str

    class Config:
        from_attributes = True


class FunkoResponse(BaseModel):
    id: int
    handle: str
    title: str
    image_url: Optional[str]
    series: List[FunkoSeriesResponse]

    class Config:
        from_attributes = True


class FunkoSearchResponse(BaseModel):
    results: List[FunkoResponse]
    total: int
    page: int
    pages: int


class SeriesSearchResponse(BaseModel):
    results: List[FunkoSeriesResponse]
    total: int


@router.get("/search", response_model=FunkoSearchResponse)
async def search_funkos(
    q: str = Query(None, description="Search query (title)"),
    series: str = Query(None, description="Filter by series name"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """
    Search Funkos by title or series.
    Returns paginated results.
    """
    query = select(Funko).options(selectinload(Funko.series))

    # Apply filters
    if q:
        search_term = f"%{q}%"
        query = query.where(Funko.title.ilike(search_term))

    if series:
        query = query.join(Funko.series).where(FunkoSeriesName.name.ilike(f"%{series}%"))

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply pagination
    offset = (page - 1) * per_page
    query = query.offset(offset).limit(per_page).order_by(Funko.title)

    result = await db.execute(query)
    funkos = result.scalars().unique().all()

    pages = (total + per_page - 1) // per_page

    return FunkoSearchResponse(
        results=[FunkoResponse.model_validate(f) for f in funkos],
        total=total,
        page=page,
        pages=pages
    )


@router.get("/series", response_model=SeriesSearchResponse)
async def get_series(
    q: str = Query(None, description="Search series name"),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db)
):
    """Get list of Funko series/categories"""
    query = select(FunkoSeriesName)

    if q:
        query = query.where(FunkoSeriesName.name.ilike(f"%{q}%"))

    query = query.order_by(FunkoSeriesName.name).limit(limit)

    result = await db.execute(query)
    series_list = result.scalars().all()

    # Get total count
    count_query = select(func.count(FunkoSeriesName.id))
    if q:
        count_query = count_query.where(FunkoSeriesName.name.ilike(f"%{q}%"))
    count_result = await db.execute(count_query)
    total = count_result.scalar() or 0

    return SeriesSearchResponse(
        results=[FunkoSeriesResponse.model_validate(s) for s in series_list],
        total=total
    )


@router.get("/{funko_id}", response_model=FunkoResponse)
async def get_funko(
    funko_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get a specific Funko by ID"""
    query = select(Funko).options(selectinload(Funko.series)).where(Funko.id == funko_id)
    result = await db.execute(query)
    funko = result.scalar_one_or_none()

    if not funko:
        raise HTTPException(status_code=404, detail="Funko not found")

    return FunkoResponse.model_validate(funko)


@router.get("/stats/count")
async def get_funko_count(db: AsyncSession = Depends(get_db)):
    """Get total count of Funkos in database"""
    result = await db.execute(select(func.count(Funko.id)))
    count = result.scalar() or 0

    series_result = await db.execute(select(func.count(FunkoSeriesName.id)))
    series_count = series_result.scalar() or 0

    return {
        "total_funkos": count,
        "total_series": series_count
    }
