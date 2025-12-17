"""
Public Bundle API Routes v1.0.0

Public endpoints for storefront bundle display.
No authentication required for listing/detail.

Per constitution_ui.json:
- Section 2: Response within target hydration time
- Section 6: Every action returns feedback state
"""
import logging
from typing import Optional
from math import ceil

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.bundle import BundleStatus
from app.services.bundle_service import BundleService
from app.schemas.bundle import (
    PublicBundleResponse,
    PublicBundleListResponse,
    PaginatedPublicBundleList,
    PublicBundleItemResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bundles", tags=["bundles"])


# ==================== Helper Functions ====================

def bundle_to_public_response(bundle) -> PublicBundleResponse:
    """Convert Bundle model to public response schema (no cost data)."""
    items = []
    for item in bundle.items:
        item_resp = PublicBundleItemResponse(
            id=item.id,
            product_id=item.product_id,
            quantity=item.quantity,
            unit_price=item.unit_price,
            line_price=item.line_price,
            display_order=item.display_order,
            is_featured=item.is_featured,
            custom_label=item.custom_label,
            product_name=item.product.name if item.product else None,
            product_sku=item.product.sku if item.product else None,
            product_image_url=item.product.image_url if item.product else None,
        )
        items.append(item_resp)

    return PublicBundleResponse(
        id=bundle.id,
        sku=bundle.sku,
        name=bundle.name,
        slug=bundle.slug,
        short_description=bundle.short_description,
        description=bundle.description,
        bundle_price=bundle.bundle_price,
        compare_at_price=bundle.compare_at_price,
        savings_amount=bundle.savings_amount,
        savings_percent=bundle.savings_percent,
        available_qty=bundle.available_qty or 0,
        image_url=bundle.image_url,
        images=bundle.images or [],
        badge_text=bundle.badge_text,
        category=bundle.category,
        tags=bundle.tags or [],
        items=items,
        item_count=len(items),
    )


def bundle_to_public_list_response(bundle) -> PublicBundleListResponse:
    """Convert Bundle model to public list response schema."""
    return PublicBundleListResponse(
        id=bundle.id,
        sku=bundle.sku,
        name=bundle.name,
        slug=bundle.slug,
        short_description=bundle.short_description,
        bundle_price=bundle.bundle_price,
        compare_at_price=bundle.compare_at_price,
        savings_amount=bundle.savings_amount,
        savings_percent=bundle.savings_percent,
        available_qty=bundle.available_qty or 0,
        image_url=bundle.image_url,
        badge_text=bundle.badge_text,
        category=bundle.category,
        item_count=len(bundle.items) if bundle.items else 0,
    )


# ==================== Public Routes ====================

@router.get("/", response_model=PaginatedPublicBundleList)
async def list_bundles(
    category: Optional[str] = Query(None, description="Filter by category"),
    sort: Optional[str] = Query("display_order", description="Sort field"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=50, description="Items per page"),
    db: AsyncSession = Depends(get_db),
):
    """
    List active bundles for storefront display.

    Only returns bundles with status=ACTIVE.
    Response cached for 5 minutes.
    """
    bundles, total = await BundleService.list_bundles(
        db,
        active_only=True,
        category=category,
        page=page,
        per_page=per_page,
    )

    return PaginatedPublicBundleList(
        items=[bundle_to_public_list_response(b) for b in bundles],
        total=total,
        page=page,
        per_page=per_page,
        pages=ceil(total / per_page) if total > 0 else 0,
    )


@router.get("/{slug}", response_model=PublicBundleResponse)
async def get_bundle_by_slug(
    slug: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get bundle detail by slug.

    Only returns if bundle status=ACTIVE.
    Response cached for 5 minutes.
    """
    bundle = await BundleService.get_by_slug(db, slug)

    if not bundle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )

    # Only return active bundles to public
    if bundle.status != BundleStatus.ACTIVE.value:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )

    return bundle_to_public_response(bundle)


@router.get("/id/{bundle_id}", response_model=PublicBundleResponse)
async def get_bundle_by_id(
    bundle_id: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Get bundle detail by ID.

    Only returns if bundle status=ACTIVE.
    """
    bundle = await BundleService.get_by_id(db, bundle_id)

    if not bundle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )

    # Only return active bundles to public
    if bundle.status != BundleStatus.ACTIVE.value:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )

    return bundle_to_public_response(bundle)


@router.get("/categories", response_model=list[str])
async def list_categories(
    db: AsyncSession = Depends(get_db),
):
    """
    Get list of bundle categories with active bundles.

    Useful for filtering UI.
    """
    from sqlalchemy import select, distinct
    from app.models.bundle import Bundle

    result = await db.execute(
        select(distinct(Bundle.category))
        .where(Bundle.status == BundleStatus.ACTIVE.value)
        .where(Bundle.category.isnot(None))
        .order_by(Bundle.category)
    )

    categories = [row[0] for row in result.all() if row[0]]
    return categories
