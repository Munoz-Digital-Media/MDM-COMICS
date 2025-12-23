"""
Admin Bundle API Routes v1.0.0

Full CRUD + lifecycle management for product bundles.

Per constitution_cyberSec.json:
- Section 3: All input validated
- CSRF protection on mutations

Per constitution_db.json:
- DB-005: Audit columns tracked
"""
import logging
from typing import Optional
from math import ceil

from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File, Form
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.api.deps import get_current_admin
from app.models.user import User
from app.models.bundle import BundleStatus
from app.services.bundle_service import BundleService
from app.services.storage import StorageService
from app.schemas.bundle import (
    BundleCreate,
    BundleUpdate,
    BundleResponse,
    BundleListResponse,
    PaginatedBundleList,
    BundleItemCreate,
    BundleItemUpdate,
    BundleItemResponse,
    BundlePricingRequest,
    BundlePricingResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/bundles", tags=["admin-bundles"])
storage = StorageService()


# ==================== Helper Functions ====================

def normalize_images(images):
    """Normalize images to dicts with url/is_primary/order/s3_key."""
    normalized = []
    for idx, img in enumerate(images or []):
        if isinstance(img, str):
            normalized.append({
                "url": img,
                "is_primary": idx == 0,
                "order": idx,
                "s3_key": None,
            })
        elif isinstance(img, dict):
            normalized.append({
                "url": img.get("url") or img.get("s3_url") or "",
                "is_primary": bool(img.get("is_primary", False)),
                "order": img.get("order", idx),
                "s3_key": img.get("s3_key"),
            })

    normalized.sort(key=lambda x: x.get("order", 0))
    if normalized:
        primary_found = any(i.get("is_primary") for i in normalized)
        if not primary_found:
            normalized[0]["is_primary"] = True
    for i, img in enumerate(normalized):
        img["order"] = i
    return normalized

def bundle_to_response(bundle) -> BundleResponse:
    """Convert Bundle model to response schema."""
    items = []
    for item in bundle.items:
        item_resp = BundleItemResponse(
            id=item.id,
            product_id=item.product_id,
            bcw_mapping_id=item.bcw_mapping_id,
            quantity=item.quantity,
            unit_price=item.unit_price,
            unit_cost=item.unit_cost,
            line_price=item.line_price,
            line_cost=item.line_cost,
            display_order=item.display_order,
            is_featured=item.is_featured,
            custom_label=item.custom_label,
            options=item.options or {},
            created_at=item.created_at,
            updated_at=item.updated_at,
            product_name=item.product.name if item.product else None,
            product_sku=item.product.sku if item.product else None,
            product_image_url=item.product.image_url if item.product else None,
        )
        items.append(item_resp)

    return BundleResponse(
        id=bundle.id,
        sku=bundle.sku,
        name=bundle.name,
        slug=bundle.slug,
        short_description=bundle.short_description,
        description=bundle.description,
        bundle_price=bundle.bundle_price,
        compare_at_price=bundle.compare_at_price,
        cost=bundle.cost,
        savings_amount=bundle.savings_amount,
        savings_percent=bundle.savings_percent,
        margin_percent=bundle.margin_percent,
        status=BundleStatus(bundle.status),
        available_qty=bundle.available_qty or 0,
        image_url=bundle.image_url,
        images=normalize_images(bundle.images),
        badge_text=bundle.badge_text,
        display_order=bundle.display_order or 0,
        category=bundle.category,
        tags=bundle.tags or [],
        start_date=bundle.start_date,
        end_date=bundle.end_date,
        created_at=bundle.created_at,
        updated_at=bundle.updated_at,
        published_at=bundle.published_at,
        items=items,
        item_count=len(items),
    )


def bundle_to_list_response(bundle) -> BundleListResponse:
    """Convert Bundle model to list response schema."""
    return BundleListResponse(
        id=bundle.id,
        sku=bundle.sku,
        name=bundle.name,
        slug=bundle.slug,
        short_description=bundle.short_description,
        bundle_price=bundle.bundle_price,
        compare_at_price=bundle.compare_at_price,
        savings_amount=bundle.savings_amount,
        savings_percent=bundle.savings_percent,
        status=BundleStatus(bundle.status),
        available_qty=bundle.available_qty or 0,
        image_url=bundle.image_url,
        images=normalize_images(bundle.images),
        badge_text=bundle.badge_text,
        display_order=bundle.display_order or 0,
        category=bundle.category,
        item_count=len(bundle.items) if bundle.items else 0,
        created_at=bundle.created_at,
    )


# ==================== Bundle CRUD Routes ====================

@router.post("/upload-image")
async def upload_bundle_image(
    file: UploadFile = File(...),
    bundle_id: int | None = Form(None),
    current_user: User = Depends(get_current_admin),
):
    """
    Upload a bundle image to S3 and return its URL/key.

    Reuses product image validation (max 10MB, common image types).
    """
    if not storage.is_configured():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage not configured. Set S3_BUCKET and credentials."
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    content_type = file.content_type or "application/octet-stream"
    result = await storage.upload_product_image(
        content=content,
        filename=file.filename or "bundle-image",
        content_type=content_type,
        product_type="bundle",
        product_id=bundle_id,
    )

    if not result.success or not result.url:
        raise HTTPException(status_code=400, detail=result.error or "Upload failed")

    return {
        "url": result.url,
        "s3_key": result.key,
        "content_type": result.content_type,
        "size_bytes": result.size_bytes,
    }

@router.get("/", response_model=PaginatedBundleList)
async def list_bundles(
    status: Optional[str] = Query(None, description="Filter by status"),
    category: Optional[str] = Query(None, description="Filter by category"),
    search: Optional[str] = Query(None, description="Search in name/SKU/description"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """List all bundles with filtering and pagination."""
    bundle_status = None
    if status:
        try:
            bundle_status = BundleStatus(status)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status: {status}"
            )

    bundles, total = await BundleService.list_bundles(
        db,
        status=bundle_status,
        category=category,
        search=search,
        page=page,
        per_page=per_page,
    )

    return PaginatedBundleList(
        items=[bundle_to_list_response(b) for b in bundles],
        total=total,
        page=page,
        per_page=per_page,
        pages=ceil(total / per_page) if total > 0 else 0,
    )


@router.post("/", response_model=BundleResponse, status_code=status.HTTP_201_CREATED)
async def create_bundle(
    data: BundleCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Create a new bundle."""
    try:
        bundle = await BundleService.create_bundle(db, data, current_user.id)
        logger.info(f"Admin {current_user.id} created bundle {bundle.sku}")
        return bundle_to_response(bundle)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/{bundle_id}", response_model=BundleResponse)
async def get_bundle(
    bundle_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Get bundle detail by ID."""
    bundle = await BundleService.get_by_id(db, bundle_id)
    if not bundle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )
    return bundle_to_response(bundle)


@router.put("/{bundle_id}", response_model=BundleResponse)
async def update_bundle(
    bundle_id: int,
    data: BundleUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Update an existing bundle."""
    bundle = await BundleService.update_bundle(db, bundle_id, data, current_user.id)
    if not bundle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )
    logger.info(f"Admin {current_user.id} updated bundle {bundle.sku}")
    return bundle_to_response(bundle)


@router.delete("/{bundle_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_bundle(
    bundle_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Delete (archive) a bundle."""
    success = await BundleService.delete_bundle(db, bundle_id, current_user.id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )
    logger.info(f"Admin {current_user.id} archived bundle {bundle_id}")


# ==================== Bundle Item Routes ====================

@router.post("/{bundle_id}/items", response_model=BundleResponse)
async def add_bundle_item(
    bundle_id: int,
    data: BundleItemCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Add an item to a bundle."""
    bundle = await BundleService.get_by_id(db, bundle_id, include_items=False)
    if not bundle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )

    try:
        await BundleService.add_item(db, bundle_id, data)
        await BundleService._recalculate_bundle(db, bundle)
        await db.commit()

        # Reload with items
        bundle = await BundleService.get_by_id(db, bundle_id)
        logger.info(f"Admin {current_user.id} added item to bundle {bundle.sku}")
        return bundle_to_response(bundle)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.put("/{bundle_id}/items/{item_id}", response_model=BundleResponse)
async def update_bundle_item(
    bundle_id: int,
    item_id: int,
    data: BundleItemUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Update a bundle item."""
    bundle = await BundleService.get_by_id(db, bundle_id)
    if not bundle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )

    # Verify item belongs to this bundle
    item = next((i for i in bundle.items if i.id == item_id), None)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found in this bundle"
        )

    await BundleService.update_item(db, item_id, data)
    await BundleService._recalculate_bundle(db, bundle)
    await db.commit()

    bundle = await BundleService.get_by_id(db, bundle_id)
    return bundle_to_response(bundle)


@router.delete("/{bundle_id}/items/{item_id}", response_model=BundleResponse)
async def remove_bundle_item(
    bundle_id: int,
    item_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Remove an item from a bundle."""
    bundle = await BundleService.get_by_id(db, bundle_id)
    if not bundle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )

    # Verify item belongs to this bundle
    item = next((i for i in bundle.items if i.id == item_id), None)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found in this bundle"
        )

    await BundleService.remove_item(db, item_id)
    await BundleService._recalculate_bundle(db, bundle)
    await db.commit()

    bundle = await BundleService.get_by_id(db, bundle_id)
    logger.info(f"Admin {current_user.id} removed item from bundle {bundle.sku}")
    return bundle_to_response(bundle)


# ==================== Lifecycle Routes ====================

@router.post("/{bundle_id}/publish", response_model=BundleResponse)
async def publish_bundle(
    bundle_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Publish a bundle (DRAFT -> ACTIVE)."""
    success, error = await BundleService.publish_bundle(db, bundle_id, current_user.id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error
        )

    bundle = await BundleService.get_by_id(db, bundle_id)
    logger.info(f"Admin {current_user.id} published bundle {bundle.sku}")
    return bundle_to_response(bundle)


@router.post("/{bundle_id}/unpublish", response_model=BundleResponse)
async def unpublish_bundle(
    bundle_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Unpublish a bundle (ACTIVE -> INACTIVE)."""
    success = await BundleService.unpublish_bundle(db, bundle_id, current_user.id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot unpublish: bundle not found or not active"
        )

    bundle = await BundleService.get_by_id(db, bundle_id)
    logger.info(f"Admin {current_user.id} unpublished bundle {bundle.sku}")
    return bundle_to_response(bundle)


@router.post("/{bundle_id}/archive", status_code=status.HTTP_204_NO_CONTENT)
async def archive_bundle(
    bundle_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Archive a bundle (any status -> ARCHIVED)."""
    success = await BundleService.delete_bundle(db, bundle_id, current_user.id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )
    logger.info(f"Admin {current_user.id} archived bundle {bundle_id}")


@router.post("/{bundle_id}/duplicate", response_model=BundleResponse, status_code=status.HTTP_201_CREATED)
async def duplicate_bundle(
    bundle_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
):
    """Duplicate a bundle as a new DRAFT."""
    new_bundle = await BundleService.duplicate_bundle(db, bundle_id, current_user.id)
    if not new_bundle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bundle not found"
        )

    new_bundle = await BundleService.get_by_id(db, new_bundle.id)
    logger.info(f"Admin {current_user.id} duplicated bundle {bundle_id} to {new_bundle.sku}")
    return bundle_to_response(new_bundle)


# ==================== Pricing Preview Route ====================

@router.post("/calculate-pricing", response_model=BundlePricingResponse)
async def calculate_pricing(
    data: BundlePricingRequest,
    current_user: User = Depends(get_current_admin),
):
    """Calculate pricing preview for proposed bundle items."""
    result = BundleService.calculate_pricing(data)
    return result
