"""
BCW Catalog Admin API v1.0.0

Provides search and management endpoints for BCW supplies catalog.
Per 20251218_bcw_search_create_proposal_v1.1.md

Endpoints:
- GET /admin/bcw/catalog/search - Search BCW catalog with inventory
- GET /admin/bcw/catalog/{mdm_sku} - Get single BCW product
- PATCH /admin/bcw/catalog/{mdm_sku}/pricing - Update pricing
- POST /admin/bcw/catalog/sync-inventory - Trigger inventory sync
- GET /admin/bcw/catalog/categories - Get BCW categories

Requires admin role for all endpoints.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_admin, get_db
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/bcw/catalog", tags=["Admin - BCW Catalog"])


# ==============================================================================
# Schemas
# ==============================================================================

class BCWPricingUpdate(BaseModel):
    """Schema for updating BCW product pricing."""
    our_price: Optional[float] = Field(None, ge=0, le=99999.99, description="Our selling price")
    min_margin_percent: Optional[float] = Field(None, ge=0, le=100, description="Minimum margin floor")


class BCWInventorySyncRequest(BaseModel):
    """Schema for inventory sync request."""
    skus: Optional[List[str]] = Field(None, description="Specific SKUs to sync (null for all)")


# ==============================================================================
# BCW Catalog Search
# ==============================================================================

@router.get("/search")
async def search_bcw_catalog(
    q: Optional[str] = Query(None, description="Search term (product name, SKU)"),
    category: Optional[str] = Query(None, description="BCW category filter"),
    in_catalog: Optional[bool] = Query(None, description="Filter by catalog status (has product)"),
    in_stock: Optional[bool] = Query(None, description="Filter by BCW stock availability"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
) -> Dict[str, Any]:
    """
    Search BCW product catalog with inventory status.

    Returns products from bcw_product_mappings joined with:
    - products table (catalog status, current stock)
    - bcw_inventory_snapshots (live BCW inventory)
    """
    # Build dynamic WHERE clause
    conditions = ["m.is_active = true"]
    params = {"offset": (page - 1) * per_page, "limit": per_page}

    if q:
        conditions.append(
            "(m.product_name ILIKE :q OR m.bcw_sku ILIKE :q OR m.mdm_sku ILIKE :q)"
        )
        params["q"] = f"%{q}%"

    if category:
        conditions.append("m.bcw_category = :category")
        params["category"] = category

    if in_catalog is not None:
        if in_catalog:
            conditions.append("p.id IS NOT NULL")
        else:
            conditions.append("p.id IS NULL")

    where_clause = " AND ".join(conditions)

    # Main query with inventory join (LATERAL for latest snapshot)
    query = f"""
        SELECT
            m.id as mapping_id,
            m.mdm_sku,
            m.bcw_sku,
            m.product_name,
            m.bcw_category,
            m.bcw_cost,
            m.bcw_msrp,
            m.our_price,
            m.min_margin_percent,
            m.is_dropship_only,
            m.sync_inventory,
            p.id as product_id,
            p.stock as catalog_stock,
            p.image_url,
            i.available_qty as bcw_available,
            i.in_stock as bcw_in_stock,
            i.checked_at as inventory_checked_at
        FROM bcw_product_mappings m
        LEFT JOIN products p ON p.sku = m.mdm_sku AND p.deleted_at IS NULL
        LEFT JOIN LATERAL (
            SELECT available_qty, in_stock, checked_at
            FROM bcw_inventory_snapshots
            WHERE sku = m.mdm_sku
            ORDER BY checked_at DESC
            LIMIT 1
        ) i ON true
        WHERE {where_clause}
        ORDER BY m.product_name
        OFFSET :offset LIMIT :limit
    """

    try:
        result = await db.execute(text(query), params)
        rows = result.fetchall()
    except Exception as e:
        logger.error(f"[bcw_catalog] Search query failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to search BCW catalog")

    # Count query for pagination
    count_query = f"""
        SELECT COUNT(*) FROM bcw_product_mappings m
        LEFT JOIN products p ON p.sku = m.mdm_sku AND p.deleted_at IS NULL
        WHERE {where_clause}
    """

    try:
        count_result = await db.execute(text(count_query), params)
        total = count_result.scalar() or 0
    except Exception as e:
        logger.warning(f"[bcw_catalog] Count query failed: {e}")
        total = len(rows)

    # Apply in_stock filter (post-query since it's from lateral join)
    results = []
    for row in rows:
        # Filter by stock if requested
        if in_stock is not None:
            if in_stock and not row.bcw_in_stock:
                continue
            if not in_stock and row.bcw_in_stock:
                continue

        results.append({
            "mapping_id": row.mapping_id,
            "mdm_sku": row.mdm_sku,
            "bcw_sku": row.bcw_sku,
            "product_name": row.product_name,
            "bcw_category": row.bcw_category,
            "image_url": row.image_url,
            "pricing": {
                "bcw_cost": float(row.bcw_cost) if row.bcw_cost else None,
                "bcw_msrp": float(row.bcw_msrp) if row.bcw_msrp else None,
                "our_price": float(row.our_price) if row.our_price else None,
                "min_margin_percent": float(row.min_margin_percent) if row.min_margin_percent else None,
            },
            "in_catalog": row.product_id is not None,
            "product_id": row.product_id,
            "catalog_stock": row.catalog_stock,
            "inventory": {
                "bcw_available": row.bcw_available,
                "bcw_in_stock": row.bcw_in_stock,
                "last_checked": row.inventory_checked_at.isoformat() if row.inventory_checked_at else None,
            } if row.bcw_available is not None else None,
            "is_dropship_only": row.is_dropship_only,
        })

    return {
        "results": results,
        "total": total,
        "page": page,
        "pages": (total + per_page - 1) // per_page if total > 0 else 1,
    }


@router.get("/categories")
async def get_bcw_categories(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
) -> Dict[str, Any]:
    """Get list of BCW product categories with counts."""
    try:
        result = await db.execute(text("""
            SELECT bcw_category, COUNT(*) as count
            FROM bcw_product_mappings
            WHERE is_active = true AND bcw_category IS NOT NULL
            GROUP BY bcw_category
            ORDER BY bcw_category
        """))
        rows = result.fetchall()

        return {
            "categories": [
                {"name": row.bcw_category, "count": row.count}
                for row in rows
            ]
        }
    except Exception as e:
        logger.error(f"[bcw_catalog] Failed to get categories: {e}")
        raise HTTPException(status_code=500, detail="Failed to get categories")


@router.get("/{mdm_sku}")
async def get_bcw_product(
    mdm_sku: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
) -> Dict[str, Any]:
    """Get single BCW product with full details."""
    query = """
        SELECT
            m.*,
            p.id as product_id,
            p.stock as catalog_stock,
            p.image_url,
            p.name as catalog_name,
            i.available_qty as bcw_available,
            i.in_stock as bcw_in_stock,
            i.checked_at as inventory_checked_at
        FROM bcw_product_mappings m
        LEFT JOIN products p ON p.sku = m.mdm_sku AND p.deleted_at IS NULL
        LEFT JOIN LATERAL (
            SELECT available_qty, in_stock, checked_at
            FROM bcw_inventory_snapshots
            WHERE sku = m.mdm_sku
            ORDER BY checked_at DESC
            LIMIT 1
        ) i ON true
        WHERE m.mdm_sku = :mdm_sku
    """

    try:
        result = await db.execute(text(query), {"mdm_sku": mdm_sku})
        row = result.fetchone()
    except Exception as e:
        logger.error(f"[bcw_catalog] Failed to get product {mdm_sku}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get product")

    if not row:
        raise HTTPException(status_code=404, detail=f"BCW product {mdm_sku} not found")

    return {
        "mdm_sku": row.mdm_sku,
        "bcw_sku": row.bcw_sku,
        "product_name": row.product_name,
        "bcw_category": row.bcw_category,
        "image_url": row.image_url,
        "pricing": {
            "bcw_cost": float(row.bcw_cost) if row.bcw_cost else None,
            "bcw_msrp": float(row.bcw_msrp) if row.bcw_msrp else None,
            "our_price": float(row.our_price) if row.our_price else None,
            "min_margin_percent": float(row.min_margin_percent) if row.min_margin_percent else None,
        },
        "in_catalog": row.product_id is not None,
        "product_id": row.product_id,
        "catalog_name": row.catalog_name,
        "catalog_stock": row.catalog_stock,
        "inventory": {
            "bcw_available": row.bcw_available,
            "bcw_in_stock": row.bcw_in_stock,
            "last_checked": row.inventory_checked_at.isoformat() if row.inventory_checked_at else None,
        } if row.bcw_available is not None else None,
        "is_active": row.is_active,
        "is_dropship_only": row.is_dropship_only,
        "sync_inventory": row.sync_inventory,
        "imported_at": row.imported_at.isoformat() if row.imported_at else None,
        "imported_from": row.imported_from,
    }


# ==============================================================================
# BCW Pricing Management
# ==============================================================================

@router.patch("/{mdm_sku}/pricing")
async def update_bcw_pricing(
    mdm_sku: str,
    pricing: BCWPricingUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
) -> Dict[str, Any]:
    """
    Update pricing for a BCW product.

    Updates bcw_product_mappings and optionally the linked product.
    """
    updates = []
    params = {"mdm_sku": mdm_sku}

    if pricing.our_price is not None:
        updates.append("our_price = :our_price")
        params["our_price"] = pricing.our_price

    if pricing.min_margin_percent is not None:
        updates.append("min_margin_percent = :min_margin")
        params["min_margin"] = pricing.min_margin_percent

    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")

    updates.append("updated_at = NOW()")

    query = f"""
        UPDATE bcw_product_mappings
        SET {', '.join(updates)}
        WHERE mdm_sku = :mdm_sku
        RETURNING id
    """

    try:
        result = await db.execute(text(query), params)
        if not result.fetchone():
            raise HTTPException(status_code=404, detail=f"BCW product {mdm_sku} not found")

        # Also update products table price if linked and our_price changed
        if pricing.our_price is not None:
            await db.execute(text("""
                UPDATE products SET price = :price, updated_at = NOW()
                WHERE sku = :mdm_sku AND deleted_at IS NULL
            """), {"mdm_sku": mdm_sku, "price": pricing.our_price})

        await db.commit()

        logger.info(f"[bcw_catalog] Pricing updated for {mdm_sku} by user {current_user.id}")

        return {"status": "updated", "mdm_sku": mdm_sku}

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"[bcw_catalog] Failed to update pricing for {mdm_sku}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update pricing")


# ==============================================================================
# BCW Inventory Sync
# ==============================================================================

@router.post("/sync-inventory")
async def trigger_inventory_sync(
    request: BCWInventorySyncRequest = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
) -> Dict[str, Any]:
    """
    Trigger manual inventory sync for BCW products.

    If skus provided, sync only those. Otherwise sync all active.
    """
    from app.services.bcw.browser_client import BCWBrowserClient
    from app.services.dropship.inventory_sync import BCWInventorySyncService
    
    skus = request.skus if request else None
    logger.info(f"[bcw_catalog] Inventory sync requested by user {current_user.id}, skus={skus}")

    # Initialize client and service
    # Note: This is a synchronous blocking call for now. In production, 
    # this should be offloaded to a background task (Celery/Arq) to avoid timeouts.
    # For MVP/Admin tool usage, we'll run it inline but be mindful of timeouts.
    
    try:
        # Use headless browser for sync
        async with BCWBrowserClient(headless=True) as client:
            # Login required for accurate inventory? Usually yes for pricing/availability
            # The service handles its own login checks if needed, or we can force it here
            # For now, we assume the client handles session injection if configured
            
            service = BCWInventorySyncService(client, db)
            
            if skus:
                # Sync specific items (faster)
                # check_items_availability returns a dict, we want to return stats
                # so we might need a different method or wrap this
                availability = await service.check_items_availability(skus)
                return {
                    "status": "completed",
                    "mode": "specific_skus",
                    "count": len(availability),
                    "results": [
                        {
                            "sku": sku, 
                            "in_stock": info.in_stock, 
                            "qty": info.available_qty
                        } for sku, info in availability.items()
                    ]
                }
            else:
                # Sync all active (slower)
                result = await service.sync_all_active_products(batch_size=20)
                return {
                    "status": "completed" if result.success else "partial_failure",
                    "mode": "full_sync",
                    "stats": {
                        "checked": result.total_checked,
                        "updated": result.updated_count,
                        "out_of_stock": result.out_of_stock_count,
                        "errors": result.error_count
                    },
                    "duration_ms": result.duration_ms
                }

    except Exception as e:
        logger.error(f"[bcw_catalog] Inventory sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# BCW Product Activation
# ==============================================================================

@router.post("/{mdm_sku}/activate")
async def activate_bcw_product(
    mdm_sku: str,
    price: float = Query(..., ge=0.01, description="Selling price for the product"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin),
) -> Dict[str, Any]:
    """
    Activate a BCW product in the catalog.

    Creates a product entry if it doesn't exist, or updates the existing one.
    Sets the price on both bcw_product_mappings and products tables.
    """
    # Get the BCW mapping
    result = await db.execute(text("""
        SELECT m.*, p.id as product_id
        FROM bcw_product_mappings m
        LEFT JOIN products p ON p.sku = m.mdm_sku AND p.deleted_at IS NULL
        WHERE m.mdm_sku = :mdm_sku
    """), {"mdm_sku": mdm_sku})
    row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"BCW product {mdm_sku} not found")

    try:
        # Update bcw_product_mappings price
        await db.execute(text("""
            UPDATE bcw_product_mappings
            SET our_price = :price, updated_at = NOW()
            WHERE mdm_sku = :mdm_sku
        """), {"mdm_sku": mdm_sku, "price": price})

        if row.product_id:
            # Update existing product
            await db.execute(text("""
                UPDATE products
                SET price = :price, is_active = true, updated_at = NOW()
                WHERE id = :product_id
            """), {"product_id": row.product_id, "price": price})
            action = "updated"
        else:
            # Create new product
            await db.execute(text("""
                INSERT INTO products (
                    sku, name, description, category, price, stock,
                    image_url, is_active, created_at, updated_at
                ) VALUES (
                    :sku, :name, :description, 'Supplies', :price, 0,
                    :image_url, true, NOW(), NOW()
                )
            """), {
                "sku": mdm_sku,
                "name": row.product_name,
                "description": f"BCW {row.product_name}. Professional-grade comic book storage and protection.",
                "price": price,
                "image_url": f"https://mdm-comics-assets.s3.us-east-2.amazonaws.com/bcw-products/{mdm_sku}/00_{row.bcw_sku.lower()}.jpg",
            })
            action = "created"

        await db.commit()

        logger.info(f"[bcw_catalog] Product {mdm_sku} {action} by user {current_user.id}")

        return {
            "status": "success",
            "action": action,
            "mdm_sku": mdm_sku,
            "price": price,
        }

    except Exception as e:
        await db.rollback()
        logger.error(f"[bcw_catalog] Failed to activate {mdm_sku}: {e}")
        raise HTTPException(status_code=500, detail="Failed to activate product")
