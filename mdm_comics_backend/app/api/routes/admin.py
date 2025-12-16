"""
Admin API Routes for Inventory Management

Per constitution_cyberSec.json Section 3:
- All admin endpoints require is_admin=True
- CSRF protection on mutations
- Input validation
"""
import logging
import hashlib
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query, UploadFile, File, Form
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text, and_, or_
from sqlalchemy.orm import selectinload

from app.core.database import get_db
from app.api.deps import get_current_admin
from app.models import User, Product, BarcodeQueue, StockMovement, InventoryAlert
from app.services.barcode_matcher import match_barcode, process_barcode_queue_item

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


# ----- Pydantic Schemas -----

class BarcodeInput(BaseModel):
    barcode: str = Field(..., min_length=8, max_length=50)
    barcode_type: str = Field(default="UPC", pattern="^(UPC|ISBN|EAN)$")
    scanned_at: Optional[datetime] = None


class BarcodeQueueRequest(BaseModel):
    barcodes: List[BarcodeInput]


class QueueProcessRequest(BaseModel):
    action: str = Field(..., pattern="^(create_product|add_to_existing|skip)$")
    product_data: Optional[dict] = None


class BatchProcessRequest(BaseModel):
    ids: List[int]
    action: str = Field(..., pattern="^(create_products|add_all|skip_all)$")
    default_stock: int = Field(default=1, ge=1)


class StockAdjustmentRequest(BaseModel):
    quantity: int  # positive for increase, negative for decrease
    movement_type: str = Field(default="adjustment", pattern="^(adjustment|damaged|returned|transfer)$")
    reason: str = Field(..., min_length=3, max_length=255)


class ProductUpdateRequest(BaseModel):
    name: Optional[str] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    stock: Optional[int] = None
    bin_id: Optional[str] = None
    upc: Optional[str] = None
    isbn: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None


# ----- Dashboard -----

@router.get("/dashboard")
async def get_dashboard(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get admin dashboard overview with key metrics."""
    # Product stats - use COALESCE for low_stock_threshold in case column doesn't exist
    try:
        product_result = await db.execute(text("""
            SELECT
                COUNT(*) as total_products,
                COALESCE(SUM(stock * price), 0) as total_value,
                COUNT(*) FILTER (WHERE stock <= COALESCE(low_stock_threshold, 5)) as low_stock_count
            FROM products
            WHERE deleted_at IS NULL
        """))
        product_row = product_result.fetchone()
        total_products = product_row[0]
        total_value = float(product_row[1])
        low_stock_count = product_row[2]
    except Exception as e:
        logger.warning(f"Product stats query failed: {e}")
        total_products = 0
        total_value = 0.0
        low_stock_count = 0

    # Pending barcode queue
    try:
        queue_result = await db.execute(text("""
            SELECT COUNT(*) FROM barcode_queue WHERE status = 'pending'
        """))
        pending_queue = queue_result.scalar() or 0
    except Exception as e:
        logger.warning(f"Queue query failed: {e}")
        pending_queue = 0

    # Recent orders (last 7 days)
    try:
        orders_result = await db.execute(text("""
            SELECT COUNT(*) FROM orders
            WHERE created_at >= NOW() - INTERVAL '7 days'
        """))
        recent_orders = orders_result.scalar() or 0
    except Exception as e:
        logger.warning(f"Orders count query failed: {e}")
        recent_orders = 0

    # Recent orders list (last 5) - join with users to get email
    recent_orders_list = []
    try:
        recent_orders_result = await db.execute(text("""
            SELECT o.id, o.order_number, u.email, o.status, o.created_at
            FROM orders o
            LEFT JOIN users u ON o.user_id = u.id
            ORDER BY o.created_at DESC
            LIMIT 5
        """))
        recent_orders_list = [
            {
                "id": row[0],
                "order_number": row[1],
                "customer_email": row[2],
                "status": row[3],
                "created_at": row[4].isoformat() if row[4] else None
            }
            for row in recent_orders_result.fetchall()
        ]
    except Exception as e:
        logger.warning(f"Recent orders query failed: {e}")

    return {
        "total_products": total_products,
        "total_value": total_value,
        "low_stock_count": low_stock_count,
        "pending_queue": pending_queue,
        "recent_orders": recent_orders,
        "recent_orders_list": recent_orders_list
    }


# ----- Barcode Queue Endpoints -----

@router.post("/barcode-queue")
async def add_to_barcode_queue(
    request: BarcodeQueueRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Add barcodes to the scan queue (mobile scanner uses this).

    Immediately attempts to match each barcode against existing products.
    If match found, can optionally auto-increment stock.
    """
    queued = 0
    queue_ids = []
    results = []

    for barcode_input in request.barcodes:
        # Attempt to match the barcode
        match_result = await match_barcode(
            db=db,
            barcode=barcode_input.barcode,
            barcode_type=barcode_input.barcode_type,
            user_id=current_user.id,
            auto_increment_stock=False  # Don't auto-increment on queue add
        )

        # Create queue entry
        queue_item = BarcodeQueue(
            barcode=barcode_input.barcode,
            barcode_type=barcode_input.barcode_type,
            user_id=current_user.id,
            scanned_at=barcode_input.scanned_at or datetime.now(timezone.utc),
            status="matched" if match_result.matched else "pending",
            matched_product_id=match_result.product_id,
            matched_comic_id=match_result.comic_id,
            matched_funko_id=match_result.funko_id,
            match_source=match_result.match_type,
            match_confidence=match_result.confidence
        )
        db.add(queue_item)
        await db.flush()

        queue_ids.append(queue_item.id)
        queued += 1

        results.append({
            "queue_id": queue_item.id,
            "barcode": barcode_input.barcode,
            "matched": match_result.matched,
            "match_type": match_result.match_type,
            "confidence": match_result.confidence,
            "message": match_result.message
        })

    await db.commit()

    return {
        "queued": queued,
        "queue_ids": queue_ids,
        "results": results
    }


@router.get("/barcode-queue")
async def list_barcode_queue(
    status: Optional[str] = Query(None, pattern="^(pending|matched|processing|processed|failed|skipped)$"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """List queued barcodes with optional status filter."""
    query = select(BarcodeQueue).order_by(BarcodeQueue.scanned_at.desc())

    if status:
        query = query.where(BarcodeQueue.status == status)

    # Get total count
    count_query = select(func.count(BarcodeQueue.id))
    if status:
        count_query = count_query.where(BarcodeQueue.status == status)
    total_result = await db.execute(count_query)
    total = total_result.scalar()

    # Get status counts
    status_counts = await db.execute(text("""
        SELECT status, COUNT(*) FROM barcode_queue GROUP BY status
    """))
    counts = {row[0]: row[1] for row in status_counts.fetchall()}

    # Get items with pagination
    query = query.offset(offset).limit(limit)
    result = await db.execute(query)
    items = result.scalars().all()

    return {
        "items": [
            {
                "id": item.id,
                "barcode": item.barcode,
                "barcode_type": item.barcode_type,
                "status": item.status,
                "matched_product_id": item.matched_product_id,
                "matched_comic_id": item.matched_comic_id,
                "match_source": item.match_source,
                "match_confidence": item.match_confidence,
                "scanned_at": item.scanned_at.isoformat() if item.scanned_at else None,
                "processed_at": item.processed_at.isoformat() if item.processed_at else None,
            }
            for item in items
        ],
        "total": total,
        "pending": counts.get("pending", 0),
        "matched": counts.get("matched", 0),
        "processed": counts.get("processed", 0)
    }


@router.post("/barcode-queue/{queue_id}/process")
async def process_queue_item(
    queue_id: int,
    request: QueueProcessRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Process a single queued barcode item."""
    result = await db.execute(
        select(BarcodeQueue).where(BarcodeQueue.id == queue_id)
    )
    queue_item = result.scalar_one_or_none()

    if not queue_item:
        raise HTTPException(status_code=404, detail="Queue item not found")

    if queue_item.status in ("processed", "skipped"):
        raise HTTPException(status_code=400, detail=f"Item already {queue_item.status}")

    process_result = await process_barcode_queue_item(
        db=db,
        queue_item=queue_item,
        action=request.action,
        product_data=request.product_data,
        user_id=current_user.id
    )

    return process_result


@router.post("/barcode-queue/batch-process")
async def batch_process_queue(
    request: BatchProcessRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Process multiple queue items at once."""
    results = []
    success = 0
    errors = 0

    for queue_id in request.ids:
        result = await db.execute(
            select(BarcodeQueue).where(BarcodeQueue.id == queue_id)
        )
        queue_item = result.scalar_one_or_none()

        if not queue_item or queue_item.status in ("processed", "skipped"):
            errors += 1
            continue

        # Determine action based on request
        if request.action == "add_all" and queue_item.matched_product_id:
            action = "add_to_existing"
            product_data = None
        elif request.action == "skip_all":
            action = "skip"
            product_data = None
        else:
            errors += 1
            continue

        try:
            process_result = await process_barcode_queue_item(
                db=db,
                queue_item=queue_item,
                action=action,
                product_data=product_data,
                user_id=current_user.id
            )
            results.append(process_result)
            success += 1
        except Exception as e:
            logger.error(f"Error processing queue item {queue_id}: {e}")
            errors += 1

    return {
        "processed": success,
        "errors": errors,
        "results": results
    }


# ----- Product Management -----

@router.get("/products")
async def list_products(
    search: Optional[str] = None,
    category: Optional[str] = None,
    low_stock: bool = False,
    include_deleted: bool = False,
    sort: str = Query("-updated_at", pattern="^-?(name|price|stock|updated_at|created_at)$"),
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Enhanced product listing with inventory data."""
    query = select(Product)

    # Exclude soft-deleted unless requested
    if not include_deleted:
        query = query.where(Product.deleted_at.is_(None))

    # Search
    if search:
        search_term = f"%{search}%"
        query = query.where(
            or_(
                Product.name.ilike(search_term),
                Product.sku.ilike(search_term),
                Product.upc.ilike(search_term),
                Product.isbn.ilike(search_term)
            )
        )

    # Category filter
    if category:
        query = query.where(Product.category == category)

    # Low stock filter
    if low_stock:
        query = query.where(Product.stock <= Product.low_stock_threshold)

    # Sorting
    sort_desc = sort.startswith("-")
    sort_field = sort.lstrip("-")
    sort_col = getattr(Product, sort_field)
    query = query.order_by(sort_col.desc() if sort_desc else sort_col)

    # Get total count
    count_query = select(func.count(Product.id))
    if not include_deleted:
        count_query = count_query.where(Product.deleted_at.is_(None))
    if search:
        count_query = count_query.where(
            or_(
                Product.name.ilike(search_term),
                Product.sku.ilike(search_term),
                Product.upc.ilike(search_term)
            )
        )
    if category:
        count_query = count_query.where(Product.category == category)
    if low_stock:
        count_query = count_query.where(Product.stock <= Product.low_stock_threshold)

    total_result = await db.execute(count_query)
    total = total_result.scalar()

    # Get summary stats
    try:
        stats_result = await db.execute(text("""
            SELECT
                COUNT(*) as total,
                COALESCE(SUM(stock * price), 0) as total_value,
                COUNT(*) FILTER (WHERE stock <= COALESCE(low_stock_threshold, 5)) as low_stock_count
            FROM products
            WHERE deleted_at IS NULL
        """))
        stats = stats_result.fetchone()
    except Exception as e:
        logger.warning(f"Product stats query failed: {e}")
        stats = (0, 0, 0)

    # Get items with pagination
    query = query.offset(offset).limit(limit)
    result = await db.execute(query)
    products = result.scalars().all()

    return {
        "items": [
            {
                "id": p.id,
                "sku": p.sku,
                "name": p.name,
                "category": p.category,
                "price": p.price,
                "original_price": p.original_price,
                "stock": p.stock,
                "low_stock_threshold": p.low_stock_threshold,
                "upc": p.upc,
                "isbn": p.isbn,
                "bin_id": p.bin_id,
                "image_url": p.image_url,
                "is_low_stock": p.stock <= p.low_stock_threshold,
                "is_deleted": p.deleted_at is not None,
                "updated_at": p.updated_at.isoformat() if p.updated_at else None,
            }
            for p in products
        ],
        "total": total,
        "total_value": float(stats[1]) if stats else 0,
        "low_stock_count": stats[2] if stats else 0
    }


@router.patch("/products/{product_id}")
async def update_product(
    product_id: int,
    request: ProductUpdateRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Update product details."""
    result = await db.execute(
        select(Product).where(Product.id == product_id)
    )
    product = result.scalar_one_or_none()

    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if product.deleted_at:
        raise HTTPException(status_code=400, detail="Cannot update deleted product")

    # Update fields
    update_data = request.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(product, field, value)

    product.updated_at = datetime.now(timezone.utc)
    await db.commit()

    return {"status": "updated", "product_id": product_id}


@router.delete("/products/{product_id}")
async def delete_product(
    product_id: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Soft delete a product."""
    result = await db.execute(
        select(Product).where(Product.id == product_id)
    )
    product = result.scalar_one_or_none()

    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if product.deleted_at:
        raise HTTPException(status_code=400, detail="Product already deleted")

    product.soft_delete()
    await db.commit()

    logger.info(f"Product {product_id} soft-deleted by user {current_user.id}")

    return {"status": "deleted", "product_id": product_id}


@router.post("/products/{product_id}/restore")
async def restore_product(
    product_id: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Restore a soft-deleted product."""
    result = await db.execute(
        select(Product).where(Product.id == product_id)
    )
    product = result.scalar_one_or_none()

    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if not product.deleted_at:
        raise HTTPException(status_code=400, detail="Product is not deleted")

    product.restore()
    await db.commit()

    logger.info(f"Product {product_id} restored by user {current_user.id}")

    return {"status": "restored", "product_id": product_id}


@router.post("/products/{product_id}/adjust-stock")
async def adjust_product_stock(
    product_id: int,
    request: StockAdjustmentRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Adjust stock with audit trail."""
    result = await db.execute(
        select(Product).where(Product.id == product_id)
    )
    product = result.scalar_one_or_none()

    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if product.deleted_at:
        raise HTTPException(status_code=400, detail="Cannot adjust stock of deleted product")

    previous_stock = product.stock
    new_stock = previous_stock + request.quantity

    if new_stock < 0:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot reduce stock below 0 (current: {previous_stock}, adjustment: {request.quantity})"
        )

    # Update stock
    product.stock = new_stock
    product.updated_at = datetime.now(timezone.utc)

    # Log movement
    movement = StockMovement(
        product_id=product_id,
        movement_type=request.movement_type,
        quantity=request.quantity,
        previous_stock=previous_stock,
        new_stock=new_stock,
        reason=request.reason,
        reference_type="manual",
        user_id=current_user.id
    )
    db.add(movement)

    await db.commit()

    logger.info(
        f"Stock adjusted for product {product_id}: {previous_stock} -> {new_stock} "
        f"({request.movement_type}: {request.quantity:+d}) by user {current_user.id}"
    )

    return {
        "status": "adjusted",
        "product_id": product_id,
        "previous_stock": previous_stock,
        "new_stock": new_stock,
        "adjustment": request.quantity
    }


@router.get("/products/{product_id}/stock-history")
async def get_stock_history(
    product_id: int,
    limit: int = Query(50, ge=1, le=200),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get stock movement history for a product."""
    result = await db.execute(
        select(StockMovement)
        .where(StockMovement.product_id == product_id)
        .order_by(StockMovement.created_at.desc())
        .limit(limit)
    )
    movements = result.scalars().all()

    return {
        "movements": [
            {
                "id": m.id,
                "movement_type": m.movement_type,
                "quantity": m.quantity,
                "previous_stock": m.previous_stock,
                "new_stock": m.new_stock,
                "reason": m.reason,
                "reference_type": m.reference_type,
                "reference_id": m.reference_id,
                "user_id": m.user_id,
                "created_at": m.created_at.isoformat() if m.created_at else None,
            }
            for m in movements
        ]
    }


# ----- Reports -----

@router.get("/reports/inventory-summary")
async def get_inventory_summary(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get inventory summary with value calculations."""
    try:
        result = await db.execute(text("""
            SELECT
                COUNT(*) as total_products,
                COALESCE(SUM(stock), 0) as total_stock_units,
                COALESCE(SUM(stock * price), 0) as total_retail_value,
                COALESCE(SUM(stock * COALESCE(original_price, price * 0.5)), 0) as total_cost_value,
                COUNT(*) FILTER (WHERE stock <= COALESCE(low_stock_threshold, 5)) as low_stock_count,
                COUNT(*) FILTER (WHERE stock = 0) as out_of_stock_count
            FROM products
            WHERE deleted_at IS NULL
        """))
        row = result.fetchone()
        total_products = row[0] if row else 0
        total_stock_units = row[1] if row else 0
        total_retail_value = float(row[2]) if row else 0.0
        total_cost_value = float(row[3]) if row else 0.0
        low_stock_count = row[4] if row else 0
        out_of_stock_count = row[5] if row else 0
    except Exception as e:
        logger.warning(f"Inventory summary query failed: {e}")
        total_products = 0
        total_stock_units = 0
        total_retail_value = 0.0
        total_cost_value = 0.0
        low_stock_count = 0
        out_of_stock_count = 0

    # Category breakdown
    by_category = {}
    try:
        category_result = await db.execute(text("""
            SELECT
                category,
                COUNT(*) as count,
                COALESCE(SUM(stock), 0) as stock,
                COALESCE(SUM(stock * price), 0) as value
            FROM products
            WHERE deleted_at IS NULL
            GROUP BY category
            ORDER BY value DESC
        """))

        by_category = {
            r[0]: {"count": r[1], "stock": r[2], "value": float(r[3])}
            for r in category_result.fetchall()
        }
    except Exception as e:
        logger.warning(f"Category breakdown query failed: {e}")

    return {
        "total_products": total_products,
        "total_stock_units": total_stock_units,
        "total_retail_value": total_retail_value,
        "total_cost_value": total_cost_value,
        "potential_margin": total_retail_value - total_cost_value,
        "low_stock_count": low_stock_count,
        "out_of_stock_count": out_of_stock_count,
        "by_category": by_category
    }


@router.get("/reports/low-stock")
async def get_low_stock_report(
    threshold: int = Query(5, ge=0, le=100),
    limit: int = Query(50, ge=1, le=200),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get products below their low stock threshold."""
    try:
        # Use raw SQL with COALESCE for robustness
        result = await db.execute(text("""
            SELECT id, sku, name, category, stock,
                   COALESCE(low_stock_threshold, :threshold) as threshold,
                   price, bin_id
            FROM products
            WHERE deleted_at IS NULL
              AND stock <= COALESCE(low_stock_threshold, :threshold)
            ORDER BY stock ASC
            LIMIT :limit
        """), {"threshold": threshold, "limit": limit})
        rows = result.fetchall()

        return {
            "items": [
                {
                    "product_id": r[0],
                    "sku": r[1],
                    "name": r[2],
                    "category": r[3],
                    "current_stock": r[4],
                    "threshold": r[5],
                    "price": float(r[6]) if r[6] else 0.0,
                    "bin_id": r[7],
                }
                for r in rows
            ]
        }
    except Exception as e:
        logger.warning(f"Low stock report query failed: {e}")
        return {"items": []}


@router.get("/reports/price-changes")
async def get_price_changes(
    days: int = Query(1, ge=1, le=90),  # Default to 1 day for daily tracking
    threshold_pct: float = Query(2.0, ge=0),  # Default to 2% threshold
    limit: int = Query(500, ge=10, le=1000),  # Configurable limit, default 500
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get significant price changes from price_changelog.

    Returns up to `limit` results, split evenly between comics and funkos.
    Default: 250 comics + 250 funkos = 500 total.
    """
    try:
        half_limit = limit // 2

        # Get comics and funkos separately to ensure even split
        comics_result = await db.execute(text("""
            SELECT
                entity_type, entity_id, entity_name, field_name,
                old_value, new_value, change_pct, changed_at
            FROM price_changelog
            WHERE changed_at > NOW() - make_interval(days => :days)
              AND ABS(COALESCE(change_pct, 0)) >= :threshold
              AND entity_type = 'comic'
            ORDER BY ABS(COALESCE(change_pct, 0)) DESC
            LIMIT :limit
        """), {"days": days, "threshold": threshold_pct, "limit": half_limit})

        funkos_result = await db.execute(text("""
            SELECT
                entity_type, entity_id, entity_name, field_name,
                old_value, new_value, change_pct, changed_at
            FROM price_changelog
            WHERE changed_at > NOW() - make_interval(days => :days)
              AND ABS(COALESCE(change_pct, 0)) >= :threshold
              AND entity_type = 'funko'
            ORDER BY ABS(COALESCE(change_pct, 0)) DESC
            LIMIT :limit
        """), {"days": days, "threshold": threshold_pct, "limit": half_limit})

        comics = comics_result.fetchall()
        funkos = funkos_result.fetchall()

        # Combine and sort by change_pct descending
        all_changes = list(comics) + list(funkos)
        all_changes.sort(key=lambda r: abs(float(r[6]) if r[6] else 0), reverse=True)

        return {
            "changes": [
                {
                    "entity_type": r[0],
                    "entity_id": r[1],
                    "entity_name": r[2],
                    "field": r[3],
                    "old_value": float(r[4]) if r[4] else 0,
                    "new_value": float(r[5]) if r[5] else 0,
                    "change_pct": float(r[6]) if r[6] else 0,
                    "changed_at": r[7].isoformat() if r[7] else None,
                }
                for r in all_changes
            ],
            "meta": {
                "comics_count": len(comics),
                "funkos_count": len(funkos),
                "total": len(all_changes),
                "days": days,
                "threshold_pct": threshold_pct,
            }
        }
    except Exception as e:
        logger.warning(f"Price changes query failed: {e}")
        # Return empty list if table doesn't exist yet
        return {"changes": [], "meta": {"comics_count": 0, "funkos_count": 0, "total": 0}}


@router.get("/reports/entity/{entity_type}/{entity_id}")
async def get_entity_details(
    entity_type: str,
    entity_id: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get detailed info for a comic or funko for the price change drawer."""
    try:
        if entity_type == "comic":
            result = await db.execute(text("""
                SELECT
                    ci.id, ci.issue_name, ci.number, ci.image, ci.cover_date,
                    ci.price_loose, ci.price_cib, ci.price_new, ci.price_graded,
                    ci.upc, ci.isbn, ci.pricecharting_id, ci.year,
                    cs.name as series_name, cp.name as publisher_name
                FROM comic_issues ci
                LEFT JOIN comic_series cs ON ci.series_id = cs.id
                LEFT JOIN comic_publishers cp ON cs.publisher_id = cp.id
                WHERE ci.id = :entity_id
            """), {"entity_id": entity_id})
            row = result.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Comic not found")

            return {
                "entity_type": "comic",
                "id": row[0],
                "name": row[1] or f"#{row[2]}",
                "number": row[2],
                "image_url": row[3],
                "cover_date": row[4].isoformat() if row[4] else None,
                "price_loose": float(row[5]) if row[5] else None,
                "price_cib": float(row[6]) if row[6] else None,
                "price_new": float(row[7]) if row[7] else None,
                "price_graded": float(row[8]) if row[8] else None,
                "upc": row[9],
                "isbn": row[10],
                "pricecharting_id": row[11],
                "year": row[12],
                "series_name": row[13],
                "publisher_name": row[14],
            }

        elif entity_type == "funko":
            result = await db.execute(text("""
                SELECT
                    id, title, handle, image_url, category, license,
                    product_type, box_number, funko_url,
                    price_loose, price_cib, price_new, pricecharting_id
                FROM funkos
                WHERE id = :entity_id
            """), {"entity_id": entity_id})
            row = result.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Funko not found")

            return {
                "entity_type": "funko",
                "id": row[0],
                "name": row[1],
                "handle": row[2],
                "image_url": row[3],
                "category": row[4],
                "license": row[5],
                "product_type": row[6],
                "box_number": row[7],
                "funko_url": row[8],
                "price_loose": float(row[9]) if row[9] else None,
                "price_cib": float(row[10]) if row[10] else None,
                "price_new": float(row[11]) if row[11] else None,
                "pricecharting_id": row[12],
            }
        else:
            raise HTTPException(status_code=400, detail=f"Unknown entity type: {entity_type}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Entity details query failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch entity details")


# ----- User Management -----

class UserCreateRequest(BaseModel):
    email: str = Field(..., min_length=3, max_length=255)
    password: str = Field(..., min_length=8, max_length=128)
    name: str = Field(..., min_length=1, max_length=100)
    is_admin: bool = False
    is_active: bool = True


class UserUpdateRequest(BaseModel):
    email: Optional[str] = Field(None, min_length=3, max_length=255)
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    password: Optional[str] = Field(None, min_length=8, max_length=128)
    is_admin: Optional[bool] = None
    is_active: Optional[bool] = None


@router.get("/users")
async def list_users(
    search: Optional[str] = None,
    is_admin: Optional[bool] = None,
    is_active: Optional[bool] = None,
    sort: str = Query("-created_at", pattern="^-?(name|email|created_at|is_admin)$"),
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """List all users with filtering and search."""
    from app.core.security import get_password_hash
    query = select(User)

    # Search by email or name
    if search:
        search_term = f"%{search}%"
        query = query.where(
            or_(
                User.email.ilike(search_term),
                User.name.ilike(search_term)
            )
        )

    # Filter by admin status
    if is_admin is not None:
        query = query.where(User.is_admin == is_admin)

    # Filter by active status
    if is_active is not None:
        query = query.where(User.is_active == is_active)

    # Sorting
    sort_desc = sort.startswith("-")
    sort_field = sort.lstrip("-")
    sort_col = getattr(User, sort_field)
    query = query.order_by(sort_col.desc() if sort_desc else sort_col)

    # Get total count
    count_query = select(func.count(User.id))
    if search:
        count_query = count_query.where(
            or_(
                User.email.ilike(search_term),
                User.name.ilike(search_term)
            )
        )
    if is_admin is not None:
        count_query = count_query.where(User.is_admin == is_admin)
    if is_active is not None:
        count_query = count_query.where(User.is_active == is_active)

    total_result = await db.execute(count_query)
    total = total_result.scalar()

    # Get users with pagination
    query = query.offset(offset).limit(limit)
    result = await db.execute(query)
    users = result.scalars().all()

    return {
        "items": [
            {
                "id": u.id,
                "email": u.email,
                "name": u.name,
                "is_admin": u.is_admin,
                "is_active": u.is_active,
                "created_at": u.created_at.isoformat() if u.created_at else None,
                "updated_at": u.updated_at.isoformat() if u.updated_at else None,
            }
            for u in users
        ],
        "total": total
    }


@router.get("/users/{user_id}")
async def get_user(
    user_id: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get a single user by ID."""
    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "is_admin": user.is_admin,
        "is_active": user.is_active,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None,
    }


@router.post("/users", status_code=201)
async def create_user(
    request: UserCreateRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Create a new user (admin only)."""
    from app.core.security import get_password_hash

    # Check if email already exists
    existing = await db.execute(
        select(User).where(User.email == request.email)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Email already registered")

    # Create user
    user = User(
        email=request.email,
        hashed_password=get_password_hash(request.password),
        name=request.name,
        is_admin=request.is_admin,
        is_active=request.is_active,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    logger.info(f"User {user.id} created by admin {current_user.id}")

    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "is_admin": user.is_admin,
        "is_active": user.is_active,
        "created_at": user.created_at.isoformat() if user.created_at else None,
    }


@router.patch("/users/{user_id}")
async def update_user(
    user_id: int,
    request: UserUpdateRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Update a user (admin only)."""
    from app.core.security import get_password_hash

    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent admin from demoting themselves
    if user_id == current_user.id and request.is_admin is False:
        raise HTTPException(
            status_code=400,
            detail="Cannot remove your own admin status"
        )

    # Prevent deactivating yourself
    if user_id == current_user.id and request.is_active is False:
        raise HTTPException(
            status_code=400,
            detail="Cannot deactivate your own account"
        )

    # Check email uniqueness if changing
    if request.email and request.email != user.email:
        existing = await db.execute(
            select(User).where(User.email == request.email)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Email already registered")

    # Update fields
    update_data = request.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        if field == "password":
            user.hashed_password = get_password_hash(value)
        else:
            setattr(user, field, value)

    user.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(user)

    logger.info(f"User {user_id} updated by admin {current_user.id}: {list(update_data.keys())}")

    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "is_admin": user.is_admin,
        "is_active": user.is_active,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None,
    }


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Delete a user (admin only). This deactivates the account."""
    if user_id == current_user.id:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete your own account"
        )

    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Soft delete by deactivating
    user.is_active = False
    user.updated_at = datetime.now(timezone.utc)
    await db.commit()

    logger.info(f"User {user_id} deleted (deactivated) by admin {current_user.id}")

    return {"status": "deleted", "user_id": user_id}


@router.post("/users/{user_id}/toggle-admin")
async def toggle_user_admin(
    user_id: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Toggle admin status for a user."""
    if user_id == current_user.id:
        raise HTTPException(
            status_code=400,
            detail="Cannot change your own admin status"
        )

    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.is_admin = not user.is_admin
    user.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(user)

    action = "promoted to" if user.is_admin else "demoted from"
    logger.info(f"User {user_id} {action} admin by {current_user.id}")

    return {
        "id": user.id,
        "email": user.email,
        "is_admin": user.is_admin,
        "message": f"User {action} admin"
    }


# ----- Pipeline Job Management -----

@router.get("/pipeline/checkpoints")
async def get_pipeline_checkpoints(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get status of all pipeline job checkpoints."""
    result = await db.execute(text("""
        SELECT
            job_name, job_type, is_running,
            last_run_started, last_run_completed,
            total_processed, total_updated, total_errors,
            last_error, updated_at
        FROM pipeline_checkpoints
        ORDER BY job_name
    """))

    checkpoints = result.fetchall()
    return {
        "checkpoints": [
            {
                "job_name": r[0],
                "job_type": r[1],
                "is_running": r[2],
                "last_run_started": r[3].isoformat() if r[3] else None,
                "last_run_completed": r[4].isoformat() if r[4] else None,
                "total_processed": r[5] or 0,
                "total_updated": r[6] or 0,
                "total_errors": r[7] or 0,
                "last_error": r[8],
                "updated_at": r[9].isoformat() if r[9] else None,
            }
            for r in checkpoints
        ]
    }


@router.post("/pipeline/checkpoints/{job_name}/clear")
async def clear_pipeline_checkpoint(
    job_name: str,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Manually clear a stale checkpoint (set is_running=false)."""
    result = await db.execute(text("""
        UPDATE pipeline_checkpoints
        SET is_running = false,
            last_error = COALESCE(last_error, '') || E'\nManually cleared by admin at ' || NOW()::text
        WHERE job_name = :name
        RETURNING job_name
    """), {"name": job_name})

    cleared = result.fetchone()
    if not cleared:
        raise HTTPException(status_code=404, detail=f"Checkpoint not found: {job_name}")

    await db.commit()
    logger.info(f"Pipeline checkpoint cleared: {job_name} by {current_user.id}")

    return {"message": f"Checkpoint cleared for job: {job_name}"}


@router.get("/pipeline/stats")
async def get_pipeline_stats(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get pipeline statistics including DLQ counts and record counts."""
    # DLQ counts by status
    dlq_result = await db.execute(text("""
        SELECT status, COUNT(*) as count
        FROM dead_letter_queue
        GROUP BY status
    """))
    dlq_counts = {r[0]: r[1] for r in dlq_result.fetchall()}

    # Record counts with pricecharting_id
    pc_result = await db.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM funkos WHERE pricecharting_id IS NOT NULL) as funkos_with_pc,
            (SELECT COUNT(*) FROM comic_issues WHERE pricecharting_id IS NOT NULL) as comics_with_pc,
            (SELECT COUNT(*) FROM funkos) as total_funkos,
            (SELECT COUNT(*) FROM comic_issues) as total_comics
    """))
    counts = pc_result.fetchone()

    # Price changelog count (last 24h)
    changelog_result = await db.execute(text("""
        SELECT COUNT(*)
        FROM price_changelog
        WHERE changed_at > NOW() - INTERVAL '24 hours'
    """))
    changes_24h = changelog_result.scalar() or 0

    return {
        "dlq": {
            "pending": dlq_counts.get("PENDING", 0),
            "retrying": dlq_counts.get("RETRYING", 0),
            "resolved": dlq_counts.get("RESOLVED", 0),
            "dead": dlq_counts.get("DEAD", 0),
        },
        "records": {
            "funkos_with_pricecharting": counts[0] if counts else 0,
            "comics_with_pricecharting": counts[1] if counts else 0,
            "total_funkos": counts[2] if counts else 0,
            "total_comics": counts[3] if counts else 0,
        },
        "price_changes_24h": changes_24h,
    }


# ----- GCD Import Management (v1.8.0) -----

class GCDImportRequest(BaseModel):
    """Request to trigger GCD import job."""
    max_records: int = Field(default=0, ge=0, description="Max records to import (0 = unlimited)")
    batch_size: int = Field(default=1000, ge=100, le=10000, description="Records per batch")
    db_path: Optional[str] = Field(default=None, description="Custom SQLite path (uses settings default if None)")


@router.post("/pipeline/gcd/import")
async def trigger_gcd_import(
    request: GCDImportRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Trigger GCD import job manually.

    This imports comics from the GCD SQLite database dump.
    Use max_records=10000 for validation, then max_records=0 for full import.

    Returns immediately with job status - import runs asynchronously.
    """
    import asyncio
    from app.jobs.pipeline_scheduler import run_gcd_import_job

    logger.info(f"GCD import triggered by admin {current_user.id}: max_records={request.max_records}")

    # Run job and store task reference to prevent garbage collection
    async def run_import():
        try:
            await run_gcd_import_job(
                db_path=request.db_path,
                batch_size=request.batch_size,
                max_records=request.max_records,
            )
        except Exception as e:
            logger.error(f"GCD import background task failed: {e}")

    # Keep task reference to prevent GC
    if not hasattr(trigger_gcd_import, '_tasks'):
        trigger_gcd_import._tasks = set()

    # Clean up completed tasks first
    trigger_gcd_import._tasks = {t for t in trigger_gcd_import._tasks if not t.done()}

    # Create and store new task
    task = asyncio.create_task(run_import())
    trigger_gcd_import._tasks.add(task)

    return {
        "status": "started",
        "message": f"GCD import started in background (max_records={request.max_records}, batch_size={request.batch_size})",
        "check_status": "/api/admin/pipeline/gcd/status"
    }


@router.get("/pipeline/gcd/status")
async def get_gcd_import_status(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get GCD import job status and statistics."""
    from app.core.config import settings

    # Get checkpoint info
    checkpoint_result = await db.execute(text("""
        SELECT
            job_name, is_running, last_run_started, last_run_completed,
            total_processed, total_updated, total_errors, last_error,
            state_data
        FROM pipeline_checkpoints
        WHERE job_name = 'gcd_import'
    """))
    checkpoint = checkpoint_result.fetchone()

    # Count records with gcd_id
    gcd_count_result = await db.execute(text("""
        SELECT COUNT(*) FROM comic_issues WHERE gcd_id IS NOT NULL
    """))
    gcd_count = gcd_count_result.scalar() or 0

    # Data quality stats for imported records (Option 5: Data Quality Summary)
    quality_result = await db.execute(text("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN image IS NOT NULL AND image != '' THEN 1 END) as with_cover,
            COUNT(CASE WHEN description IS NOT NULL AND description != '' THEN 1 END) as with_description,
            COUNT(CASE WHEN isbn IS NOT NULL AND isbn != '' THEN 1 END) as with_isbn,
            COUNT(CASE WHEN upc IS NOT NULL AND upc != '' THEN 1 END) as with_upc,
            COUNT(CASE WHEN cover_date IS NOT NULL THEN 1 END) as with_cover_date,
            COUNT(CASE WHEN pricecharting_id IS NOT NULL THEN 1 END) as with_pricing,
            COUNT(DISTINCT gcd_publisher_id) as unique_publishers,
            COUNT(DISTINCT gcd_series_id) as unique_series
        FROM comic_issues
        WHERE gcd_id IS NOT NULL
    """))
    quality_row = quality_result.fetchone()

    data_quality = None
    if quality_row and quality_row[0] > 0:
        total = quality_row[0]
        data_quality = {
            "total_imported": total,
            "with_cover_image": quality_row[1],
            "with_description": quality_row[2],
            "with_isbn": quality_row[3],
            "with_upc": quality_row[4],
            "with_cover_date": quality_row[5],
            "with_pricing": quality_row[6],
            "unique_publishers": quality_row[7],
            "unique_series": quality_row[8],
            # Percentages
            "pct_with_cover": round((quality_row[1] / total) * 100, 1) if total > 0 else 0,
            "pct_with_description": round((quality_row[2] / total) * 100, 1) if total > 0 else 0,
            "pct_with_isbn": round((quality_row[3] / total) * 100, 1) if total > 0 else 0,
            "pct_with_pricing": round((quality_row[6] / total) * 100, 1) if total > 0 else 0,
        }

    # Get GCD adapter validation info
    gcd_info = {
        "enabled": settings.GCD_IMPORT_ENABLED,
        "dump_path": settings.GCD_DUMP_PATH,
        "batch_size": settings.GCD_IMPORT_BATCH_SIZE,
        "max_records": settings.GCD_IMPORT_MAX_RECORDS,
    }

    # Validate dump exists
    import os
    dump_exists = os.path.exists(settings.GCD_DUMP_PATH)
    gcd_info["dump_exists"] = dump_exists

    # Get total count from GCD if dump exists
    if dump_exists:
        try:
            from app.adapters.gcd import GCDAdapter
            adapter = GCDAdapter()
            gcd_info["dump_total_count"] = adapter.get_total_count(settings.GCD_DUMP_PATH)
        except Exception as e:
            gcd_info["dump_total_count"] = None
            gcd_info["dump_error"] = str(e)

    return {
        "settings": gcd_info,
        "checkpoint": {
            "is_running": checkpoint[1] if checkpoint else False,
            "last_run_started": checkpoint[2].isoformat() if checkpoint and checkpoint[2] else None,
            "last_run_completed": checkpoint[3].isoformat() if checkpoint and checkpoint[3] else None,
            "total_processed": checkpoint[4] if checkpoint else 0,
            "total_updated": checkpoint[5] if checkpoint else 0,
            "total_errors": checkpoint[6] if checkpoint else 0,
            "last_error": checkpoint[7] if checkpoint else None,
            "current_offset": checkpoint[8].get("offset", 0) if checkpoint and checkpoint[8] else 0,
        } if checkpoint else None,
        "imported_count": gcd_count,
        "data_quality": data_quality,
    }


class ResetCheckpointRequest(BaseModel):
    """Request to reset a checkpoint - requires explicit confirmation."""
    confirm: bool = False


@router.post("/pipeline/gcd/reset-checkpoint")
async def reset_gcd_checkpoint(
    request: ResetCheckpointRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Reset GCD import checkpoint to start fresh.

    WARNING: This will cause the next import to RE-PROCESS ALL RECORDS from the beginning.
    This can take 10+ hours and waste resources re-updating existing records.

    v1.10.2: Requires explicit confirmation to prevent accidental resets.

    Request body must include: {"confirm": true}
    """
    # v1.10.2: Require explicit confirmation
    if not request.confirm:
        # Get current state to show what would be lost
        result = await db.execute(text("""
            SELECT state_data, total_processed FROM pipeline_checkpoints
            WHERE job_name = 'gcd_import'
        """))
        row = result.fetchone()
        current_offset = 0
        if row and row.state_data:
            current_offset = row.state_data.get("offset", 0) if isinstance(row.state_data, dict) else 0

        gcd_count = await db.execute(text(
            "SELECT COUNT(*) FROM comic_issues WHERE gcd_id IS NOT NULL"
        ))
        db_count = gcd_count.scalar() or 0

        return {
            "status": "confirmation_required",
            "message": "This will reset the checkpoint to offset 0. Send {\"confirm\": true} to proceed.",
            "warning": f"Current offset is {current_offset:,}, DB has {db_count:,} records. "
                       f"Resetting will cause re-processing of all {db_count:,} records.",
        }

    # Get current offset for logging
    result = await db.execute(text("""
        SELECT state_data FROM pipeline_checkpoints WHERE job_name = 'gcd_import'
    """))
    row = result.fetchone()
    old_offset = 0
    if row and row.state_data:
        old_offset = row.state_data.get("offset", 0) if isinstance(row.state_data, dict) else 0

    await db.execute(text("""
        UPDATE pipeline_checkpoints
        SET state_data = '{"offset": 0}'::jsonb,
            is_running = false,
            total_processed = 0,
            total_updated = 0,
            total_errors = 0,
            last_error = 'Checkpoint MANUALLY reset by admin (was offset ' ||
                :old_offset || ') at ' || NOW()::text
        WHERE job_name = 'gcd_import'
    """), {"old_offset": old_offset})
    await db.commit()

    logger.warning(
        f"GCD import checkpoint RESET by admin {current_user.id} "
        f"(was offset {old_offset:,}, now 0)"
    )

    return {
        "status": "reset",
        "message": f"GCD import checkpoint has been reset from offset {old_offset:,} to 0",
        "warning": "Next import will re-process all records from the beginning"
    }


@router.post("/pipeline/gcd/clear-stale-lock")
async def clear_gcd_stale_lock(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Clear stale is_running lock without resetting progress.

    Use this when the job is stuck with is_running=true but no actual progress.
    This clears the lock so a new import can start from where it left off.
    """
    # Clear just is_running, keep everything else intact
    await db.execute(text("""
        UPDATE pipeline_checkpoints
        SET is_running = false,
            last_error = 'Stale lock cleared by admin at ' || NOW()::text
        WHERE job_name = 'gcd_import'
    """))
    await db.commit()

    logger.info(f"GCD import stale lock cleared by admin {current_user.id}")

    return {"status": "cleared", "message": "Stale lock cleared, import can resume from current offset"}


@router.post("/pipeline/gcd/sync-offset")
async def sync_gcd_offset(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Sync checkpoint offset to actual DB count.

    Use this to skip re-processing records that are already imported.
    Sets offset = COUNT(*) FROM comic_issues WHERE gcd_id IS NOT NULL
    """
    # Get actual count of imported records
    result = await db.execute(text("""
        SELECT COUNT(*) FROM comic_issues WHERE gcd_id IS NOT NULL
    """))
    actual_count = result.scalar() or 0

    # Update checkpoint with synced offset
    # Build the JSON string in Python to avoid SQL parameter binding issues with JSONB
    state_json = f'{{"offset": {actual_count}}}'
    error_msg = f"Offset synced to DB count {actual_count} by admin"
    await db.execute(text("""
        UPDATE pipeline_checkpoints
        SET state_data = CAST(:state_json AS jsonb),
            is_running = false,
            last_error = :error_msg
        WHERE job_name = 'gcd_import'
    """), {
        "state_json": state_json,
        "error_msg": error_msg
    })
    await db.commit()

    logger.info(f"GCD import offset synced to {actual_count:,} by admin {current_user.id}")

    return {
        "status": "synced",
        "message": f"Offset synced to actual DB count: {actual_count:,}",
        "new_offset": actual_count
    }


@router.get("/pipeline/gcd/validate")
async def validate_gcd_dump(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Validate GCD SQLite dump schema before import.

    Returns table info and sample data to verify dump is readable.
    """
    from app.core.config import settings
    from app.adapters.gcd import GCDAdapter

    import os
    if not os.path.exists(settings.GCD_DUMP_PATH):
        raise HTTPException(
            status_code=404,
            detail=f"GCD dump not found at: {settings.GCD_DUMP_PATH}"
        )

    adapter = GCDAdapter()
    validation = adapter.validate_schema(settings.GCD_DUMP_PATH)

    return validation


# ----- Multi-Source Enrichment (v1.10.0) -----

class MSERequest(BaseModel):
    """Multi-Source Enrichment job request."""
    batch_size: int = Field(default=100, ge=1, le=1000, description="Records per batch")
    max_records: int = Field(default=0, ge=0, description="Max records (0=unlimited)")


@router.post("/pipeline/mse/run")
async def trigger_mse_job(
    request: MSERequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Trigger multi-source enrichment job.

    Uses source rotator to fetch descriptions from Metron/Comic Vine with failover.
    """
    import asyncio
    from app.jobs.pipeline_scheduler import run_multi_source_enrichment_job

    # Check if already running
    result = await db.execute(text("""
        SELECT is_running FROM pipeline_checkpoints
        WHERE job_name = 'multi_source_enrichment'
    """))
    row = result.fetchone()
    if row and row[0]:
        raise HTTPException(
            status_code=409,
            detail="Multi-source enrichment job is already running"
        )

    # Run in background
    async def run_job():
        try:
            await run_multi_source_enrichment_job(
                batch_size=request.batch_size,
                max_records=request.max_records,
            )
        except Exception as e:
            logger.error(f"MSE job failed: {e}")

    # Keep task reference to prevent GC
    if not hasattr(trigger_mse_job, '_tasks'):
        trigger_mse_job._tasks = set()
    trigger_mse_job._tasks = {t for t in trigger_mse_job._tasks if not t.done()}
    task = asyncio.create_task(run_job())
    trigger_mse_job._tasks.add(task)

    return {
        "status": "started",
        "message": f"MSE job started (batch_size={request.batch_size}, max_records={request.max_records})",
        "check_status": "/api/admin/pipeline/mse/status"
    }


@router.get("/pipeline/mse/status")
async def get_mse_status(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get multi-source enrichment job status."""
    # Get checkpoint
    result = await db.execute(text("""
        SELECT job_name, is_running, total_processed, total_updated, total_errors,
               last_run_started, last_run_completed, last_error, state_data
        FROM pipeline_checkpoints
        WHERE job_name = 'multi_source_enrichment'
    """))
    row = result.fetchone()

    checkpoint = None
    if row:
        checkpoint = {
            "is_running": row[1],
            "total_processed": row[2],
            "total_enriched": row[3],
            "total_errors": row[4],
            "last_run_started": row[5].isoformat() if row[5] else None,
            "last_run_completed": row[6].isoformat() if row[6] else None,
            "last_error": row[7],
            "state_data": row[8],
        }

    # Get source quotas
    result = await db.execute(text("""
        SELECT source_name, requests_today, daily_limit, is_healthy, circuit_state
        FROM source_quotas
        ORDER BY source_name
    """))
    quotas = [
        {
            "source": row[0],
            "requests_today": row[1],
            "daily_limit": row[2],
            "remaining": row[2] - row[1],
            "is_healthy": row[3],
            "circuit_state": row[4],
        }
        for row in result.fetchall()
    ]

    # Count comics needing enrichment
    result = await db.execute(text("""
        SELECT COUNT(*) FROM comic_issues
        WHERE description IS NULL OR description = ''
    """))
    needs_enrichment = result.scalar()

    return {
        "checkpoint": checkpoint,
        "source_quotas": quotas,
        "comics_needing_enrichment": needs_enrichment,
    }


# ----- PriceCharting Matching & Daily Price Sync (v1.11.0) -----

class PCMatchingRequest(BaseModel):
    """PriceCharting matching job request."""
    batch_size: int = Field(default=100, ge=10, le=500, description="Records per batch")
    max_records: int = Field(default=0, ge=0, description="Max records (0=unlimited)")


@router.post("/pipeline/pricecharting/match")
async def trigger_pricecharting_matching(
    request: PCMatchingRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Trigger PriceCharting matching job manually.

    Matches BOTH comics AND funkos to PriceCharting IDs using:
    - ISBN lookup (highest confidence for comics)
    - UPC lookup (works for both)
    - Fuzzy title matching (fallback)

    This job runs daily to:
    1. Discover new matches for unmatched records
    2. Enable price tracking via full_price_sync job
    3. Feed price_changelog for AI/ML model training

    High-confidence matches (score >= 9) auto-link.
    Lower confidence matches queue for human review.
    """
    import asyncio
    from app.jobs.pipeline_scheduler import run_pricecharting_matching_job

    # Check if already running
    result = await db.execute(text("""
        SELECT is_running FROM pipeline_checkpoints
        WHERE job_name = 'pricecharting_matching'
    """))
    row = result.fetchone()
    if row and row[0]:
        raise HTTPException(
            status_code=409,
            detail="PriceCharting matching job is already running"
        )

    logger.info(f"PriceCharting matching triggered by admin {current_user.id}")

    # Run in background
    async def run_job():
        try:
            await run_pricecharting_matching_job(
                batch_size=request.batch_size,
                max_records=request.max_records,
            )
        except Exception as e:
            logger.error(f"PriceCharting matching job failed: {e}")

    # Keep task reference to prevent GC
    if not hasattr(trigger_pricecharting_matching, '_tasks'):
        trigger_pricecharting_matching._tasks = set()
    trigger_pricecharting_matching._tasks = {t for t in trigger_pricecharting_matching._tasks if not t.done()}
    task = asyncio.create_task(run_job())
    trigger_pricecharting_matching._tasks.add(task)

    return {
        "status": "started",
        "message": f"PriceCharting matching started for Funkos + Comics (batch_size={request.batch_size})",
        "check_status": "/api/admin/pipeline/pricecharting/status"
    }


@router.get("/pipeline/pricecharting/status")
async def get_pricecharting_matching_status(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get PriceCharting matching job status for both Funkos and Comics."""
    # Get checkpoint - v1.20.0: Include control_signal for pause/stop state
    result = await db.execute(text("""
        SELECT job_name, is_running, total_processed, total_updated, total_errors,
               last_run_started, last_run_completed, last_error, state_data,
               control_signal, paused_at
        FROM pipeline_checkpoints
        WHERE job_name = 'pricecharting_matching'
    """))
    row = result.fetchone()

    checkpoint = None
    if row:
        checkpoint = {
            "is_running": row[1],
            "total_processed": row[2],
            "total_matched": row[3],
            "total_errors": row[4],
            "last_run_started": row[5].isoformat() if row[5] else None,
            "last_run_completed": row[6].isoformat() if row[6] else None,
            "last_error": row[7],
            "state_data": row[8],
            "control_signal": row[9] if len(row) > 9 else "run",
            "paused_at": row[10].isoformat() if len(row) > 10 and row[10] else None,
        }

    # Get matching stats for BOTH entity types
    comics_matched = await db.execute(text(
        "SELECT COUNT(*) FROM comic_issues WHERE pricecharting_id IS NOT NULL"
    ))
    funkos_matched = await db.execute(text(
        "SELECT COUNT(*) FROM funkos WHERE pricecharting_id IS NOT NULL"
    ))
    comics_total = await db.execute(text("SELECT COUNT(*) FROM comic_issues"))
    funkos_total = await db.execute(text("SELECT COUNT(*) FROM funkos"))

    # Get queue stats by entity type
    queue_result = await db.execute(text("""
        SELECT entity_type, COUNT(*)
        FROM match_review_queue
        WHERE status = 'pending'
        GROUP BY entity_type
    """))
    queue_by_type = {row[0]: row[1] for row in queue_result.fetchall()}

    # Price changelog stats (last 24h for AI/ML tracking)
    changelog_result = await db.execute(text("""
        SELECT entity_type, COUNT(*) as changes
        FROM price_changelog
        WHERE changed_at > NOW() - INTERVAL '24 hours'
        GROUP BY entity_type
    """))
    price_changes_24h = {row[0]: row[1] for row in changelog_result.fetchall()}

    return {
        "checkpoint": checkpoint,
        "matching_stats": {
            "comics_matched": comics_matched.scalar() or 0,
            "comics_total": comics_total.scalar() or 0,
            "funkos_matched": funkos_matched.scalar() or 0,
            "funkos_total": funkos_total.scalar() or 0,
        },
        "review_queue": {
            "comics_pending": queue_by_type.get("comic", 0),
            "funkos_pending": queue_by_type.get("funko", 0),
            "total_pending": sum(queue_by_type.values()),
        },
        "price_changes_24h": price_changes_24h,
    }


# ----- UPC Backfill Job (v1.12.0) -----

class UPCBackfillRequest(BaseModel):
    """UPC backfill job request."""
    batch_size: int = Field(default=100, ge=1, le=500)
    max_records: int = Field(default=0, ge=0)  # 0 = unlimited


@router.post("/pipeline/upc-backfill/run")
async def trigger_upc_backfill(
    request: UPCBackfillRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger UPC backfill job manually.

    Recovers missing UPCs/ISBNs from multiple sources:
    1. Metron API (if metron_id exists)
    2. ComicBookRealm (scrape by series/issue)

    Prioritizes US publishers (Marvel, DC, Image, etc.) and comics from 1980+.
    """
    import asyncio
    from app.jobs.pipeline_scheduler import run_upc_backfill_job

    # Check if already running
    running_check = await db.execute(text("""
        SELECT is_running FROM pipeline_checkpoints
        WHERE job_name = 'upc_backfill'
    """))
    row = running_check.fetchone()
    if row and row[0]:
        raise HTTPException(
            status_code=409,
            detail="UPC backfill job is already running"
        )

    logger.info(f"UPC backfill triggered by admin {current_user.id}")

    async def run_job():
        try:
            await run_upc_backfill_job(
                batch_size=request.batch_size,
                max_records=request.max_records
            )
        except Exception as e:
            logger.error(f"UPC backfill job failed: {e}")

    if not hasattr(trigger_upc_backfill, '_tasks'):
        trigger_upc_backfill._tasks = set()
    trigger_upc_backfill._tasks = {t for t in trigger_upc_backfill._tasks if not t.done()}
    task = asyncio.create_task(run_job())
    trigger_upc_backfill._tasks.add(task)

    return {
        "status": "started",
        "message": f"UPC backfill started (batch_size={request.batch_size})",
        "check_status": "/api/admin/pipeline/upc-backfill/status"
    }


@router.get("/pipeline/upc-backfill/status")
async def get_upc_backfill_status(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get UPC backfill job status and statistics."""
    # Get checkpoint
    checkpoint_result = await db.execute(text("""
        SELECT id, job_name, is_running, total_processed, total_updated, total_errors,
               state_data, last_run_started, last_run_completed, last_error, updated_at
        FROM pipeline_checkpoints
        WHERE job_name = 'upc_backfill'
    """))
    row = checkpoint_result.fetchone()

    checkpoint = None
    if row:
        checkpoint = {
            "id": row[0],
            "job_name": row[1],
            "is_running": row[2],
            "total_processed": row[3],
            "total_updated": row[4],
            "total_errors": row[5],
            "state_data": row[6],
            "last_run_started": row[7].isoformat() if row[7] else None,
            "last_run_completed": row[8].isoformat() if row[8] else None,
            "last_error": row[9],
            "updated_at": row[10].isoformat() if row[10] else None,
        }

    # Get UPC/ISBN coverage stats
    coverage_result = await db.execute(text("""
        SELECT
            COUNT(*) as total_comics,
            COUNT(CASE WHEN upc IS NOT NULL AND upc <> '' THEN 1 END) as with_upc,
            COUNT(CASE WHEN isbn IS NOT NULL AND isbn <> '' THEN 1 END) as with_isbn,
            COUNT(CASE WHEN (upc IS NULL OR upc = '') AND (isbn IS NULL OR isbn = '') THEN 1 END) as missing_both,
            COUNT(CASE WHEN upc IS NOT NULL AND upc <> '' OR isbn IS NOT NULL AND isbn <> '' THEN 1 END) as has_identifier
        FROM comic_issues
    """))
    coverage = coverage_result.fetchone()

    # Get US publisher breakdown
    us_publishers_result = await db.execute(text("""
        SELECT
            publisher_name,
            COUNT(*) as total,
            COUNT(CASE WHEN upc IS NOT NULL AND upc <> '' THEN 1 END) as with_upc
        FROM comic_issues
        WHERE publisher_name IN (
            'Marvel', 'DC', 'DC Comics', 'Image', 'Image Comics', 'Dark Horse',
            'IDW', 'IDW Publishing', 'BOOM! Studios', 'Dynamite', 'Valiant'
        )
        GROUP BY publisher_name
        ORDER BY total DESC
        LIMIT 10
    """))
    us_breakdown = [
        {"publisher": row[0], "total": row[1], "with_upc": row[2], "pct": round(row[2]/row[1]*100, 1) if row[1] else 0}
        for row in us_publishers_result.fetchall()
    ]

    return {
        "checkpoint": checkpoint,
        "coverage": {
            "total_comics": coverage[0] if coverage else 0,
            "with_upc": coverage[1] if coverage else 0,
            "with_isbn": coverage[2] if coverage else 0,
            "missing_both": coverage[3] if coverage else 0,
            "has_identifier": coverage[4] if coverage else 0,
            "upc_pct": round(coverage[1]/coverage[0]*100, 1) if coverage and coverage[0] else 0,
            "isbn_pct": round(coverage[2]/coverage[0]*100, 1) if coverage and coverage[0] else 0,
        },
        "us_publishers": us_breakdown,
    }


# ============================================================================
# SEQUENTIAL EXHAUSTIVE ENRICHMENT ENDPOINTS - v2.0.0
# ============================================================================

class SequentialEnrichmentRequest(BaseModel):
    """Request model for sequential enrichment job."""
    batch_size: int = Field(default=100, ge=10, le=500, description="Comics per batch (v2.0 uses parallel processing)")
    max_records: int = Field(default=0, ge=0, description="Maximum records to process (0 = unlimited)")


@router.post("/pipeline/sequential-enrichment/run")
async def trigger_sequential_enrichment(
    request: SequentialEnrichmentRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger Parallel Optimized Enrichment v2.0.0

    v2.0 Performance Improvements:
    - 5 comics processed CONCURRENTLY (semaphore-limited)
    - Phase 1: All sources queried in PARALLEL per comic
    - Phase 2: PriceCharting queried with UPC from Phase 1
    - Batch database writes every 50 comics
    - Publisher pre-filtering skips irrelevant Fandom sources
    - ~10-20x throughput improvement over v1.x

    Algorithm:
    1. Fetch batch of 100 comics
    2. Process 5 concurrently with parallel source queries
    3. Batch write updates every 50 comics
    4. Checkpoint every 50 comics

    Sources (in health-priority order):
    - Metron API (high-quality structured data)
    - ComicVine API (comprehensive coverage)
    - PriceCharting API (pricing data)
    - ComicBookRealm (market metrics via scraping)

    Rate limiting is intelligent per-source:
    - Healthy sources run with minimal delay
    - Rate-limited sources get exponential backoff
    - Blocked sources are skipped (others continue)
    """
    import asyncio
    from app.jobs.sequential_enrichment import run_sequential_exhaustive_enrichment_job

    # Check if already running
    running_check = await db.execute(text("""
        SELECT is_running FROM pipeline_checkpoints
        WHERE job_name = 'sequential_enrichment'
    """))
    row = running_check.fetchone()
    if row and row[0]:
        raise HTTPException(
            status_code=409,
            detail="Sequential enrichment job is already running"
        )

    logger.info(f"Sequential enrichment triggered by admin {current_user.id}")

    async def run_job():
        try:
            await run_sequential_exhaustive_enrichment_job(
                batch_size=request.batch_size,
                max_records=request.max_records
            )
        except Exception as e:
            logger.error(f"Sequential enrichment job failed: {e}")

    if not hasattr(trigger_sequential_enrichment, '_tasks'):
        trigger_sequential_enrichment._tasks = set()
    trigger_sequential_enrichment._tasks = {t for t in trigger_sequential_enrichment._tasks if not t.done()}
    task = asyncio.create_task(run_job())
    trigger_sequential_enrichment._tasks.add(task)

    return {
        "status": "started",
        "message": f"Sequential enrichment started (batch_size={request.batch_size})",
        "check_status": "/api/admin/pipeline/sequential-enrichment/status"
    }


@router.get("/pipeline/sequential-enrichment/status")
async def get_sequential_enrichment_status(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get Sequential Enrichment job status and source health metrics."""
    # Get checkpoint - v1.20.0: Include control_signal for pause/stop state
    checkpoint_result = await db.execute(text("""
        SELECT id, job_name, is_running, total_processed, total_updated, total_errors,
               state_data, last_run_started, last_run_completed, last_error, updated_at,
               control_signal, paused_at
        FROM pipeline_checkpoints
        WHERE job_name = 'sequential_enrichment'
    """))
    row = checkpoint_result.fetchone()

    checkpoint = None
    if row:
        checkpoint = {
            "id": row[0],
            "job_name": row[1],
            "is_running": row[2],
            "total_processed": row[3],
            "total_updated": row[4],
            "total_errors": row[5],
            "state_data": row[6],
            "last_run_started": row[7].isoformat() if row[7] else None,
            "last_run_completed": row[8].isoformat() if row[8] else None,
            "last_error": row[9],
            "updated_at": row[10].isoformat() if row[10] else None,
            "control_signal": row[11] if len(row) > 11 else "run",
            "paused_at": row[12].isoformat() if len(row) > 12 and row[12] else None,
        }

    # Get enrichment coverage stats
    coverage_result = await db.execute(text("""
        SELECT
            COUNT(*) as total_comics,
            COUNT(CASE WHEN description IS NOT NULL AND description <> '' THEN 1 END) as with_description,
            COUNT(CASE WHEN metron_id IS NOT NULL THEN 1 END) as with_metron,
            COUNT(CASE WHEN comicvine_id IS NOT NULL THEN 1 END) as with_comicvine,
            COUNT(CASE WHEN pricecharting_id IS NOT NULL THEN 1 END) as with_pricecharting,
            COUNT(CASE WHEN upc IS NOT NULL AND upc <> '' THEN 1 END) as with_upc,
            COUNT(CASE WHEN isbn IS NOT NULL AND isbn <> '' THEN 1 END) as with_isbn,
            COUNT(CASE WHEN est_print_run IS NOT NULL THEN 1 END) as with_market_metrics,
            COUNT(CASE WHEN image IS NOT NULL AND image <> '' THEN 1 END) as with_image
        FROM comic_issues
    """))
    coverage = coverage_result.fetchone()

    total = coverage[0] if coverage else 0

    return {
        "checkpoint": checkpoint,
        "coverage": {
            "total_comics": total,
            "with_description": coverage[1] if coverage else 0,
            "with_metron": coverage[2] if coverage else 0,
            "with_comicvine": coverage[3] if coverage else 0,
            "with_pricecharting": coverage[4] if coverage else 0,
            "with_upc": coverage[5] if coverage else 0,
            "with_isbn": coverage[6] if coverage else 0,
            "with_market_metrics": coverage[7] if coverage else 0,
            "with_image": coverage[8] if coverage else 0,
            "description_pct": round(coverage[1]/total*100, 1) if total else 0,
            "metron_pct": round(coverage[2]/total*100, 1) if total else 0,
            "comicvine_pct": round(coverage[3]/total*100, 1) if total else 0,
            "pricecharting_pct": round(coverage[4]/total*100, 1) if total else 0,
            "upc_pct": round(coverage[5]/total*100, 1) if total else 0,
            "isbn_pct": round(coverage[6]/total*100, 1) if total else 0,
            "market_metrics_pct": round(coverage[7]/total*100, 1) if total else 0,
            "image_pct": round(coverage[8]/total*100, 1) if total else 0,
        },
        # MSE-002: Full list of implemented sources (11 total)
        "sources": [
            "metron", "comicvine",
            "marvel_fandom", "dc_fandom", "image_fandom",
            "idw_fandom", "darkhorse_fandom", "dynamite_fandom",
            "mycomicshop", "pricecharting", "comicbookrealm"
        ],
        "algorithm": "sequential_exhaustive"
    }


# ============================================================================
# COVER INGESTION ENDPOINTS (v1.21.0)
# ============================================================================

class CoverIngestionPreviewRequest(BaseModel):
    """Request to preview cover ingestion from a folder."""
    folder_path: str = Field(..., description="Path to folder containing cover images")
    limit: int = Field(default=100, ge=1, le=500, description="Max files to preview")


class CoverIngestionRequest(BaseModel):
    """Request to ingest covers from a folder.

    All items are queued to Match Review for human approval.
    Products are only created after approval.
    """
    folder_path: str = Field(..., description="Path to folder containing cover images")
    limit: Optional[int] = Field(default=None, ge=1, description="Max files to process (for testing)")


class SingleCoverIngestionRequest(BaseModel):
    """Request to ingest a single cover image.

    Queued to Match Review for human approval.
    """
    file_path: str = Field(..., description="Path to single cover image")
    base_path: str = Field(..., description="Base folder path for metadata extraction")


@router.post("/cover-ingestion/preview")
async def preview_cover_ingestion(
    request: CoverIngestionPreviewRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Preview what would be ingested from a folder.

    Returns parsed metadata and match results without creating any records.
    Use this to validate folder structure before actual ingestion.
    """
    import os
    from app.services.cover_ingestion import get_cover_ingestion_service

    # Validate folder exists
    if not os.path.exists(request.folder_path):
        raise HTTPException(status_code=404, detail=f"Folder not found: {request.folder_path}")

    if not os.path.isdir(request.folder_path):
        raise HTTPException(status_code=400, detail=f"Path is not a directory: {request.folder_path}")

    service = get_cover_ingestion_service(db)
    previews = await service.scan_folder_preview(request.folder_path, request.limit)

    # Aggregate stats
    stats = {
        "total_files": len(previews),
        "will_create": len([p for p in previews if p["disposition"] in ("auto_link", "review")]),
        "high_confidence": len([p for p in previews if p["match_score"] >= 8]),
        "low_confidence": len([p for p in previews if 0 < p["match_score"] < 5]),
        "no_match": len([p for p in previews if p["match_score"] == 0]),
        "publishers": list(set(p["publisher"] for p in previews if p["publisher"])),
    }

    return {
        "stats": stats,
        "previews": previews
    }


@router.post("/cover-ingestion/ingest")
async def ingest_covers(
    request: CoverIngestionRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Ingest cover images from a folder.

    All items are queued to Match Review for human approval.
    Products are only created after approval in the Match Review screen.

    This supports:
    - Current inventory (create product on approval)
    - Historical/sold items (link to comic_issue for model training)
    """
    import os
    from app.services.cover_ingestion import get_cover_ingestion_service

    # Validate folder exists
    if not os.path.exists(request.folder_path):
        raise HTTPException(status_code=404, detail=f"Folder not found: {request.folder_path}")

    if not os.path.isdir(request.folder_path):
        raise HTTPException(status_code=400, detail=f"Path is not a directory: {request.folder_path}")

    service = get_cover_ingestion_service(db)
    result = await service.ingest_folder(
        folder_path=request.folder_path,
        user_id=current_user.id,
        limit=request.limit
    )

    return {
        "success": True,
        "total_files": result.total_files,
        "processed": result.processed,
        "queued_for_review": result.queued_for_review,
        "high_confidence": result.high_confidence,
        "medium_confidence": result.medium_confidence,
        "low_confidence": result.low_confidence,
        "skipped": result.skipped,
        "errors": result.errors,
        "error_details": result.error_details[:10] if result.error_details else []
    }


@router.post("/cover-ingestion/single")
async def ingest_single_cover(
    request: SingleCoverIngestionRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Ingest a single cover image.

    Queued to Match Review for human approval.
    Useful for testing or mobile scanning workflow.
    """
    import os
    from app.services.cover_ingestion import get_cover_ingestion_service

    # Validate file exists
    if not os.path.exists(request.file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {request.file_path}")

    if not os.path.isfile(request.file_path):
        raise HTTPException(status_code=400, detail=f"Path is not a file: {request.file_path}")

    service = get_cover_ingestion_service(db)
    result = await service.ingest_single_cover(
        file_path=request.file_path,
        base_path=request.base_path,
        user_id=current_user.id
    )

    return {
        "success": result.success,
        "file_path": result.file_path,
        "queue_id": result.queue_id,
        "comic_issue_id": result.comic_issue_id,
        "match_score": result.match_score,
        "disposition": result.disposition,
        "skipped": result.skipped,
        "skip_reason": result.skip_reason,
        "error": result.error,
        "metadata": {
            "publisher": result.metadata.publisher if result.metadata else None,
            "series": result.metadata.series if result.metadata else None,
            "volume": result.metadata.volume if result.metadata else None,
            "issue_number": result.metadata.issue_number if result.metadata else None,
            "variant_code": result.metadata.variant_code if result.metadata else None,
            "cgc_grade": result.metadata.cgc_grade if result.metadata else None,
        } if result.metadata else None
    }


@router.get("/cover-ingestion/stats")
async def get_cover_ingestion_stats(
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Get statistics about cover ingestion.

    Shows queue status, processed items, and confidence breakdowns.
    """
    # Queue items for cover ingestion (all statuses)
    # Include ALL cover-related entity_types and candidate_sources:
    # - entity_type: 'cover_ingestion' (folder scan), 'cover_upload' (CLI upload)
    # - candidate_source: 'local_cover' (CLI), 'local_folder' (service), 'local_upload' (API)
    queue_result = await db.execute(text("""
        SELECT
            COUNT(*) as total_queued,
            COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
            COUNT(CASE WHEN status = 'approved' THEN 1 END) as approved,
            COUNT(CASE WHEN status = 'rejected' THEN 1 END) as rejected,
            COUNT(CASE WHEN status = 'pending' AND match_score >= 8 THEN 1 END) as pending_high,
            COUNT(CASE WHEN status = 'pending' AND match_score >= 5 AND match_score < 8 THEN 1 END) as pending_medium,
            COUNT(CASE WHEN status = 'pending' AND match_score < 5 THEN 1 END) as pending_low
        FROM match_review_queue
        WHERE entity_type IN ('cover_ingestion', 'cover_upload')
           OR candidate_source IN ('local_cover', 'local_folder', 'local_upload')
    """))
    queue_row = queue_result.fetchone()

    # Products created via cover ingestion approval
    products_result = await db.execute(text("""
        SELECT
            COUNT(*) as total_products,
            COUNT(CASE WHEN is_graded = true THEN 1 END) as graded_products,
            COUNT(CASE WHEN pricecharting_id IS NOT NULL THEN 1 END) as linked_to_pricecharting,
            COALESCE(SUM(price * stock), 0) as total_value
        FROM products
        WHERE update_reason LIKE '%Cover ingestion%'
        AND deleted_at IS NULL
    """))
    products_row = products_result.fetchone()

    return {
        "queue": {
            "total_queued": queue_row[0] if queue_row else 0,
            "pending": queue_row[1] if queue_row else 0,
            "approved": queue_row[2] if queue_row else 0,
            "rejected": queue_row[3] if queue_row else 0,
            "pending_high_confidence": queue_row[4] if queue_row else 0,
            "pending_medium_confidence": queue_row[5] if queue_row else 0,
            "pending_low_confidence": queue_row[6] if queue_row else 0,
        },
        "products_created": {
            "total": products_row[0] if products_row else 0,
            "graded": products_row[1] if products_row else 0,
            "linked_to_pricecharting": products_row[2] if products_row else 0,
            "total_value": float(products_row[3]) if products_row else 0.0,
        }
    }


# ============================================================================
# Browser Cover Upload (v1.22.0)
# ============================================================================

@router.post("/cover-ingestion/upload")
async def upload_cover_from_browser(
    file: UploadFile = File(...),
    publisher: str = Form(default=""),
    series: str = Form(default=""),
    volume: Optional[int] = Form(default=None),
    issue_number: str = Form(default=""),
    variant_code: Optional[str] = Form(default=None),
    cgc_grade: Optional[float] = Form(default=None),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Upload a cover image directly from the browser.

    This endpoint:
    1. Validates the uploaded image
    2. Uploads to S3
    3. Tries to match to comic_issues database
    4. Queues to Match Review for human approval

    Form fields are optional - if metadata not provided, user must fill in
    Match Review screen.
    """
    from datetime import timedelta
    from app.services.storage import StorageService
    from app.models.match_review import MatchReviewQueue
    from app.services.match_review_service import route_match

    # Validate file type
    ALLOWED_TYPES = {"image/jpeg", "image/png", "image/webp"}
    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type: {file.content_type}. Allowed: {list(ALLOWED_TYPES)}"
        )

    # Read file content
    content = await file.read()

    # Validate file size (max 10MB)
    MAX_SIZE = 10 * 1024 * 1024
    if len(content) > MAX_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"File too large. Max size: {MAX_SIZE // (1024*1024)}MB"
        )

    # Generate file hash for deduplication
    file_hash = hashlib.md5(content).hexdigest()

    # Check if already in queue
    existing = await db.execute(
        select(MatchReviewQueue).where(
            MatchReviewQueue.candidate_id == file_hash
        )
    )
    existing_item = existing.scalar_one_or_none()
    if existing_item:
        return {
            "success": True,
            "message": "Image already in review queue",
            "queue_id": existing_item.id,
            "already_exists": True
        }

    # Upload to S3
    storage = StorageService()
    if not storage.is_configured():
        raise HTTPException(
            status_code=500,
            detail="S3 storage not configured"
        )

    upload_result = await storage.upload_product_image(
        content=content,
        filename=file.filename or "cover.jpg",
        content_type=file.content_type,
        product_type="covers",
    )

    if not upload_result.success:
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {upload_result.error}"
        )

    # Try to match to comic_issues if metadata provided
    matched_issue_id = None
    match_score = 0
    match_method = "browser_upload"

    if series and issue_number:
        # Basic matching using series name and issue number
        from app.models.comic_data import ComicIssue, ComicSeries

        # Search for matching issues
        series_pattern = f"%{series}%"
        result = await db.execute(
            select(ComicIssue)
            .join(ComicSeries, ComicIssue.series_id == ComicSeries.id)
            .where(
                ComicSeries.name.ilike(series_pattern),
                ComicIssue.number == issue_number
            )
            .limit(5)
        )
        issues = result.scalars().all()

        if len(issues) == 1:
            matched_issue_id = issues[0].id
            match_score = 7
            match_method = "series_issue_browser"
        elif issues:
            # Try to filter by publisher
            for issue in issues:
                if issue.publisher_name and publisher.lower() in issue.publisher_name.lower():
                    matched_issue_id = issue.id
                    match_score = 6
                    match_method = "series_issue_publisher_browser"
                    break
            if not matched_issue_id:
                matched_issue_id = issues[0].id
                match_score = 5
                match_method = "series_issue_ambiguous_browser"

    # Build product name
    product_name = series or "Unknown Comic"
    if volume:
        product_name += f" Vol. {volume}"
    if issue_number:
        product_name += f" #{issue_number}"
    if variant_code:
        product_name += f" ({variant_code} Variant)"
    if cgc_grade:
        product_name += f" CGC {cgc_grade}"

    # Determine disposition
    disposition = route_match(match_method, match_score, 1 if matched_issue_id else 0)

    # Queue for Match Review
    queue_item = MatchReviewQueue(
        entity_type="cover_upload",
        entity_id=0,  # No product yet - created on approval
        candidate_source="browser_upload",
        candidate_id=file_hash,
        candidate_name=product_name,
        candidate_data={
            "s3_url": upload_result.url,
            "s3_key": upload_result.key,
            "filename": file.filename,
            "content_type": file.content_type,
            "file_size": len(content),
            "publisher": publisher,
            "series": series,
            "volume": volume,
            "issue_number": issue_number,
            "variant_code": variant_code,
            "cgc_grade": cgc_grade,
            "matched_issue_id": matched_issue_id,
            "product_template": {
                "name": product_name,
                "category": "comics",
                "subcategory": publisher or "Unknown",
                "publisher": publisher,
                "issue_number": issue_number,
                "cgc_grade": cgc_grade,
                "is_graded": cgc_grade is not None,
                "stock": 1,
            }
        },
        match_method=match_method,
        match_score=match_score,
        match_details={
            "matched_issue_id": matched_issue_id,
            "disposition": disposition.value,
            "uploaded_by": current_user.id,
        },
        status="pending",
        expires_at=datetime.now(timezone.utc) + timedelta(days=30)
    )

    db.add(queue_item)
    await db.commit()
    await db.refresh(queue_item)

    logger.info(
        f"Browser upload queued: {file.filename} -> Queue #{queue_item.id} "
        f"(match_score={match_score}, s3_url={upload_result.url})"
    )

    return {
        "success": True,
        "message": "Cover uploaded and queued for review",
        "queue_id": queue_item.id,
        "s3_url": upload_result.url,
        "match_score": match_score,
        "match_method": match_method,
        "matched_issue_id": matched_issue_id,
        "disposition": disposition.value,
        "already_exists": False
    }


@router.post("/cover-ingestion/update/{queue_id}")
async def update_cover_for_queue_item(
    queue_id: int,
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Replace the cover image for an existing Match Review queue item.

    Use this to fix incorrect images before approval.
    """
    from app.services.storage import StorageService
    from app.models.match_review import MatchReviewQueue

    # Get existing queue item
    queue_item = await db.get(MatchReviewQueue, queue_id)
    if not queue_item:
        raise HTTPException(status_code=404, detail="Queue item not found")

    if queue_item.status != "pending":
        raise HTTPException(
            status_code=400,
            detail=f"Cannot update cover for item with status: {queue_item.status}"
        )

    # Validate file type
    ALLOWED_TYPES = {"image/jpeg", "image/png", "image/webp"}
    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type: {file.content_type}"
        )

    # Read and validate file
    content = await file.read()
    MAX_SIZE = 10 * 1024 * 1024
    if len(content) > MAX_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"File too large. Max size: {MAX_SIZE // (1024*1024)}MB"
        )

    # Upload new cover to S3
    storage = StorageService()
    if not storage.is_configured():
        raise HTTPException(
            status_code=500,
            detail="S3 storage not configured"
        )

    upload_result = await storage.upload_product_image(
        content=content,
        filename=file.filename or "cover.jpg",
        content_type=file.content_type,
        product_type="covers",
    )

    if not upload_result.success:
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {upload_result.error}"
        )

    # Delete old S3 file if exists
    old_key = queue_item.candidate_data.get("s3_key") if queue_item.candidate_data else None
    if old_key:
        try:
            await storage.delete_object(old_key)
        except Exception as e:
            logger.warning(f"Failed to delete old cover: {e}")

    # Update queue item with new cover
    new_hash = hashlib.md5(content).hexdigest()
    candidate_data = queue_item.candidate_data or {}
    candidate_data.update({
        "s3_url": upload_result.url,
        "s3_key": upload_result.key,
        "filename": file.filename,
        "content_type": file.content_type,
        "file_size": len(content),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_by": current_user.id,
    })

    queue_item.candidate_data = candidate_data
    queue_item.candidate_id = new_hash  # Update dedup hash

    await db.commit()

    logger.info(f"Cover updated for queue item #{queue_id}: {upload_result.url}")

    return {
        "success": True,
        "message": "Cover image updated",
        "queue_id": queue_id,
        "s3_url": upload_result.url,
    }


# =============================================================================
# DATA INGESTION API v1.23.0
# =============================================================================


class DataIngestionRequest(BaseModel):
    """Request to trigger a data ingestion job."""
    source: str = Field(..., description="Data source identifier (e.g., pricecharting, gcd)")
    file_path: str = Field(..., description="Path to data file")
    table_name: str = Field(..., description="Target database table")
    format: str = Field(default="csv", pattern="^(csv|json)$", description="File format")
    options: Optional[dict] = Field(default=None, description="Ingestion options")


@router.post("/ingest-data")
async def trigger_data_ingestion(
    request: DataIngestionRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Trigger a data ingestion job.

    This endpoint queues a background job to ingest data from the specified
    file into the database using bulk operations.

    The job runs asynchronously - check pipeline checkpoints for status.

    Per constitution_db.json:
    - Data ingestion via services, not standalone scripts
    - No schema modifications (data only)
    """
    import asyncio
    import os

    # Validate file exists
    if not os.path.exists(request.file_path):
        raise HTTPException(
            status_code=400,
            detail=f"File not found: {request.file_path}"
        )

    # Parse options
    options = request.options or {}
    batch_size = options.get("batch_size", 1000)
    skip_existing = options.get("skip_existing", True)
    update_existing = options.get("update_existing", False)
    dry_run = options.get("dry_run", False)

    logger.info(
        f"Data ingestion triggered by admin {current_user.id}: "
        f"source={request.source}, file={request.file_path}, table={request.table_name}"
    )

    if dry_run:
        # For dry run, execute synchronously and return preview
        from app.services.data_ingestion import DataIngestionService, IngestionOptions

        service = DataIngestionService(db)
        ing_options = IngestionOptions(
            batch_size=batch_size,
            skip_existing=skip_existing,
            update_existing=update_existing,
            dry_run=True,
        )

        if request.format == "csv":
            stats = await service.ingest_csv(
                source=request.source,
                file_path=request.file_path,
                table_name=request.table_name,
                options=ing_options,
            )
        else:
            stats = await service.ingest_json(
                source=request.source,
                file_path=request.file_path,
                table_name=request.table_name,
                options=ing_options,
            )

        return {
            "status": "dry_run_complete",
            "message": "Dry run completed - no data was inserted",
            "stats": stats.to_dict(),
        }

    # For real ingestion, run in background
    if request.format == "csv":
        from app.jobs.data_ingestion import run_csv_ingestion_job

        async def run_job():
            try:
                await run_csv_ingestion_job(
                    ctx={},
                    source=request.source,
                    file_path=request.file_path,
                    table_name=request.table_name,
                    batch_size=batch_size,
                    skip_existing=skip_existing,
                    update_existing=update_existing,
                )
            except Exception as e:
                logger.error(f"CSV ingestion job failed: {e}")
    else:
        from app.jobs.data_ingestion import run_json_ingestion_job

        async def run_job():
            try:
                await run_json_ingestion_job(
                    ctx={},
                    source=request.source,
                    file_path=request.file_path,
                    table_name=request.table_name,
                    batch_size=batch_size,
                    skip_existing=skip_existing,
                    update_existing=update_existing,
                )
            except Exception as e:
                logger.error(f"JSON ingestion job failed: {e}")

    # Track background tasks
    if not hasattr(trigger_data_ingestion, '_tasks'):
        trigger_data_ingestion._tasks = set()

    # Clean up completed tasks
    trigger_data_ingestion._tasks = {t for t in trigger_data_ingestion._tasks if not t.done()}

    # Start background job
    task = asyncio.create_task(run_job())
    trigger_data_ingestion._tasks.add(task)

    return {
        "status": "started",
        "message": f"Data ingestion job started for {request.source}",
        "source": request.source,
        "file": request.file_path,
        "table": request.table_name,
        "check_status": f"/api/admin/pipeline/checkpoints",
    }


@router.get("/ingest-data/status/{source}")
async def get_ingestion_status(
    source: str,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get status of a data ingestion job."""
    job_name = f"csv_ingest_{source}"

    result = await db.execute(text("""
        SELECT
            job_name, is_running, total_processed, total_updated, total_errors,
            last_error, last_run_started, last_run_completed, updated_at
        FROM pipeline_checkpoints
        WHERE job_name = :job_name OR job_name = :json_job_name
    """), {"job_name": job_name, "json_job_name": f"json_ingest_{source}"})

    row = result.fetchone()

    if not row:
        return {
            "status": "not_found",
            "message": f"No ingestion job found for source: {source}",
        }

    return {
        "job_name": row.job_name,
        "is_running": row.is_running,
        "total_processed": row.total_processed or 0,
        "total_updated": row.total_updated or 0,
        "total_errors": row.total_errors or 0,
        "last_error": row.last_error,
        "last_run_started": row.last_run_started.isoformat() if row.last_run_started else None,
        "last_run_completed": row.last_run_completed.isoformat() if row.last_run_completed else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
