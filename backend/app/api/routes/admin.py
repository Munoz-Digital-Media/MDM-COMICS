"""
Admin API Routes for Inventory Management

Per constitution_cyberSec.json Section 3:
- All admin endpoints require is_admin=True
- CSRF protection on mutations
- Input validation
"""
import logging
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query
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
    days: int = Query(7, ge=1, le=90),
    threshold_pct: float = Query(10.0, ge=0),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get significant price changes from price_changelog."""
    try:
        # Use make_interval for proper PostgreSQL parameterized interval
        result = await db.execute(text("""
            SELECT
                entity_type, entity_id, entity_name, field_name,
                old_value, new_value, change_pct, changed_at
            FROM price_changelog
            WHERE changed_at > NOW() - make_interval(days => :days)
              AND ABS(COALESCE(change_pct, 0)) >= :threshold
            ORDER BY ABS(COALESCE(change_pct, 0)) DESC
            LIMIT 100
        """), {"days": days, "threshold": threshold_pct})

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
                for r in result.fetchall()
            ]
        }
    except Exception as e:
        logger.warning(f"Price changes query failed: {e}")
        # Return empty list if table doesn't exist yet
        return {"changes": []}


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
