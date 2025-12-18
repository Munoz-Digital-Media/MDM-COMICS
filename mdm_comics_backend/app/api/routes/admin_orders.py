"""
Admin Orders Routes

Fulfillment module endpoints for managing all orders.
Requires admin authentication.
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_, text
from sqlalchemy.orm import selectinload
from datetime import datetime

from app.core.database import get_db
from app.models.order import Order, OrderItem
from app.models.user import User
from app.api.deps import get_current_admin

router = APIRouter(prefix="/admin/orders", tags=["admin-orders"])


@router.get("/")
async def list_all_orders(
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by status"),
    search: Optional[str] = Query(None, description="Search by order number"),
    date_from: Optional[str] = Query(None, description="Filter from date"),
    date_to: Optional[str] = Query(None, description="Filter to date"),
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Get all orders with filters (admin only)."""
    # Use raw SQL to avoid ORM mapping issues with missing columns
    base_sql = """
        SELECT o.id, o.user_id, o.order_number, o.status,
               o.subtotal, o.shipping_cost, o.tax, o.total,
               o.shipping_address, o.shipping_method, o.tracking_number,
               o.created_at, o.updated_at, o.notes
        FROM orders o
        WHERE 1=1
    """
    count_sql = "SELECT COUNT(*) FROM orders o WHERE 1=1"
    params = {}

    if status_filter:
        base_sql += " AND o.status = :status"
        count_sql += " AND o.status = :status"
        params["status"] = status_filter

    if search:
        base_sql += " AND o.order_number ILIKE :search"
        count_sql += " AND o.order_number ILIKE :search"
        params["search"] = f"%{search}%"

    if date_from:
        base_sql += " AND o.created_at >= :date_from"
        count_sql += " AND o.created_at >= :date_from"
        params["date_from"] = date_from

    if date_to:
        base_sql += " AND o.created_at <= :date_to"
        count_sql += " AND o.created_at <= :date_to"
        params["date_to"] = date_to

    # Get count
    count_result = await db.execute(text(count_sql), params)
    total = count_result.scalar() or 0

    # Get orders
    base_sql += " ORDER BY o.created_at DESC LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset

    result = await db.execute(text(base_sql), params)
    rows = result.fetchall()

    # Get order items for each order
    orders = []
    for row in rows:
        order_id = row[0]
        items_result = await db.execute(
            text("""
                SELECT id, product_id, product_name, product_sku, price, quantity
                FROM order_items WHERE order_id = :order_id
            """),
            {"order_id": order_id}
        )
        items = [
            {
                "id": item[0],
                "product_id": item[1],
                "product_name": item[2],
                "product_sku": item[3],
                "price": float(item[4]) if item[4] else 0,
                "quantity": item[5],
            }
            for item in items_result.fetchall()
        ]

        orders.append({
            "id": row[0],
            "user_id": row[1],
            "order_number": row[2],
            "status": row[3],
            "subtotal": float(row[4]) if row[4] else 0,
            "shipping_cost": float(row[5]) if row[5] else 0,
            "tax": float(row[6]) if row[6] else 0,
            "total": float(row[7]) if row[7] else 0,
            "shipping_address": row[8],
            "shipping_method": row[9],
            "tracking_number": row[10],
            "created_at": row[11].isoformat() if row[11] else None,
            "updated_at": row[12].isoformat() if row[12] else None,
            "items": items,
        })

    return {"orders": orders, "total": total, "limit": limit, "offset": offset}


@router.get("/{order_id}")
async def get_order(
    order_id: int,
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get single order details (admin only)."""
    result = await db.execute(
        text("""
            SELECT id, user_id, order_number, status,
                   subtotal, shipping_cost, tax, total,
                   shipping_address, shipping_method, tracking_number,
                   created_at, updated_at, notes
            FROM orders WHERE id = :order_id
        """),
        {"order_id": order_id}
    )
    row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Order not found")

    # Get items
    items_result = await db.execute(
        text("""
            SELECT id, product_id, product_name, product_sku, price, quantity
            FROM order_items WHERE order_id = :order_id
        """),
        {"order_id": order_id}
    )
    items = [
        {
            "id": item[0],
            "product_id": item[1],
            "product_name": item[2],
            "product_sku": item[3],
            "price": float(item[4]) if item[4] else 0,
            "quantity": item[5],
        }
        for item in items_result.fetchall()
    ]

    return {
        "id": row[0],
        "user_id": row[1],
        "order_number": row[2],
        "status": row[3],
        "subtotal": float(row[4]) if row[4] else 0,
        "shipping_cost": float(row[5]) if row[5] else 0,
        "tax": float(row[6]) if row[6] else 0,
        "total": float(row[7]) if row[7] else 0,
        "shipping_address": row[8],
        "shipping_method": row[9],
        "tracking_number": row[10],
        "created_at": row[11].isoformat() if row[11] else None,
        "updated_at": row[12].isoformat() if row[12] else None,
        "notes": row[13],
        "items": items,
    }


@router.patch("/{order_id}/status")
async def update_order_status(
    order_id: int,
    status_update: dict,
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update order status (admin only)."""
    # Check order exists
    result = await db.execute(
        text("SELECT id FROM orders WHERE id = :order_id"),
        {"order_id": order_id}
    )
    if not result.fetchone():
        raise HTTPException(status_code=404, detail="Order not found")

    # Build update
    updates = []
    params = {"order_id": order_id}

    new_status = status_update.get("status")
    if new_status:
        updates.append("status = :status")
        params["status"] = new_status

    tracking = status_update.get("tracking_number")
    if tracking:
        updates.append("tracking_number = :tracking")
        params["tracking"] = tracking

    if updates:
        updates.append("updated_at = NOW()")
        sql = f"UPDATE orders SET {', '.join(updates)} WHERE id = :order_id"
        await db.execute(text(sql), params)
        await db.commit()

    # Return updated order
    result = await db.execute(
        text("""
            SELECT id, order_number, status, tracking_number, updated_at
            FROM orders WHERE id = :order_id
        """),
        {"order_id": order_id}
    )
    row = result.fetchone()

    return {
        "id": row[0],
        "order_number": row[1],
        "status": row[2],
        "tracking_number": row[3],
        "updated_at": row[4].isoformat() if row[4] else None,
    }
