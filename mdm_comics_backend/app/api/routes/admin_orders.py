"""
Admin Orders Routes

Fulfillment module endpoints for managing all orders.
Requires admin authentication.
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
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
    status: Optional[str] = Query(None, description="Filter by status"),
    search: Optional[str] = Query(None, description="Search by order number or email"),
    date_from: Optional[str] = Query(None, description="Filter from date (ISO format)"),
    date_to: Optional[str] = Query(None, description="Filter to date (ISO format)"),
    limit: int = Query(25, ge=1, le=100, description="Items per page"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Get all orders with filters (admin only).
    """
    query = select(Order).options(selectinload(Order.items))
    count_query = select(func.count(Order.id))

    # Apply filters
    if status:
        query = query.where(Order.status == status)
        count_query = count_query.where(Order.status == status)

    if search:
        search_filter = or_(
            Order.order_number.ilike(f"%{search}%"),
        )
        query = query.where(search_filter)
        count_query = count_query.where(search_filter)

    if date_from:
        try:
            from_date = datetime.fromisoformat(date_from.replace('Z', '+00:00'))
            query = query.where(Order.created_at >= from_date)
            count_query = count_query.where(Order.created_at >= from_date)
        except ValueError:
            pass

    if date_to:
        try:
            to_date = datetime.fromisoformat(date_to.replace('Z', '+00:00'))
            query = query.where(Order.created_at <= to_date)
            count_query = count_query.where(Order.created_at <= to_date)
        except ValueError:
            pass

    # Get total count
    count_result = await db.execute(count_query)
    total = count_result.scalar() or 0

    # Get paginated orders
    query = query.order_by(Order.created_at.desc()).offset(offset).limit(limit)
    result = await db.execute(query)
    orders = result.scalars().all()

    return {
        "orders": [
            {
                "id": o.id,
                "order_number": o.order_number,
                "user_id": o.user_id,
                "status": o.status,
                "subtotal": float(o.subtotal) if o.subtotal else 0,
                "shipping_cost": float(o.shipping_cost) if o.shipping_cost else 0,
                "tax": float(o.tax) if o.tax else 0,
                "total": float(o.total) if o.total else 0,
                "shipping_address": o.shipping_address,
                "shipping_method": o.shipping_method,
                "tracking_number": o.tracking_number,
                "created_at": o.created_at.isoformat() if o.created_at else None,
                "updated_at": o.updated_at.isoformat() if o.updated_at else None,
                "items": [
                    {
                        "id": item.id,
                        "product_id": item.product_id,
                        "product_name": item.product_name,
                        "product_sku": item.product_sku,
                        "price": float(item.price) if item.price else 0,
                        "quantity": item.quantity,
                    }
                    for item in o.items
                ],
            }
            for o in orders
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/{order_id}")
async def get_order(
    order_id: int,
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Get single order details (admin only).
    """
    result = await db.execute(
        select(Order)
        .where(Order.id == order_id)
        .options(selectinload(Order.items))
    )
    order = result.scalar_one_or_none()

    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found"
        )

    return {
        "id": order.id,
        "order_number": order.order_number,
        "user_id": order.user_id,
        "status": order.status,
        "subtotal": float(order.subtotal) if order.subtotal else 0,
        "shipping_cost": float(order.shipping_cost) if order.shipping_cost else 0,
        "tax": float(order.tax) if order.tax else 0,
        "total": float(order.total) if order.total else 0,
        "shipping_address": order.shipping_address,
        "shipping_method": order.shipping_method,
        "tracking_number": order.tracking_number,
        "notes": order.notes,
        "created_at": order.created_at.isoformat() if order.created_at else None,
        "updated_at": order.updated_at.isoformat() if order.updated_at else None,
        "items": [
            {
                "id": item.id,
                "product_id": item.product_id,
                "product_name": item.product_name,
                "product_sku": item.product_sku,
                "price": float(item.price) if item.price else 0,
                "quantity": item.quantity,
            }
            for item in order.items
        ],
    }


@router.patch("/{order_id}/status")
async def update_order_status(
    order_id: int,
    status_update: dict,
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Update order status (admin only).
    """
    result = await db.execute(
        select(Order).where(Order.id == order_id)
    )
    order = result.scalar_one_or_none()

    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found"
        )

    new_status = status_update.get("status")
    if new_status:
        order.status = new_status

    tracking_number = status_update.get("tracking_number")
    if tracking_number:
        order.tracking_number = tracking_number

    await db.commit()
    await db.refresh(order)

    return {
        "id": order.id,
        "order_number": order.order_number,
        "status": order.status,
        "tracking_number": order.tracking_number,
        "updated_at": order.updated_at.isoformat() if order.updated_at else None,
    }
