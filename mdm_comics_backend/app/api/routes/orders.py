"""
Order routes

P2-10: Added pagination support
"""
import uuid
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from app.core.database import get_db
from app.models.order import Order, OrderItem
from app.models.cart import CartItem
from app.models.user import User
from app.schemas.order import OrderCreate, OrderResponse, OrderList
from app.api.deps import get_current_user

router = APIRouter()

# P2-10: Pagination limits
MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 20


def generate_order_number() -> str:
    """Generate unique order number"""
    return f"MDM-{uuid.uuid4().hex[:8].upper()}"


@router.get("", response_model=OrderList)
async def list_orders(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description="Items per page"),
):
    """
    Get current user's orders with pagination.

    P2-10: Added pagination to prevent unbounded queries.
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        # Get total count
        count_result = await db.execute(
            select(func.count(Order.id)).where(Order.user_id == user.id)
        )
        total = count_result.scalar() or 0
        logger.info(f"[orders] User {user.id} has {total} orders")

        # Get paginated orders
        offset = (page - 1) * per_page
        result = await db.execute(
            select(Order)
            .where(Order.user_id == user.id)
            .options(selectinload(Order.items))
            .order_by(Order.created_at.desc())
            .offset(offset)
            .limit(per_page)
        )
        orders = result.scalars().all()
        logger.info(f"[orders] Retrieved {len(orders)} orders for page {page}")

        return OrderList(orders=orders, total=total)
    except Exception as e:
        logger.error(f"[orders] Error listing orders for user {user.id}: {e}", exc_info=True)
        raise


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get single order"""
    result = await db.execute(
        select(Order)
        .where(Order.id == order_id, Order.user_id == user.id)
        .options(selectinload(Order.items))
    )
    order = result.scalar_one_or_none()
    
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Order not found"
        )
    
    return order


@router.post("", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    order_data: OrderCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Create order from cart"""
    # Get cart items
    result = await db.execute(
        select(CartItem)
        .where(CartItem.user_id == user.id)
        .options(selectinload(CartItem.product))
    )
    cart_items = result.scalars().all()
    
    if not cart_items:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cart is empty"
        )
    
    # Validate stock
    for item in cart_items:
        if item.quantity > item.product.stock:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Insufficient stock for {item.product.name}"
            )
    
    # Calculate totals
    subtotal = sum(item.product.price * item.quantity for item in cart_items)
    shipping_cost = 0.0 if subtotal >= 50 else 5.99
    tax = round(subtotal * 0.08, 2)  # 8% tax - adjust per location
    total = round(subtotal + shipping_cost + tax, 2)
    
    # Create order
    order = Order(
        user_id=user.id,
        order_number=generate_order_number(),
        status="pending",
        subtotal=subtotal,
        shipping_cost=shipping_cost,
        tax=tax,
        total=total,
        shipping_address=order_data.shipping_address.model_dump(),
        shipping_method=order_data.shipping_method,
        payment_method=order_data.payment_method,
        notes=order_data.notes
    )
    db.add(order)
    await db.flush()  # Get order ID
    
    # Create order items and update stock
    for cart_item in cart_items:
        order_item = OrderItem(
            order_id=order.id,
            product_id=cart_item.product_id,
            product_name=cart_item.product.name,
            product_sku=cart_item.product.sku,
            price=cart_item.product.price,
            quantity=cart_item.quantity,
            # BCW Refund Request Module v1.0.0: Copy category/source for refund eligibility
            category=cart_item.product.category,
            source=getattr(cart_item.product, 'source', None),
        )
        db.add(order_item)
        
        # Reduce stock
        cart_item.product.stock -= cart_item.quantity
        
        # Remove from cart
        await db.delete(cart_item)
    
    await db.commit()
    await db.refresh(order)
    
    # Reload with items
    result = await db.execute(
        select(Order)
        .where(Order.id == order.id)
        .options(selectinload(Order.items))
    )
    order = result.scalar_one()
    
    return order
