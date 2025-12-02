"""
Order routes
"""
import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.database import get_db
from app.models.order import Order, OrderItem
from app.models.cart import CartItem
from app.models.user import User
from app.schemas.order import OrderCreate, OrderResponse, OrderList
from app.api.deps import get_current_user

router = APIRouter()


def generate_order_number() -> str:
    """Generate unique order number"""
    return f"MDM-{uuid.uuid4().hex[:8].upper()}"


@router.get("", response_model=OrderList)
async def list_orders(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get current user's orders"""
    result = await db.execute(
        select(Order)
        .where(Order.user_id == user.id)
        .options(selectinload(Order.items))
        .order_by(Order.created_at.desc())
    )
    orders = result.scalars().all()
    
    return OrderList(orders=orders, total=len(orders))


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
            quantity=cart_item.quantity
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
