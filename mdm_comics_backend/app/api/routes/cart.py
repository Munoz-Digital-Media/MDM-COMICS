"""
Cart routes
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.database import get_db
from app.models.cart import CartItem
from app.models.product import Product
from app.models.user import User
from app.schemas.cart import CartItemCreate, CartItemUpdate, CartResponse
from app.api.deps import get_current_user

router = APIRouter()


@router.get("", response_model=CartResponse)
async def get_cart(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get current user's cart"""
    result = await db.execute(
        select(CartItem)
        .where(CartItem.user_id == user.id)
        .options(selectinload(CartItem.product))
    )
    items = result.scalars().all()
    
    subtotal = sum(item.product.price * item.quantity for item in items)
    item_count = sum(item.quantity for item in items)
    
    return CartResponse(
        items=items,
        subtotal=round(subtotal, 2),
        item_count=item_count
    )


@router.post("/items", status_code=status.HTTP_201_CREATED)
async def add_to_cart(
    item_data: CartItemCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Add item to cart"""
    # Check product exists and has stock
    result = await db.execute(select(Product).where(Product.id == item_data.product_id))
    product = result.scalar_one_or_none()
    
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    
    if product.stock < item_data.quantity:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Only {product.stock} items in stock"
        )
    
    # Check if already in cart
    result = await db.execute(
        select(CartItem).where(
            CartItem.user_id == user.id,
            CartItem.product_id == item_data.product_id
        )
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        new_qty = existing.quantity + item_data.quantity
        if new_qty > product.stock:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot add more. Only {product.stock} in stock"
            )
        existing.quantity = new_qty
    else:
        cart_item = CartItem(
            user_id=user.id,
            product_id=item_data.product_id,
            quantity=item_data.quantity
        )
        db.add(cart_item)
    
    await db.commit()
    
    return {"message": "Item added to cart"}


@router.patch("/items/{item_id}")
async def update_cart_item(
    item_id: int,
    update_data: CartItemUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Update cart item quantity"""
    result = await db.execute(
        select(CartItem)
        .where(CartItem.id == item_id, CartItem.user_id == user.id)
        .options(selectinload(CartItem.product))
    )
    item = result.scalar_one_or_none()
    
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cart item not found"
        )
    
    if update_data.quantity > item.product.stock:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Only {item.product.stock} items in stock"
        )
    
    if update_data.quantity <= 0:
        await db.delete(item)
    else:
        item.quantity = update_data.quantity
    
    await db.commit()
    
    return {"message": "Cart updated"}


@router.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_from_cart(
    item_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Remove item from cart"""
    result = await db.execute(
        select(CartItem).where(CartItem.id == item_id, CartItem.user_id == user.id)
    )
    item = result.scalar_one_or_none()
    
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cart item not found"
        )
    
    await db.delete(item)
    await db.commit()


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def clear_cart(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Clear entire cart"""
    result = await db.execute(
        select(CartItem).where(CartItem.user_id == user.id)
    )
    items = result.scalars().all()
    
    for item in items:
        await db.delete(item)
    
    await db.commit()
