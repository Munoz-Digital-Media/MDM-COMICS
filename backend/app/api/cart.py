from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db import get_db, CartItem, Product, User
from app.schemas import CartItemCreate, CartItemUpdate, CartResponse, CartItemResponse
from app.api.auth import get_current_user

router = APIRouter(prefix="/cart", tags=["Cart"])

FREE_SHIPPING_THRESHOLD = Decimal("50.00")
SHIPPING_COST = Decimal("5.99")


@router.get("", response_model=CartResponse)
async def get_cart(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get current user's cart."""
    result = await db.execute(
        select(CartItem)
        .where(CartItem.user_id == current_user.id)
        .options(selectinload(CartItem.product))
    )
    items = result.scalars().all()
    
    subtotal = sum(
        Decimal(str(item.product.price)) * item.quantity 
        for item in items
    )
    shipping = Decimal("0") if subtotal >= FREE_SHIPPING_THRESHOLD else SHIPPING_COST
    
    return CartResponse(
        items=[
            CartItemResponse(
                id=item.id,
                product=item.product,
                quantity=item.quantity,
            )
            for item in items
        ],
        subtotal=subtotal,
        shipping=shipping,
        total=subtotal + shipping,
    )


@router.post("/items", response_model=CartItemResponse, status_code=status.HTTP_201_CREATED)
async def add_to_cart(
    item_data: CartItemCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Add item to cart."""
    # Check if product exists and has stock
    product_result = await db.execute(
        select(Product).where(Product.id == item_data.product_id)
    )
    product = product_result.scalar_one_or_none()
    
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    
    if product.stock < item_data.quantity:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Only {product.stock} items available"
        )
    
    # Check if item already in cart
    existing_result = await db.execute(
        select(CartItem).where(
            CartItem.user_id == current_user.id,
            CartItem.product_id == item_data.product_id
        )
    )
    existing = existing_result.scalar_one_or_none()
    
    if existing:
        # Update quantity
        new_quantity = existing.quantity + item_data.quantity
        if new_quantity > product.stock:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Only {product.stock} items available"
            )
        existing.quantity = new_quantity
        await db.commit()
        await db.refresh(existing)
        
        return CartItemResponse(
            id=existing.id,
            product=product,
            quantity=existing.quantity,
        )
    
    # Create new cart item
    cart_item = CartItem(
        user_id=current_user.id,
        product_id=item_data.product_id,
        quantity=item_data.quantity,
    )
    db.add(cart_item)
    await db.commit()
    await db.refresh(cart_item)
    
    return CartItemResponse(
        id=cart_item.id,
        product=product,
        quantity=cart_item.quantity,
    )


@router.patch("/items/{item_id}", response_model=CartItemResponse)
async def update_cart_item(
    item_id: int,
    item_data: CartItemUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Update cart item quantity."""
    result = await db.execute(
        select(CartItem)
        .where(CartItem.id == item_id, CartItem.user_id == current_user.id)
        .options(selectinload(CartItem.product))
    )
    cart_item = result.scalar_one_or_none()
    
    if not cart_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cart item not found"
        )
    
    if item_data.quantity <= 0:
        await db.delete(cart_item)
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_204_NO_CONTENT,
            detail="Item removed from cart"
        )
    
    if item_data.quantity > cart_item.product.stock:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Only {cart_item.product.stock} items available"
        )
    
    cart_item.quantity = item_data.quantity
    await db.commit()
    await db.refresh(cart_item)
    
    return CartItemResponse(
        id=cart_item.id,
        product=cart_item.product,
        quantity=cart_item.quantity,
    )


@router.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_from_cart(
    item_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Remove item from cart."""
    result = await db.execute(
        select(CartItem).where(
            CartItem.id == item_id,
            CartItem.user_id == current_user.id
        )
    )
    cart_item = result.scalar_one_or_none()
    
    if not cart_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cart item not found"
        )
    
    await db.delete(cart_item)
    await db.commit()


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def clear_cart(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Clear all items from cart."""
    result = await db.execute(
        select(CartItem).where(CartItem.user_id == current_user.id)
    )
    items = result.scalars().all()
    
    for item in items:
        await db.delete(item)
    
    await db.commit()
