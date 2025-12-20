"""
Cart routes

v1.7.1: Fixed stock race condition with pessimistic locking
v1.7.2: Optimized subtotal calculation using SQL aggregation
v1.8.0: Added bundle support
"""
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, func, or_
from sqlalchemy.orm import selectinload

from app.core.database import get_db
from app.models.cart import CartItem
from app.models.product import Product
from app.models.bundle import Bundle, BundleStatus
from app.models.user import User
from app.schemas.cart import CartItemCreate, CartItemUpdate, CartResponse
from app.api.deps import get_current_user

router = APIRouter()


@router.get("", response_model=CartResponse)
async def get_cart(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get current user's cart with SQL-optimized subtotal calculation"""
    # Fetch cart items with eager loading
    result = await db.execute(
        select(CartItem)
        .where(CartItem.user_id == user.id)
        .options(
            selectinload(CartItem.product),
            selectinload(CartItem.bundle)
        )
    )
    items = result.scalars().all()

    # Calculate subtotal in SQL for precision and performance
    # Sum of (product price * qty) + (bundle price * qty)
    
    # 1. Product subtotal
    product_subtotal_result = await db.execute(
        select(func.coalesce(func.sum(CartItem.quantity * Product.price), Decimal("0.00")))
        .join(Product, CartItem.product_id == Product.id)
        .where(CartItem.user_id == user.id)
    )
    product_subtotal = product_subtotal_result.scalar() or Decimal("0.00")

    # 2. Bundle subtotal
    bundle_subtotal_result = await db.execute(
        select(func.coalesce(func.sum(CartItem.quantity * Bundle.bundle_price), Decimal("0.00")))
        .join(Bundle, CartItem.bundle_id == Bundle.id)
        .where(CartItem.user_id == user.id)
    )
    bundle_subtotal = bundle_subtotal_result.scalar() or Decimal("0.00")

    subtotal = product_subtotal + bundle_subtotal
    item_count = sum(item.quantity for item in items)

    return CartResponse(
        items=items,
        subtotal=round(float(subtotal), 2),
        item_count=item_count
    )


@router.post("/items", status_code=status.HTTP_201_CREATED)
async def add_to_cart(
    item_data: CartItemCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Add item or bundle to cart with pessimistic locking to prevent overselling"""
    
    if item_data.product_id:
        # Product Logic (Existing)
        # Acquire FOR UPDATE lock on product to prevent race condition
        result = await db.execute(
            select(Product)
            .where(Product.id == item_data.product_id)
            .with_for_update()  # Pessimistic lock prevents concurrent overselling
        )
        product = result.scalar_one_or_none()

        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )

        # Check if already in cart (while holding product lock)
        result = await db.execute(
            select(CartItem).where(
                CartItem.user_id == user.id,
                CartItem.product_id == item_data.product_id
            )
        )
        existing = result.scalar_one_or_none()

        # Calculate total requested quantity
        total_requested = item_data.quantity + (existing.quantity if existing else 0)

        if total_requested > product.stock:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Only {product.stock} items in stock"
            )

        if existing:
            existing.quantity = total_requested
        else:
            cart_item = CartItem(
                user_id=user.id,
                product_id=item_data.product_id,
                quantity=item_data.quantity
            )
            db.add(cart_item)

    elif item_data.bundle_id:
        # Bundle Logic (New)
        result = await db.execute(
            select(Bundle).where(Bundle.id == item_data.bundle_id)
        )
        bundle = result.scalar_one_or_none()
        
        if not bundle or bundle.status != BundleStatus.ACTIVE.value:
             raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Bundle not found or inactive"
            )
            
        if bundle.available_qty < item_data.quantity:
             raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Only {bundle.available_qty} bundles available"
            )

        # Check existing cart bundle
        result = await db.execute(
            select(CartItem).where(
                CartItem.user_id == user.id,
                CartItem.bundle_id == item_data.bundle_id
            )
        )
        existing = result.scalar_one_or_none()
        
        total_requested = item_data.quantity + (existing.quantity if existing else 0)
        
        if total_requested > bundle.available_qty:
             raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Only {bundle.available_qty} bundles available"
            )
            
        if existing:
            existing.quantity = total_requested
        else:
            cart_item = CartItem(
                user_id=user.id,
                bundle_id=item_data.bundle_id,
                quantity=item_data.quantity
            )
            db.add(cart_item)
            
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Must provide product_id or bundle_id"
        )

    await db.commit()

    return {"message": "Item added to cart"}


@router.patch("/items/{item_id}")
async def update_cart_item(
    item_id: int,
    update_data: CartItemUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Update cart item quantity with pessimistic locking"""
    # First get the cart item
    result = await db.execute(
        select(CartItem)
        .where(CartItem.id == item_id, CartItem.user_id == user.id)
    )
    item = result.scalar_one_or_none()

    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cart item not found"
        )

    if item.product_id:
        # Lock the product
        product_result = await db.execute(
            select(Product)
            .where(Product.id == item.product_id)
            .with_for_update()
        )
        product = product_result.scalar_one()

        if update_data.quantity > product.stock:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Only {product.stock} items in stock"
            )
    elif item.bundle_id:
        # Check bundle stock
        bundle_result = await db.execute(
            select(Bundle).where(Bundle.id == item.bundle_id)
        )
        bundle = bundle_result.scalar_one()
        
        if update_data.quantity > bundle.available_qty:
             raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Only {bundle.available_qty} bundles available"
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
    """Clear entire cart using bulk delete (performance optimization)"""
    # Use bulk delete instead of N+1 pattern
    await db.execute(
        delete(CartItem).where(CartItem.user_id == user.id)
    )
    await db.commit()