"""
Product routes

P2-6: Admin actions are audit logged
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.core.database import get_db
from app.core.audit_log import log_admin_action, ACTION_PRODUCT_CREATE, ACTION_PRODUCT_UPDATE, ACTION_PRODUCT_DELETE
from app.models.product import Product
from app.models.user import User
from app.schemas.product import ProductCreate, ProductUpdate, ProductResponse, ProductList
from app.api.deps import get_current_admin

router = APIRouter()


@router.get("", response_model=ProductList)
async def list_products(
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    search: Optional[str] = None,
    featured: Optional[bool] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    in_stock: Optional[bool] = None,
    sort: str = Query("featured", regex="^(featured|price_asc|price_desc|rating|newest)$"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """List products with filtering, sorting, and pagination"""
    query = select(Product)
    
    # Filters
    if category:
        query = query.where(Product.category == category)
    if subcategory:
        query = query.where(Product.subcategory == subcategory)
    if featured is not None:
        query = query.where(Product.featured == featured)
    if min_price is not None:
        query = query.where(Product.price >= min_price)
    if max_price is not None:
        query = query.where(Product.price <= max_price)
    if in_stock:
        query = query.where(Product.stock > 0)
    if search:
        search_term = f"%{search}%"
        query = query.where(
            Product.name.ilike(search_term) |
            Product.description.ilike(search_term)
        )
    
    # Sorting
    if sort == "price_asc":
        query = query.order_by(Product.price.asc())
    elif sort == "price_desc":
        query = query.order_by(Product.price.desc())
    elif sort == "rating":
        query = query.order_by(Product.rating.desc())
    elif sort == "newest":
        query = query.order_by(Product.created_at.desc())
    else:  # featured
        query = query.order_by(Product.featured.desc(), Product.created_at.desc())
    
    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total = await db.scalar(count_query)
    
    # Pagination
    offset = (page - 1) * per_page
    query = query.offset(offset).limit(per_page)
    
    result = await db.execute(query)
    products = result.scalars().all()
    
    return ProductList(
        products=products,
        total=total,
        page=page,
        per_page=per_page
    )


@router.get("/{product_id}", response_model=ProductResponse)
async def get_product(product_id: int, db: AsyncSession = Depends(get_db)):
    """Get single product by ID"""
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()
    
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    
    return product


@router.post("", response_model=ProductResponse, status_code=status.HTTP_201_CREATED)
async def create_product(
    request: Request,
    product_data: ProductCreate,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """Create new product (admin only)"""
    # Check SKU uniqueness
    result = await db.execute(select(Product).where(Product.sku == product_data.sku))
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="SKU already exists"
        )

    product = Product(**product_data.model_dump())
    db.add(product)
    await db.commit()
    await db.refresh(product)

    # P2-6: Audit log
    log_admin_action(
        action=ACTION_PRODUCT_CREATE,
        user_id=admin.id,
        user_email=admin.email,
        resource_type="product",
        resource_id=product.id,
        details={"sku": product.sku, "name": product.name},
        ip_address=request.client.host if request.client else None,
    )

    return product


@router.patch("/{product_id}", response_model=ProductResponse)
async def update_product(
    request: Request,
    product_id: int,
    update_data: ProductUpdate,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """Update product (admin only)"""
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()

    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )

    update_dict = update_data.model_dump(exclude_unset=True)
    for field, value in update_dict.items():
        setattr(product, field, value)

    await db.commit()
    await db.refresh(product)

    # P2-6: Audit log
    log_admin_action(
        action=ACTION_PRODUCT_UPDATE,
        user_id=admin.id,
        user_email=admin.email,
        resource_type="product",
        resource_id=product.id,
        details={"fields_updated": list(update_dict.keys())},
        ip_address=request.client.host if request.client else None,
    )

    return product


@router.delete("/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_product(
    request: Request,
    product_id: int,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """Delete product (admin only)"""
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()

    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )

    # Capture info before deletion
    product_info = {"sku": product.sku, "name": product.name}

    await db.delete(product)
    await db.commit()

    # P2-6: Audit log
    log_admin_action(
        action=ACTION_PRODUCT_DELETE,
        user_id=admin.id,
        user_email=admin.email,
        resource_type="product",
        resource_id=product_id,
        details=product_info,
        ip_address=request.client.host if request.client else None,
    )
