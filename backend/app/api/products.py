from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from slugify import slugify

from app.db import get_db, Product, Category
from app.schemas import ProductCreate, ProductUpdate, ProductResponse, ProductListResponse

router = APIRouter(prefix="/products", tags=["Products"])


@router.get("", response_model=ProductListResponse)
async def list_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    category: Optional[str] = None,
    search: Optional[str] = None,
    featured: Optional[bool] = None,
    sort_by: str = Query("featured", regex="^(featured|price_asc|price_desc|rating|newest)$"),
    db: AsyncSession = Depends(get_db)
):
    """List products with filtering and pagination."""
    query = select(Product)
    count_query = select(func.count(Product.id))
    
    # Filters
    if category:
        cat_result = await db.execute(select(Category).where(Category.slug == category))
        cat = cat_result.scalar_one_or_none()
        if cat:
            query = query.where(Product.category_id == cat.id)
            count_query = count_query.where(Product.category_id == cat.id)
    
    if search:
        search_term = f"%{search}%"
        query = query.where(
            Product.name.ilike(search_term) | 
            Product.description.ilike(search_term)
        )
        count_query = count_query.where(
            Product.name.ilike(search_term) | 
            Product.description.ilike(search_term)
        )
    
    if featured is not None:
        query = query.where(Product.featured == featured)
        count_query = count_query.where(Product.featured == featured)
    
    # Sorting
    if sort_by == "featured":
        query = query.order_by(Product.featured.desc(), Product.created_at.desc())
    elif sort_by == "price_asc":
        query = query.order_by(Product.price.asc())
    elif sort_by == "price_desc":
        query = query.order_by(Product.price.desc())
    elif sort_by == "rating":
        query = query.order_by(Product.rating.desc().nullslast())
    elif sort_by == "newest":
        query = query.order_by(Product.created_at.desc())
    
    # Pagination
    offset = (page - 1) * page_size
    query = query.offset(offset).limit(page_size)
    
    # Execute
    result = await db.execute(query)
    products = result.scalars().all()
    
    count_result = await db.execute(count_query)
    total = count_result.scalar()
    
    return ProductListResponse(
        products=products,
        total=total,
        page=page,
        page_size=page_size
    )


@router.get("/{slug}", response_model=ProductResponse)
async def get_product(slug: str, db: AsyncSession = Depends(get_db)):
    """Get a single product by slug."""
    result = await db.execute(select(Product).where(Product.slug == slug))
    product = result.scalar_one_or_none()
    
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    
    return product


@router.post("", response_model=ProductResponse, status_code=status.HTTP_201_CREATED)
async def create_product(
    product_data: ProductCreate,
    db: AsyncSession = Depends(get_db)
    # TODO: Add admin auth dependency
):
    """Create a new product (admin only)."""
    # Generate slug
    base_slug = slugify(product_data.name)
    slug = base_slug
    counter = 1
    
    while True:
        result = await db.execute(select(Product).where(Product.slug == slug))
        if not result.scalar_one_or_none():
            break
        slug = f"{base_slug}-{counter}"
        counter += 1
    
    # Verify category exists
    cat_result = await db.execute(select(Category).where(Category.id == product_data.category_id))
    if not cat_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Category not found"
        )
    
    product = Product(
        slug=slug,
        **product_data.model_dump()
    )
    db.add(product)
    await db.commit()
    await db.refresh(product)
    
    return product


@router.patch("/{slug}", response_model=ProductResponse)
async def update_product(
    slug: str,
    product_data: ProductUpdate,
    db: AsyncSession = Depends(get_db)
    # TODO: Add admin auth dependency
):
    """Update a product (admin only)."""
    result = await db.execute(select(Product).where(Product.slug == slug))
    product = result.scalar_one_or_none()
    
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    
    update_data = product_data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(product, field, value)
    
    await db.commit()
    await db.refresh(product)
    
    return product


@router.delete("/{slug}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_product(
    slug: str,
    db: AsyncSession = Depends(get_db)
    # TODO: Add admin auth dependency
):
    """Delete a product (admin only)."""
    result = await db.execute(select(Product).where(Product.slug == slug))
    product = result.scalar_one_or_none()
    
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    
    await db.delete(product)
    await db.commit()
