"""
Coupon API Routes

Public validation endpoint + Admin CRUD.
"""

import logging
from typing import Any, Dict, List, Optional
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.database import get_db
from app.api.deps import get_current_user, get_current_admin, get_optional_user
from app.models.user import User
from app.models.coupon import Coupon, CouponCampaign
from app.services.coupon_service import get_coupon_service, CouponValidationError

logger = logging.getLogger(__name__)
router = APIRouter()


# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================

class CartItem(BaseModel):
    product_id: int
    category: str
    price: float
    quantity: int


class ValidateCouponRequest(BaseModel):
    code: str
    cart_items: List[CartItem]
    cart_total: float


class CouponResponse(BaseModel):
    valid: bool
    code: str
    discount_type: Optional[str] = None
    discount_value: Optional[float] = None
    discount_amount: Optional[float] = None
    message: Optional[str] = None


class CreateCouponRequest(BaseModel):
    code: str
    name: str
    description: Optional[str] = None
    discount_type: str  # percentage, fixed_amount, free_shipping
    discount_value: float
    minimum_order_value: Optional[float] = None
    maximum_discount: Optional[float] = None
    applies_to: str = "all"
    applicable_categories: List[str] = Field(default_factory=list)
    applicable_product_ids: List[int] = Field(default_factory=list)
    excluded_product_ids: List[int] = Field(default_factory=list)
    usage_limit_total: Optional[int] = None
    usage_limit_per_user: int = 1
    first_order_only: bool = False
    registered_users_only: bool = False
    starts_at: Optional[str] = None
    expires_at: Optional[str] = None


class CouponListResponse(BaseModel):
    id: int
    code: str
    name: str
    discount_type: str
    discount_value: float
    usage_count: int
    is_active: bool
    expires_at: Optional[str] = None


# ============================================================================
# PUBLIC ENDPOINTS
# ============================================================================

@router.post("/validate", response_model=CouponResponse)
async def validate_coupon(
    payload: ValidateCouponRequest,
    db: AsyncSession = Depends(get_db),
    user: Optional[User] = Depends(get_optional_user),
):
    """
    Validate a coupon code against the current cart.

    Returns discount amount if valid, error message if not.
    """
    service = get_coupon_service()

    try:
        cart_items = [item.model_dump() for item in payload.cart_items]
        coupon, discount = await service.validate_coupon(
            db=db,
            code=payload.code,
            cart_items=cart_items,
            cart_total=Decimal(str(payload.cart_total)),
            user=user,
        )

        return CouponResponse(
            valid=True,
            code=coupon.code,
            discount_type=coupon.discount_type,
            discount_value=float(coupon.discount_value),
            discount_amount=float(discount),
            message=f"${discount:.2f} discount applied!",
        )

    except CouponValidationError as e:
        return CouponResponse(
            valid=False,
            code=payload.code,
            message=e.message,
        )


@router.post("/apply")
async def apply_coupon(
    payload: ValidateCouponRequest,
    db: AsyncSession = Depends(get_db),
    user: Optional[User] = Depends(get_optional_user),
):
    """
    Apply a coupon to the cart (record usage).

    Call this when user confirms they want to use the coupon.
    """
    service = get_coupon_service()

    try:
        cart_items = [item.model_dump() for item in payload.cart_items]
        coupon, discount = await service.validate_coupon(
            db=db,
            code=payload.code,
            cart_items=cart_items,
            cart_total=Decimal(str(payload.cart_total)),
            user=user,
        )

        usage = await service.apply_coupon(
            db=db,
            coupon=coupon,
            discount=discount,
            cart_total=Decimal(str(payload.cart_total)),
            user=user,
        )

        await db.commit()

        return {
            "applied": True,
            "usage_id": usage.id,
            "code": coupon.code,
            "discount_amount": float(discount),
            "new_total": float(Decimal(str(payload.cart_total)) - discount),
        }

    except CouponValidationError as e:
        raise HTTPException(status_code=400, detail=e.message)


# ============================================================================
# ADMIN ENDPOINTS
# ============================================================================

@router.get("/admin/list", response_model=List[CouponListResponse])
async def list_coupons(
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
    active_only: bool = False,
):
    """List all coupons (admin only)."""
    query = select(Coupon).order_by(Coupon.created_at.desc())

    if active_only:
        query = query.where(Coupon.is_active == True)

    result = await db.execute(query)
    coupons = result.scalars().all()

    return [
        CouponListResponse(
            id=c.id,
            code=c.code,
            name=c.name,
            discount_type=c.discount_type,
            discount_value=float(c.discount_value),
            usage_count=c.usage_count or 0,
            is_active=c.is_active,
            expires_at=c.expires_at.isoformat() if c.expires_at else None,
        )
        for c in coupons
    ]


@router.post("/admin/create")
async def create_coupon(
    payload: CreateCouponRequest,
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Create a new coupon (admin only)."""
    service = get_coupon_service()

    # Check for duplicate code
    existing = await db.execute(
        select(Coupon).where(Coupon.code == payload.code.upper().strip())
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Coupon code already exists")

    coupon = await service.create_coupon(
        db=db,
        data=payload.model_dump(),
        created_by=admin.id,
    )

    await db.commit()

    return {"created": True, "id": coupon.id, "code": coupon.code}


@router.patch("/admin/{coupon_id}/toggle")
async def toggle_coupon(
    coupon_id: int,
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Toggle coupon active status (admin only)."""
    result = await db.execute(select(Coupon).where(Coupon.id == coupon_id))
    coupon = result.scalar_one_or_none()

    if not coupon:
        raise HTTPException(status_code=404, detail="Coupon not found")

    coupon.is_active = not coupon.is_active
    await db.commit()

    return {"id": coupon_id, "is_active": coupon.is_active}


@router.delete("/admin/{coupon_id}")
async def delete_coupon(
    coupon_id: int,
    admin: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Delete a coupon (admin only)."""
    result = await db.execute(select(Coupon).where(Coupon.id == coupon_id))
    coupon = result.scalar_one_or_none()

    if not coupon:
        raise HTTPException(status_code=404, detail="Coupon not found")

    await db.delete(coupon)
    await db.commit()

    return {"deleted": True, "id": coupon_id}
