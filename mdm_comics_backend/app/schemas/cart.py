"""
Cart schemas
"""
from typing import List, Optional
from pydantic import BaseModel

from app.schemas.product import ProductResponse
from app.schemas.bundle import PublicBundleListResponse

class CartItemCreate(BaseModel):
    product_id: Optional[int] = None
    bundle_id: Optional[int] = None
    quantity: int = 1


class CartItemUpdate(BaseModel):
    quantity: int


class CartItemResponse(BaseModel):
    id: int
    product: Optional[ProductResponse] = None
    bundle: Optional[PublicBundleListResponse] = None
    quantity: int

    class Config:
        from_attributes = True


class CartResponse(BaseModel):
    items: List[CartItemResponse]
    subtotal: float
    item_count: int
