"""
Cart schemas
"""
from typing import List
from pydantic import BaseModel

from app.schemas.product import ProductResponse


class CartItemCreate(BaseModel):
    product_id: int
    quantity: int = 1


class CartItemUpdate(BaseModel):
    quantity: int


class CartItemResponse(BaseModel):
    id: int
    product: ProductResponse
    quantity: int

    class Config:
        from_attributes = True


class CartResponse(BaseModel):
    items: List[CartItemResponse]
    subtotal: float
    item_count: int
