"""
Order schemas
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel


class ShippingAddress(BaseModel):
    name: str
    street: str
    city: str
    state: str
    zip_code: str
    country: str = "US"
    phone: Optional[str] = None


class OrderItemResponse(BaseModel):
    id: int
    product_id: int
    product_name: str
    product_sku: Optional[str]
    price: float
    quantity: int

    class Config:
        from_attributes = True


class OrderCreate(BaseModel):
    shipping_address: ShippingAddress
    shipping_method: str = "standard"
    payment_method: str
    notes: Optional[str] = None


class OrderResponse(BaseModel):
    id: int
    order_number: str
    status: str
    subtotal: float
    shipping_cost: float
    tax: float
    total: float
    shipping_address: Dict[str, Any]
    shipping_method: Optional[str]
    tracking_number: Optional[str]
    items: List[OrderItemResponse]
    created_at: datetime
    paid_at: Optional[datetime]
    shipped_at: Optional[datetime]

    class Config:
        from_attributes = True


class OrderList(BaseModel):
    orders: List[OrderResponse]
    total: int
