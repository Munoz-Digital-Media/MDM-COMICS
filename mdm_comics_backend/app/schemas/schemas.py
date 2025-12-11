from datetime import datetime
from decimal import Decimal
from typing import Optional, List
from pydantic import BaseModel, EmailStr, Field


# ============================================================================
# USER SCHEMAS
# ============================================================================
class UserBase(BaseModel):
    email: EmailStr
    name: str


class UserCreate(UserBase):
    password: str = Field(min_length=6)


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserResponse(UserBase):
    id: int
    is_active: bool
    is_admin: bool
    created_at: datetime
    
    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    user_id: Optional[int] = None


# ============================================================================
# CATEGORY SCHEMAS
# ============================================================================
class CategoryBase(BaseModel):
    name: str
    slug: str
    icon: Optional[str] = None


class CategoryCreate(CategoryBase):
    pass


class CategoryResponse(CategoryBase):
    id: int
    
    class Config:
        from_attributes = True


# ============================================================================
# PRODUCT SCHEMAS
# ============================================================================
class ProductBase(BaseModel):
    name: str
    description: Optional[str] = None
    price: Decimal
    original_price: Optional[Decimal] = None
    stock: int = 0
    subcategory: Optional[str] = None
    featured: bool = False
    tags: List[str] = []
    image_url: Optional[str] = None
    images: List[str] = []


class ProductCreate(ProductBase):
    category_id: int


class ProductUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    price: Optional[Decimal] = None
    original_price: Optional[Decimal] = None
    stock: Optional[int] = None
    category_id: Optional[int] = None
    subcategory: Optional[str] = None
    featured: Optional[bool] = None
    tags: Optional[List[str]] = None
    image_url: Optional[str] = None
    images: Optional[List[str]] = None


class GradeBreakdown(BaseModel):
    corner_wear: Optional[float] = None
    spine_stress: Optional[float] = None
    color_fading: Optional[float] = None
    page_quality: Optional[float] = None
    centering: Optional[float] = None


class ProductResponse(ProductBase):
    id: int
    slug: str
    category_id: int
    rating: Optional[Decimal] = None
    estimated_grade: Optional[Decimal] = None
    grade_confidence: Optional[Decimal] = None
    grade_breakdown: Optional[GradeBreakdown] = None
    is_graded: bool
    actual_grade: Optional[Decimal] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class ProductListResponse(BaseModel):
    products: List[ProductResponse]
    total: int
    page: int
    page_size: int


# ============================================================================
# GRADE ANALYSIS SCHEMAS
# ============================================================================
class GradeAnalysisRequest(BaseModel):
    image_url: Optional[str] = None  # URL to existing image
    # For direct upload, we'll handle file separately


class GradeAnalysisResponse(BaseModel):
    id: int
    product_id: int
    estimated_grade: Decimal
    confidence: Decimal
    breakdown: GradeBreakdown
    image_url: str
    model_version: str
    created_at: datetime
    
    class Config:
        from_attributes = True


# ============================================================================
# CART SCHEMAS
# ============================================================================
class CartItemBase(BaseModel):
    product_id: int
    quantity: int = 1


class CartItemCreate(CartItemBase):
    pass


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
    subtotal: Decimal
    shipping: Decimal
    total: Decimal


# ============================================================================
# ORDER SCHEMAS
# ============================================================================
class ShippingAddress(BaseModel):
    name: str
    street: str
    city: str
    state: str
    zip_code: str
    country: str = "USA"


class OrderCreate(BaseModel):
    shipping_address: ShippingAddress
    payment_method: str


class OrderItemResponse(BaseModel):
    id: int
    product_id: int
    product_name: str
    quantity: int
    price: Decimal
    
    class Config:
        from_attributes = True


class OrderResponse(BaseModel):
    id: int
    status: str
    total: Decimal
    shipping_cost: Decimal
    shipping_address: Optional[ShippingAddress] = None
    payment_method: Optional[str] = None
    items: List[OrderItemResponse]
    created_at: datetime
    
    class Config:
        from_attributes = True
