"""
Product schemas
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class ProductBase(BaseModel):
    name: str
    description: Optional[str] = None
    category: str
    subcategory: Optional[str] = None
    price: float
    original_price: Optional[float] = None


class ProductCreate(ProductBase):
    sku: str
    stock: int = 0
    image_url: Optional[str] = None
    images: List[str] = []
    tags: List[str] = []
    featured: bool = False
    
    # Comic-specific
    issue_number: Optional[str] = None
    publisher: Optional[str] = None
    year: Optional[int] = None
    artist: Optional[str] = None
    writer: Optional[str] = None
    
    # Grading
    cgc_grade: Optional[float] = None
    is_graded: bool = False


class ProductUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    price: Optional[float] = None
    original_price: Optional[float] = None
    stock: Optional[int] = None
    image_url: Optional[str] = None
    images: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    featured: Optional[bool] = None
    cgc_grade: Optional[float] = None
    estimated_grade: Optional[float] = None
    is_graded: Optional[bool] = None


class ProductResponse(ProductBase):
    id: int
    sku: str
    stock: int
    image_url: Optional[str]
    images: List[str]
    tags: List[str]
    featured: bool
    rating: float
    review_count: int
    
    # Comic-specific
    issue_number: Optional[str]
    publisher: Optional[str]
    year: Optional[int]
    
    # Grading
    cgc_grade: Optional[float]
    estimated_grade: Optional[float]
    grade_confidence: Optional[float]
    is_graded: bool
    
    created_at: datetime

    class Config:
        from_attributes = True


class ProductList(BaseModel):
    products: List[ProductResponse]
    total: int
    page: int
    per_page: int
