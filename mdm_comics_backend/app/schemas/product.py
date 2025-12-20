"""
Product schemas
"""
from datetime import datetime
from typing import Optional, List, Literal
from pydantic import BaseModel, field_validator, Field

from app.models.product import GradingCompany, GradeLabel


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

    # Barcode fields
    upc: Optional[str] = None
    isbn: Optional[str] = None

    # Comic-specific
    issue_number: Optional[str] = None
    publisher: Optional[str] = None
    year: Optional[int] = None
    artist: Optional[str] = None
    writer: Optional[str] = None

    # Grading
    cgc_grade: Optional[float] = Field(None, ge=0.5, le=10.0, description="Numeric grade 0.5-10.0")
    is_graded: bool = False
    grading_company: Optional[GradingCompany] = None
    certification_number: Optional[str] = Field(None, max_length=50)
    grade_label: Optional[GradeLabel] = None


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

    # Barcode fields
    upc: Optional[str] = None
    isbn: Optional[str] = None

    # Comic-specific
    issue_number: Optional[str] = None
    publisher: Optional[str] = None
    year: Optional[int] = None

    # Physical dimensions (for supplies) - in inches
    interior_width: Optional[float] = None
    interior_height: Optional[float] = None
    interior_length: Optional[float] = None
    exterior_width: Optional[float] = None
    exterior_height: Optional[float] = None
    exterior_length: Optional[float] = None

    # Physical properties
    weight: Optional[str] = None
    material: Optional[str] = None

    # Grading
    cgc_grade: Optional[float] = Field(None, ge=0.5, le=10.0, description="Numeric grade 0.5-10.0")
    estimated_grade: Optional[float] = None
    is_graded: Optional[bool] = None
    grading_company: Optional[GradingCompany] = None
    certification_number: Optional[str] = Field(None, max_length=50)
    grade_label: Optional[GradeLabel] = None


class ProductResponse(ProductBase):
    id: int
    sku: str
    stock: int = 0
    image_url: Optional[str] = None
    images: List[str] = []
    tags: List[str] = []
    featured: bool = False
    rating: float = 0.0
    review_count: int = 0

    # Barcode fields
    upc: Optional[str] = None
    isbn: Optional[str] = None

    # Comic-specific
    issue_number: Optional[str] = None
    publisher: Optional[str] = None
    year: Optional[int] = None

    # Physical dimensions (for supplies) - in inches
    interior_width: Optional[float] = None
    interior_height: Optional[float] = None
    interior_length: Optional[float] = None
    exterior_width: Optional[float] = None
    exterior_height: Optional[float] = None
    exterior_length: Optional[float] = None

    # Physical properties
    weight: Optional[str] = None
    material: Optional[str] = None

    # Grading
    cgc_grade: Optional[float] = None
    estimated_grade: Optional[float] = None
    grade_confidence: Optional[float] = None
    is_graded: bool = False
    grading_company: Optional[str] = None  # Returns enum value as string
    certification_number: Optional[str] = None
    grade_label: Optional[str] = None  # Returns enum value as string

    created_at: Optional[datetime] = None

    # Handle NULL values from database
    @field_validator('stock', 'review_count', mode='before')
    @classmethod
    def default_int(cls, v):
        return v if v is not None else 0

    @field_validator('rating', mode='before')
    @classmethod
    def default_float(cls, v):
        return v if v is not None else 0.0

    @field_validator('images', 'tags', mode='before')
    @classmethod
    def default_list(cls, v):
        return v if v is not None else []

    @field_validator('featured', 'is_graded', mode='before')
    @classmethod
    def default_bool(cls, v):
        return v if v is not None else False

    class Config:
        from_attributes = True


class ProductList(BaseModel):
    products: List[ProductResponse]
    total: int
    page: int
    per_page: int
