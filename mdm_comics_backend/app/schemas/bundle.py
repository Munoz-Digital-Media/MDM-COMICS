"""
Pydantic Schemas for Bundle Builder Tool v1.0.0

Request/response schemas for bundle API endpoints.

Per constitution_cyberSec.json section 3:
- All input validated with schema-first approach
- Field constraints enforced via Pydantic validators
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator
import re


class BundleStatus(str, Enum):
    """Bundle lifecycle status."""
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ARCHIVED = "ARCHIVED"


# ==================== Bundle Item Schemas ====================

class BundleItemBase(BaseModel):
    """Base schema for bundle items."""
    product_id: int = Field(..., gt=0, description="Product ID to include in bundle")
    quantity: int = Field(default=1, ge=1, le=100, description="Quantity of this product")
    unit_price: Optional[Decimal] = Field(None, ge=0, description="Override price per unit")
    unit_cost: Optional[Decimal] = Field(None, ge=0, description="Cost per unit")
    display_order: int = Field(default=0, ge=0, description="Display order in bundle")
    is_featured: bool = Field(default=False, description="Highlight this item")
    custom_label: Optional[str] = Field(None, max_length=100, description="Override product name")
    options: Dict[str, Any] = Field(default_factory=dict, description="Item-specific options")


class BundleItemCreate(BundleItemBase):
    """Schema for adding an item to a bundle."""
    pass


class BundleItemUpdate(BaseModel):
    """Schema for updating a bundle item."""
    quantity: Optional[int] = Field(None, ge=1, le=100)
    unit_price: Optional[Decimal] = Field(None, ge=0)
    unit_cost: Optional[Decimal] = Field(None, ge=0)
    display_order: Optional[int] = Field(None, ge=0)
    is_featured: Optional[bool] = None
    custom_label: Optional[str] = Field(None, max_length=100)
    options: Optional[Dict[str, Any]] = None


class BundleItemResponse(BaseModel):
    """Response schema for bundle item."""
    id: int
    product_id: int
    bcw_mapping_id: Optional[int] = None
    quantity: int
    unit_price: Optional[Decimal] = None
    unit_cost: Optional[Decimal] = None
    line_price: Optional[Decimal] = None
    line_cost: Optional[Decimal] = None
    display_order: int
    is_featured: bool
    custom_label: Optional[str] = None
    options: Dict[str, Any]
    created_at: datetime
    updated_at: datetime

    # Product info (populated from relationship)
    product_name: Optional[str] = None
    product_sku: Optional[str] = None
    product_image_url: Optional[str] = None

    model_config = {"from_attributes": True}


# ==================== Bundle Schemas ====================

class BundleImage(BaseModel):
    """Bundle image with ordering and primary flag."""
    url: str = Field(..., max_length=1000, description="Public URL of the image")
    is_primary: bool = Field(default=False, description="Whether this image is the primary")
    order: int = Field(default=0, ge=0, description="Display order (0-based)")
    s3_key: Optional[str] = Field(None, description="Optional S3 key for cleanup/reference")

    @model_validator(mode="after")
    def normalize(self):
        # Trim whitespace on URL
        if self.url:
            self.url = self.url.strip()
        return self

class BundleBase(BaseModel):
    """Base schema for bundles."""
    name: str = Field(..., min_length=1, max_length=255, description="Bundle display name")
    short_description: Optional[str] = Field(None, max_length=500, description="Brief description")
    description: Optional[str] = Field(None, description="Full markdown description")
    bundle_price: Decimal = Field(..., gt=0, le=9999.99, description="Final sale price")
    category: Optional[str] = Field(None, max_length=100, description="Bundle category")
    tags: List[str] = Field(default_factory=list, description="Tags for filtering")
    image_url: Optional[str] = Field(None, max_length=500, description="Primary image URL")
    images: List[BundleImage] = Field(default_factory=list, description="Image gallery with primary and order")
    badge_text: Optional[str] = Field(None, max_length=50, description="Badge text e.g., 'Best Seller'")
    display_order: int = Field(default=0, ge=0, description="Display order for sorting")
    start_date: Optional[datetime] = Field(None, description="Sale period start")
    end_date: Optional[datetime] = Field(None, description="Sale period end")


class BundleCreate(BundleBase):
    """Schema for creating a new bundle."""
    sku: Optional[str] = Field(None, max_length=50, description="Custom SKU (auto-generated if not provided)")
    slug: Optional[str] = Field(None, max_length=255, description="URL slug (auto-generated if not provided)")
    items: List[BundleItemCreate] = Field(default_factory=list, description="Initial bundle items")

    @field_validator("sku")
    @classmethod
    def validate_sku(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        # SKU format: BDL-{CATEGORY}-{NUMBER} or custom
        if not re.match(r"^[A-Z0-9\-]+$", v):
            raise ValueError("SKU must contain only uppercase letters, numbers, and hyphens")
        return v

    @field_validator("slug")
    @classmethod
    def validate_slug(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if not re.match(r"^[a-z0-9\-]+$", v):
            raise ValueError("Slug must contain only lowercase letters, numbers, and hyphens")
        return v

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start_date and self.end_date:
            if self.start_date >= self.end_date:
                raise ValueError("end_date must be after start_date")
        return self

    @model_validator(mode="after")
    def validate_images(self):
        # If images empty but image_url provided, seed from image_url
        if not self.images and self.image_url:
            self.images = [BundleImage(url=self.image_url, is_primary=True, order=0)]

        if len(self.images) == 0:
            raise ValueError("At least one image is required")
        if len(self.images) > 9:
            raise ValueError("Maximum 9 images allowed")

        # Ensure one primary
        primary_count = sum(1 for img in self.images if img.is_primary)
        if primary_count == 0 and self.images:
            self.images[0].is_primary = True
        elif primary_count > 1:
            # Normalize to single primary: keep first primary, demote others
            primary_seen = False
            for img in self.images:
                if img.is_primary:
                    if primary_seen:
                        img.is_primary = False
                    else:
                        primary_seen = True

        # Normalize orders as sequential based on current ordering
        for idx, img in enumerate(self.images):
            img.order = idx

        # Sync image_url with primary
        primary = next((img for img in self.images if img.is_primary), self.images[0])
        self.image_url = primary.url
        return self


class BundleUpdate(BaseModel):
    """Schema for updating a bundle."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    short_description: Optional[str] = Field(None, max_length=500)
    description: Optional[str] = None
    bundle_price: Optional[Decimal] = Field(None, gt=0, le=9999.99)
    category: Optional[str] = Field(None, max_length=100)
    tags: Optional[List[str]] = None
    image_url: Optional[str] = Field(None, max_length=500)
    images: Optional[List[BundleImage]] = None
    badge_text: Optional[str] = Field(None, max_length=50)
    display_order: Optional[int] = Field(None, ge=0)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    @model_validator(mode="after")
    def validate_images(self):
        # Only enforce when images provided
        if self.images is not None:
            if len(self.images) == 0:
                raise ValueError("At least one image is required")
            if len(self.images) > 9:
                raise ValueError("Maximum 9 images allowed")

            primary_count = sum(1 for img in self.images if img.is_primary)
            if primary_count == 0 and self.images:
                self.images[0].is_primary = True
            elif primary_count > 1:
                primary_seen = False
                for img in self.images:
                    if img.is_primary:
                        if primary_seen:
                            img.is_primary = False
                        else:
                            primary_seen = True

            for idx, img in enumerate(self.images):
                img.order = idx

            primary = next((img for img in self.images if img.is_primary), self.images[0])
            self.image_url = primary.url
        return self


class BundleResponse(BaseModel):
    """Response schema for bundle detail."""
    id: int
    sku: str
    name: str
    slug: str
    short_description: Optional[str] = None
    description: Optional[str] = None
    bundle_price: Decimal
    compare_at_price: Optional[Decimal] = None
    cost: Optional[Decimal] = None
    savings_amount: Optional[Decimal] = None
    savings_percent: Optional[Decimal] = None
    margin_percent: Optional[Decimal] = None
    status: BundleStatus
    available_qty: int
    image_url: Optional[str] = None
    images: List[BundleImage]
    badge_text: Optional[str] = None
    display_order: int
    category: Optional[str] = None
    tags: List[str]
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    published_at: Optional[datetime] = None
    items: List[BundleItemResponse] = []
    item_count: int = 0

    model_config = {"from_attributes": True}


class BundleListResponse(BaseModel):
    """Response schema for bundle list item (without full item details)."""
    id: int
    sku: str
    name: str
    slug: str
    short_description: Optional[str] = None
    bundle_price: Decimal
    compare_at_price: Optional[Decimal] = None
    savings_amount: Optional[Decimal] = None
    savings_percent: Optional[Decimal] = None
    status: BundleStatus
    available_qty: int
    image_url: Optional[str] = None
    images: List[BundleImage] = []
    badge_text: Optional[str] = None
    display_order: int
    category: Optional[str] = None
    item_count: int = 0
    created_at: datetime

    model_config = {"from_attributes": True}


class PaginatedBundleList(BaseModel):
    """Paginated response for bundle list."""
    items: List[BundleListResponse]
    total: int
    page: int
    per_page: int
    pages: int


# ==================== Public Bundle Schemas ====================

class PublicBundleItemResponse(BaseModel):
    """Public response schema for bundle item (no cost data)."""
    id: int
    product_id: int
    quantity: int
    unit_price: Optional[Decimal] = None
    line_price: Optional[Decimal] = None
    display_order: int
    is_featured: bool
    custom_label: Optional[str] = None
    product_name: Optional[str] = None
    product_sku: Optional[str] = None
    product_image_url: Optional[str] = None

    model_config = {"from_attributes": True}


class PublicBundleResponse(BaseModel):
    """Public response schema for bundle detail (no cost/margin data)."""
    id: int
    sku: str
    name: str
    slug: str
    short_description: Optional[str] = None
    description: Optional[str] = None
    bundle_price: Decimal
    compare_at_price: Optional[Decimal] = None
    savings_amount: Optional[Decimal] = None
    savings_percent: Optional[Decimal] = None
    available_qty: int
    image_url: Optional[str] = None
    images: List[BundleImage]
    badge_text: Optional[str] = None
    category: Optional[str] = None
    tags: List[str]
    items: List[PublicBundleItemResponse] = []
    item_count: int = 0

    model_config = {"from_attributes": True}


class PublicBundleListResponse(BaseModel):
    """Public response for bundle list."""
    id: int
    sku: str
    name: str
    slug: str
    short_description: Optional[str] = None
    bundle_price: Decimal
    compare_at_price: Optional[Decimal] = None
    savings_amount: Optional[Decimal] = None
    savings_percent: Optional[Decimal] = None
    available_qty: int
    image_url: Optional[str] = None
    badge_text: Optional[str] = None
    category: Optional[str] = None
    item_count: int = 0

    model_config = {"from_attributes": True}


class PaginatedPublicBundleList(BaseModel):
    """Paginated response for public bundle list."""
    items: List[PublicBundleListResponse]
    total: int
    page: int
    per_page: int
    pages: int


# ==================== Pricing Calculation Schemas ====================

class BundlePricingItem(BaseModel):
    """Item for pricing calculation."""
    product_id: int
    quantity: int = Field(ge=1)
    unit_price: Optional[Decimal] = None
    unit_cost: Optional[Decimal] = None


class BundlePricingRequest(BaseModel):
    """Request for bundle pricing calculation (preview mode)."""
    items: List[BundlePricingItem]
    bundle_price: Decimal = Field(gt=0)


class BundlePricingResponse(BaseModel):
    """Response for bundle pricing calculation."""
    total_cost: Decimal
    total_compare_at: Decimal
    bundle_price: Decimal
    savings_amount: Decimal
    savings_percent: Decimal
    margin_amount: Decimal
    margin_percent: Decimal
    is_margin_healthy: bool
    margin_warning: Optional[str] = None


# ==================== Cart Integration Schemas ====================

class BundleCartItemCreate(BaseModel):
    """Schema for adding bundle to cart."""
    bundle_id: int = Field(..., gt=0)
    quantity: int = Field(default=1, ge=1, le=10)


class BundleCartItemResponse(BaseModel):
    """Response schema for bundle cart item."""
    id: int
    bundle_id: int
    quantity: int
    price_snapshot: Optional[Decimal] = None
    added_at: datetime
    bundle: PublicBundleListResponse

    model_config = {"from_attributes": True}
