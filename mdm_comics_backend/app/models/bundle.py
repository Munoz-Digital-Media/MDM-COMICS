"""
Bundle Model for BCW Dropship Integration v1.0.0

Supports dynamic product bundles with:
- Flexible item composition
- Automatic margin calculation
- Savings display
- Active/inactive status

Per constitution_db.json:
- DB-001: Numeric(12,2) for monetary fields
- DB-003: FK with appropriate ON DELETE
- DB-004: Indexes on query columns
- DB-005: Audit columns
"""
from datetime import datetime, timezone
from typing import Optional, List
from enum import Enum as PyEnum

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, JSON,
    Numeric, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class BundleStatus(str, PyEnum):
    """Bundle lifecycle status."""
    DRAFT = "DRAFT"           # Not yet published
    ACTIVE = "ACTIVE"         # Live and purchasable
    INACTIVE = "INACTIVE"     # Temporarily disabled
    ARCHIVED = "ARCHIVED"     # Permanently retired


class Bundle(Base):
    """
    Product bundle configuration.

    Bundles allow grouping multiple products at a discounted price.
    """
    __tablename__ = "bundles"

    id = Column(Integer, primary_key=True, index=True)

    # Identification
    sku = Column(String(50), unique=True, nullable=False, index=True)  # e.g., "BDL-STARTER-001"
    name = Column(String(255), nullable=False, index=True)
    slug = Column(String(255), unique=True, nullable=False, index=True)  # URL-friendly name

    # Description
    short_description = Column(String(500), nullable=True)  # For listings
    description = Column(Text, nullable=True)  # Full markdown description

    # Pricing - DB-001: Numeric(12,2)
    bundle_price = Column(Numeric(12, 2), nullable=False)  # Final sale price
    compare_at_price = Column(Numeric(12, 2), nullable=True)  # Individual items total
    cost = Column(Numeric(12, 2), nullable=True)  # Our total cost

    # Calculated fields (updated on save)
    savings_amount = Column(Numeric(12, 2), nullable=True)  # compare_at - bundle_price
    savings_percent = Column(Numeric(5, 2), nullable=True)  # Percentage saved
    margin_percent = Column(Numeric(5, 2), nullable=True)  # (bundle_price - cost) / bundle_price

    # Status
    status = Column(String(20), default=BundleStatus.DRAFT.value, nullable=False, index=True)

    # Inventory (computed from items)
    available_qty = Column(Integer, default=0)  # Min qty across all items

    # Display
    image_url = Column(String(500), nullable=True)
    images = Column(JSON, default=lambda: [])  # Additional images - use lambda to avoid mutable default
    badge_text = Column(String(50), nullable=True)  # e.g., "Best Seller", "New"
    display_order = Column(Integer, default=0)  # For sorting

    # Categorization
    category = Column(String(100), nullable=True, index=True)  # e.g., "Starter Kits"
    tags = Column(JSON, default=lambda: [])  # For filtering - use lambda to avoid mutable default

    # Validity
    start_date = Column(DateTime(timezone=True), nullable=True)  # Optional sale period
    end_date = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    published_at = Column(DateTime(timezone=True), nullable=True)

    # DB-005: Audit columns
    created_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    updated_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    # Relationships
    items = relationship("BundleItem", back_populates="bundle", cascade="all, delete-orphan",
                        order_by="BundleItem.display_order")
    creator = relationship("User", foreign_keys=[created_by])
    updater = relationship("User", foreign_keys=[updated_by])

    # Indexes
    __table_args__ = (
        Index('ix_bundles_status_category', 'status', 'category'),
        Index('ix_bundles_active', 'status', postgresql_where=(status == 'ACTIVE')),
    )

    def calculate_totals(self):
        """Calculate derived pricing fields from items."""
        total_cost = sum(item.line_cost or 0 for item in self.items)
        total_compare = sum(item.line_price or 0 for item in self.items)

        self.cost = total_cost
        self.compare_at_price = total_compare
        self.savings_amount = total_compare - self.bundle_price if total_compare else 0

        if total_compare and total_compare > 0:
            self.savings_percent = (self.savings_amount / total_compare) * 100
        else:
            self.savings_percent = 0

        if self.bundle_price and self.bundle_price > 0:
            self.margin_percent = ((self.bundle_price - total_cost) / self.bundle_price) * 100
        else:
            self.margin_percent = 0

    def calculate_availability(self):
        """Calculate available quantity from items."""
        if not self.items:
            self.available_qty = 0
            return

        # Min qty across all items (limited by scarcest item)
        quantities = []
        for item in self.items:
            if item.product and item.product.stock is not None:
                max_bundles = item.product.stock // item.quantity
                quantities.append(max_bundles)

        self.available_qty = min(quantities) if quantities else 0


class BundleItem(Base):
    """
    Individual item within a bundle.

    Links products to bundles with quantity and pricing.
    """
    __tablename__ = "bundle_items"

    id = Column(Integer, primary_key=True, index=True)

    # Links
    bundle_id = Column(Integer, ForeignKey("bundles.id", ondelete="CASCADE"), nullable=False, index=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False, index=True)

    # Alternative: BCW mapping for dropship items
    bcw_mapping_id = Column(Integer, ForeignKey("bcw_product_mappings.id", ondelete="SET NULL"), nullable=True)

    # Quantity in bundle
    quantity = Column(Integer, default=1, nullable=False)

    # Pricing at time of bundle creation (denormalized for stability)
    unit_price = Column(Numeric(12, 2), nullable=True)  # Individual item price
    unit_cost = Column(Numeric(12, 2), nullable=True)   # Our cost per unit
    line_price = Column(Numeric(12, 2), nullable=True)  # unit_price * quantity
    line_cost = Column(Numeric(12, 2), nullable=True)   # unit_cost * quantity

    # Display
    display_order = Column(Integer, default=0)  # Order in bundle listing
    is_featured = Column(Boolean, default=False)  # Highlight this item
    custom_label = Column(String(100), nullable=True)  # Override product name in bundle

    # Optional: Item-specific options
    options = Column(JSON, default=lambda: {})  # e.g., {"color": "Black"} - use lambda to avoid mutable default

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    bundle = relationship("Bundle", back_populates="items")
    product = relationship("Product")
    bcw_mapping = relationship("BCWProductMapping")

    # Constraints
    __table_args__ = (
        UniqueConstraint('bundle_id', 'product_id', 'options', name='uq_bundle_product_options'),
        Index('ix_bundle_items_bundle', 'bundle_id'),
        Index('ix_bundle_items_product', 'product_id'),
    )

    def calculate_line_totals(self):
        """Calculate line price and cost."""
        if self.unit_price:
            self.line_price = self.unit_price * self.quantity
        if self.unit_cost:
            self.line_cost = self.unit_cost * self.quantity
