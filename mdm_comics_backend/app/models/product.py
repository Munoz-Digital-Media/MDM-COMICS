"""
Product model

Updated for Admin Console Inventory System v1.3.0:
- Added UPC, ISBN, bin_id for barcode scanning
- Added pricecharting_id for price sync linkage
- Added deleted_at for soft delete (preserves order history)
- Fixed timestamps to timezone-aware (NASTY-008)

DB-005/DB-006: Added audit columns and check constraints per constitution_db.json
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, Index, Numeric, ForeignKey, CheckConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=False, index=True)
    description = Column(Text)

    # Categorization
    category = Column(String, nullable=False, index=True)  # comics, funko
    subcategory = Column(String, index=True)  # Marvel, DC, etc.

    # Pricing - DB-001: Numeric(12,2) per constitution_db.json Section 5
    price = Column(Numeric(12, 2), nullable=False)  # Street value (from PriceCharting)
    original_price = Column(Numeric(12, 2))  # Our cost basis
    cost = Column(Numeric(12, 2))  # Deprecated - use original_price

    # Inventory
    stock = Column(Integer, default=0)
    low_stock_threshold = Column(Integer, default=5)

    # Barcode fields (Phase 1)
    upc = Column(String(50), nullable=True, index=True)
    isbn = Column(String(20), nullable=True, index=True)
    bin_id = Column(String(50), nullable=True, index=True)  # Location: "BIN-A1", "SHELF-3-B"
    last_stock_check = Column(DateTime(timezone=True), nullable=True)

    # External integrations
    pricecharting_id = Column(Integer, nullable=True, index=True)  # HIGH-006

    # Soft delete (HIGH-002: preserves order FK references)
    deleted_at = Column(DateTime(timezone=True), nullable=True, index=True)

    # Media
    image_url = Column(String)
    images = Column(JSON, default=list)  # Additional images

    # Comic-specific fields
    issue_number = Column(String)
    publisher = Column(String)
    year = Column(Integer)
    artist = Column(String)
    writer = Column(String)

    # Grading
    cgc_grade = Column(Float)  # Actual CGC grade if graded
    estimated_grade = Column(Float)  # AI estimated grade
    grade_confidence = Column(Float)  # AI confidence score
    is_graded = Column(Boolean, default=False)

    # Metadata
    tags = Column(JSON, default=list)
    featured = Column(Boolean, default=False)
    rating = Column(Float, default=0.0)
    review_count = Column(Integer, default=0)

    # Physical dimensions (for supplies like bins, cases) - in inches
    interior_width = Column(Float)
    interior_height = Column(Float)
    interior_length = Column(Float)
    exterior_width = Column(Float)
    exterior_height = Column(Float)
    exterior_length = Column(Float)

    # Timestamps - NASTY-008: Fixed to timezone-aware
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        server_default=func.now()
    )

    # DB-005: Audit trail columns per constitution_db.json Section 5
    updated_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    update_reason = Column(String(255), nullable=True)

    # Relationships
    cart_items = relationship("CartItem", back_populates="product")
    order_items = relationship("OrderItem", back_populates="product")
    stock_movements = relationship("StockMovement", back_populates="product")
    updater = relationship("User", foreign_keys=[updated_by])
    # BCW Dropship Integration v1.0.0
    bcw_inventory_snapshots = relationship("BCWInventorySnapshot", back_populates="product")
    bcw_mapping = relationship("BCWProductMapping", back_populates="product", uselist=False)

    # Indexes and constraints (PERF-003, PERF-004, DB-006)
    __table_args__ = (
        Index("ix_products_upc_isbn", upc, isbn),
        Index("ix_products_active", id, postgresql_where=(deleted_at.is_(None))),
        # DB-006: Check constraints for data integrity
        CheckConstraint('stock >= 0', name='check_stock_non_negative'),
        CheckConstraint('price > 0', name='check_price_positive'),
    )

    @property
    def is_deleted(self) -> bool:
        """Check if product is soft-deleted"""
        return self.deleted_at is not None

    def soft_delete(self):
        """Mark product as deleted without removing from database"""
        self.deleted_at = datetime.now(timezone.utc)

    def restore(self):
        """Restore a soft-deleted product"""
        self.deleted_at = None
