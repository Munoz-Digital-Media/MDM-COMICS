from datetime import datetime
from decimal import Decimal
from typing import Optional, List
from sqlalchemy import String, Text, Numeric, Integer, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base


class User(Base):
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    hashed_password: Mapped[str] = mapped_column(String(255))
    name: Mapped[str] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    orders: Mapped[List["Order"]] = relationship(back_populates="user")
    cart_items: Mapped[List["CartItem"]] = relationship(back_populates="user")


class Category(Base):
    __tablename__ = "categories"
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    slug: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    icon: Mapped[Optional[str]] = mapped_column(String(50))
    
    # Relationships
    products: Mapped[List["Product"]] = relationship(back_populates="category")


class Product(Base):
    __tablename__ = "products"
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    original_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    stock: Mapped[int] = mapped_column(Integer, default=0)
    
    # Category
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    subcategory: Mapped[Optional[str]] = mapped_column(String(100))
    
    # Metadata
    featured: Mapped[bool] = mapped_column(Boolean, default=False)
    rating: Mapped[Optional[Decimal]] = mapped_column(Numeric(2, 1))
    tags: Mapped[Optional[list]] = mapped_column(JSON, default=list)
    
    # Images
    image_url: Mapped[Optional[str]] = mapped_column(String(500))
    images: Mapped[Optional[list]] = mapped_column(JSON, default=list)
    
    # AI Grade Estimation
    estimated_grade: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 1))
    grade_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2))
    grade_breakdown: Mapped[Optional[dict]] = mapped_column(JSON)
    is_graded: Mapped[bool] = mapped_column(Boolean, default=False)  # CGC/CBCS graded
    actual_grade: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 1))  # If officially graded
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    category: Mapped["Category"] = relationship(back_populates="products")
    order_items: Mapped[List["OrderItem"]] = relationship(back_populates="product")
    cart_items: Mapped[List["CartItem"]] = relationship(back_populates="product")
    grade_analyses: Mapped[List["GradeAnalysis"]] = relationship(back_populates="product")


class GradeAnalysis(Base):
    """Stores AI grade analysis history for a product."""
    __tablename__ = "grade_analyses"
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    
    # Analysis results
    estimated_grade: Mapped[Decimal] = mapped_column(Numeric(3, 1))
    confidence: Mapped[Decimal] = mapped_column(Numeric(3, 2))
    
    # Breakdown scores (0-10 scale)
    corner_wear: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 1))
    spine_stress: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 1))
    color_fading: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 1))
    page_quality: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 1))
    centering: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 1))
    
    # Image used for analysis
    image_url: Mapped[str] = mapped_column(String(500))
    
    # Model info
    model_version: Mapped[str] = mapped_column(String(50))
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Relationships
    product: Mapped["Product"] = relationship(back_populates="grade_analyses")


class CartItem(Base):
    __tablename__ = "cart_items"
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    quantity: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user: Mapped["User"] = relationship(back_populates="cart_items")
    product: Mapped["Product"] = relationship(back_populates="cart_items")


class Order(Base):
    __tablename__ = "orders"
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    
    # Order details
    status: Mapped[str] = mapped_column(String(50), default="pending")  # pending, paid, shipped, delivered, cancelled
    total: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    shipping_cost: Mapped[Decimal] = mapped_column(Numeric(10, 2), default=0)
    
    # Shipping info
    shipping_address: Mapped[Optional[dict]] = mapped_column(JSON)
    
    # Payment
    payment_method: Mapped[Optional[str]] = mapped_column(String(50))
    payment_id: Mapped[Optional[str]] = mapped_column(String(255))
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user: Mapped["User"] = relationship(back_populates="orders")
    items: Mapped[List["OrderItem"]] = relationship(back_populates="order")


class OrderItem(Base):
    __tablename__ = "order_items"
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"))
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    quantity: Mapped[int] = mapped_column(Integer)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 2))  # Price at time of purchase
    
    # Relationships
    order: Mapped["Order"] = relationship(back_populates="items")
    product: Mapped["Product"] = relationship(back_populates="order_items")
