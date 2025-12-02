"""
Product model
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON
from sqlalchemy.orm import relationship

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
    
    # Pricing
    price = Column(Float, nullable=False)
    original_price = Column(Float)  # For sales
    cost = Column(Float)  # Our cost (for margin calc)
    
    # Inventory
    stock = Column(Integer, default=0)
    low_stock_threshold = Column(Integer, default=5)
    
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
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    cart_items = relationship("CartItem", back_populates="product")
    order_items = relationship("OrderItem", back_populates="product")
