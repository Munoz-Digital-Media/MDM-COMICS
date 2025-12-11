"""
Funko POP database models

Updated for Admin Console Inventory System v1.3.0:
- Added price columns for PriceCharting sync (BLOCK-002)
- Added pricecharting_id for external linking
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Table, ForeignKey, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


# Many-to-many relationship table for Funko and Series
funko_series = Table(
    'funko_series',
    Base.metadata,
    Column('funko_id', Integer, ForeignKey('funkos.id'), primary_key=True),
    Column('series_id', Integer, ForeignKey('funko_series_names.id'), primary_key=True)
)


class FunkoSeriesName(Base):
    """Funko series/category (e.g., Pop! Animation, Pop! Vinyl, Chase Pieces)"""
    __tablename__ = "funko_series_names"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)

    # Relationship
    funkos = relationship("Funko", secondary=funko_series, back_populates="series")


class Funko(Base):
    """Funko POP product from the catalog"""
    __tablename__ = "funkos"

    id = Column(Integer, primary_key=True, index=True)
    handle = Column(String(500), unique=True, nullable=False, index=True)
    title = Column(String(500), nullable=False, index=True)
    image_url = Column(Text, nullable=True)

    # Enriched fields from Funko.com
    category = Column(String(255), nullable=True, index=True)  # e.g., "Star Trek", "Marvel"
    license = Column(String(255), nullable=True, index=True)   # e.g., "Star Trek", "Disney"
    product_type = Column(String(100), nullable=True, index=True)  # e.g., "Pop!", "Pop! & Buddy"
    box_number = Column(String(50), nullable=True, index=True)  # e.g., "1755"
    funko_url = Column(Text, nullable=True)  # URL to Funko.com product page

    # BLOCK-002: PriceCharting integration
    pricecharting_id = Column(Integer, nullable=True, index=True)
    price_loose = Column(Numeric(12, 2), nullable=True)  # Out-of-box value
    price_cib = Column(Numeric(12, 2), nullable=True)    # Complete in box
    price_new = Column(Numeric(12, 2), nullable=True)    # Sealed/new

    # v1.7.0: Sales volume for demand correlation (AI/ML feature)
    sales_volume = Column(Integer, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationship
    series = relationship("FunkoSeriesName", secondary=funko_series, back_populates="funkos")
