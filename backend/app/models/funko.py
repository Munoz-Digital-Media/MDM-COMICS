"""
Funko POP database models
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Table, ForeignKey
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

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationship
    series = relationship("FunkoSeriesName", secondary=funko_series, back_populates="funkos")
