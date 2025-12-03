"""
Comic Data Models - Local cache of Metron data
Stores ALL data points from Metron API for internal use
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, Boolean, ForeignKey, Table, Float, JSON
from sqlalchemy.orm import relationship
from app.core.database import Base


# Many-to-many association tables
issue_characters = Table(
    'issue_characters',
    Base.metadata,
    Column('issue_id', Integer, ForeignKey('comic_issues.id'), primary_key=True),
    Column('character_id', Integer, ForeignKey('comic_characters.id'), primary_key=True)
)

issue_creators = Table(
    'issue_creators',
    Base.metadata,
    Column('issue_id', Integer, ForeignKey('comic_issues.id'), primary_key=True),
    Column('creator_id', Integer, ForeignKey('comic_creators.id'), primary_key=True),
    Column('role', String(100))  # writer, artist, colorist, etc.
)

issue_arcs = Table(
    'issue_arcs',
    Base.metadata,
    Column('issue_id', Integer, ForeignKey('comic_issues.id'), primary_key=True),
    Column('arc_id', Integer, ForeignKey('comic_arcs.id'), primary_key=True)
)


class ComicPublisher(Base):
    """Comic book publishers - Marvel, DC, Image, etc."""
    __tablename__ = 'comic_publishers'

    id = Column(Integer, primary_key=True)
    metron_id = Column(Integer, unique=True, index=True)  # Metron's ID
    name = Column(String(255), nullable=False)
    founded = Column(Integer)  # Year founded
    image = Column(Text)  # Logo URL

    # Raw Metron response for future parsing
    raw_data = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    series = relationship("ComicSeries", back_populates="publisher")


class ComicSeries(Base):
    """Comic book series - Amazing Spider-Man, Batman, etc."""
    __tablename__ = 'comic_series'

    id = Column(Integer, primary_key=True)
    metron_id = Column(Integer, unique=True, index=True)
    name = Column(String(500), nullable=False, index=True)
    sort_name = Column(String(500))
    volume = Column(Integer)
    year_began = Column(Integer, index=True)
    year_ended = Column(Integer)
    issue_count = Column(Integer)
    image = Column(Text)  # Series cover/logo

    # Publisher relationship
    publisher_id = Column(Integer, ForeignKey('comic_publishers.id'))
    publisher = relationship("ComicPublisher", back_populates="series")

    # Description and notes
    description = Column(Text)

    # Series type (ongoing, limited, one-shot, etc.)
    series_type = Column(String(100))

    # Raw Metron response
    raw_data = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    issues = relationship("ComicIssue", back_populates="series")


class ComicIssue(Base):
    """Individual comic book issues - the main data we care about"""
    __tablename__ = 'comic_issues'

    id = Column(Integer, primary_key=True)
    metron_id = Column(Integer, unique=True, index=True)

    # Series relationship
    series_id = Column(Integer, ForeignKey('comic_series.id'))
    series = relationship("ComicSeries", back_populates="issues")

    # Issue details
    number = Column(String(50), index=True)  # Can be "1", "1A", "Annual 1", etc.
    issue_name = Column(String(500))  # Story title if any
    cover_date = Column(Date, index=True)
    store_date = Column(Date)  # Actual release date

    # Cover image - THE MONEY SHOT
    image = Column(Text)

    # Pricing and specs
    price = Column(Float)  # Cover price
    page_count = Column(Integer)
    upc = Column(String(50))
    sku = Column(String(50))
    isbn = Column(String(20))

    # Description/synopsis
    description = Column(Text)

    # Variant info
    is_variant = Column(Boolean, default=False)
    variant_of_id = Column(Integer, ForeignKey('comic_issues.id'))
    variant_name = Column(String(255))  # "Jim Lee Cover", "1:25 Variant", etc.

    # Ratings and maturity
    rating = Column(String(50))  # Teen, Mature, etc.

    # Raw Metron response - STORE EVERYTHING
    raw_data = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_fetched = Column(DateTime)  # When we last pulled from Metron

    # Relationships
    characters = relationship("ComicCharacter", secondary=issue_characters, back_populates="issues")
    creators = relationship("ComicCreator", secondary=issue_creators, back_populates="issues")
    arcs = relationship("ComicArc", secondary=issue_arcs, back_populates="issues")
    variants = relationship("ComicIssue", backref="variant_of", remote_side=[id])


class ComicCharacter(Base):
    """Characters appearing in comics"""
    __tablename__ = 'comic_characters'

    id = Column(Integer, primary_key=True)
    metron_id = Column(Integer, unique=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    alias = Column(String(255))  # Real name
    description = Column(Text)
    image = Column(Text)

    # Raw Metron response
    raw_data = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    issues = relationship("ComicIssue", secondary=issue_characters, back_populates="characters")


class ComicCreator(Base):
    """Writers, artists, colorists, etc."""
    __tablename__ = 'comic_creators'

    id = Column(Integer, primary_key=True)
    metron_id = Column(Integer, unique=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    birth_date = Column(Date)
    death_date = Column(Date)
    description = Column(Text)
    image = Column(Text)

    # Raw Metron response
    raw_data = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    issues = relationship("ComicIssue", secondary=issue_creators, back_populates="creators")


class ComicArc(Base):
    """Story arcs spanning multiple issues"""
    __tablename__ = 'comic_arcs'

    id = Column(Integer, primary_key=True)
    metron_id = Column(Integer, unique=True, index=True)
    name = Column(String(500), nullable=False, index=True)
    description = Column(Text)
    image = Column(Text)

    # Raw Metron response
    raw_data = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    issues = relationship("ComicIssue", secondary=issue_arcs, back_populates="arcs")


class MetronAPILog(Base):
    """Log all Metron API calls - for debugging and rate limiting"""
    __tablename__ = 'metron_api_logs'

    id = Column(Integer, primary_key=True)
    endpoint = Column(String(255), nullable=False)
    params = Column(JSON)
    response_code = Column(Integer)
    response_size = Column(Integer)  # Bytes
    duration_ms = Column(Integer)

    # Who triggered it
    user_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    ip_address = Column(String(50))

    # Timestamp
    created_at = Column(DateTime, default=datetime.utcnow)
