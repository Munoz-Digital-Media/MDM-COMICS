"""
Comic Data Models - Local cache of Metron data
Stores ALL data points from Metron API for internal use

v1.5.0: Added PriceCharting integration fields to ComicIssue
        (resolves RISK-005 schema drift from pipeline spec)
"""
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, Boolean, ForeignKey, Table, Float, JSON, Numeric, LargeBinary
from sqlalchemy.orm import relationship
from app.core.database import Base
from app.core.utils import utcnow


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
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

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
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    # Relationships
    issues = relationship("ComicIssue", back_populates="series")


class ComicIssue(Base):
    """Individual comic book issues - the main data we care about
    
    v1.6.0: Added GCD (Grand Comics Database) integration fields
    - gcd_id: Unique GCD issue identifier
    - gcd_series_id: GCD series foreign key
    - gcd_publisher_id: GCD publisher foreign key
    """
    __tablename__ = 'comic_issues'

    id = Column(Integer, primary_key=True)
    metron_id = Column(Integer, unique=True, index=True)
    
    # GCD Integration (v1.6.0 - Grand Comics Database)
    # CC-BY-SA 4.0 licensed bibliographic data - NO IMAGES
    gcd_id = Column(Integer, unique=True, index=True, nullable=True)
    gcd_series_id = Column(Integer, index=True, nullable=True)
    gcd_publisher_id = Column(Integer, index=True, nullable=True)

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
    isbn = Column(String(50))  # Extended from 20 for GCD ISBNs with hyphens

    # Description/synopsis
    description = Column(Text)

    # Variant info
    is_variant = Column(Boolean, default=False)
    variant_of_id = Column(Integer, ForeignKey('comic_issues.id'))
    variant_name = Column(String(255))  # "Jim Lee Cover", "1:25 Variant", etc.

    # Ratings and maturity
    rating = Column(String(50))  # Teen, Mature, etc.

    # Perceptual hash of cover image for image search (BE-003 optimization)
    # 64-bit pHash stored as hex string - indexed for efficient lookup
    cover_hash = Column(String(16), index=True)
    cover_hash_prefix = Column(String(8), index=True)
    cover_hash_bytes = Column(LargeBinary(8))

    # -------------------------------------------------------------------------
    # S3 Image Storage (v1.9.5 - Image Acquisition System)
    # Own the images, don't depend on external URLs
    # -------------------------------------------------------------------------
    cover_s3_key = Column(String(255), index=True)    # S3 key: covers/{id}.jpg
    thumb_s3_key = Column(String(255))                 # S3 key: thumbs/{id}_sm.jpg
    image_acquired_at = Column(DateTime)               # When image was downloaded
    image_checksum = Column(String(64))                # SHA-256 of original image

    # -------------------------------------------------------------------------
    # PriceCharting Integration (v1.5.0 - resolves RISK-005 schema drift)
    # These fields are synced by import_pricecharting_comics.py and price_sync_daily.py
    # -------------------------------------------------------------------------
    pricecharting_id = Column(Integer, unique=True, index=True)
    price_loose = Column(Numeric(12, 2))      # Ungraded/raw value
    price_cib = Column(Numeric(12, 2))        # Complete in box
    price_new = Column(Numeric(12, 2))        # Sealed/new
    price_graded = Column(Numeric(12, 2))     # Generic graded value
    price_bgs_10 = Column(Numeric(12, 2))     # BGS 10 (pristine)
    price_cgc_98 = Column(Numeric(12, 2))     # CGC 9.8
    price_cgc_96 = Column(Numeric(12, 2))     # CGC 9.6
    asin = Column(String(20))                 # Amazon ASIN
    sales_volume = Column(Integer)            # PriceCharting sales volume
    handle = Column(String(255))              # URL-friendly identifier
    year = Column(Integer)                    # Publication year
    series_name = Column(String(255))         # Extracted series name

    # Raw Metron response - STORE EVERYTHING
    raw_data = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)
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
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

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
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

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
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

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
    created_at = Column(DateTime, default=utcnow)
