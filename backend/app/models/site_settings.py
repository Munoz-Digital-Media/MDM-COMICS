"""
Site Settings and Brand Assets Models

v1.0.0: Brand asset management for newsletters, social media, and site branding.
Stores brand assets (logos, banners) in S3, with metadata and versioning in DB.

Note: Product/comic images remain as external URL hotlinks (Metron, Funko, PriceCharting).
This is ONLY for MDM Comics brand assets.
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, DateTime, ForeignKey,
    Index, Boolean
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class SiteSettings(Base):
    """
    Key-value store for site configuration.

    Used for branding URLs, feature flags, and other site-wide settings.
    """
    __tablename__ = "site_settings"

    id = Column(Integer, primary_key=True, index=True)

    # Key-value
    key = Column(String(100), unique=True, nullable=False, index=True)
    value = Column(Text, nullable=False)
    value_type = Column(String(20), default="string")  # string, url, json, boolean, number

    # Organization
    category = Column(String(50), default="general", index=True)  # branding, newsletter, social, system
    description = Column(Text)

    # Optional link to brand asset
    brand_asset_id = Column(Integer, ForeignKey("brand_assets.id"), nullable=True)

    # Audit
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    # Relationships
    brand_asset = relationship("BrandAsset", back_populates="site_settings")


class BrandAsset(Base):
    """
    Brand assets stored in S3 (logos, banners, icons).

    Supports versioning - old versions archived but retained for 7 years.
    """
    __tablename__ = "brand_assets"

    id = Column(Integer, primary_key=True, index=True)

    # Identity
    name = Column(String(255), nullable=False)  # "The Rack Factor Logo"
    slug = Column(String(255), unique=True, nullable=False, index=True)  # "rack-factor-logo"

    # Classification
    asset_type = Column(String(50), nullable=False, index=True)  # logo, banner, icon, favicon, social

    # Current version info (denormalized for fast access)
    current_version = Column(Integer, default=1)
    url = Column(Text, nullable=False)  # S3 public URL
    s3_key = Column(Text, nullable=False)  # S3 object key

    # File metadata
    content_type = Column(String(100), nullable=False)  # image/png
    file_size = Column(Integer, nullable=False)  # bytes
    width = Column(Integer)  # pixels
    height = Column(Integer)  # pixels
    checksum = Column(String(64))  # MD5 hash

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    # Soft delete
    deleted_at = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    versions = relationship("BrandAssetVersion", back_populates="asset", order_by="desc(BrandAssetVersion.version)")
    site_settings = relationship("SiteSettings", back_populates="brand_asset")

    __table_args__ = (
        Index("idx_brand_assets_active", "deleted_at", postgresql_where=(deleted_at.is_(None))),
    )


class BrandAssetVersion(Base):
    """
    Version history for brand assets.

    When a new version is uploaded, the old version is archived here.
    Retained for 7 years (warm storage), then cold storage.
    """
    __tablename__ = "brand_asset_versions"

    id = Column(Integer, primary_key=True, index=True)
    asset_id = Column(Integer, ForeignKey("brand_assets.id", ondelete="CASCADE"), nullable=False)

    # Version info
    version = Column(Integer, nullable=False)

    # S3 location (may be archived path)
    url = Column(Text, nullable=False)
    s3_key = Column(Text, nullable=False)

    # File metadata at time of upload
    content_type = Column(String(100), nullable=False)
    file_size = Column(Integer, nullable=False)
    width = Column(Integer)
    height = Column(Integer)
    checksum = Column(String(64))

    # Storage lifecycle
    storage_class = Column(String(50), default="STANDARD")  # STANDARD, STANDARD_IA, GLACIER
    archived_at = Column(DateTime(timezone=True))  # When moved to archive folder

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)

    # Relationships
    asset = relationship("BrandAsset", back_populates="versions")

    __table_args__ = (
        Index("idx_brand_asset_versions_lookup", "asset_id", "version", unique=True),
    )


# Default site settings to seed
DEFAULT_SITE_SETTINGS = [
    {
        "key": "rack_factor_logo_url",
        "value": "",
        "value_type": "url",
        "category": "branding",
        "description": "The Rack Factor newsletter logo",
    },
    {
        "key": "site_logo_url",
        "value": "",
        "value_type": "url",
        "category": "branding",
        "description": "Main MDM Comics logo",
    },
    {
        "key": "site_logo_dark_url",
        "value": "",
        "value_type": "url",
        "category": "branding",
        "description": "Logo for dark backgrounds",
    },
    {
        "key": "favicon_url",
        "value": "",
        "value_type": "url",
        "category": "branding",
        "description": "Site favicon",
    },
    {
        "key": "email_header_logo_url",
        "value": "",
        "value_type": "url",
        "category": "branding",
        "description": "Logo for transactional emails",
    },
    {
        "key": "og_default_image_url",
        "value": "",
        "value_type": "url",
        "category": "branding",
        "description": "Default Open Graph image for social sharing",
    },
]
