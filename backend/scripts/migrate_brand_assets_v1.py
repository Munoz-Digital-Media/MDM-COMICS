"""
Database migration script for Brand Asset Management System v1.0.0
Adds site_settings, brand_assets, and brand_asset_versions tables.

Run via: railway run python scripts/migrate_brand_assets_v1.py
"""
import asyncio
import os
import sys

# Ensure app modules can be imported
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def migrate():
    """Run brand asset migrations."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not set")
        return False

    # Convert postgres:// to postgresql+asyncpg://
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif database_url.startswith("postgresql://") and "+asyncpg" not in database_url:
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    print(f"Connecting to database...")
    engine = create_async_engine(database_url, echo=False)

    async with engine.begin() as conn:
        # ============================================================
        # 1. Create brand_assets table
        # ============================================================
        print("\n=== Creating brand_assets table ===")
        try:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS brand_assets (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    slug VARCHAR(255) UNIQUE NOT NULL,
                    asset_type VARCHAR(50) NOT NULL,
                    current_version INTEGER DEFAULT 1,
                    url TEXT NOT NULL,
                    s3_key TEXT NOT NULL,
                    content_type VARCHAR(100) NOT NULL,
                    file_size INTEGER NOT NULL,
                    width INTEGER,
                    height INTEGER,
                    checksum VARCHAR(64),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    deleted_at TIMESTAMP WITH TIME ZONE
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_brand_assets_slug ON brand_assets(slug)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_brand_assets_type ON brand_assets(asset_type)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_brand_assets_active ON brand_assets(deleted_at) WHERE deleted_at IS NULL"))
            print("  + Created brand_assets table")
        except Exception as e:
            print(f"  ! Error creating brand_assets table: {e}")

        # ============================================================
        # 2. Create brand_asset_versions table
        # ============================================================
        print("\n=== Creating brand_asset_versions table ===")
        try:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS brand_asset_versions (
                    id SERIAL PRIMARY KEY,
                    asset_id INTEGER NOT NULL REFERENCES brand_assets(id) ON DELETE CASCADE,
                    version INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    s3_key TEXT NOT NULL,
                    content_type VARCHAR(100) NOT NULL,
                    file_size INTEGER NOT NULL,
                    width INTEGER,
                    height INTEGER,
                    checksum VARCHAR(64),
                    storage_class VARCHAR(50) DEFAULT 'STANDARD',
                    archived_at TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL
                )
            """))
            await conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_brand_asset_versions_lookup ON brand_asset_versions(asset_id, version)"))
            print("  + Created brand_asset_versions table")
        except Exception as e:
            print(f"  ! Error creating brand_asset_versions table: {e}")

        # ============================================================
        # 3. Create site_settings table
        # ============================================================
        print("\n=== Creating site_settings table ===")
        try:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS site_settings (
                    id SERIAL PRIMARY KEY,
                    key VARCHAR(100) UNIQUE NOT NULL,
                    value TEXT NOT NULL,
                    value_type VARCHAR(20) DEFAULT 'string',
                    category VARCHAR(50) DEFAULT 'general',
                    description TEXT,
                    brand_asset_id INTEGER REFERENCES brand_assets(id) ON DELETE SET NULL,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_site_settings_key ON site_settings(key)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_site_settings_category ON site_settings(category)"))
            print("  + Created site_settings table")
        except Exception as e:
            print(f"  ! Error creating site_settings table: {e}")

        # ============================================================
        # 4. Seed default site settings
        # ============================================================
        print("\n=== Seeding default site settings ===")
        default_settings = [
            ("rack_factor_logo_url", "", "url", "branding", "The Rack Factor newsletter logo"),
            ("site_logo_url", "", "url", "branding", "Main MDM Comics logo"),
            ("site_logo_dark_url", "", "url", "branding", "Logo for dark backgrounds"),
            ("favicon_url", "", "url", "branding", "Site favicon"),
            ("email_header_logo_url", "", "url", "branding", "Logo for transactional emails"),
            ("og_default_image_url", "", "url", "branding", "Default Open Graph image for social sharing"),
        ]

        for key, value, value_type, category, description in default_settings:
            try:
                await conn.execute(text("""
                    INSERT INTO site_settings (key, value, value_type, category, description)
                    VALUES (:key, :value, :value_type, :category, :description)
                    ON CONFLICT (key) DO NOTHING
                """), {
                    "key": key,
                    "value": value,
                    "value_type": value_type,
                    "category": category,
                    "description": description
                })
                print(f"  + Seeded setting: {key}")
            except Exception as e:
                print(f"  ! Error seeding {key}: {e}")

        print("\n=== Brand Asset Migration complete ===")

    await engine.dispose()
    return True


if __name__ == "__main__":
    success = asyncio.run(migrate())
    sys.exit(0 if success else 1)
