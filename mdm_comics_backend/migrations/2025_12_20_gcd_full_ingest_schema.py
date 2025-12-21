"""
Migration: GCD Full Ingest Schema Expansion (IMP-20251220-GCD-FULL-INGEST)

Adds missing tables and columns to support full GCD data ingestion:
- comic_stories
- comic_brands
- comic_indicia_publishers
- story_characters (M2M)
- story_creators (M2M)
- Expanded fields for publishers, series, issues, creators, and characters.
"""
import asyncio
import logging
import os
import sys

from sqlalchemy import text

# Ensure app modules are importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_migration():
    async with AsyncSessionLocal() as db:
        try:
            logger.info("Starting GCD Full Ingest Schema Expansion...")

            # 1. Create comic_indicia_publishers
            logger.info("Creating comic_indicia_publishers table")
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS comic_indicia_publishers (
                    id SERIAL PRIMARY KEY,
                    gcd_id INTEGER UNIQUE,
                    name VARCHAR(255) NOT NULL,
                    publisher_id INTEGER REFERENCES comic_publishers(id),
                    year_began INTEGER,
                    year_ended INTEGER,
                    is_surrogate BOOLEAN DEFAULT FALSE,
                    notes TEXT,
                    url VARCHAR(255)
                )
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_comic_indicia_publishers_gcd_id ON comic_indicia_publishers(gcd_id)"))

            # 2. Create comic_brands
            logger.info("Creating comic_brands table")
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS comic_brands (
                    id SERIAL PRIMARY KEY,
                    gcd_id INTEGER UNIQUE,
                    name VARCHAR(255) NOT NULL,
                    publisher_id INTEGER REFERENCES comic_publishers(id),
                    year_began INTEGER,
                    year_ended INTEGER,
                    notes TEXT,
                    url VARCHAR(255)
                )
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_comic_brands_gcd_id ON comic_brands(gcd_id)"))

            # 3. Update comic_publishers
            logger.info("Updating comic_publishers table")
            await db.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_publishers' AND column_name='gcd_id') THEN
                        ALTER TABLE comic_publishers ADD COLUMN gcd_id INTEGER UNIQUE;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_publishers' AND column_name='year_ended') THEN
                        ALTER TABLE comic_publishers ADD COLUMN year_ended INTEGER;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_publishers' AND column_name='url') THEN
                        ALTER TABLE comic_publishers ADD COLUMN url VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_publishers' AND column_name='notes') THEN
                        ALTER TABLE comic_publishers ADD COLUMN notes TEXT;
                    END IF;
                END $$;
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_comic_publishers_gcd_id ON comic_publishers(gcd_id)"))

            # 4. Update comic_series
            logger.info("Updating comic_series table")
            await db.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_series' AND column_name='gcd_id') THEN
                        ALTER TABLE comic_series ADD COLUMN gcd_id INTEGER UNIQUE;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_series' AND column_name='format') THEN
                        ALTER TABLE comic_series ADD COLUMN format VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_series' AND column_name='publication_dates') THEN
                        ALTER TABLE comic_series ADD COLUMN publication_dates VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_series' AND column_name='color') THEN
                        ALTER TABLE comic_series ADD COLUMN color VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_series' AND column_name='dimensions') THEN
                        ALTER TABLE comic_series ADD COLUMN dimensions VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_series' AND column_name='paper_stock') THEN
                        ALTER TABLE comic_series ADD COLUMN paper_stock VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_series' AND column_name='binding') THEN
                        ALTER TABLE comic_series ADD COLUMN binding VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_series' AND column_name='publishing_format') THEN
                        ALTER TABLE comic_series ADD COLUMN publishing_format VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_series' AND column_name='is_current') THEN
                        ALTER TABLE comic_series ADD COLUMN is_current BOOLEAN DEFAULT FALSE;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_series' AND column_name='notes') THEN
                        ALTER TABLE comic_series ADD COLUMN notes TEXT;
                    END IF;
                END $$;
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_comic_series_gcd_id ON comic_series(gcd_id)"))

            # 5. Update comic_issues
            logger.info("Updating comic_issues table")
            await db.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_issues' AND column_name='brand_id') THEN
                        ALTER TABLE comic_issues ADD COLUMN brand_id INTEGER REFERENCES comic_brands(id);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_issues' AND column_name='indicia_publisher_id') THEN
                        ALTER TABLE comic_issues ADD COLUMN indicia_publisher_id INTEGER REFERENCES comic_indicia_publishers(id);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_issues' AND column_name='publication_date') THEN
                        ALTER TABLE comic_issues ADD COLUMN publication_date VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_issues' AND column_name='key_date') THEN
                        ALTER TABLE comic_issues ADD COLUMN key_date VARCHAR(10);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_issues' AND column_name='gcd_price') THEN
                        ALTER TABLE comic_issues ADD COLUMN gcd_price VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_issues' AND column_name='variant_cover_status') THEN
                        ALTER TABLE comic_issues ADD COLUMN variant_cover_status INTEGER;
                    END IF;
                END $$;
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_comic_issues_key_date ON comic_issues(key_date)"))

            # 6. Create comic_stories
            logger.info("Creating comic_stories table")
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS comic_stories (
                    id SERIAL PRIMARY KEY,
                    gcd_id INTEGER UNIQUE,
                    issue_id INTEGER REFERENCES comic_issues(id) ON DELETE CASCADE,
                    title VARCHAR(500),
                    feature VARCHAR(500),
                    sequence_number INTEGER,
                    page_count NUMERIC(10, 3),
                    script TEXT,
                    pencils TEXT,
                    inks TEXT,
                    colors TEXT,
                    letters TEXT,
                    editing TEXT,
                    genre VARCHAR(255),
                    synopsis TEXT,
                    reprint_notes TEXT,
                    notes TEXT,
                    type_name VARCHAR(100),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_comic_stories_gcd_id ON comic_stories(gcd_id)"))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_comic_stories_issue_id ON comic_stories(issue_id)"))

            # 7. Update comic_characters
            logger.info("Updating comic_characters table")
            await db.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_characters' AND column_name='gcd_id') THEN
                        ALTER TABLE comic_characters ADD COLUMN gcd_id INTEGER UNIQUE;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_characters' AND column_name='year_first_published') THEN
                        ALTER TABLE comic_characters ADD COLUMN year_first_published INTEGER;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_characters' AND column_name='universe_id') THEN
                        ALTER TABLE comic_characters ADD COLUMN universe_id INTEGER;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_characters' AND column_name='notes') THEN
                        ALTER TABLE comic_characters ADD COLUMN notes TEXT;
                    END IF;
                END $$;
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_comic_characters_gcd_id ON comic_characters(gcd_id)"))

            # 8. Update comic_creators
            logger.info("Updating comic_creators table")
            await db.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_creators' AND column_name='gcd_id') THEN
                        ALTER TABLE comic_creators ADD COLUMN gcd_id INTEGER UNIQUE;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_creators' AND column_name='official_name') THEN
                        ALTER TABLE comic_creators ADD COLUMN official_name VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_creators' AND column_name='birth_country') THEN
                        ALTER TABLE comic_creators ADD COLUMN birth_country VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_creators' AND column_name='death_country') THEN
                        ALTER TABLE comic_creators ADD COLUMN death_country VARCHAR(255);
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_creators' AND column_name='notes') THEN
                        ALTER TABLE comic_creators ADD COLUMN notes TEXT;
                    END IF;
                END $$;
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_comic_creators_gcd_id ON comic_creators(gcd_id)"))

            # 9. Create story_characters M2M
            logger.info("Creating story_characters junction table")
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS story_characters (
                    story_id INTEGER REFERENCES comic_stories(id) ON DELETE CASCADE,
                    character_id INTEGER REFERENCES comic_characters(id) ON DELETE CASCADE,
                    is_origin BOOLEAN DEFAULT FALSE,
                    is_death BOOLEAN DEFAULT FALSE,
                    is_flashback BOOLEAN DEFAULT FALSE,
                    PRIMARY KEY (story_id, character_id)
                )
            """))

            # 10. Create story_creators M2M
            logger.info("Creating story_creators junction table")
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS story_creators (
                    story_id INTEGER REFERENCES comic_stories(id) ON DELETE CASCADE,
                    creator_id INTEGER REFERENCES comic_creators(id) ON DELETE CASCADE,
                    role VARCHAR(100),
                    credited_as VARCHAR(255),
                    PRIMARY KEY (story_id, creator_id, role)
                )
            """))

            await db.commit()
            logger.info("Migration successful!")

        except Exception as e:
            await db.rollback()
            logger.error(f"Migration failed: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(run_migration())
