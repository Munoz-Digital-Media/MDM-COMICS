"""
Migration: GCD Full Ingest Schema Expansion v3 (IMP-20251220-GCD-FULL-INGEST)

Non-transactional robust migration to avoid long-held locks and handle timeouts.
"""
import asyncio
import logging
import os
import sys

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Ensure app modules are importable
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from app.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_migration():
    engine = create_async_engine(str(settings.DATABASE_URL))
    
    steps = [
        ("comic_indicia_publishers", """
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
        """),
        ("ix_comic_indicia_publishers_gcd_id", "CREATE INDEX IF NOT EXISTS ix_comic_indicia_publishers_gcd_id ON comic_indicia_publishers(gcd_id)"),
        ("comic_brands", """
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
        """),
        ("ix_comic_brands_gcd_id", "CREATE INDEX IF NOT EXISTS ix_comic_brands_gcd_id ON comic_brands(gcd_id)"),
        ("update_comic_publishers", """
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
        """),
        ("ix_comic_publishers_gcd_id", "CREATE INDEX IF NOT EXISTS ix_comic_publishers_gcd_id ON comic_publishers(gcd_id)"),
        ("update_comic_series", """
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
        """),
        ("ix_comic_series_gcd_id", "CREATE INDEX IF NOT EXISTS ix_comic_series_gcd_id ON comic_series(gcd_id)"),
        ("update_comic_issues", """
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
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='comic_issues' AND column_name='gcd_variant_of_id') THEN
                    ALTER TABLE comic_issues ADD COLUMN gcd_variant_of_id INTEGER;
                END IF;
            END $$;
        """),
        ("ix_comic_issues_gcd_variant_of_id", "CREATE INDEX IF NOT EXISTS ix_comic_issues_gcd_variant_of_id ON comic_issues(gcd_variant_of_id)"),
        ("ix_comic_issues_key_date", "CREATE INDEX IF NOT EXISTS ix_comic_issues_key_date ON comic_issues(key_date)"),
        ("update_stories", """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='feature') THEN
                    ALTER TABLE stories ADD COLUMN feature VARCHAR(500);
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='script') THEN
                    ALTER TABLE stories ADD COLUMN script TEXT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='pencils') THEN
                    ALTER TABLE stories ADD COLUMN pencils TEXT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='inks') THEN
                    ALTER TABLE stories ADD COLUMN inks TEXT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='colors') THEN
                    ALTER TABLE stories ADD COLUMN colors TEXT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='letters') THEN
                    ALTER TABLE stories ADD COLUMN letters TEXT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='editing') THEN
                    ALTER TABLE stories ADD COLUMN editing TEXT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='genre') THEN
                    ALTER TABLE stories ADD COLUMN genre VARCHAR(255);
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='reprint_notes') THEN
                    ALTER TABLE stories ADD COLUMN reprint_notes TEXT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stories' AND column_name='notes') THEN
                    ALTER TABLE stories ADD COLUMN notes TEXT;
                END IF;
            END $$;
        """),
        ("ix_stories_gcd_story_id", "CREATE INDEX IF NOT EXISTS ix_stories_gcd_story_id ON stories(gcd_story_id)"),
        ("ix_stories_comic_issue_id", "CREATE INDEX IF NOT EXISTS ix_stories_comic_issue_id ON stories(comic_issue_id)"),
        ("update_comic_characters", """
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
        """),
        ("ix_comic_characters_gcd_id", "CREATE INDEX IF NOT EXISTS ix_comic_characters_gcd_id ON comic_characters(gcd_id)"),
        ("update_comic_creators", """
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
        """),
        ("ix_comic_creators_gcd_id", "CREATE INDEX IF NOT EXISTS ix_comic_creators_gcd_id ON comic_creators(gcd_id)"),
        ("update_story_characters", """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='story_characters' AND column_name='is_origin') THEN
                    ALTER TABLE story_characters ADD COLUMN is_origin BOOLEAN DEFAULT FALSE;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='story_characters' AND column_name='is_death') THEN
                    ALTER TABLE story_characters ADD COLUMN is_death BOOLEAN DEFAULT FALSE;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='story_characters' AND column_name='is_flashback') THEN
                    ALTER TABLE story_characters ADD COLUMN is_flashback BOOLEAN DEFAULT FALSE;
                END IF;
            END $$;
        """),
        ("update_story_creators", """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='story_creators' AND column_name='credited_as') THEN
                    ALTER TABLE story_creators ADD COLUMN credited_as VARCHAR(255);
                END IF;
            END $$;
        """),
        ("comic_reprints", """
            CREATE TABLE IF NOT EXISTS comic_reprints (
                id SERIAL PRIMARY KEY,
                gcd_id INTEGER UNIQUE,
                origin_story_id INTEGER REFERENCES stories(id) ON DELETE CASCADE,
                target_story_id INTEGER REFERENCES stories(id) ON DELETE CASCADE,
                notes TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """),
        ("ix_comic_reprints_gcd_id", "CREATE INDEX IF NOT EXISTS ix_comic_reprints_gcd_id ON comic_reprints(gcd_id)"),
        ("ix_comic_reprints_origin_story_id", "CREATE INDEX IF NOT EXISTS ix_comic_reprints_origin_story_id ON comic_reprints(origin_story_id)"),
        ("ix_comic_reprints_target_story_id", "CREATE INDEX IF NOT EXISTS ix_comic_reprints_target_story_id ON comic_reprints(target_story_id)")
    ]

    for name, sql in steps:
        logger.info(f"Processing step: {name}")
        async with engine.connect() as conn:
            try:
                await conn.execute(text(sql))
                await conn.commit()
                logger.info(f"Step {name} SUCCESSFUL")
            except Exception as e:
                await conn.rollback()
                logger.error(f"Step {name} FAILED: {e}")

    await engine.dispose()
    logger.info("Migration complete.")


if __name__ == "__main__":
    asyncio.run(run_migration())
