#!/usr/bin/env python3
"""
Migration: Add S3 image storage columns to comic_issues table

v1.9.5 - Image Acquisition System

Adds:
- cover_s3_key: S3 key for full-resolution cover image
- thumb_s3_key: S3 key for thumbnail image
- image_acquired_at: Timestamp when image was downloaded
- image_checksum: SHA-256 checksum of original image

Also adds indexes for efficient querying.

Usage:
    python scripts/migrate_image_columns.py
    python scripts/migrate_image_columns.py --dry-run
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.core.database import AsyncSessionLocal


MIGRATION_SQL = """
-- v1.9.5: Add S3 image storage columns
-- Safe to run multiple times (IF NOT EXISTS)

-- Add cover_s3_key column
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'comic_issues' AND column_name = 'cover_s3_key'
    ) THEN
        ALTER TABLE comic_issues ADD COLUMN cover_s3_key VARCHAR(255);
        CREATE INDEX IF NOT EXISTS idx_comic_issues_cover_s3_key ON comic_issues(cover_s3_key);
        RAISE NOTICE 'Added cover_s3_key column';
    ELSE
        RAISE NOTICE 'cover_s3_key column already exists';
    END IF;
END $$;

-- Add thumb_s3_key column
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'comic_issues' AND column_name = 'thumb_s3_key'
    ) THEN
        ALTER TABLE comic_issues ADD COLUMN thumb_s3_key VARCHAR(255);
        RAISE NOTICE 'Added thumb_s3_key column';
    ELSE
        RAISE NOTICE 'thumb_s3_key column already exists';
    END IF;
END $$;

-- Add image_acquired_at column
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'comic_issues' AND column_name = 'image_acquired_at'
    ) THEN
        ALTER TABLE comic_issues ADD COLUMN image_acquired_at TIMESTAMPTZ;
        RAISE NOTICE 'Added image_acquired_at column';
    ELSE
        RAISE NOTICE 'image_acquired_at column already exists';
    END IF;
END $$;

-- Add image_checksum column
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'comic_issues' AND column_name = 'image_checksum'
    ) THEN
        ALTER TABLE comic_issues ADD COLUMN image_checksum VARCHAR(64);
        RAISE NOTICE 'Added image_checksum column';
    ELSE
        RAISE NOTICE 'image_checksum column already exists';
    END IF;
END $$;

-- Add composite index for finding comics needing image acquisition
CREATE INDEX IF NOT EXISTS idx_comic_issues_needs_image
    ON comic_issues(id)
    WHERE image IS NOT NULL AND cover_s3_key IS NULL;
"""


async def run_migration(dry_run: bool = False):
    """Run the migration."""
    print("=" * 60)
    print("Migration: Add S3 image storage columns (v1.9.5)")
    print("=" * 60)

    if dry_run:
        print("\n[DRY RUN] Would execute:")
        print(MIGRATION_SQL)
        return

    async with AsyncSessionLocal() as db:
        try:
            # Check current state
            result = await db.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'comic_issues'
                AND column_name IN ('cover_s3_key', 'thumb_s3_key', 'image_acquired_at', 'image_checksum')
            """))
            existing = [row[0] for row in result.fetchall()]

            print(f"\nExisting columns: {existing or 'None'}")
            print(f"Columns to add: {set(['cover_s3_key', 'thumb_s3_key', 'image_acquired_at', 'image_checksum']) - set(existing)}")

            # Run migration
            print("\nRunning migration...")
            await db.execute(text(MIGRATION_SQL))
            await db.commit()

            # Verify
            result = await db.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'comic_issues'
                AND column_name IN ('cover_s3_key', 'thumb_s3_key', 'image_acquired_at', 'image_checksum')
            """))
            final = [row[0] for row in result.fetchall()]

            print(f"\nFinal columns: {final}")
            print("\nMigration completed successfully!")

        except Exception as e:
            print(f"\nMigration failed: {e}")
            raise


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    asyncio.run(run_migration(dry_run))
