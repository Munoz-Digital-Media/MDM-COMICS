"""
Migration: Add CLZ (Comic Collector) columns to comic_issues table.

These columns support importing data from CLZ Comic Collector exports,
including creator credits, key issue info, and storage location.

All string columns use TEXT for unlimited length.
"""
import asyncio
import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


async def migrate():
    """Add CLZ columns to comic_issues table."""
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        print("DATABASE_URL not set, skipping migration")
        return

    engine = create_async_engine(database_url)

    # All columns - using TEXT for unlimited length
    columns = [
        # CLZ metadata
        ("genre", "TEXT"),
        ("storage_box", "TEXT"),
        ("story_arc", "TEXT"),
        ("subtitle", "TEXT"),
        ("is_key_issue", "BOOLEAN DEFAULT FALSE"),
        ("key_category", "TEXT"),
        ("key_reason", "TEXT"),
        # Creator credits - all TEXT
        ("clz_artist", "TEXT"),
        ("clz_characters", "TEXT"),
        ("colorist", "TEXT"),
        ("cover_artist", "TEXT"),
        ("cover_colorist", "TEXT"),
        ("cover_inker", "TEXT"),
        ("cover_painter", "TEXT"),
        ("cover_penciller", "TEXT"),
        ("cover_separator", "TEXT"),
        ("clz_creators", "TEXT"),
        ("editor", "TEXT"),
        ("editor_in_chief", "TEXT"),
        ("inker", "TEXT"),
        ("layouts", "TEXT"),
        ("letterer", "TEXT"),
        ("painter", "TEXT"),
        ("penciller", "TEXT"),
        ("plotter", "TEXT"),
        ("scripter", "TEXT"),
        ("separator", "TEXT"),
        ("translator", "TEXT"),
        ("writer", "TEXT"),
        # Raw CLZ data as JSON
        ("clz_raw_data", "JSONB"),
        # Back cover S3 storage (v2.0.0)
        ("back_cover_s3_key", "VARCHAR(255)"),
    ]

    async with engine.begin() as conn:
        for col_name, col_type in columns:
            try:
                # Add column if not exists
                await conn.execute(
                    text(f"ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                )
                print(f"  Added/verified column: {col_name}")
            except Exception as e:
                print(f"  Error with {col_name}: {e}")

        # Alter existing VARCHAR columns to TEXT (in case they were created with smaller size)
        varchar_to_text = [
            "genre", "storage_box", "story_arc", "subtitle", "key_category",
            "colorist", "cover_artist", "cover_colorist", "cover_inker",
            "cover_painter", "cover_penciller", "cover_separator", "editor",
            "editor_in_chief", "inker", "layouts", "letterer", "painter",
            "penciller", "plotter", "scripter", "separator", "translator", "writer"
        ]

        for col_name in varchar_to_text:
            try:
                await conn.execute(
                    text(f"ALTER TABLE comic_issues ALTER COLUMN {col_name} TYPE TEXT")
                )
            except Exception:
                pass  # Column might already be TEXT or not exist

    await engine.dispose()
    print("CLZ columns migration complete!")


def main():
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
