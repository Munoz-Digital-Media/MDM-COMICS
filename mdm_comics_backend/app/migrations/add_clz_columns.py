"""
Migration: Add CLZ (Comic Collector) columns to comic_issues table.

These columns support importing data from CLZ Comic Collector exports,
including creator credits, key issue info, and storage location.
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

    columns = [
        # CLZ metadata
        ("genre", "VARCHAR(255)"),
        ("storage_box", "VARCHAR(255)"),
        ("story_arc", "VARCHAR(500)"),
        ("subtitle", "VARCHAR(500)"),
        ("is_key_issue", "BOOLEAN DEFAULT FALSE"),
        ("key_category", "VARCHAR(255)"),
        ("key_reason", "TEXT"),
        # Creator credits
        ("clz_artist", "TEXT"),
        ("clz_characters", "TEXT"),
        ("colorist", "VARCHAR(500)"),
        ("cover_artist", "VARCHAR(500)"),
        ("cover_colorist", "VARCHAR(500)"),
        ("cover_inker", "VARCHAR(500)"),
        ("cover_painter", "VARCHAR(500)"),
        ("cover_penciller", "VARCHAR(500)"),
        ("cover_separator", "VARCHAR(500)"),
        ("clz_creators", "TEXT"),
        ("editor", "VARCHAR(500)"),
        ("editor_in_chief", "VARCHAR(500)"),
        ("inker", "VARCHAR(500)"),
        ("layouts", "VARCHAR(500)"),
        ("letterer", "VARCHAR(500)"),
        ("painter", "VARCHAR(500)"),
        ("penciller", "VARCHAR(500)"),
        ("plotter", "VARCHAR(500)"),
        ("scripter", "VARCHAR(500)"),
        ("separator", "VARCHAR(500)"),
        ("translator", "VARCHAR(500)"),
        ("writer", "VARCHAR(500)"),
    ]

    async with engine.begin() as conn:
        for col_name, col_type in columns:
            try:
                await conn.execute(
                    text(f"ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                )
                print(f"  Added/verified column: {col_name}")
            except Exception as e:
                print(f"  Error with {col_name}: {e}")

    await engine.dispose()
    print("CLZ columns migration complete!")


def main():
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
