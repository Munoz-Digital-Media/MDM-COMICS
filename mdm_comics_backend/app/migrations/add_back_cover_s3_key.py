"""
Migration: Add back_cover_s3_key column to comic_issues table.

Stores the S3 key for back cover images: covers/{id}/back.jpg
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
    """Add back_cover_s3_key column to comic_issues table."""
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        print("DATABASE_URL not set, skipping migration")
        return

    engine = create_async_engine(database_url)

    async with engine.begin() as conn:
        try:
            await conn.execute(
                text("ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS back_cover_s3_key VARCHAR(255)")
            )
            print("  Added column: back_cover_s3_key")
        except Exception as e:
            print(f"  Error adding back_cover_s3_key: {e}")

    await engine.dispose()
    print("Back cover S3 key migration complete!")


def main():
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
