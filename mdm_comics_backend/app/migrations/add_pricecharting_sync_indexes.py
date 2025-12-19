"""
Migration: Add indexes on pricecharting_synced_at columns

Document ID: PC-ANALYSIS-2025-12-18
Priority: P0

Problem:
The incremental sync queries filter by pricecharting_synced_at:
    WHERE pricecharting_synced_at < NOW() - INTERVAL '24 hours'

Without an index, this causes sequential scans on large tables.

Solution:
Add partial indexes on pricecharting_synced_at for records that have
a pricecharting_id (the only ones that matter for sync).
"""
import asyncio
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker


async def run_migration():
    """Add indexes on pricecharting_synced_at columns"""

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        return False

    # Convert to async URL if needed
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    indexes_to_add = [
        # Funko index - partial index for records with pricecharting_id
        {
            "name": "idx_funkos_pricecharting_synced_at",
            "table": "funkos",
            "sql": """
                CREATE INDEX IF NOT EXISTS idx_funkos_pricecharting_synced_at
                ON funkos (pricecharting_synced_at)
                WHERE pricecharting_id IS NOT NULL
            """
        },
        # Comic issues index - partial index for records with pricecharting_id
        {
            "name": "idx_comic_issues_pricecharting_synced_at",
            "table": "comic_issues",
            "sql": """
                CREATE INDEX IF NOT EXISTS idx_comic_issues_pricecharting_synced_at
                ON comic_issues (pricecharting_synced_at)
                WHERE pricecharting_id IS NOT NULL
            """
        },
    ]

    async with async_session() as session:
        print("Adding pricecharting_synced_at indexes...")
        print("-" * 50)

        added_count = 0
        skipped_count = 0

        for idx in indexes_to_add:
            # Check if index already exists
            check_sql = text("""
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = :table AND indexname = :name
            """)
            result = await session.execute(check_sql, {
                "table": idx["table"],
                "name": idx["name"]
            })
            exists = result.fetchone()

            if exists:
                print(f"  {idx['name']}: already exists (SKIP)")
                skipped_count += 1
            else:
                # Create the index
                await session.execute(text(idx["sql"]))
                print(f"  {idx['name']}: created (OK)")
                added_count += 1

        await session.commit()

        print("-" * 50)
        print(f"Migration complete: {added_count} indexes added, {skipped_count} already existed")

    await engine.dispose()
    return True


if __name__ == "__main__":
    success = asyncio.run(run_migration())
    sys.exit(0 if success else 1)
