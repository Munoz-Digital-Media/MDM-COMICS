"""
Migration: Add weight and material columns to products table

Adds physical property fields:
- weight: VARCHAR(50) for weight with units (e.g., "2.5 lbs")
- material: VARCHAR(100) for material description (e.g., "Polypropylene")
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
    """Add weight and material columns to products table"""

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        return False

    # Convert to async URL if needed
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    columns_to_add = [
        ("weight", "VARCHAR(50)"),
        ("material", "VARCHAR(100)"),
    ]

    async with async_session() as session:
        print("Adding weight and material columns to products table...")
        print("-" * 50)

        added_count = 0
        skipped_count = 0

        for col_name, col_type in columns_to_add:
            # Check if column already exists
            check_sql = text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'products' AND column_name = :col_name
            """)
            result = await session.execute(check_sql, {"col_name": col_name})
            exists = result.fetchone()

            if exists:
                print(f"  {col_name}: already exists (SKIP)")
                skipped_count += 1
            else:
                # Add the column
                alter_sql = text(f"ALTER TABLE products ADD COLUMN {col_name} {col_type}")
                await session.execute(alter_sql)
                print(f"  {col_name}: added (OK)")
                added_count += 1

        await session.commit()

        print("-" * 50)
        print(f"Migration complete: {added_count} columns added, {skipped_count} already existed")

    await engine.dispose()
    return True


if __name__ == "__main__":
    success = asyncio.run(run_migration())
    sys.exit(0 if success else 1)
