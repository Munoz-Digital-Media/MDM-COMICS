"""
Migration: Add case_quantity, case_weight, case_dimensions columns to products table.

These columns support bulk/case ordering for supplies.
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
    """Add missing case columns to products table."""
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        print("DATABASE_URL not set, skipping migration")
        return

    engine = create_async_engine(database_url)

    columns = [
        ("case_quantity", "INTEGER"),
        ("case_weight", "NUMERIC(10,2)"),
        ("case_dimensions", "VARCHAR(100)"),
    ]

    async with engine.begin() as conn:
        for col_name, col_type in columns:
            try:
                await conn.execute(
                    text(f"ALTER TABLE products ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                )
                print(f"  Added/verified column: {col_name}")
            except Exception as e:
                print(f"  Error with {col_name}: {e}")

    await engine.dispose()
    print("Product case columns migration complete!")


def main():
    asyncio.run(migrate())


if __name__ == "__main__":
    main()
