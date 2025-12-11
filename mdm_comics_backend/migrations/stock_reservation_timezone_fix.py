"""
Migration: Fix stock_reservations datetime columns to use timezone-aware types

Issue: The expires_at and created_at columns were TIMESTAMP WITHOUT TIME ZONE,
but the Python code uses timezone-aware datetime (datetime.now(timezone.utc)).
This causes "can't subtract offset-naive and offset-aware datetimes" errors.

Fix: Alter columns to TIMESTAMP WITH TIME ZONE.
"""
import asyncio
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from app.core.database import AsyncSessionLocal


async def migrate():
    """Apply timezone fix migration."""
    print("=" * 60)
    print("Migration: stock_reservations timezone fix")
    print("=" * 60)

    async with AsyncSessionLocal() as db:
        try:
            # Check if table exists
            result = await db.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'stock_reservations'
                )
            """))
            exists = result.scalar()

            if not exists:
                print("Table stock_reservations does not exist yet - skipping")
                return

            # Get current column types
            result = await db.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'stock_reservations'
                AND column_name IN ('expires_at', 'created_at')
            """))
            columns = result.fetchall()

            print("\nCurrent column types:")
            for col in columns:
                print(f"  {col[0]}: {col[1]}")

            # Alter columns to TIMESTAMP WITH TIME ZONE
            print("\nAltering columns to TIMESTAMP WITH TIME ZONE...")

            await db.execute(text("""
                ALTER TABLE stock_reservations
                ALTER COLUMN expires_at TYPE TIMESTAMP WITH TIME ZONE
                USING expires_at AT TIME ZONE 'UTC'
            """))
            print("  - expires_at: converted")

            await db.execute(text("""
                ALTER TABLE stock_reservations
                ALTER COLUMN created_at TYPE TIMESTAMP WITH TIME ZONE
                USING created_at AT TIME ZONE 'UTC'
            """))
            print("  - created_at: converted")

            await db.commit()

            # Verify
            result = await db.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'stock_reservations'
                AND column_name IN ('expires_at', 'created_at')
            """))
            columns = result.fetchall()

            print("\nNew column types:")
            for col in columns:
                print(f"  {col[0]}: {col[1]}")

            print("\n" + "=" * 60)
            print("Migration complete!")
            print("=" * 60)

        except Exception as e:
            print(f"\nError during migration: {e}")
            await db.rollback()
            raise


if __name__ == "__main__":
    asyncio.run(migrate())
