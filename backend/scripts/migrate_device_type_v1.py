"""Migrate device_type column from VARCHAR(50) to VARCHAR(150).

This fixes the issue where user-agent strings (125+ chars) were truncated
to 100 chars in code but the column only allowed 50.
"""
import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def migrate():
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return False

    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql+asyncpg://', 1)

    engine = create_async_engine(db_url)

    try:
        async with engine.begin() as conn:
            # Check current column size
            result = await conn.execute(text("""
                SELECT character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'user_sessions'
                AND column_name = 'device_type'
            """))
            row = result.fetchone()

            if row:
                current_size = row[0]
                print(f"Current device_type column size: {current_size}")

                if current_size and current_size >= 150:
                    print("Column already at 150+ chars, no migration needed")
                    return True
            else:
                print("Column not found - table may not exist yet")
                return True

            # Alter column to VARCHAR(150)
            print("Altering device_type column to VARCHAR(150)...")
            await conn.execute(text("""
                ALTER TABLE user_sessions
                ALTER COLUMN device_type TYPE VARCHAR(150)
            """))
            print("Migration complete!")
            return True

    except Exception as e:
        print(f"Migration error: {e}")
        return False
    finally:
        await engine.dispose()


if __name__ == "__main__":
    success = asyncio.run(migrate())
    sys.exit(0 if success else 1)
