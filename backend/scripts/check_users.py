"""Check users in database."""
import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

async def check_users():
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return

    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql+asyncpg://', 1)

    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        result = await conn.execute(text('SELECT id, email, is_admin, is_active FROM users ORDER BY id'))
        users = result.fetchall()
        print(f"\n=== Found {len(users)} users ===")
        for u in users:
            print(f"ID: {u[0]}, Email: {u[1]}, Admin: {u[2]}, Active: {u[3]}")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(check_users())
