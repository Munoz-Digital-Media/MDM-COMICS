"""Reset user password."""
import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
import bcrypt

async def reset_password():
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return

    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql+asyncpg://', 1)

    # New password
    new_password = "admin123"  # Simple test password
    hashed = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        # Update password for munozdigitalmedia@gmail.com
        await conn.execute(
            text("UPDATE users SET hashed_password = :pwd WHERE email = :email"),
            {"pwd": hashed, "email": "munozdigitalmedia@gmail.com"}
        )
        print(f"Password reset for munozdigitalmedia@gmail.com")
        print(f"New password: {new_password}")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(reset_password())
