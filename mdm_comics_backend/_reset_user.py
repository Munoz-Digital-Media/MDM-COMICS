import asyncio
from sqlalchemy import text
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

async def main():
    from app.core.database import AsyncSessionLocal
    
    email = "munozdigitalmedia@gmail.com"
    new_password = "TempPass123!"  # You'll change this after login
    
    async with AsyncSessionLocal() as db:
        # Check if user exists
        result = await db.execute(text(
            "SELECT id, email, failed_login_count, locked_until FROM users WHERE email = :email"
        ), {"email": email})
        user = result.fetchone()
        
        if not user:
            print(f"User {email} not found!")
            return
            
        print(f"Found user: id={user[0]}, email={user[1]}")
        print(f"Failed attempts: {user[2]}, locked_until: {user[3]}")
        
        # Reset password and clear lockout
        hashed = pwd_context.hash(new_password)
        await db.execute(text("""
            UPDATE users 
            SET hashed_password = :pwd,
                failed_login_count = 0,
                locked_until = NULL
            WHERE email = :email
        """), {"email": email, "pwd": hashed})
        await db.commit()
        
        print(f"Password reset to: {new_password}")
        print("Lockout cleared. You can login now.")

asyncio.run(main())
