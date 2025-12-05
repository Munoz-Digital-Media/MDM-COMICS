"""
Security utilities - password hashing, JWT tokens

P2-6: Uses timezone-aware datetime (datetime.now(timezone.utc)) instead of deprecated utcnow()
P2-8: Added JTI (JWT ID) for token revocation support
"""
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
from jose import JWTError, jwt

from app.core.config import settings


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    return bcrypt.checkpw(
        plain_password.encode("utf-8"),
        hashed_password.encode("utf-8")
    )


def get_password_hash(password: str) -> str:
    """Generate password hash"""
    return bcrypt.hashpw(
        password.encode("utf-8"),
        bcrypt.gensalt()
    ).decode("utf-8")


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token"""
    to_encode = data.copy()
    # Ensure sub is always a string for JWT spec compliance
    if "sub" in to_encode:
        to_encode["sub"] = str(to_encode["sub"])
    now = datetime.now(timezone.utc)
    expire = now + (expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES))
    # P2-8: Add JTI for token revocation support
    to_encode.update({
        "exp": expire,
        "type": "access",
        "jti": str(uuid.uuid4()),
        "iat": now,
    })
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def create_refresh_token(data: dict) -> str:
    """Create JWT refresh token"""
    to_encode = data.copy()
    # Ensure sub is always a string for JWT spec compliance
    if "sub" in to_encode:
        to_encode["sub"] = str(to_encode["sub"])
    now = datetime.now(timezone.utc)
    expire = now + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    # P2-8: Add JTI for token revocation support
    to_encode.update({
        "exp": expire,
        "type": "refresh",
        "jti": str(uuid.uuid4()),
        "iat": now,
    })
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    """Decode and validate JWT token"""
    try:
        # Skip strict subject validation to allow both int and string sub claims
        payload = jwt.decode(
            token, 
            settings.SECRET_KEY, 
            algorithms=[settings.ALGORITHM],
            options={"verify_sub": False}
        )
        return payload
    except JWTError:
        return None
