"""
Database configuration and session management

P2-7: Configurable connection pooling for Railway vs local dev
"""
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import NullPool

from app.core.config import settings

# P2-7: Configure pool based on environment
# Use NullPool for serverless/Railway to avoid connection issues
# Use QueuePool with configured sizes for traditional deployments
pool_config = {}

if settings.ENVIRONMENT == "production":
    # Production: Use connection pooling with configured limits
    pool_config = {
        "pool_size": settings.DB_POOL_SIZE,
        "max_overflow": settings.DB_MAX_OVERFLOW,
        "pool_recycle": settings.DB_POOL_RECYCLE,
        "pool_pre_ping": True,  # Verify connections before use
    }
else:
    # Development: Simpler pool for local development
    pool_config = {
        "pool_size": 2,
        "max_overflow": 5,
        "pool_pre_ping": True,
    }

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    future=True,
    **pool_config,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()


async def get_db() -> AsyncSession:
    """Dependency for getting async database sessions"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


from contextlib import asynccontextmanager


@asynccontextmanager
async def get_db_session():
    """
    Context manager for database sessions outside FastAPI request context.

    Use this in:
    - Background jobs (ARQ workers)
    - Webhook handlers
    - CLI scripts
    - Service-to-service calls

    Usage:
        async with get_db_session() as db:
            result = await db.execute(...)
            await db.commit()
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
