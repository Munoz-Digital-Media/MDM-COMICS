"""
Migration: Create metron_rate_budget table

Document: IMPL-2025-1221-METRON-RL
Governance: constitution_db.json §5, constitution_cyberSec.json §12
Classification: TIER_0
Retention: 7 days (operational, no PII)

Persists Metron API rate limit counters for:
- Daily request tracking across process restarts
- UTC midnight reset logic
- Rate limit hardening compliance

v1.0.0 - Initial implementation
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker


async def run_migration():
    """Create metron_rate_budget table for rate limit persistence"""

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        return False

    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        print("Creating metron_rate_budget table...")
        print("-" * 60)

        # Check if table exists
        check_sql = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'metron_rate_budget'
        """)
        result = await session.execute(check_sql)
        exists = result.fetchone()

        if exists:
            print("  Table already exists (SKIP)")
        else:
            # Create table
            create_sql = text("""
                CREATE TABLE metron_rate_budget (
                    -- Primary key: UTC date (YYYY-MM-DD)
                    date_utc                DATE PRIMARY KEY,

                    -- Request counter
                    request_count           INTEGER NOT NULL DEFAULT 0,

                    -- Rate limit events
                    rate_limit_count        INTEGER NOT NULL DEFAULT 0,
                    last_rate_limit_at      TIMESTAMPTZ,

                    -- Cooldown state
                    cooldown_until          TIMESTAMPTZ,
                    consecutive_429s        INTEGER NOT NULL DEFAULT 0,

                    -- Metadata
                    last_updated            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                    -- Constraints
                    CONSTRAINT chk_mrb_request_count CHECK (request_count >= 0),
                    CONSTRAINT chk_mrb_rate_limit_count CHECK (rate_limit_count >= 0),
                    CONSTRAINT chk_mrb_consecutive_429s CHECK (consecutive_429s >= 0)
                )
            """)
            await session.execute(create_sql)
            print("  Table created (OK)")

            # Create index for cleanup
            await session.execute(text("""
                CREATE INDEX idx_mrb_created_at ON metron_rate_budget(created_at)
            """))
            print("  Index idx_mrb_created_at: created (OK)")

            # Add comment
            await session.execute(text("""
                COMMENT ON TABLE metron_rate_budget IS
                'TIER_0 operational data for Metron API rate limit tracking. No PII. Retention: 7 days.'
            """))
            print("  Table comment added (OK)")

        # Create cleanup function (retention: 7 days)
        cleanup_sql = text("""
            CREATE OR REPLACE FUNCTION cleanup_metron_rate_budget()
            RETURNS INTEGER AS $$
            DECLARE
                deleted_count INTEGER;
            BEGIN
                DELETE FROM metron_rate_budget
                WHERE date_utc < CURRENT_DATE - INTERVAL '7 days';
                GET DIAGNOSTICS deleted_count = ROW_COUNT;
                RETURN deleted_count;
            END;
            $$ LANGUAGE plpgsql;
        """)
        await session.execute(cleanup_sql)
        print("  Cleanup function created (OK)")

        await session.commit()
        print("-" * 60)
        print("Migration complete: metron_rate_budget")

    await engine.dispose()
    return True


if __name__ == "__main__":
    success = asyncio.run(run_migration())
    sys.exit(0 if success else 1)
