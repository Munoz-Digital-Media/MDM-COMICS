"""
Migration: Create api_call_metrics table

Document: 20251219_pipeline_instrumentation_proposal.md
Governance: constitution_db.json §5, §7
Classification: TIER_0
Retention: 90 days

Tracks API call performance metrics:
- Response times per API source
- Success/failure rates
- Error categorization
- Circuit breaker state
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker


async def run_migration():
    """Create api_call_metrics table"""

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        return False

    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        print("Creating api_call_metrics table...")
        print("-" * 60)

        # Check if table exists
        check_sql = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'api_call_metrics'
        """)
        result = await session.execute(check_sql)
        exists = result.fetchone()

        if exists:
            print("  Table already exists (SKIP)")
        else:
            # Create table
            create_sql = text("""
                CREATE TABLE api_call_metrics (
                    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),

                    -- Relationship
                    batch_id                VARCHAR(64) NOT NULL,

                    -- API identification
                    api_source              VARCHAR(32) NOT NULL,
                    endpoint_category       VARCHAR(64),

                    -- Timing
                    call_started_at         TIMESTAMPTZ NOT NULL,
                    call_completed_at       TIMESTAMPTZ,
                    response_time_ms        INTEGER,

                    -- Outcome
                    http_status             INTEGER,
                    success                 BOOLEAN NOT NULL DEFAULT false,
                    error_category          VARCHAR(64),
                    retry_count             INTEGER DEFAULT 0,

                    -- Circuit breaker state
                    circuit_state           VARCHAR(16),

                    -- Metadata
                    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                    CONSTRAINT chk_acm_api_source CHECK (api_source IN (
                        'metron', 'comicvine', 'pricecharting', 'marvel_fandom',
                        'dc_fandom', 'image_fandom', 'idw_fandom', 'dark_horse_fandom',
                        'dynamite_fandom', 'mycomicshop', 'cbr', 'gcd', 'bcw'
                    )),
                    CONSTRAINT chk_acm_circuit_state CHECK (
                        circuit_state IS NULL OR circuit_state IN ('closed', 'open', 'half_open')
                    )
                )
            """)
            await session.execute(create_sql)
            print("  Table created (OK)")

            # Create indexes
            indexes = [
                ("idx_acm_batch_id",
                 "CREATE INDEX idx_acm_batch_id ON api_call_metrics(batch_id)"),
                ("idx_acm_api_source_created",
                 "CREATE INDEX idx_acm_api_source_created ON api_call_metrics(api_source, created_at DESC)"),
                ("idx_acm_slow_calls",
                 "CREATE INDEX idx_acm_slow_calls ON api_call_metrics(response_time_ms) WHERE response_time_ms > 1000"),
                ("idx_acm_created_at",
                 "CREATE INDEX idx_acm_created_at ON api_call_metrics(created_at DESC)"),
            ]

            for idx_name, idx_sql in indexes:
                try:
                    await session.execute(text(idx_sql))
                    print(f"  Index {idx_name}: created (OK)")
                except Exception as e:
                    if "already exists" in str(e):
                        print(f"  Index {idx_name}: already exists (SKIP)")
                    else:
                        raise

            # Add comment
            await session.execute(text("""
                COMMENT ON TABLE api_call_metrics IS
                'TIER_0 API call performance metrics. No PII, no request/response bodies. Retention: 90 days.'
            """))
            print("  Table comment added (OK)")

        await session.commit()
        print("-" * 60)
        print("Migration complete: api_call_metrics")

    await engine.dispose()
    return True


if __name__ == "__main__":
    success = asyncio.run(run_migration())
    sys.exit(0 if success else 1)
