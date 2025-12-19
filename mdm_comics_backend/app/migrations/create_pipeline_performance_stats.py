"""
Migration: Create pipeline_performance_stats materialized view

Document: 20251219_pipeline_instrumentation_proposal.md
Governance: constitution_observability.json
Classification: TIER_0

Provides:
- Statistical analysis of batch durations (P50, P75, P90, P95, P99)
- Data-driven stall threshold recommendations (1.5x P95)
- 7-day rolling window for freshness
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker


async def run_migration():
    """Create pipeline_performance_stats materialized view"""

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        return False

    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        print("Creating pipeline_performance_stats materialized view...")
        print("-" * 60)

        # Check if view exists
        check_sql = text("""
            SELECT matviewname
            FROM pg_matviews
            WHERE matviewname = 'pipeline_performance_stats'
        """)
        result = await session.execute(check_sql)
        exists = result.fetchone()

        if exists:
            print("  Materialized view already exists (SKIP)")
        else:
            # Create materialized view
            create_sql = text("""
                CREATE MATERIALIZED VIEW pipeline_performance_stats AS
                SELECT
                    pipeline_type,
                    environment,

                    -- Central tendency
                    ROUND(AVG(batch_duration_ms)) AS avg_duration_ms,
                    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY batch_duration_ms))::INTEGER AS p50_duration_ms,
                    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY batch_duration_ms))::INTEGER AS p75_duration_ms,
                    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY batch_duration_ms))::INTEGER AS p90_duration_ms,
                    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY batch_duration_ms))::INTEGER AS p95_duration_ms,
                    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY batch_duration_ms))::INTEGER AS p99_duration_ms,

                    -- Recommended stall threshold (1.5x P95)
                    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY batch_duration_ms) * 1.5)::INTEGER AS recommended_stall_threshold_ms,

                    -- Sample size
                    COUNT(*) AS sample_count,

                    -- Time window
                    MIN(created_at) AS window_start,
                    MAX(created_at) AS window_end

                FROM pipeline_batch_metrics
                WHERE
                    status = 'completed'
                    AND created_at > NOW() - INTERVAL '7 days'
                    AND batch_duration_ms IS NOT NULL
                GROUP BY pipeline_type, environment
            """)
            await session.execute(create_sql)
            print("  Materialized view created (OK)")

            # Create unique index for concurrent refresh
            await session.execute(text("""
                CREATE UNIQUE INDEX idx_pps_pipeline_env
                ON pipeline_performance_stats(pipeline_type, environment)
            """))
            print("  Unique index created (OK)")

        # Create or replace refresh function
        await session.execute(text("""
            CREATE OR REPLACE FUNCTION refresh_pipeline_performance_stats()
            RETURNS void AS $$
            BEGIN
                REFRESH MATERIALIZED VIEW CONCURRENTLY pipeline_performance_stats;
            END;
            $$ LANGUAGE plpgsql
        """))
        print("  Refresh function created/updated (OK)")

        await session.commit()
        print("-" * 60)
        print("Migration complete: pipeline_performance_stats")

    await engine.dispose()
    return True


if __name__ == "__main__":
    success = asyncio.run(run_migration())
    sys.exit(0 if success else 1)
