"""
Migration: Create pipeline_batch_metrics table

Document: 20251219_pipeline_instrumentation_proposal.md
Governance: constitution_db.json §5, §11
Classification: TIER_0
Retention: 90 days

Tracks batch processing performance with millisecond precision for:
- Data-driven stall detection thresholds
- Operational dashboards for pipeline health
- Audit-ready performance artifacts
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker


async def run_migration():
    """Create pipeline_batch_metrics table"""

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        return False

    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        print("Creating pipeline_batch_metrics table...")
        print("-" * 60)

        # Check if table exists
        check_sql = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'pipeline_batch_metrics'
        """)
        result = await session.execute(check_sql)
        exists = result.fetchone()

        if exists:
            print("  Table already exists (SKIP)")
        else:
            # Create table
            create_sql = text("""
                CREATE TABLE pipeline_batch_metrics (
                    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),

                    -- Batch identification
                    batch_id                VARCHAR(64) NOT NULL,
                    pipeline_type           VARCHAR(32) NOT NULL,
                    environment             VARCHAR(16) NOT NULL DEFAULT 'prod',

                    -- Timing metrics
                    batch_started_at        TIMESTAMPTZ NOT NULL,
                    batch_completed_at      TIMESTAMPTZ,
                    batch_duration_ms       INTEGER,

                    -- Volume metrics
                    records_in_batch        INTEGER NOT NULL,
                    records_processed       INTEGER DEFAULT 0,
                    records_enriched        INTEGER DEFAULT 0,
                    records_skipped         INTEGER DEFAULT 0,
                    records_failed          INTEGER DEFAULT 0,

                    -- Outcome
                    status                  VARCHAR(16) NOT NULL DEFAULT 'running',
                    error_category          VARCHAR(64),

                    -- Stall detection
                    last_heartbeat_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    stall_detected_at       TIMESTAMPTZ,
                    self_healed_at          TIMESTAMPTZ,

                    -- Immutability (hash chain)
                    record_hash             VARCHAR(128),
                    prev_record_hash        VARCHAR(128),

                    -- Metadata
                    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                    CONSTRAINT chk_pbm_status CHECK (status IN ('running', 'completed', 'failed', 'stalled', 'self_healed')),
                    CONSTRAINT chk_pbm_pipeline_type CHECK (pipeline_type IN (
                        'sequential_enrichment', 'gcd_import', 'pricecharting', 'metron_sync',
                        'funko_pricecharting_match', 'comic_pricecharting_match',
                        'funko_price_sync', 'comic_price_sync', 'comic_enrichment',
                        'cover_enrichment', 'bcw_inventory_sync', 'image_acquisition'
                    ))
                )
            """)
            await session.execute(create_sql)
            print("  Table created (OK)")

            # Create indexes
            indexes = [
                ("idx_pbm_pipeline_type_created",
                 "CREATE INDEX idx_pbm_pipeline_type_created ON pipeline_batch_metrics(pipeline_type, created_at DESC)"),
                ("idx_pbm_status",
                 "CREATE INDEX idx_pbm_status ON pipeline_batch_metrics(status) WHERE status IN ('running', 'stalled')"),
                ("idx_pbm_created_at",
                 "CREATE INDEX idx_pbm_created_at ON pipeline_batch_metrics(created_at DESC)"),
                ("idx_pbm_batch_id",
                 "CREATE INDEX idx_pbm_batch_id ON pipeline_batch_metrics(batch_id)"),
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
                COMMENT ON TABLE pipeline_batch_metrics IS
                'TIER_0 operational metrics for pipeline batch processing. No PII. Retention: 90 days.'
            """))
            print("  Table comment added (OK)")

        await session.commit()
        print("-" * 60)
        print("Migration complete: pipeline_batch_metrics")

    await engine.dispose()
    return True


if __name__ == "__main__":
    success = asyncio.run(run_migration())
    sys.exit(0 if success else 1)
