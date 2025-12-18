"""
PriceCharting Autonomous Resilience System Migration v1.0.0

Document ID: IMPL-PC-2025-12-17
Status: APPROVED

This migration implements:
- Phase 1.1: Reset pricecharting_matching checkpoint
- Phase 1.2: Add circuit breaker columns to pipeline_checkpoints
- Phase 1.3: Create self_healing_audit table

Per constitution_db.json:
- DB-001: snake_case for all tables and columns
- DB-003: FK with appropriate ON DELETE
- DB-004: Indexes on query columns
- DB-011: Schema changes via migration

Per constitution_logging.json:
- Hash-chain logs for tamper evidence
"""
import asyncio
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


async def migrate_pricecharting_resilience(engine):
    """
    Apply PriceCharting Autonomous Resilience schema changes.

    This is an idempotent migration - safe to run multiple times.
    """
    logger.info("Starting PriceCharting Resilience migration...")

    async with engine.begin() as conn:
        # ==================== Phase 1.1: Reset pricecharting_matching checkpoint ====================
        logger.info("Phase 1.1: Resetting pricecharting_matching checkpoint...")

        # Check if checkpoint exists
        result = await conn.execute(text("""
            SELECT job_name FROM pipeline_checkpoints
            WHERE job_name = 'pricecharting_matching'
        """))
        checkpoint_exists = result.fetchone() is not None

        if checkpoint_exists:
            await conn.execute(text("""
                UPDATE pipeline_checkpoints
                SET control_signal = 'run',
                    paused_at = NULL,
                    state_data = jsonb_build_object(
                        'phase', 'funkos',
                        'funko_last_id', 0,
                        'comic_last_id', 0
                    ),
                    total_errors = 0,
                    last_error = 'MANUAL RESET: Cleared by resilience migration 2025-12-17',
                    is_running = FALSE,
                    updated_at = NOW()
                WHERE job_name = 'pricecharting_matching'
            """))
            logger.info("Reset pricecharting_matching checkpoint to clean state")
        else:
            logger.info("pricecharting_matching checkpoint does not exist yet - will be created on first run")

        # ==================== Phase 1.2: Add circuit breaker columns ====================
        logger.info("Phase 1.2: Adding circuit breaker columns to pipeline_checkpoints...")

        # Check which columns exist
        result = await conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'pipeline_checkpoints'
              AND column_name IN (
                  'circuit_state',
                  'circuit_failure_count',
                  'circuit_last_failure',
                  'circuit_backoff_multiplier'
              )
        """))
        existing_cols = {row[0] for row in result.fetchall()}

        # Add circuit_state column
        if 'circuit_state' not in existing_cols:
            await conn.execute(text("""
                ALTER TABLE pipeline_checkpoints
                ADD COLUMN circuit_state VARCHAR(20) DEFAULT 'CLOSED'
            """))
            await conn.execute(text("""
                COMMENT ON COLUMN pipeline_checkpoints.circuit_state
                IS 'Circuit breaker state: CLOSED, OPEN, HALF_OPEN'
            """))
            logger.info("Added circuit_state column")
        else:
            logger.info("circuit_state column already exists")

        # Add circuit_failure_count column
        if 'circuit_failure_count' not in existing_cols:
            await conn.execute(text("""
                ALTER TABLE pipeline_checkpoints
                ADD COLUMN circuit_failure_count INTEGER DEFAULT 0
            """))
            await conn.execute(text("""
                COMMENT ON COLUMN pipeline_checkpoints.circuit_failure_count
                IS 'Consecutive failure count for circuit breaker'
            """))
            logger.info("Added circuit_failure_count column")
        else:
            logger.info("circuit_failure_count column already exists")

        # Add circuit_last_failure column
        if 'circuit_last_failure' not in existing_cols:
            await conn.execute(text("""
                ALTER TABLE pipeline_checkpoints
                ADD COLUMN circuit_last_failure TIMESTAMPTZ
            """))
            await conn.execute(text("""
                COMMENT ON COLUMN pipeline_checkpoints.circuit_last_failure
                IS 'Timestamp of last circuit breaker failure'
            """))
            logger.info("Added circuit_last_failure column")
        else:
            logger.info("circuit_last_failure column already exists")

        # Add circuit_backoff_multiplier column
        if 'circuit_backoff_multiplier' not in existing_cols:
            await conn.execute(text("""
                ALTER TABLE pipeline_checkpoints
                ADD COLUMN circuit_backoff_multiplier INTEGER DEFAULT 1
            """))
            await conn.execute(text("""
                COMMENT ON COLUMN pipeline_checkpoints.circuit_backoff_multiplier
                IS 'Exponential backoff multiplier (1, 2, 4, 8, 16)'
            """))
            logger.info("Added circuit_backoff_multiplier column")
        else:
            logger.info("circuit_backoff_multiplier column already exists")

        # ==================== Phase 1.3: Create self_healing_audit table ====================
        logger.info("Phase 1.3: Creating self_healing_audit table...")

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS self_healing_audit (
                id SERIAL PRIMARY KEY,
                action VARCHAR(50) NOT NULL,
                job_name VARCHAR(100) NOT NULL,
                details JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                hash_chain VARCHAR(128)
            )
        """))
        logger.info("Created/verified self_healing_audit table")

        # Add comments for documentation
        await conn.execute(text("""
            COMMENT ON TABLE self_healing_audit
            IS 'Audit trail for self-healing actions per constitution_logging.json'
        """))
        await conn.execute(text("""
            COMMENT ON COLUMN self_healing_audit.action
            IS 'Action type: JOB_AUTO_RESUMED, CIRCUIT_OPENED, CIRCUIT_CLOSED, STALE_FLAG_CLEARED'
        """))
        await conn.execute(text("""
            COMMENT ON COLUMN self_healing_audit.hash_chain
            IS 'SHA-512 hash chain for tamper evidence per constitution_logging.json'
        """))

        # Create indexes for self_healing_audit
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_self_healing_audit_job ON self_healing_audit(job_name, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_self_healing_audit_action ON self_healing_audit(action, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_self_healing_audit_created ON self_healing_audit(created_at DESC)",
        ]:
            await conn.execute(text(idx_sql))
        logger.info("Created indexes for self_healing_audit")

        # ==================== Create independent job checkpoints ====================
        logger.info("Creating independent job checkpoints for split jobs...")

        # funko_pricecharting_match checkpoint
        await conn.execute(text("""
            INSERT INTO pipeline_checkpoints (
                job_name,
                job_type,
                is_running,
                control_signal,
                state_data,
                circuit_state,
                circuit_failure_count,
                circuit_backoff_multiplier,
                created_at,
                updated_at
            )
            VALUES (
                'funko_pricecharting_match',
                'pricecharting',
                FALSE,
                'run',
                '{"last_id": 0}'::jsonb,
                'CLOSED',
                0,
                1,
                NOW(),
                NOW()
            )
            ON CONFLICT (job_name) DO NOTHING
        """))
        logger.info("Created/verified funko_pricecharting_match checkpoint")

        # comic_pricecharting_match checkpoint
        await conn.execute(text("""
            INSERT INTO pipeline_checkpoints (
                job_name,
                job_type,
                is_running,
                control_signal,
                state_data,
                circuit_state,
                circuit_failure_count,
                circuit_backoff_multiplier,
                created_at,
                updated_at
            )
            VALUES (
                'comic_pricecharting_match',
                'pricecharting',
                FALSE,
                'run',
                '{"last_id": 0}'::jsonb,
                'CLOSED',
                0,
                1,
                NOW(),
                NOW()
            )
            ON CONFLICT (job_name) DO NOTHING
        """))
        logger.info("Created/verified comic_pricecharting_match checkpoint")

        # funko_price_sync checkpoint
        await conn.execute(text("""
            INSERT INTO pipeline_checkpoints (
                job_name,
                job_type,
                is_running,
                control_signal,
                state_data,
                circuit_state,
                circuit_failure_count,
                circuit_backoff_multiplier,
                created_at,
                updated_at
            )
            VALUES (
                'funko_price_sync',
                'pricecharting',
                FALSE,
                'run',
                '{"last_id": 0}'::jsonb,
                'CLOSED',
                0,
                1,
                NOW(),
                NOW()
            )
            ON CONFLICT (job_name) DO NOTHING
        """))
        logger.info("Created/verified funko_price_sync checkpoint")

        # comic_price_sync checkpoint
        await conn.execute(text("""
            INSERT INTO pipeline_checkpoints (
                job_name,
                job_type,
                is_running,
                control_signal,
                state_data,
                circuit_state,
                circuit_failure_count,
                circuit_backoff_multiplier,
                created_at,
                updated_at
            )
            VALUES (
                'comic_price_sync',
                'pricecharting',
                FALSE,
                'run',
                '{"last_id": 0}'::jsonb,
                'CLOSED',
                0,
                1,
                NOW(),
                NOW()
            )
            ON CONFLICT (job_name) DO NOTHING
        """))
        logger.info("Created/verified comic_price_sync checkpoint")

    logger.info("PriceCharting Resilience migration complete!")


async def rollback_pricecharting_resilience(engine):
    """
    Rollback the PriceCharting Resilience schema changes.

    Per constitution_db.json Section 1:
    > "Every migration includes dry-run (shadow DB), rollback SQL"
    """
    logger.info("Rolling back PriceCharting Resilience migration...")

    async with engine.begin() as conn:
        # Remove circuit breaker columns
        await conn.execute(text("""
            ALTER TABLE pipeline_checkpoints
            DROP COLUMN IF EXISTS circuit_state,
            DROP COLUMN IF EXISTS circuit_failure_count,
            DROP COLUMN IF EXISTS circuit_last_failure,
            DROP COLUMN IF EXISTS circuit_backoff_multiplier
        """))
        logger.info("Removed circuit breaker columns")

        # Drop self_healing_audit table
        await conn.execute(text("""
            DROP TABLE IF EXISTS self_healing_audit
        """))
        logger.info("Dropped self_healing_audit table")

        # Remove independent job checkpoints
        await conn.execute(text("""
            DELETE FROM pipeline_checkpoints
            WHERE job_name IN (
                'funko_pricecharting_match',
                'comic_pricecharting_match',
                'funko_price_sync',
                'comic_price_sync'
            )
        """))
        logger.info("Removed independent job checkpoints")

    logger.info("PriceCharting Resilience rollback complete!")


async def run_migration():
    """Run the migration using the app's database engine."""
    from app.core.database import engine

    await migrate_pricecharting_resilience(engine)


if __name__ == "__main__":
    asyncio.run(run_migration())
