"""
Job Checkpoints Table Migration v1.0.0

Creates the job_checkpoints table for tracking job state persistence
across restarts (e.g., inbound_processor initial sweep tracking).

Per constitution_db.json:
- DB-001: snake_case for all tables and columns
- DB-011: Schema changes via migration
"""
import asyncio
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


async def migrate_job_checkpoints(engine):
    """
    Create the job_checkpoints table for job state persistence.

    This is an idempotent migration - safe to run multiple times.
    """
    logger.info("Starting job_checkpoints migration...")

    async with engine.begin() as conn:
        # Create job_checkpoints table
        logger.info("Creating job_checkpoints table...")

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS job_checkpoints (
                id SERIAL PRIMARY KEY,
                job_name VARCHAR(100) NOT NULL UNIQUE,
                state_data JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        logger.info("Created/verified job_checkpoints table")

        # Add comments for documentation
        await conn.execute(text("""
            COMMENT ON TABLE job_checkpoints
            IS 'Persistent state storage for jobs that need to track progress across restarts'
        """))
        await conn.execute(text("""
            COMMENT ON COLUMN job_checkpoints.job_name
            IS 'Unique job identifier (e.g., inbound_processor)'
        """))
        await conn.execute(text("""
            COMMENT ON COLUMN job_checkpoints.state_data
            IS 'JSON state data for job-specific checkpoint information'
        """))

        # Create index for job_name lookups
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_job_checkpoints_job_name
            ON job_checkpoints(job_name)
        """))
        logger.info("Created index for job_checkpoints")

    logger.info("job_checkpoints migration complete!")


async def rollback_job_checkpoints(engine):
    """
    Rollback the job_checkpoints schema changes.

    Per constitution_db.json Section 1:
    > "Every migration includes dry-run (shadow DB), rollback SQL"
    """
    logger.info("Rolling back job_checkpoints migration...")

    async with engine.begin() as conn:
        await conn.execute(text("""
            DROP TABLE IF EXISTS job_checkpoints
        """))
        logger.info("Dropped job_checkpoints table")

    logger.info("job_checkpoints rollback complete!")


async def run_migration():
    """Run the migration using the app's database engine."""
    from app.core.database import engine

    await migrate_job_checkpoints(engine)


if __name__ == "__main__":
    asyncio.run(run_migration())
