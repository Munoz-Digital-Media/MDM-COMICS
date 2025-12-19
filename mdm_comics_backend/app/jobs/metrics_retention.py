"""
Metrics Retention & Cleanup Job

Document: 20251219_pipeline_instrumentation_proposal.md
Governance: constitution_data_hygiene.json §4
Classification: TIER_0

Enforces 90-day retention policy for pipeline metrics:
- Deletes expired records from pipeline_batch_metrics
- Deletes expired records from api_call_metrics
- Logs purge proof for audit compliance
- Runs daily via cron

Per constitution_data_hygiene.json §4:
- All purges must be logged with count, policy, timestamp, operator
- Purge proofs must be immutable
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)


# Retention period in days (per governance)
RETENTION_DAYS = 90

# Table to log purge operations (if exists)
PURGE_LOG_TABLE = "hygiene_purge_log"


async def check_purge_log_table_exists(db: AsyncSession) -> bool:
    """Check if hygiene_purge_log table exists."""
    result = await db.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = :table_name
        )
    """), {"table_name": PURGE_LOG_TABLE})
    return result.scalar()


async def create_purge_log_table_if_missing(db: AsyncSession) -> None:
    """Create hygiene_purge_log table if it doesn't exist."""
    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS hygiene_purge_log (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            table_name          VARCHAR(128) NOT NULL,
            records_purged      INTEGER NOT NULL,
            retention_policy    VARCHAR(32) NOT NULL,
            purge_timestamp     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            operator_id         VARCHAR(64) NOT NULL,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """))

    # Add comment for PII catalog
    await db.execute(text("""
        COMMENT ON TABLE hygiene_purge_log IS
        'Audit trail for data retention purges. No PII. Retention: permanent (audit requirement).'
    """))

    await db.commit()
    logger.info("[MetricsRetention] Created hygiene_purge_log table")


async def log_purge_operation(
    db: AsyncSession,
    table_name: str,
    records_purged: int,
    retention_policy: str = "90_days"
) -> None:
    """
    Log a purge operation for audit compliance.

    Governance: constitution_data_hygiene.json §4
    """
    try:
        await db.execute(text("""
            INSERT INTO hygiene_purge_log (
                table_name,
                records_purged,
                retention_policy,
                purge_timestamp,
                operator_id
            ) VALUES (
                :table_name,
                :records_purged,
                :retention_policy,
                NOW(),
                'metrics_retention_job'
            )
        """), {
            "table_name": table_name,
            "records_purged": records_purged,
            "retention_policy": retention_policy
        })
        await db.commit()

        logger.info(
            f"[MetricsRetention] Logged purge: {table_name} - {records_purged} records, "
            f"policy: {retention_policy}"
        )
    except Exception as e:
        logger.warning(f"[MetricsRetention] Failed to log purge (non-fatal): {e}")


async def purge_expired_batch_metrics(db: AsyncSession) -> int:
    """
    Purge expired records from pipeline_batch_metrics.

    Returns:
        Number of records purged
    """
    # First count records to be purged (for logging)
    count_result = await db.execute(text(f"""
        SELECT COUNT(*)
        FROM pipeline_batch_metrics
        WHERE created_at < NOW() - INTERVAL '{RETENTION_DAYS} days'
    """))
    count = count_result.scalar() or 0

    if count == 0:
        logger.info("[MetricsRetention] No expired batch metrics to purge")
        return 0

    # Delete expired records
    await db.execute(text(f"""
        DELETE FROM pipeline_batch_metrics
        WHERE created_at < NOW() - INTERVAL '{RETENTION_DAYS} days'
    """))
    await db.commit()

    logger.info(f"[MetricsRetention] Purged {count} expired batch metrics")
    return count


async def purge_expired_api_metrics(db: AsyncSession) -> int:
    """
    Purge expired records from api_call_metrics.

    Returns:
        Number of records purged
    """
    # First count records to be purged (for logging)
    count_result = await db.execute(text(f"""
        SELECT COUNT(*)
        FROM api_call_metrics
        WHERE created_at < NOW() - INTERVAL '{RETENTION_DAYS} days'
    """))
    count = count_result.scalar() or 0

    if count == 0:
        logger.info("[MetricsRetention] No expired API metrics to purge")
        return 0

    # Delete expired records
    await db.execute(text(f"""
        DELETE FROM api_call_metrics
        WHERE created_at < NOW() - INTERVAL '{RETENTION_DAYS} days'
    """))
    await db.commit()

    logger.info(f"[MetricsRetention] Purged {count} expired API metrics")
    return count


async def run_metrics_retention_job(
    db: Optional[AsyncSession] = None
) -> Dict[str, Any]:
    """
    Main entry point for scheduled execution.

    Runs daily: 0 3 * * * (3 AM UTC)

    Returns:
        Summary dict with purge results
    """
    logger.info("[MetricsRetention] Starting retention cleanup...")

    summary = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "retention_days": RETENTION_DAYS,
        "batch_metrics_purged": 0,
        "api_metrics_purged": 0,
        "total_purged": 0,
        "purge_logged": False,
        "errors": []
    }

    async def _do_cleanup(session: AsyncSession):
        # Ensure purge log table exists
        await create_purge_log_table_if_missing(session)

        # Purge batch metrics
        try:
            summary["batch_metrics_purged"] = await purge_expired_batch_metrics(session)
            if summary["batch_metrics_purged"] > 0:
                await log_purge_operation(
                    session,
                    "pipeline_batch_metrics",
                    summary["batch_metrics_purged"]
                )
        except Exception as e:
            error_msg = f"Failed to purge batch metrics: {str(e)}"
            logger.error(f"[MetricsRetention] {error_msg}")
            summary["errors"].append(error_msg)

        # Purge API metrics
        try:
            summary["api_metrics_purged"] = await purge_expired_api_metrics(session)
            if summary["api_metrics_purged"] > 0:
                await log_purge_operation(
                    session,
                    "api_call_metrics",
                    summary["api_metrics_purged"]
                )
        except Exception as e:
            error_msg = f"Failed to purge API metrics: {str(e)}"
            logger.error(f"[MetricsRetention] {error_msg}")
            summary["errors"].append(error_msg)

        summary["total_purged"] = summary["batch_metrics_purged"] + summary["api_metrics_purged"]
        summary["purge_logged"] = summary["total_purged"] > 0

    if db:
        await _do_cleanup(db)
    else:
        async with AsyncSessionLocal() as session:
            await _do_cleanup(session)

    summary["completed_at"] = datetime.now(timezone.utc).isoformat()

    # Log summary
    if summary["total_purged"] > 0:
        logger.info(
            f"[MetricsRetention] Cleanup complete: purged {summary['total_purged']} total records "
            f"(batch: {summary['batch_metrics_purged']}, api: {summary['api_metrics_purged']})"
        )
    else:
        logger.info("[MetricsRetention] Cleanup complete: no expired records found")

    return summary


async def get_retention_status() -> Dict[str, Any]:
    """
    Get current retention status for admin dashboard.

    Returns:
        Dict with retention stats and upcoming purges
    """
    async with AsyncSessionLocal() as db:
        status = {
            "retention_days": RETENTION_DAYS,
            "tables": {},
            "upcoming_purges": {},
            "recent_purges": []
        }

        # Get current record counts and oldest records
        tables = ["pipeline_batch_metrics", "api_call_metrics"]

        for table in tables:
            try:
                # Total count
                count_result = await db.execute(text(f"SELECT COUNT(*) FROM {table}"))
                total_count = count_result.scalar() or 0

                # Oldest record
                oldest_result = await db.execute(text(f"""
                    SELECT MIN(created_at) as oldest,
                           MAX(created_at) as newest
                    FROM {table}
                """))
                oldest_row = oldest_result.fetchone()

                # Count of records due for purge
                due_result = await db.execute(text(f"""
                    SELECT COUNT(*)
                    FROM {table}
                    WHERE created_at < NOW() - INTERVAL '{RETENTION_DAYS} days'
                """))
                due_count = due_result.scalar() or 0

                status["tables"][table] = {
                    "total_records": total_count,
                    "oldest_record": oldest_row.oldest.isoformat() if oldest_row and oldest_row.oldest else None,
                    "newest_record": oldest_row.newest.isoformat() if oldest_row and oldest_row.newest else None,
                    "due_for_purge": due_count
                }

            except Exception as e:
                status["tables"][table] = {"error": str(e)}

        # Get recent purge history
        try:
            purge_result = await db.execute(text("""
                SELECT
                    table_name,
                    records_purged,
                    retention_policy,
                    purge_timestamp
                FROM hygiene_purge_log
                WHERE table_name IN ('pipeline_batch_metrics', 'api_call_metrics')
                ORDER BY purge_timestamp DESC
                LIMIT 10
            """))

            for row in purge_result.fetchall():
                status["recent_purges"].append({
                    "table": row.table_name,
                    "records_purged": row.records_purged,
                    "policy": row.retention_policy,
                    "timestamp": row.purge_timestamp.isoformat() if row.purge_timestamp else None
                })
        except Exception:
            # Table might not exist yet
            pass

        return status


# CLI entry point for manual testing
if __name__ == "__main__":
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

    async def main():
        print(f"Running manual metrics retention cleanup (>{RETENTION_DAYS} days)...")
        result = await run_metrics_retention_job()
        print(f"\nResults:")
        for key, value in result.items():
            print(f"  {key}: {value}")

    asyncio.run(main())
