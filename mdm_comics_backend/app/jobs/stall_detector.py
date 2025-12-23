"""
Stall Detection Job

Document: 20251219_pipeline_instrumentation_proposal.md
Governance: constitution_observability.json, constitution_logging.json
Classification: TIER_0

Detects stalled pipeline batches and triggers self-healing:
- Runs every 2 minutes
- Uses adaptive thresholds from pipeline_performance_stats
- Integrates with existing checkpoint-based self-healing
- Logs all actions for audit trail
"""
import asyncio
import logging
from typing import Optional, List, Dict, Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.services.pipeline_metrics import (
    PipelineType,
    pipeline_metrics,
)

logger = logging.getLogger(__name__)


# All pipeline types to monitor for stalls
MONITORED_PIPELINE_TYPES: List[str] = [
    PipelineType.SEQUENTIAL_ENRICHMENT.value,
    PipelineType.GCD_IMPORT.value,
    PipelineType.PRICECHARTING.value,
    PipelineType.METRON_SYNC.value,
    PipelineType.FUNKO_PRICECHARTING_MATCH.value,
    PipelineType.COMIC_PRICECHARTING_MATCH.value,
    PipelineType.FUNKO_PRICE_SYNC.value,
    PipelineType.COMIC_PRICE_SYNC.value,
    PipelineType.BCW_INVENTORY_SYNC.value,
    PipelineType.IMAGE_ACQUISITION.value,
]

# Maximum self-heal attempts per batch before giving up
MAX_SELF_HEAL_ATTEMPTS = 3

# Track self-heal attempts in memory (reset on restart)
_self_heal_attempts: Dict[str, int] = {}


async def attempt_self_heal(
    batch_id: str,
    pipeline_type: str,
    db: AsyncSession
) -> bool:
    """
    Attempt to self-heal a stalled batch.

    Self-healing actions:
    1. Clear any stale checkpoints for this pipeline
    2. Mark the batch as self-healed
    3. Log the recovery attempt

    Args:
        batch_id: The stalled batch ID
        pipeline_type: Type of pipeline
        db: Database session

    Returns:
        True if self-heal succeeded, False if max attempts exceeded
    """
    global _self_heal_attempts

    # Track attempts
    attempts = _self_heal_attempts.get(batch_id, 0) + 1
    _self_heal_attempts[batch_id] = attempts

    if attempts > MAX_SELF_HEAL_ATTEMPTS:
        logger.error(
            f"[StallDetector] CRITICAL: Batch {batch_id} exceeded max self-heal attempts "
            f"({attempts}/{MAX_SELF_HEAL_ATTEMPTS}). Manual intervention required."
        )
        return False

    logger.info(
        f"[StallDetector] Attempting self-heal for batch {batch_id} "
        f"(attempt {attempts}/{MAX_SELF_HEAL_ATTEMPTS})"
    )

    try:
        # Map pipeline types to job names for checkpoint clearing
        pipeline_to_job_map = {
            "sequential_enrichment": "sequential_enrichment",
            "gcd_import": "gcd_import",
            "pricecharting": "pricecharting_matching",
            "metron_sync": "metron_sync",
            "funko_pricecharting_match": "funko_pricecharting_match",
            "comic_pricecharting_match": "comic_pricecharting_match",
            "funko_price_sync": "funko_price_sync",
            "comic_price_sync": "comic_price_sync",
            "bcw_inventory_sync": "bcw_inventory_sync",
            "image_acquisition": "image_acquisition",
        }

        job_name = pipeline_to_job_map.get(pipeline_type)

        if job_name:
            # Clear stale checkpoint (allow job to restart)
            # Note: use paused_at (timestamp) not is_paused (doesn't exist)
            result = await db.execute(text("""
                UPDATE pipeline_checkpoints
                SET is_running = false,
                    paused_at = NULL,
                    control_signal = 'run',
                    last_error = COALESCE(last_error, '') || E'\n[StallDetector] Auto-cleared at ' || NOW()::text
                WHERE job_name = :job_name
                AND is_running = true
                RETURNING job_name
            """), {"job_name": job_name})

            cleared = result.fetchone()
            if cleared:
                logger.info(f"[StallDetector] Cleared stale checkpoint for job: {job_name}")

        # Mark batch as self-healed in metrics
        await pipeline_metrics.mark_self_healed(batch_id, db)

        # Clear from attempts tracker on success (batch is done)
        if batch_id in _self_heal_attempts:
            del _self_heal_attempts[batch_id]

        logger.info(f"[StallDetector] Successfully self-healed batch {batch_id}")
        return True

    except Exception as e:
        logger.error(f"[StallDetector] Self-heal failed for batch {batch_id}: {e}")
        return False


async def run_stall_detection_sweep(db: Optional[AsyncSession] = None) -> Dict[str, Any]:
    """
    Run a complete stall detection sweep across all pipeline types.

    This is the main entry point for scheduled execution.

    Returns:
        Summary dict with detection results
    """
    logger.info("[StallDetector] Starting stall detection sweep...")

    summary = {
        "pipelines_checked": 0,
        "stalled_batches_found": 0,
        "self_heals_attempted": 0,
        "self_heals_succeeded": 0,
        "self_heals_failed": 0,
        "errors": []
    }

    async def _do_sweep(session: AsyncSession):
        for pipeline_type in MONITORED_PIPELINE_TYPES:
            summary["pipelines_checked"] += 1

            try:
                # Detect stalled batches
                stalled_batches = await pipeline_metrics.detect_and_handle_stalls(
                    pipeline_type, session
                )

                if stalled_batches:
                    summary["stalled_batches_found"] += len(stalled_batches)
                    logger.warning(
                        f"[StallDetector] Detected {len(stalled_batches)} stalled batches "
                        f"for {pipeline_type}: {stalled_batches}"
                    )

                    # Attempt self-heal for each stalled batch
                    for batch_id in stalled_batches:
                        summary["self_heals_attempted"] += 1

                        success = await attempt_self_heal(batch_id, pipeline_type, session)
                        if success:
                            summary["self_heals_succeeded"] += 1
                        else:
                            summary["self_heals_failed"] += 1

            except Exception as e:
                error_msg = f"Error checking {pipeline_type}: {str(e)}"
                logger.error(f"[StallDetector] {error_msg}")
                summary["errors"].append(error_msg)

        # Refresh performance stats periodically (every sweep)
        try:
            await pipeline_metrics.refresh_performance_stats(session)
        except Exception as e:
            logger.warning(f"[StallDetector] Failed to refresh performance stats: {e}")

    if db:
        await _do_sweep(db)
    else:
        async with AsyncSessionLocal() as session:
            await _do_sweep(session)

    # Log summary
    if summary["stalled_batches_found"] > 0:
        logger.warning(
            f"[StallDetector] Sweep complete: found {summary['stalled_batches_found']} stalled, "
            f"healed {summary['self_heals_succeeded']}/{summary['self_heals_attempted']}"
        )
    else:
        logger.info("[StallDetector] Sweep complete: no stalled batches detected")

    return summary


async def run_stall_detection_job() -> Dict[str, Any]:
    """
    Job entry point for scheduler integration.

    Runs every 2 minutes: */2 * * * *
    """
    return await run_stall_detection_sweep()


async def get_stall_detection_status() -> Dict[str, Any]:
    """
    Get current stall detection status for admin dashboard.

    Returns:
        Dict with current thresholds, running batches, and recent stalls
    """
    async with AsyncSessionLocal() as db:
        status = {
            "thresholds_by_pipeline": {},
            "running_batches": [],
            "recent_stalls": [],
            "recent_self_heals": []
        }

        # Get thresholds for each pipeline type
        for pipeline_type in MONITORED_PIPELINE_TYPES:
            try:
                threshold_ms = await pipeline_metrics.get_stall_threshold_ms(pipeline_type, db)
                stats = await pipeline_metrics.get_performance_stats(pipeline_type, db)

                status["thresholds_by_pipeline"][pipeline_type] = {
                    "threshold_ms": threshold_ms,
                    "threshold_minutes": round(threshold_ms / 60000, 1),
                    "sample_count": stats.sample_count if stats else 0,
                    "p95_ms": stats.p95_duration_ms if stats else None,
                    "is_adaptive": stats is not None and stats.sample_count >= 50
                }
            except Exception:
                status["thresholds_by_pipeline"][pipeline_type] = {
                    "threshold_ms": 480000,  # 8 min fallback
                    "threshold_minutes": 8.0,
                    "sample_count": 0,
                    "p95_ms": None,
                    "is_adaptive": False
                }

        # Get currently running batches
        result = await db.execute(text("""
            SELECT
                batch_id,
                pipeline_type,
                batch_started_at,
                last_heartbeat_at,
                records_in_batch,
                records_processed,
                EXTRACT(EPOCH FROM (NOW() - last_heartbeat_at)) * 1000 AS ms_since_heartbeat
            FROM pipeline_batch_metrics
            WHERE status = 'running'
            ORDER BY batch_started_at DESC
            LIMIT 20
        """))

        for row in result.fetchall():
            status["running_batches"].append({
                "batch_id": row.batch_id,
                "pipeline_type": row.pipeline_type,
                "started_at": row.batch_started_at.isoformat() if row.batch_started_at else None,
                "last_heartbeat": row.last_heartbeat_at.isoformat() if row.last_heartbeat_at else None,
                "records_in_batch": row.records_in_batch,
                "records_processed": row.records_processed,
                "ms_since_heartbeat": int(row.ms_since_heartbeat) if row.ms_since_heartbeat else 0
            })

        # Get recent stalls
        result = await db.execute(text("""
            SELECT
                batch_id,
                pipeline_type,
                stall_detected_at,
                status,
                self_healed_at
            FROM pipeline_batch_metrics
            WHERE stall_detected_at IS NOT NULL
            ORDER BY stall_detected_at DESC
            LIMIT 10
        """))

        for row in result.fetchall():
            status["recent_stalls"].append({
                "batch_id": row.batch_id,
                "pipeline_type": row.pipeline_type,
                "detected_at": row.stall_detected_at.isoformat() if row.stall_detected_at else None,
                "status": row.status,
                "self_healed_at": row.self_healed_at.isoformat() if row.self_healed_at else None
            })

        # Get recent self-heals
        result = await db.execute(text("""
            SELECT
                batch_id,
                pipeline_type,
                stall_detected_at,
                self_healed_at,
                batch_duration_ms
            FROM pipeline_batch_metrics
            WHERE status = 'self_healed'
            ORDER BY self_healed_at DESC
            LIMIT 10
        """))

        for row in result.fetchall():
            status["recent_self_heals"].append({
                "batch_id": row.batch_id,
                "pipeline_type": row.pipeline_type,
                "stall_detected_at": row.stall_detected_at.isoformat() if row.stall_detected_at else None,
                "self_healed_at": row.self_healed_at.isoformat() if row.self_healed_at else None,
                "duration_ms": row.batch_duration_ms
            })

        return status


# CLI entry point for manual testing
if __name__ == "__main__":
    import os
    import sys

    # Add project root to path
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

    async def main():
        print("Running manual stall detection sweep...")
        result = await run_stall_detection_job()
        print(f"\nResults:")
        for key, value in result.items():
            print(f"  {key}: {value}")

    asyncio.run(main())
