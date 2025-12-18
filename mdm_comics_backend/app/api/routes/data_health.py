"""
Data Health Admin API v1.0.0

Per 20251207_MDM_COMICS_DATA_ACQUISITION_PIPELINE.json:
- Admin portal for data health monitoring
- Merge/conflict review
- Error triage interface
- ETL metrics dashboard

Requires admin role for all endpoints.
"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.core.adapter_registry import adapter_registry
from app.core.utils import utcnow
from app.models.pipeline import (
    DeadLetterQueue,
    DLQStatus,
    PipelineCheckpoint,
    DataQuarantine,
    QuarantineReason,
    FieldChangelog,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data-health", tags=["Data Health Admin"])


# ==============================================================================
# Data Health Overview
# ==============================================================================

@router.get("/overview")
async def get_data_health_overview(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Get overall data health metrics.

    Returns counts, statuses, and recent activity.
    Gracefully handles missing tables (returns 0 counts).
    """
    now = utcnow()
    last_24h = now - timedelta(hours=24)

    # Get adapter statuses
    adapter_stats = adapter_registry.get_stats()

    # Dead Letter Queue stats (graceful if table missing)
    dlq_pending_count = 0
    try:
        dlq_pending = await db.execute(
            text("SELECT COUNT(*) FROM dead_letter_queue WHERE status = 'pending'")
        )
        dlq_pending_count = dlq_pending.scalar() or 0
    except Exception as e:
        logger.warning(f"[data_health] dead_letter_queue query failed: {e}")
        await db.rollback()

    # Quarantine stats (graceful if table missing)
    quarantine_pending_count = 0
    try:
        quarantine_pending = await db.execute(
            text("SELECT COUNT(*) FROM data_quarantine WHERE is_resolved = false")
        )
        quarantine_pending_count = quarantine_pending.scalar() or 0
    except Exception as e:
        logger.warning(f"[data_health] data_quarantine query failed: {e}")
        await db.rollback()

    # Recent changes (last 24h) - graceful if table missing
    changes_24h_count = 0
    try:
        changes_24h = await db.execute(
            text("""
                SELECT COUNT(*) FROM price_changelog
                WHERE changed_at >= :since
            """),
            {"since": last_24h}
        )
        changes_24h_count = changes_24h.scalar() or 0
    except Exception as e:
        logger.warning(f"[data_health] price_changelog query failed: {e}")
        await db.rollback()

    # Pipeline job status (graceful if table missing)
    running_jobs_count = 0
    try:
        running_jobs = await db.execute(
            text("SELECT COUNT(*) FROM pipeline_checkpoints WHERE is_running = true")
        )
        running_jobs_count = running_jobs.scalar() or 0
    except Exception as e:
        logger.warning(f"[data_health] pipeline_checkpoints query failed: {e}")
        await db.rollback()

    # Get last sync times for each entity type (graceful if table missing)
    last_sync_by_type = {}
    try:
        last_syncs = await db.execute(
            text("""
                SELECT entity_type, MAX(changed_at) as last_sync
                FROM price_changelog
                GROUP BY entity_type
            """)
        )
        last_sync_by_type = {
            row[0]: row[1].isoformat() if row[1] else None
            for row in last_syncs.fetchall()
        }
    except Exception as e:
        logger.warning(f"[data_health] price_changelog sync times query failed: {e}")
        await db.rollback()

    return {
        "status": "healthy" if dlq_pending_count < 100 else "degraded",
        "timestamp": now.isoformat(),
        "adapters": adapter_stats,
        "dead_letter_queue": {
            "pending_count": dlq_pending_count,
        },
        "quarantine": {
            "pending_count": quarantine_pending_count,
        },
        "changes": {
            "last_24h": changes_24h_count,
        },
        "pipeline": {
            "running_jobs": running_jobs_count,
        },
        "last_sync_by_type": last_sync_by_type,
    }


# ==============================================================================
# Dead Letter Queue Management
# ==============================================================================

@router.get("/dlq")
async def get_dead_letter_queue(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
    status: Optional[str] = Query(None, description="Filter by status"),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
) -> Dict[str, Any]:
    """Get dead letter queue entries."""
    query = "SELECT * FROM dead_letter_queue WHERE 1=1"
    params = {}

    if status:
        query += " AND status = :status"
        params["status"] = status
    if job_type:
        query += " AND job_type = :job_type"
        params["job_type"] = job_type

    query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset

    result = await db.execute(text(query), params)
    rows = result.fetchall()

    # Get total count
    count_query = "SELECT COUNT(*) FROM dead_letter_queue WHERE 1=1"
    if status:
        count_query += " AND status = :status"
    if job_type:
        count_query += " AND job_type = :job_type"

    count_result = await db.execute(text(count_query), params)
    total = count_result.scalar() or 0

    return {
        "items": [
            {
                "id": row.id,
                "job_type": row.job_type,
                "entity_type": row.entity_type,
                "entity_id": row.entity_id,
                "external_id": row.external_id,
                "error_message": row.error_message[:500] if row.error_message else None,
                "error_type": row.error_type,
                "status": row.status,
                "retry_count": row.retry_count,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
            for row in rows
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.post("/dlq/{dlq_id}/retry")
async def retry_dlq_entry(
    dlq_id: int,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """Mark a DLQ entry for retry."""
    result = await db.execute(
        text("""
            UPDATE dead_letter_queue
            SET status = 'pending',
                retry_count = retry_count + 1,
                next_retry_at = NOW()
            WHERE id = :id AND status != 'resolved'
            RETURNING id
        """),
        {"id": dlq_id}
    )
    await db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="DLQ entry not found or already resolved")

    return {"success": True, "message": "Entry marked for retry"}


@router.post("/dlq/{dlq_id}/resolve")
async def resolve_dlq_entry(
    dlq_id: int,
    notes: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """Manually resolve a DLQ entry."""
    result = await db.execute(
        text("""
            UPDATE dead_letter_queue
            SET status = 'resolved',
                resolved_at = NOW(),
                resolved_by_user_id = :user_id,
                resolution_notes = :notes
            WHERE id = :id
            RETURNING id
        """),
        {"id": dlq_id, "user_id": current_user.id, "notes": notes}
    )
    await db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="DLQ entry not found")

    return {"success": True, "message": "Entry resolved"}


# ==============================================================================
# Data Quarantine Management
# ==============================================================================

@router.get("/quarantine")
async def get_quarantine_entries(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
    reason: Optional[str] = Query(None, description="Filter by quarantine reason"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    is_resolved: bool = Query(False, description="Include resolved entries"),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
) -> Dict[str, Any]:
    """Get quarantined data entries."""
    query = "SELECT * FROM data_quarantine WHERE 1=1"
    params = {}

    if reason:
        query += " AND reason = :reason"
        params["reason"] = reason
    if entity_type:
        query += " AND entity_type = :entity_type"
        params["entity_type"] = entity_type
    if not is_resolved:
        query += " AND is_resolved = false"

    query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset

    result = await db.execute(text(query), params)
    rows = result.fetchall()

    # Get total count
    count_query = "SELECT COUNT(*) FROM data_quarantine WHERE 1=1"
    if reason:
        count_query += " AND reason = :reason"
    if entity_type:
        count_query += " AND entity_type = :entity_type"
    if not is_resolved:
        count_query += " AND is_resolved = false"

    count_result = await db.execute(text(count_query), params)
    total = count_result.scalar() or 0

    return {
        "items": [
            {
                "id": row.id,
                "entity_type": row.entity_type,
                "entity_id": row.entity_id,
                "reason": row.reason,
                "confidence_score": float(row.confidence_score) if row.confidence_score else None,
                "data_source": row.data_source,
                "quarantined_data": row.quarantined_data,
                "potential_match_ids": row.potential_match_ids,
                "is_resolved": row.is_resolved,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
            for row in rows
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.post("/quarantine/{quarantine_id}/resolve")
async def resolve_quarantine_entry(
    quarantine_id: int,
    action: str = Query(..., description="Resolution action: accept, reject, merge, manual_edit"),
    notes: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """Resolve a quarantined data entry."""
    valid_actions = ["accept", "reject", "merge", "manual_edit"]
    if action not in valid_actions:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid action. Must be one of: {valid_actions}"
        )

    result = await db.execute(
        text("""
            UPDATE data_quarantine
            SET is_resolved = true,
                resolved_at = NOW(),
                resolved_by_user_id = :user_id,
                resolution_action = :action,
                resolution_notes = :notes
            WHERE id = :id
            RETURNING id
        """),
        {
            "id": quarantine_id,
            "user_id": current_user.id,
            "action": action,
            "notes": notes
        }
    )
    await db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Quarantine entry not found")

    return {"success": True, "message": f"Entry resolved with action: {action}"}


# ==============================================================================
# Pipeline Job Management
# ==============================================================================

@router.get("/pipeline/jobs")
async def get_pipeline_jobs(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """Get all pipeline job checkpoints."""
    result = await db.execute(
        text("""
            SELECT * FROM pipeline_checkpoints
            ORDER BY last_run_started DESC NULLS LAST
        """)
    )
    rows = result.fetchall()

    return {
        "jobs": [
            {
                "id": row.id,
                "job_name": row.job_name,
                "job_type": row.job_type,
                "is_running": row.is_running,
                "total_processed": row.total_processed,
                "total_updated": row.total_updated,
                "total_errors": row.total_errors,
                "last_run_started": row.last_run_started.isoformat() if row.last_run_started else None,
                "last_run_completed": row.last_run_completed.isoformat() if row.last_run_completed else None,
                "last_error": row.last_error[:200] if row.last_error else None,
            }
            for row in rows
        ]
    }


@router.post("/pipeline/jobs/{job_name}/reset")
async def reset_pipeline_job(
    job_name: str,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """Reset a pipeline job checkpoint."""
    result = await db.execute(
        text("""
            UPDATE pipeline_checkpoints
            SET is_running = false,
                last_processed_id = NULL,
                last_page = NULL,
                cursor = NULL,
                last_error = NULL
            WHERE job_name = :job_name
            RETURNING id
        """),
        {"job_name": job_name}
    )
    await db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Job not found")

    return {"success": True, "message": "Job checkpoint reset"}


# ==============================================================================
# Change History / Changelog
# ==============================================================================

@router.get("/changelog")
async def get_changelog(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    data_source: Optional[str] = Query(None, description="Filter by data source"),
    days: int = Query(7, le=90, description="Number of days to look back"),
    limit: int = Query(100, le=500),
    offset: int = Query(0),
) -> Dict[str, Any]:
    """Get recent change history."""
    since = utcnow() - timedelta(days=days)

    query = """
        SELECT * FROM price_changelog
        WHERE changed_at >= :since
    """
    params = {"since": since}

    if entity_type:
        query += " AND entity_type = :entity_type"
        params["entity_type"] = entity_type
    if data_source:
        query += " AND data_source = :data_source"
        params["data_source"] = data_source

    query += " ORDER BY changed_at DESC LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset

    result = await db.execute(text(query), params)
    rows = result.fetchall()

    return {
        "changes": [
            {
                "id": row.id,
                "entity_type": row.entity_type,
                "entity_id": row.entity_id,
                "entity_name": row.entity_name,
                "field_name": row.field_name,
                "old_value": float(row.old_value) if row.old_value else None,
                "new_value": float(row.new_value) if row.new_value else None,
                "change_pct": float(row.change_pct) if row.change_pct else None,
                "data_source": row.data_source,
                "reason": row.reason,
                "changed_at": row.changed_at.isoformat() if row.changed_at else None,
            }
            for row in rows
        ],
        "days_included": days,
        "limit": limit,
        "offset": offset,
    }


@router.get("/changelog/summary")
async def get_changelog_summary(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
    days: int = Query(7, le=90),
) -> Dict[str, Any]:
    """Get summary statistics for recent changes."""
    since = utcnow() - timedelta(days=days)

    # Get counts by entity type
    by_type = await db.execute(
        text("""
            SELECT entity_type, COUNT(*) as change_count
            FROM price_changelog
            WHERE changed_at >= :since
            GROUP BY entity_type
        """),
        {"since": since}
    )
    by_type_data = {row[0]: row[1] for row in by_type.fetchall()}

    # Get counts by source
    by_source = await db.execute(
        text("""
            SELECT data_source, COUNT(*) as change_count
            FROM price_changelog
            WHERE changed_at >= :since
            GROUP BY data_source
        """),
        {"since": since}
    )
    by_source_data = {row[0]: row[1] for row in by_source.fetchall()}

    # Get significant changes (>10%)
    significant = await db.execute(
        text("""
            SELECT COUNT(*)
            FROM price_changelog
            WHERE changed_at >= :since AND ABS(change_pct) >= 10
        """),
        {"since": since}
    )
    significant_count = significant.scalar() or 0

    # Get average change percentage
    avg_change = await db.execute(
        text("""
            SELECT AVG(ABS(change_pct))
            FROM price_changelog
            WHERE changed_at >= :since AND change_pct IS NOT NULL
        """),
        {"since": since}
    )
    avg_change_pct = avg_change.scalar() or 0

    return {
        "period_days": days,
        "since": since.isoformat(),
        "total_changes": sum(by_type_data.values()),
        "by_entity_type": by_type_data,
        "by_source": by_source_data,
        "significant_changes": significant_count,
        "avg_change_pct": round(float(avg_change_pct), 2) if avg_change_pct else 0,
    }


# ==============================================================================
# Adapter Health
# ==============================================================================

@router.get("/adapters")
async def get_adapter_status(
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """Get status of all data source adapters."""
    return adapter_registry.get_stats()


@router.post("/adapters/{adapter_name}/enable")
async def enable_adapter(
    adapter_name: str,
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """Enable a data source adapter."""
    if adapter_registry.enable(adapter_name):
        return {"success": True, "message": f"Adapter '{adapter_name}' enabled"}
    raise HTTPException(status_code=404, detail=f"Adapter '{adapter_name}' not found")


@router.post("/adapters/{adapter_name}/disable")
async def disable_adapter(
    adapter_name: str,
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """Disable a data source adapter."""
    if adapter_registry.disable(adapter_name):
        return {"success": True, "message": f"Adapter '{adapter_name}' disabled"}
    raise HTTPException(status_code=404, detail=f"Adapter '{adapter_name}' not found")


@router.post("/adapters/health-check")
async def run_adapter_health_checks(
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """Run health checks on all adapters."""
    results = await adapter_registry.health_check_all()
    return {
        "results": results,
        "healthy_count": sum(1 for v in results.values() if v),
        "unhealthy_count": sum(1 for v in results.values() if not v),
    }


# ==============================================================================
# Pipeline Job Control
# ==============================================================================

@router.post("/jobs/{job_name}/run")
async def run_job_manually(
    job_name: str,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """Manually trigger a pipeline job."""
    try:
        from app.jobs.pipeline_scheduler import pipeline_scheduler
        await pipeline_scheduler.run_job_now(job_name)
        return {"success": True, "message": f"Job '{job_name}' triggered"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Job failed: {str(e)}")


@router.post("/jobs/{job_name}/start")
async def start_job(
    job_name: str,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Start or resume a pipeline job.

    v1.20.0: Sets control_signal='run' and triggers the job.
    If job was paused, it resumes from last checkpoint.
    """
    # Clear any pause/stop signal and set to run
    result = await db.execute(text("""
        UPDATE pipeline_checkpoints
        SET control_signal = 'run',
            paused_at = NULL,
            updated_at = NOW()
        WHERE job_name = :name
        RETURNING id, state_data, is_running
    """), {"name": job_name})
    row = result.fetchone()
    await db.commit()

    if not row:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found")

    if row.is_running:
        return {
            "success": True,
            "message": f"Job '{job_name}' is already running",
            "status": "running",
            "last_id": row.state_data.get("last_id") if row.state_data else None
        }

    # Trigger the job
    try:
        from app.jobs.pipeline_scheduler import pipeline_scheduler
        await pipeline_scheduler.run_job_now(job_name)
        return {
            "success": True,
            "message": f"Job '{job_name}' started",
            "status": "started",
            "last_id": row.state_data.get("last_id") if row.state_data else None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start job: {str(e)}")


@router.post("/jobs/{job_name}/pause")
async def pause_job(
    job_name: str,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Pause a running pipeline job.

    v1.20.0: Sets control_signal='pause'. Job will checkpoint immediately
    and stop processing. Cron will auto-resume on next cycle.

    State is preserved - resume continues from last checkpoint.
    """
    result = await db.execute(text("""
        UPDATE pipeline_checkpoints
        SET control_signal = 'pause',
            paused_at = NOW(),
            paused_by_user_id = :user_id,
            updated_at = NOW()
        WHERE job_name = :name
        RETURNING id, is_running, state_data
    """), {"name": job_name, "user_id": current_user.id})
    row = result.fetchone()
    await db.commit()

    if not row:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found")

    last_id = row.state_data.get("last_id") if row.state_data else None

    if not row.is_running:
        return {
            "success": True,
            "message": f"Job '{job_name}' is not running (already idle)",
            "status": "idle",
            "last_id": last_id
        }

    logger.info(f"[JOB_CONTROL] Pause requested for {job_name} by user {current_user.id}")

    return {
        "success": True,
        "message": f"Pause signal sent to '{job_name}'. Job will pause at next checkpoint.",
        "status": "pause_requested",
        "last_id": last_id,
        "note": "Cron will auto-resume on next cycle if not manually restarted"
    }


@router.post("/jobs/{job_name}/stop")
async def stop_job(
    job_name: str,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Stop a running pipeline job.

    v1.20.0: Sets control_signal='stop'. Job will checkpoint immediately,
    stop processing, and release the lock. Cron will auto-resume on next cycle.

    State is preserved - resume continues from last checkpoint.
    NOTE: State is NEVER reset. Use /reset endpoint to start from beginning.
    """
    result = await db.execute(text("""
        UPDATE pipeline_checkpoints
        SET control_signal = 'stop',
            paused_at = NOW(),
            paused_by_user_id = :user_id,
            updated_at = NOW()
        WHERE job_name = :name
        RETURNING id, is_running, state_data
    """), {"name": job_name, "user_id": current_user.id})
    row = result.fetchone()
    await db.commit()

    if not row:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found")

    last_id = row.state_data.get("last_id") if row.state_data else None

    if not row.is_running:
        return {
            "success": True,
            "message": f"Job '{job_name}' is not running (already idle)",
            "status": "idle",
            "last_id": last_id
        }

    logger.info(f"[JOB_CONTROL] Stop requested for {job_name} by user {current_user.id}")

    return {
        "success": True,
        "message": f"Stop signal sent to '{job_name}'. Job will stop at next checkpoint.",
        "status": "stop_requested",
        "last_id": last_id,
        "note": "Cron will auto-resume on next cycle. State preserved."
    }


@router.get("/jobs/{job_name}/status")
async def get_job_status(
    job_name: str,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Get detailed status of a pipeline job.

    v1.20.0: Includes control_signal state for pause/stop awareness.
    """
    result = await db.execute(text("""
        SELECT
            job_name,
            job_type,
            is_running,
            control_signal,
            total_processed,
            total_updated,
            total_errors,
            state_data,
            last_run_started,
            last_run_completed,
            last_error,
            paused_at,
            updated_at
        FROM pipeline_checkpoints
        WHERE job_name = :name
    """), {"name": job_name})
    row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found")

    # Determine effective status
    if row.is_running:
        if row.control_signal == 'pause':
            status = "pausing"
        elif row.control_signal == 'stop':
            status = "stopping"
        else:
            status = "running"
    else:
        if row.control_signal == 'pause':
            status = "paused"
        elif row.control_signal == 'stop':
            status = "stopped"
        else:
            status = "idle"

    return {
        "job_name": row.job_name,
        "job_type": row.job_type,
        "status": status,
        "is_running": row.is_running,
        "control_signal": row.control_signal,
        "progress": {
            "total_processed": row.total_processed,
            "total_updated": row.total_updated,
            "total_errors": row.total_errors,
            "last_id": row.state_data.get("last_id") if row.state_data else None,
        },
        "timing": {
            "last_run_started": row.last_run_started.isoformat() if row.last_run_started else None,
            "last_run_completed": row.last_run_completed.isoformat() if row.last_run_completed else None,
            "paused_at": row.paused_at.isoformat() if row.paused_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        },
        "last_error": row.last_error[:500] if row.last_error else None,
    }


# ==============================================================================
# Database Migrations (Admin-Only)
# ==============================================================================

@router.post("/migrations/job-control")
async def run_job_control_migration(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    v1.20.0: Add job control columns to pipeline_checkpoints.

    Adds:
    - control_signal: 'run', 'pause', 'stop'
    - paused_at: Timestamp when paused
    - paused_by_user_id: Who paused the job

    Safe to run multiple times (uses IF NOT EXISTS pattern).
    """
    try:
        # Check if column already exists
        check = await db.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_name = 'pipeline_checkpoints' AND column_name = 'control_signal'
            )
        """))
        exists = check.scalar()

        if exists:
            return {
                "success": True,
                "message": "Job control columns already exist",
                "status": "already_migrated"
            }

        # Add the columns
        await db.execute(text("""
            ALTER TABLE pipeline_checkpoints
            ADD COLUMN IF NOT EXISTS control_signal VARCHAR(20) DEFAULT 'run' NOT NULL,
            ADD COLUMN IF NOT EXISTS paused_at TIMESTAMP WITH TIME ZONE,
            ADD COLUMN IF NOT EXISTS paused_by_user_id INTEGER REFERENCES users(id)
        """))

        # Add index for control_signal
        await db.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_checkpoint_control
            ON pipeline_checkpoints (control_signal)
        """))

        await db.commit()

        logger.info("[MIGRATION] Job control columns added to pipeline_checkpoints")

        return {
            "success": True,
            "message": "Job control migration completed",
            "columns_added": ["control_signal", "paused_at", "paused_by_user_id"],
            "index_created": "ix_checkpoint_control"
        }

    except Exception as e:
        logger.error(f"[MIGRATION] Job control migration failed: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Migration failed: {str(e)}")


@router.post("/migrations/price-snapshots")
async def run_price_snapshots_migration(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Run the price_snapshots table migration.

    Creates the price_snapshots table and indexes for AI/ML training data.
    Safe to run multiple times (uses IF NOT EXISTS).
    """
    try:
        # Check if table already exists
        check = await db.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'price_snapshots'
            )
        """))
        table_exists = check.scalar()

        if table_exists:
            # Get row count
            count_result = await db.execute(text("SELECT COUNT(*) FROM price_snapshots"))
            row_count = count_result.scalar() or 0
            return {
                "success": True,
                "message": "Table already exists",
                "table": "price_snapshots",
                "row_count": row_count
            }

        # Create the price_snapshots table
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS price_snapshots (
                id BIGSERIAL PRIMARY KEY,
                snapshot_date DATE NOT NULL,
                entity_type VARCHAR(50) NOT NULL,
                entity_id INTEGER NOT NULL,
                pricecharting_id INTEGER,
                price_loose NUMERIC(12, 2),
                price_cib NUMERIC(12, 2),
                price_new NUMERIC(12, 2),
                price_graded NUMERIC(12, 2),
                price_bgs_10 NUMERIC(12, 2),
                price_cgc_98 NUMERIC(12, 2),
                price_cgc_96 NUMERIC(12, 2),
                sales_volume INTEGER,
                price_changed BOOLEAN NOT NULL DEFAULT FALSE,
                days_since_change INTEGER,
                volatility_7d NUMERIC(8, 4),
                volatility_30d NUMERIC(8, 4),
                trend_7d NUMERIC(8, 4),
                trend_30d NUMERIC(8, 4),
                momentum NUMERIC(8, 4),
                data_source VARCHAR(50) NOT NULL DEFAULT 'pricecharting',
                confidence_score NUMERIC(3, 2),
                is_stale BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                CONSTRAINT ck_price_snapshots_entity_type
                    CHECK (entity_type IN ('funko', 'comic')),
                CONSTRAINT ck_price_snapshots_confidence_range
                    CHECK (confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 1))
            )
        """))

        # Create indexes
        indexes = [
            ("idx_price_snapshots_lookup", "CREATE INDEX IF NOT EXISTS idx_price_snapshots_lookup ON price_snapshots (entity_type, entity_id, snapshot_date DESC)"),
            ("idx_price_snapshots_date", "CREATE INDEX IF NOT EXISTS idx_price_snapshots_date ON price_snapshots (snapshot_date DESC)"),
            ("idx_price_snapshots_pricecharting", "CREATE INDEX IF NOT EXISTS idx_price_snapshots_pricecharting ON price_snapshots (pricecharting_id) WHERE pricecharting_id IS NOT NULL"),
            ("idx_price_snapshots_volatile", "CREATE INDEX IF NOT EXISTS idx_price_snapshots_volatile ON price_snapshots (volatility_30d DESC NULLS LAST) WHERE volatility_30d IS NOT NULL"),
            ("idx_price_snapshots_stale", "CREATE INDEX IF NOT EXISTS idx_price_snapshots_stale ON price_snapshots (entity_type, entity_id) WHERE is_stale = TRUE"),
            ("idx_price_snapshots_unique", "CREATE UNIQUE INDEX IF NOT EXISTS idx_price_snapshots_unique ON price_snapshots (entity_type, entity_id, snapshot_date)"),
        ]

        created_indexes = []
        for name, sql in indexes:
            await db.execute(text(sql))
            created_indexes.append(name)

        await db.commit()

        return {
            "success": True,
            "message": "Migration completed successfully",
            "table": "price_snapshots",
            "indexes_created": created_indexes
        }

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Migration failed: {str(e)}")
