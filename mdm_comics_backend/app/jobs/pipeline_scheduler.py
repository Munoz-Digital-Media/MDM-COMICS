"""
Pipeline Job Scheduler v1.24.0

Automated data acquisition jobs that ACTUALLY RUN.

Jobs:
1. Comic Enrichment - Fetch metadata from Metron, match to existing comics
2. Funko Enrichment - Fetch data from PriceCharting, update prices
3. DLQ Retry - Retry failed jobs from dead letter queue
4. Quarantine Cleanup - Auto-resolve old low-priority quarantine items
5. Daily Price Snapshot - Capture price state for AI/ML training (v1.7.0)
6. GCD Import - Import comics from Grand Comics Database SQLite dump (v1.8.0)
7. Self-Healing - Auto-detect and restart stalled jobs (v1.9.0)
8. Full Price Sync - Process entire DB for PriceCharting prices (v1.9.3)
9. Cover Hash Backfill - Generate perceptual hashes for image search (v1.9.4)
10. Image Acquisition - Download covers to S3, generate thumbnails+hashes (v1.9.5)
11. Multi-Source Enrichment - Rotate between Metron/ComicVine with failover (v1.10.0)
12. Stall Detection - Adaptive stall detection with data-driven thresholds (v1.24.0)
13. Metrics Retention - 90-day retention cleanup for pipeline metrics (v1.24.0)

All jobs use checkpoints for crash recovery and log to DLQ on failure.

Self-Healing (v1.9.0):
- Runs every 10 minutes to detect stalled jobs
- Job considered stalled if no checkpoint update for 15+ minutes
- Auto-restarts stuck jobs (max 5 per day per job)
- Prevents infinite restart loops with daily rate limiting

Automatic Offset Sync (v1.9.2):
- GCD import job ALWAYS syncs offset to actual DB count on exit
- Ensures correct resume position whether job completes, fails, or is killed
- Prevents re-processing of already-imported records after interruption

Pipeline Instrumentation (v1.24.0):
- Tracks batch metrics with millisecond precision
- Records API call performance per source
- Uses adaptive stall thresholds from P95 percentiles
- 90-day retention per governance requirements
"""
import asyncio
import json
import logging
import traceback
from datetime import datetime, timedelta, date
from decimal import Decimal
from typing import Optional, List, Dict, Any
from uuid import uuid4

from sqlalchemy import text, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.core.utils import utcnow
from app.core.http_client import get_pricecharting_client
from app.adapters.metron_adapter import MetronAdapter
from app.utils.db_sanitizer import sanitize_date, sanitize_decimal, sanitize_string
from app.models.pipeline import (
    PipelineCheckpoint,
    DeadLetterQueue,
    DLQStatus,
    DataQuarantine,
    QuarantineReason,
    FieldChangelog,
    ChangeReason,
    FieldProvenance,
)

# v1.13.0: Sequential exhaustive enrichment
from app.jobs.sequential_enrichment import run_sequential_exhaustive_enrichment_job

# v1.21.0: Inbound cover processor
from app.services.inbound_processor import run_inbound_processor

# v1.22.0: BCW Dropship Integration
from app.jobs.bcw_sync import (
    run_bcw_inventory_sync_job,
    run_bcw_full_inventory_sync_job,
    run_bcw_order_status_sync_job,
    run_bcw_email_processing_job,
    run_bcw_quote_cleanup_job,
    run_bcw_selector_health_job,
)

# v1.23.0: PriceCharting Independent Jobs (Autonomous Resilience System)
from app.jobs.pricecharting_jobs import (
    run_funko_pricecharting_match_job,
    run_comic_pricecharting_match_job,
    run_funko_price_sync_job,
    run_comic_price_sync_job,
)

# v1.24.0: Pipeline Instrumentation (Stall Detection & Metrics)
from app.jobs.stall_detector import run_stall_detection_job
from app.jobs.metrics_retention import run_metrics_retention_job
from app.jobs.conventions_refresh import run_convention_refresh_job

logger = logging.getLogger(__name__)


# =============================================================================
# CHECKPOINT MANAGEMENT
# =============================================================================

# Stale checkpoint timeout in hours - if a job claims to be "running" for longer
# than this, we assume it crashed and clear the flag
# PC-ANALYSIS-2025-12-18: Reduced from 4 to 1 hour to minimize lockout window after crashes
STALE_CHECKPOINT_TIMEOUT_HOURS = 1

# Self-healing configuration
SELF_HEAL_CHECK_INTERVAL_MINUTES = 10  # How often to check for stalled jobs
SELF_HEAL_STALL_THRESHOLD_MINUTES = 15  # Job considered stalled if no progress for this long
SELF_HEAL_MAX_AUTO_RESTARTS = 5  # Max auto-restarts before giving up (per 24h period)
# v1.23.0: Auto-unpause threshold (Autonomous Resilience System)
SELF_HEAL_STALE_PAUSE_THRESHOLD_MINUTES = 30  # Auto-unpause jobs paused longer than this


async def clear_stale_checkpoints(db: AsyncSession) -> int:
    """
    Clear is_running flags for jobs that have been "running" for too long.

    This handles the case where a container was killed mid-job, leaving
    is_running = true forever. Returns the number of stale checkpoints cleared.

    v1.10.1: For gcd_import, also sync offset to actual DB count to prevent
    re-processing already-imported records.
    """
    result = await db.execute(text("""
        UPDATE pipeline_checkpoints
        SET is_running = false,
            last_error = COALESCE(last_error, '') || E'\nCleared stale checkpoint at ' || NOW()::text
        WHERE is_running = true
        AND last_run_started IS NOT NULL
        AND last_run_started < NOW() - make_interval(hours => :timeout_hours)
        RETURNING job_name
    """), {"timeout_hours": STALE_CHECKPOINT_TIMEOUT_HOURS})

    cleared = result.fetchall()
    if cleared:
        await db.commit()
        for row in cleared:
            logger.warning(f"[CHECKPOINT] Cleared stale checkpoint for job: {row.job_name}")

            # v1.10.1: Sync GCD import offset to actual DB count
            # This prevents re-processing records that were already imported
            # before the job was killed/timed out
            if row.job_name == "gcd_import":
                try:
                    count_result = await db.execute(text(
                        "SELECT COUNT(*) FROM comic_issues WHERE gcd_id IS NOT NULL"
                    ))
                    actual_count = count_result.scalar() or 0

                    await db.execute(text("""
                        UPDATE pipeline_checkpoints
                        SET state_data = jsonb_build_object('offset', :offset),
                            total_processed = :offset
                        WHERE job_name = 'gcd_import'
                    """), {"offset": actual_count})
                    await db.commit()

                    logger.info(
                        f"[CHECKPOINT] Synced gcd_import offset to actual DB count: {actual_count:,}"
                    )
                except Exception as e:
                    logger.error(f"[CHECKPOINT] Failed to sync gcd_import offset: {e}")

    return len(cleared)


async def get_or_create_checkpoint(db: AsyncSession, job_name: str, job_type: str) -> dict:
    """Get existing checkpoint or create new one."""
    result = await db.execute(
        text("SELECT * FROM pipeline_checkpoints WHERE job_name = :name"),
        {"name": job_name}
    )
    row = result.fetchone()

    if row:
        return {
            "id": row.id,
            "job_name": row.job_name,
            "last_processed_id": row.last_processed_id,
            "last_page": row.last_page,
            "cursor": row.cursor,
            "total_processed": row.total_processed,
            "total_updated": row.total_updated,
            "total_errors": row.total_errors,
            "state_data": row.state_data,
            "is_running": row.is_running,
        }

    # Create new checkpoint
    await db.execute(
        text("""
            INSERT INTO pipeline_checkpoints (job_name, job_type, created_at, updated_at)
            VALUES (:name, :type, NOW(), NOW())
        """),
        {"name": job_name, "type": job_type}
    )
    await db.commit()

    return {
        "job_name": job_name,
        "last_processed_id": None,
        "last_page": None,
        "cursor": None,
        "total_processed": 0,
        "total_updated": 0,
        "total_errors": 0,
        "state_data": None,
        "is_running": False,
    }


async def try_claim_job(db: AsyncSession, job_name: str, job_type: str, batch_id: str) -> tuple[bool, dict]:
    """
    Atomically try to claim a job for execution.

    Uses SELECT FOR UPDATE + conditional UPDATE to prevent race conditions
    where multiple instances try to start the same job simultaneously.

    Returns:
        Tuple of (claimed: bool, checkpoint: dict)
        - If claimed=True, the job is now locked to this instance
        - If claimed=False, another instance is running or claimed first
    """
    # First ensure checkpoint exists
    await db.execute(
        text("""
            INSERT INTO pipeline_checkpoints (job_name, job_type, created_at, updated_at)
            VALUES (:name, :type, NOW(), NOW())
            ON CONFLICT (job_name) DO NOTHING
        """),
        {"name": job_name, "type": job_type}
    )

    # Atomically claim the job with FOR UPDATE SKIP LOCKED
    # This prevents multiple instances from both getting the same row
    result = await db.execute(
        text("""
            UPDATE pipeline_checkpoints
            SET is_running = true,
                last_run_started = NOW(),
                current_batch_id = :batch_id,
                updated_at = NOW()
            WHERE job_name = :name
            AND is_running = false
            RETURNING id, job_name, last_processed_id, last_page, cursor,
                      total_processed, total_updated, total_errors, state_data
        """),
        {"name": job_name, "batch_id": batch_id}
    )
    row = result.fetchone()
    await db.commit()

    if row:
        return True, {
            "id": row.id,
            "job_name": row.job_name,
            "last_processed_id": row.last_processed_id,
            "last_page": row.last_page,
            "cursor": row.cursor,
            "total_processed": row.total_processed,
            "total_updated": row.total_updated,
            "total_errors": row.total_errors,
            "state_data": row.state_data,
            "is_running": True,
        }

    # Could not claim - either already running or race condition
    # Fetch current state for logging
    result = await db.execute(
        text("SELECT * FROM pipeline_checkpoints WHERE job_name = :name"),
        {"name": job_name}
    )
    current = result.fetchone()

    return False, {
        "job_name": job_name,
        "is_running": current.is_running if current else False,
        "last_processed_id": current.last_processed_id if current else None,
    }


async def update_checkpoint(
    db: AsyncSession,
    job_name: str,
    last_processed_id: Optional[int] = None,
    last_page: Optional[int] = None,
    cursor: Optional[str] = None,
    processed_delta: int = 0,
    updated_delta: int = 0,
    errors_delta: int = 0,
    is_running: Optional[bool] = None,
    batch_id: Optional[str] = None,
    last_error: Optional[str] = None,
    state_data: Optional[dict] = None,
):
    """Update checkpoint with progress."""
    updates = ["updated_at = NOW()"]
    params = {"name": job_name}

    if last_processed_id is not None:
        updates.append("last_processed_id = :last_id")
        params["last_id"] = last_processed_id
    if last_page is not None:
        updates.append("last_page = :last_page")
        params["last_page"] = last_page
    if cursor is not None:
        updates.append("cursor = :cursor")
        params["cursor"] = cursor
    if processed_delta:
        updates.append("total_processed = total_processed + :proc_delta")
        params["proc_delta"] = processed_delta
    if updated_delta:
        updates.append("total_updated = total_updated + :upd_delta")
        params["upd_delta"] = updated_delta
    if errors_delta:
        updates.append("total_errors = total_errors + :err_delta")
        params["err_delta"] = errors_delta
    if is_running is not None:
        updates.append("is_running = :running")
        params["running"] = is_running
        if is_running:
            updates.append("last_run_started = NOW()")
        else:
            updates.append("last_run_completed = NOW()")
    if batch_id is not None:
        # Use raw UUID value with SQLAlchemy text - asyncpg handles UUID casting
        updates.append("current_batch_id = :batch_id")
        params["batch_id"] = batch_id  # Pass as string, asyncpg will handle
    if last_error is not None:
        updates.append("last_error = :error")
        params["error"] = last_error[:1000]
    if state_data is not None:
        updates.append("state_data = :state")
        params["state"] = json.dumps(state_data)

    await db.execute(
        text(f"UPDATE pipeline_checkpoints SET {', '.join(updates)} WHERE job_name = :name"),
        params
    )
    await db.commit()


async def add_to_dlq(
    db: AsyncSession,
    job_type: str,
    error_message: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    external_id: Optional[str] = None,
    error_type: Optional[str] = None,
    error_trace: Optional[str] = None,
    request_data: Optional[dict] = None,
    batch_id: Optional[str] = None,
):
    """Add failed job to dead letter queue."""
    await db.execute(
        text("""
            INSERT INTO dead_letter_queue
            (job_type, batch_id, entity_type, entity_id, external_id,
             error_message, error_type, error_trace, created_at, status)
            VALUES (:job_type, :batch_id, :entity_type, :entity_id, :external_id,
                    :error_message, :error_type, :error_trace, NOW(), 'PENDING')
        """),
        {
            "job_type": job_type,
            "batch_id": batch_id,  # asyncpg handles UUID string
            "entity_type": entity_type,
            "entity_id": entity_id,
            "external_id": external_id,
            "error_message": error_message[:2000],
            "error_type": error_type,
            "error_trace": error_trace[:5000] if error_trace else None,
        }
    )
    await db.commit()


# =============================================================================
# COMIC ENRICHMENT JOB (v1.8.0 - GCD-Primary with Metron fallback)
# =============================================================================

async def run_gcd_import_job():
    """
    Import comics from GCD SQLite dump.

    v2.1.0: Full-spectrum ingestion (IMP-20251220-GCD-FULL-INGEST)
    - Multi-pass import: Brands -> Indicia -> Creators -> Characters -> Issues -> Stories -> M2M
    - Populates expanded schema with bibliographic and content data
    - Handles large dataset with memory-efficient streaming
    """
    from app.adapters.gcd import ensure_gcd_dump_exists, GCDAdapter
    from app.core.config import settings
    from sqlalchemy.dialects.postgresql import insert

    job_name = "gcd_import"
    batch_id = str(uuid4())

    if not settings.GCD_IMPORT_ENABLED:
        logger.info(f"[{job_name}] Job disabled in settings")
        return

    # Ensure dump exists
    if not ensure_gcd_dump_exists():
        logger.error(f"[{job_name}] GCD dump not found and download failed")
        return

    logger.info(f"[{job_name}] Starting GCD full import (batch: {batch_id})")

    async with AsyncSessionLocal() as db:
        # Atomically claim job
        claimed, checkpoint = await try_claim_job(db, job_name, "ingestion", batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running, skipping")
            return

        # Restore state
        state_data = checkpoint.get("state_data") or {}
        current_mode = state_data.get("mode", "brands") 
        current_offset = state_data.get("offset", 0)
        
        # Define mode sequence
        MODES = [
            "brands", "indicia_publishers", "creators", "characters", 
            "issues", "stories", "story_credits", "story_characters", "reprints"
        ]
        
        # Fast-forward to current mode
        try:
            mode_index = MODES.index(current_mode)
        except ValueError:
            mode_index = 0
            current_mode = "brands"
            current_offset = 0

        adapter = GCDAdapter()
        stats = {mode: {"processed": 0, "errors": 0} for mode in MODES}

        try:
            for i in range(mode_index, len(MODES)):
                mode = MODES[i]
                logger.info(f"[{job_name}] Starting phase: {mode} (offset: {current_offset})")
                
                await update_checkpoint(db, job_name, last_error=f"Phase: {mode}", batch_id=batch_id)

                limit = settings.GCD_IMPORT_MAX_RECORDS if settings.GCD_IMPORT_MAX_RECORDS > 0 else 0
                
                for batch in adapter.import_from_sqlite(
                    settings.GCD_DUMP_PATH,
                    batch_size=settings.GCD_IMPORT_BATCH_SIZE,
                    offset=current_offset,
                    limit=limit,
                    import_mode=mode
                ):
                    try:
                        # Map mode to table and conflict keys
                        config = {
                            "brands": ("comic_brands", ["gcd_id"]),
                            "indicia_publishers": ("comic_indicia_publishers", ["gcd_id"]),
                            "creators": ("comic_creators", ["gcd_id"]),
                            "characters": ("comic_characters", ["gcd_id"]),
                            "issues": ("comic_issues", ["gcd_id"]),
                            "stories": ("stories", ["gcd_story_id"]),
                            "story_credits": ("story_creators", ["story_id", "creator_id", "role"]),
                            "story_characters": ("story_characters", ["story_id", "character_id"]),
                            "reprints": ("comic_reprints", ["gcd_id"])
                        }
                        
                        target_table, index_elements = config[mode]
                        
                        # Prepare batch data
                        cleaned_batch = []
                        if mode == "indicia_publishers":
                            for rec in batch:
                                if 'is_surrogate' in rec:
                                    rec['is_surrogate'] = bool(rec['is_surrogate'])
                        elif mode == "issues":
                            from dateutil import parser
                            for rec in batch:
                                if rec.get('store_date'):
                                    try:
                                        rec['store_date'] = parser.parse(rec['store_date']).date()
                                    except:
                                        rec['store_date'] = None
                        elif mode == "story_characters":
                            # Map story_characters using subqueries for IDs
                            # Convert 0/1 to Booleans for PG
                            for rec in batch:
                                for key in ['is_origin', 'is_death', 'is_flashback']:
                                    if key in rec:
                                        rec[key] = bool(rec[key])
                        
                        # Get all unique keys in the batch
                        if batch:
                            all_keys = set()
                            for record in batch:
                                all_keys.update(record.keys())
                            
                            for record in batch:
                                cleaned_rec = {k: record.get(k) for k in all_keys if not k.startswith('_')}
                                cleaned_batch.append(cleaned_rec)

                        if cleaned_batch:
                                if mode == "story_credits":
                                    await db.execute(text("""
                                        INSERT INTO story_creators (story_id, creator_id, role, credited_as)
                                        SELECT s.id, c.id, :role, :credited_as
                                        FROM stories s, comic_creators c
                                        WHERE s.gcd_story_id = :gcd_story_id 
                                          AND c.gcd_id = :gcd_creator_id
                                        ON CONFLICT (story_id, creator_id, role) DO NOTHING
                                    """), batch)
                                elif mode == "story_characters":
                                    # Map story_characters using subqueries for IDs
                                    # Convert 0/1 to Booleans for PG
                                    for rec in batch:
                                        for key in ['is_origin', 'is_death', 'is_flashback']:
                                            if key in rec:
                                                rec[key] = bool(rec[key])
                                                
                                    await db.execute(text("""
                                        INSERT INTO story_characters (story_id, character_id, is_origin, is_death, is_flashback)
                                        SELECT s.id, c.id, :is_origin, :is_death, :is_flashback
                                        FROM stories s, comic_characters c
                                        WHERE s.gcd_story_id = :gcd_story_id 
                                          AND c.gcd_id = :gcd_character_id
                                        ON CONFLICT (story_id, character_id) DO NOTHING
                                    """), batch)
                                else:
                                                                    # Generic raw SQL upsert
                                                                    # Filter all_keys for the SQL statement
                                                                    columns = [k for k in all_keys if not k.startswith('_')]
                                                                    col_names = ", ".join(columns)
                                                                    col_placeholders = ", ".join([f":{c}" for c in columns])
                                                                    
                                                                    conflict_clause = ""
                                                                    if mode in ["issues", "stories", "creators", "characters", "brands", "indicia_publishers"]:
                                                                        update_parts = [f"{c} = EXCLUDED.{c}" for c in columns if c not in index_elements and c != 'id']
                                                                        update_parts.append("updated_at = NOW()")
                                                                        conflict_clause = f"ON CONFLICT ({', '.join(index_elements)}) DO UPDATE SET {', '.join(update_parts)}"
                                                                    else:
                                                                        conflict_clause = f"ON CONFLICT ({', '.join(index_elements)}) DO NOTHING"
                                                                    
                                                                    sql = f"INSERT INTO {target_table} ({col_names}) VALUES ({col_placeholders}) {conflict_clause}"
                                                                    await db.execute(text(sql), cleaned_batch)                            
                        stats[mode]["processed"] += len(batch)
                        current_offset += len(batch)

                        await update_checkpoint(
                            db, job_name,
                            processed_delta=len(batch),
                            state_data={"mode": mode, "offset": current_offset}
                        )
                        await db.commit()

                    except Exception as e:
                        logger.error(f"[{job_name}] Error in batch ({mode}): {e}")
                        stats[mode]["errors"] += 1
                        await db.rollback()

                # Phase complete
                logger.info(f"[{job_name}] Phase {mode} complete. Processed {stats[mode]['processed']}")
                current_offset = 0 
                if i < len(MODES) - 1:
                    await update_checkpoint(db, job_name, state_data={"mode": MODES[i+1], "offset": 0})
                
        except Exception as e:
            logger.error(f"[{job_name}] Critical job failure: {e}")
            await update_checkpoint(db, job_name, last_error=str(e), errors_delta=1)
            
        finally:
            await update_checkpoint(db, job_name, is_running=False)
            logger.info(f"[{job_name}] Job finished. Stats: {stats}")



async def run_funko_price_check_job():
    """
    Quick price check for Funkos with PriceCharting IDs.

    This is a FAST job that runs frequently to catch price changes.
    Full sync is done by run_full_price_sync_job() daily.
    """
    job_name = "funko_price_check"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting Funko price check (batch: {batch_id})")

    async with AsyncSessionLocal() as db:
        # Atomically claim job to prevent race condition
        claimed, checkpoint = await try_claim_job(db, job_name, "price_check", batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or claim failed, skipping")
            return

        stats = {"checked": 0, "updated": 0, "errors": 0}

        try:
            import os
            pc_token = os.getenv("PRICECHARTING_API_TOKEN")
            if not pc_token:
                logger.error(f"[{job_name}] PRICECHARTING_API_TOKEN not set")
                await update_checkpoint(db, job_name, is_running=False, last_error="Missing API token")
                return

            async with get_pricecharting_client() as client:
                # Get ALL Funkos with pricecharting_id, ordered by last check
                result = await db.execute(text("""
                    SELECT id, pricecharting_id, price_loose, title
                    FROM funkos
                    WHERE pricecharting_id IS NOT NULL
                    ORDER BY updated_at ASC NULLS FIRST
                """))

                funkos = result.fetchall()

                if not funkos:
                    logger.info(f"[{job_name}] No Funkos to check")
                    await update_checkpoint(db, job_name, is_running=False)
                    return

                logger.info(f"[{job_name}] Checking {len(funkos)} Funkos")

                for funko in funkos:
                    funko_id, pc_id, current_price, title = funko.id, funko.pricecharting_id, funko.price_loose, funko.title

                    try:
                        response = await client.get(
                            "https://www.pricecharting.com/api/product",
                            params={"t": pc_token, "id": pc_id}
                        )

                        if response.status_code != 200:
                            stats["errors"] += 1
                            continue

                        data = response.json()
                        new_price_cents = data.get("loose-price")

                        if new_price_cents is not None:
                            new_price = round(int(new_price_cents) / 100, 2)

                            if current_price is None or abs(float(current_price) - new_price) > 0.01:
                                # Price changed!
                                old_price = float(current_price) if current_price else 0
                                change_pct = ((new_price - old_price) / old_price * 100) if old_price > 0 else 0

                                await db.execute(text("""
                                    UPDATE funkos SET price_loose = :price, updated_at = NOW()
                                    WHERE id = :id
                                """), {"id": funko_id, "price": new_price})

                                # Log the change - asyncpg handles UUID strings natively
                                await db.execute(text("""
                                    INSERT INTO price_changelog
                                    (entity_type, entity_id, entity_name, field_name, old_value, new_value, change_pct, data_source, reason, sync_batch_id)
                                    VALUES ('funko', :id, :name, 'price_loose', :old, :new, :pct, 'pricecharting', 'price_check', :batch)
                                """), {
                                    "id": funko_id,
                                    "name": title[:200] if title else None,
                                    "old": old_price,
                                    "new": new_price,
                                    "pct": round(change_pct, 2),
                                    "batch": batch_id
                                })

                                stats["updated"] += 1
                                logger.info(f"[{job_name}] Price update: {title[:50]} ${old_price:.2f} -> ${new_price:.2f}")

                        stats["checked"] += 1

                    except Exception as e:
                        logger.error(f"[{job_name}] Error checking Funko {funko_id}: {e}")
                        stats["errors"] += 1

                await db.commit()

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            await update_checkpoint(db, job_name, last_error=str(e))

        finally:
            await update_checkpoint(
                db, job_name,
                is_running=False,
                processed_delta=stats["checked"],
                updated_delta=stats["updated"],
                errors_delta=stats["errors"]
            )

        logger.info(f"[{job_name}] Complete: {stats['checked']} checked, {stats['updated']} price changes, {stats['errors']} errors")


# =============================================================================
# FULL PRICE SYNC JOB (REMOVED - IMPL-2025-12-21-PC-REFACTOR PHASE 3)
# =============================================================================
# This job has been removed. Use the independent jobs instead:
#   - run_funko_price_sync_job() for Funkos (in pricecharting_jobs.py)
#   - run_comic_price_sync_job() for Comics (in pricecharting_jobs.py)

async def run_full_price_sync_job():
    """
    REMOVED: Use run_funko_price_sync_job() and run_comic_price_sync_job() instead.

    This stub is kept only for backwards compatibility with any external callers.
    See IMPL-2025-12-21-PC-REFACTOR for migration details.
    """
    logger.error(
        "[REMOVED] run_full_price_sync_job() has been removed. "
        "Use run_funko_price_sync_job() and run_comic_price_sync_job() instead."
    )
    return {
        "status": "error",
        "message": "This job has been removed. Use run_funko_price_sync_job() and run_comic_price_sync_job() instead."
    }


# =============================================================================
# PRICECHARTING MATCHING JOB (REMOVED - IMPL-2025-12-21-PC-REFACTOR PHASE 3)
# =============================================================================
# This job has been removed. Use the independent jobs instead:
#   - run_funko_pricecharting_match_job() for Funkos (in pricecharting_jobs.py)
#   - run_comic_pricecharting_match_job() for Comics (in pricecharting_jobs.py)

async def run_pricecharting_matching_job(batch_size: int = 100, max_records: int = 0):
    """
    REMOVED: Use run_funko_pricecharting_match_job() and run_comic_pricecharting_match_job() instead.

    This stub is kept only for backwards compatibility with any external callers.
    See IMPL-2025-12-21-PC-REFACTOR for migration details.
    """
    logger.error(
        "[REMOVED] run_pricecharting_matching_job() has been removed. "
        "Use run_funko_pricecharting_match_job() and run_comic_pricecharting_match_job() instead."
    )
    return {
        "status": "error",
        "message": "This job has been removed. Use run_funko_pricecharting_match_job() and run_comic_pricecharting_match_job() instead."
    }


# =============================================================================
# COMPREHENSIVE ENRICHMENT JOB (v1.10.3 - All sources, all fields, parallel)
# =============================================================================

async def run_comprehensive_enrichment_job(batch_size: int = 50, max_records: int = 0):
    """
    COMPREHENSIVE multi-source enrichment - queries ALL sources in PARALLEL.

    This job enriches ALL missing fields from ALL available sources simultaneously:

    SOURCES QUERIED (in parallel):
    - Metron API: covers, descriptions, creators, characters, arcs
    - ComicVine API: covers, descriptions, creators, characters
    - ComicBookRealm (scraper): covers, pricing, CGC census, grading
    - MyComicShop (scraper): covers, retail pricing
    - PriceCharting API: market prices (loose, CIB, graded)

    FIELDS ENRICHED:
    - image (cover URL)
    - description
    - price (market/retail)
    - price_loose, price_cib, price_new, price_graded (when available)
    - cgc_census (graded population)

    MERGE PRIORITY:
    1. APIs first (Metron > ComicVine > PriceCharting)
    2. Scrapers fallback (ComicBookRealm > MyComicShop)

    Args:
        batch_size: Records per batch (default 50 - lower due to parallel queries)
        max_records: Max total (0 = unlimited)
    """
    import asyncio as aio
    from app.services.source_rotator import source_rotator, SourceCapability
    from app.adapters import (
        MetronAdapter,
        create_comicvine_adapter,
        create_comicbookrealm_adapter,
        create_mycomicshop_adapter
    )
    import os

    job_name = "comprehensive_enrichment"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting COMPREHENSIVE enrichment (batch: {batch_id})")
    logger.info(f"[{job_name}] Querying: Metron, ComicVine, ComicBookRealm, MyComicShop, PriceCharting")

    # Initialize all adapters
    adapters = {}
    try:
        adapters["metron"] = MetronAdapter()
        logger.info(f"[{job_name}] Metron adapter ready")
    except Exception as e:
        logger.warning(f"[{job_name}] Metron init failed: {e}")

    try:
        adapters["comicvine"] = await create_comicvine_adapter()
        logger.info(f"[{job_name}] ComicVine adapter ready")
    except Exception as e:
        logger.warning(f"[{job_name}] ComicVine init failed: {e}")

    try:
        adapters["comicbookrealm"] = await create_comicbookrealm_adapter()
        logger.info(f"[{job_name}] ComicBookRealm adapter ready")
    except Exception as e:
        logger.warning(f"[{job_name}] ComicBookRealm init failed: {e}")

    try:
        adapters["mycomicshop"] = await create_mycomicshop_adapter()
        logger.info(f"[{job_name}] MyComicShop adapter ready")
    except Exception as e:
        logger.warning(f"[{job_name}] MyComicShop init failed: {e}")

    pc_token = os.getenv("PRICECHARTING_API_TOKEN")
    if pc_token:
        logger.info(f"[{job_name}] PriceCharting API token configured")
    else:
        logger.warning(f"[{job_name}] PriceCharting API token missing")

    stats = {
        "processed": 0,
        "covers_enriched": 0,
        "descriptions_enriched": 0,
        "prices_enriched": 0,
        "failed": 0,
        "by_source": {},
    }

    async with AsyncSessionLocal() as db:
        claimed, checkpoint = await try_claim_job(db, job_name, "enrichment", batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running, skipping")
            return {"status": "skipped"}

        try:
            state_data = checkpoint.get("state_data") or {}
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            while True:
                # Find comics needing ANY enrichment
                result = await db.execute(text("""
                    SELECT id, metron_id, comicvine_id, pricecharting_id,
                           issue_name, number, upc, isbn,
                           image, description, price
                    FROM comic_issues
                    WHERE (
                        image IS NULL OR image = ''
                        OR description IS NULL OR description = ''
                        OR price IS NULL OR price = 0
                    )
                    AND id > :last_id
                    ORDER BY id
                    LIMIT :limit
                """), {"last_id": last_id, "limit": batch_size})

                comics = result.fetchall()

                if not comics:
                    logger.info(f"[{job_name}] No more comics need enrichment")
                    break

                logger.info(f"[{job_name}] Processing {len(comics)} comics in parallel")

                for comic in comics:
                    comic_id = comic.id
                    last_id = comic_id
                    stats["processed"] += 1

                    needs_cover = not comic.image
                    needs_desc = not comic.description
                    needs_price = not comic.price or comic.price == 0

                    try:
                        # Build search query
                        search_query = comic.issue_name or ""
                        if comic.number:
                            search_query = f"{search_query} #{comic.number}"

                        # =========================================================
                        # PARALLEL QUERIES - Fire all requests simultaneously
                        # =========================================================
                        tasks = []
                        task_sources = []

                        # Metron (by ID or search)
                        if "metron" in adapters:
                            async def query_metron():
                                try:
                                    if comic.metron_id:
                                        data = await adapters["metron"].fetch_by_id(str(comic.metron_id))
                                    else:
                                        result = await adapters["metron"].fetch_page(q=search_query[:100])
                                        data = result.records[0] if result.success and result.records else None
                                    return adapters["metron"].normalize(data) if data else {}
                                except:
                                    return {}
                            tasks.append(query_metron())
                            task_sources.append("metron")

                        # ComicVine (by ID or search)
                        if "comicvine" in adapters:
                            async def query_comicvine():
                                try:
                                    if comic.comicvine_id:
                                        data = await adapters["comicvine"].fetch_by_id(str(comic.comicvine_id))
                                    else:
                                        result = await adapters["comicvine"].fetch_page(q=search_query[:100])
                                        data = result.records[0] if result.success and result.records else None
                                    return adapters["comicvine"].normalize(data) if data else {}
                                except:
                                    return {}
                            tasks.append(query_comicvine())
                            task_sources.append("comicvine")

                        # ComicBookRealm (scraper)
                        if "comicbookrealm" in adapters and search_query:
                            async def query_cbr():
                                try:
                                    result = await adapters["comicbookrealm"].fetch_page(q=search_query[:100])
                                    return adapters["comicbookrealm"].normalize(result.records[0]) if result.success and result.records else {}
                                except:
                                    return {}
                            tasks.append(query_cbr())
                            task_sources.append("comicbookrealm")

                        # MyComicShop (scraper)
                        if "mycomicshop" in adapters and search_query:
                            async def query_mcs():
                                try:
                                    result = await adapters["mycomicshop"].fetch_page(q=search_query[:100])
                                    return adapters["mycomicshop"].normalize(result.records[0]) if result.success and result.records else {}
                                except:
                                    return {}
                            tasks.append(query_mcs())
                            task_sources.append("mycomicshop")

                        # PriceCharting (API)
                        if pc_token and comic.pricecharting_id:
                            async def query_pricecharting():
                                try:
                                    async with get_pricecharting_client() as client:
                                        response = await client.get(
                                            "https://www.pricecharting.com/api/product",
                                            params={"t": pc_token, "id": comic.pricecharting_id}
                                        )
                                        if response.status_code == 200:
                                            data = response.json()
                                            return {
                                                "price_loose": int(data.get("loose-price", 0)) / 100 if data.get("loose-price") else None,
                                                "price_cib": int(data.get("cib-price", 0)) / 100 if data.get("cib-price") else None,
                                                "price_new": int(data.get("new-price", 0)) / 100 if data.get("new-price") else None,
                                                "price_graded": int(data.get("graded-price", 0)) / 100 if data.get("graded-price") else None,
                                            }
                                except:
                                    return {}
                                return {}
                            tasks.append(query_pricecharting())
                            task_sources.append("pricecharting")

                        # Execute all queries in parallel
                        if tasks:
                            results = await aio.gather(*tasks, return_exceptions=True)
                        else:
                            results = []

                        # =========================================================
                        # MERGE RESULTS - Priority: APIs first, then scrapers
                        # =========================================================
                        merged = {}
                        source_used = {}

                        for i, result in enumerate(results):
                            if isinstance(result, Exception) or not result:
                                continue
                            source = task_sources[i]

                            # Merge cover (first wins)
                            if needs_cover and not merged.get("image"):
                                cover = result.get("cover_image_url") or result.get("image")
                                if cover:
                                    merged["image"] = cover
                                    source_used["image"] = source

                            # Merge description (first wins)
                            if needs_desc and not merged.get("description"):
                                desc = result.get("description")
                                if desc and len(desc) > 20:
                                    merged["description"] = desc[:5000]
                                    source_used["description"] = source

                            # Merge price (prefer PC > CBR > MCS)
                            if needs_price and not merged.get("price"):
                                price = (result.get("price_loose") or result.get("price")
                                        or result.get("price_guide") or result.get("retail_price"))
                                if price and float(price) > 0:
                                    merged["price"] = float(price)
                                    source_used["price"] = source

                        # =========================================================
                        # UPDATE DATABASE
                        # =========================================================
                        if merged:
                            updates = []
                            params = {"id": comic_id}

                            if merged.get("image"):
                                updates.append("image = :image")
                                params["image"] = merged["image"]
                                stats["covers_enriched"] += 1
                                stats["by_source"][source_used.get("image", "unknown")] = stats["by_source"].get(source_used.get("image", "unknown"), 0) + 1

                            if merged.get("description"):
                                updates.append("description = :description")
                                params["description"] = merged["description"]
                                stats["descriptions_enriched"] += 1
                                stats["by_source"][source_used.get("description", "unknown")] = stats["by_source"].get(source_used.get("description", "unknown"), 0) + 1

                            if merged.get("price"):
                                updates.append("price = :price")
                                params["price"] = merged["price"]
                                stats["prices_enriched"] += 1
                                stats["by_source"][source_used.get("price", "unknown")] = stats["by_source"].get(source_used.get("price", "unknown"), 0) + 1

                            if updates:
                                updates.append("updated_at = NOW()")
                                await db.execute(text(f"""
                                    UPDATE comic_issues
                                    SET {', '.join(updates)}
                                    WHERE id = :id
                                """), params)
                        else:
                            stats["failed"] += 1

                    except Exception as e:
                        stats["failed"] += 1
                        logger.warning(f"[{job_name}] Error enriching comic {comic_id}: {e}")

                # Commit batch and checkpoint
                await db.commit()
                await db.execute(text("""
                    UPDATE pipeline_checkpoints
                    SET state_data = jsonb_build_object('last_id', CAST(:last_id AS integer))
                    WHERE job_name = :name
                """), {"name": job_name, "last_id": last_id})
                await db.commit()

                total_enriched = stats["covers_enriched"] + stats["descriptions_enriched"] + stats["prices_enriched"]
                logger.info(
                    f"[{job_name}] Batch: {stats['processed']} processed, "
                    f"{total_enriched} enrichments ({stats['covers_enriched']}c/{stats['descriptions_enriched']}d/{stats['prices_enriched']}p)"
                )

                if max_records and stats["processed"] >= max_records:
                    break

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()
            await update_checkpoint(db, job_name, last_error=str(e), errors_delta=1)

        finally:
            total_enriched = stats["covers_enriched"] + stats["descriptions_enriched"] + stats["prices_enriched"]
            await update_checkpoint(
                db, job_name,
                is_running=False,
                processed_delta=stats["processed"],
                updated_delta=total_enriched,
                errors_delta=stats["failed"],
            )

    logger.info(
        f"[{job_name}] COMPLETE: {stats['processed']} processed, "
        f"covers={stats['covers_enriched']}, desc={stats['descriptions_enriched']}, "
        f"prices={stats['prices_enriched']}, failed={stats['failed']}. "
        f"Sources: {stats['by_source']}"
    )

    return {"status": "completed", "batch_id": batch_id, "stats": stats}


# =============================================================================
# COVER ENRICHMENT JOB (v1.10.5 - Phase 3: Cover Images from MSE Sources)
# =============================================================================

async def run_cover_enrichment_job(batch_size: int = 100, max_records: int = 0):
    """
    Phase 3 Cover Enrichment - Fetch cover images from MSE sources.

    This job specifically targets comics without cover images and enriches them
    using ComicVine (primary) with scraper fallbacks.

    SOURCE PRIORITY:
    1. ComicVine API - Best quality, comprehensive coverage
    2. ComicBookRealm (scraper) - Good fallback
    3. MyComicShop (scraper) - Additional coverage

    Rate Limits (constitution_cyberSec.json compliant):
    - ComicVine: 200/hour = ~3.3/min, we use 1/sec with gaps
    - Scrapers: 0.5/sec (robots.txt compliant)

    Args:
        batch_size: Comics to process per batch
        max_records: Max total to process (0 = unlimited)
    """
    from app.adapters import create_comicvine_adapter
    from app.utils.db_sanitizer import sanitize_url
    import asyncio as aio

    job_name = "cover_enrichment"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting Phase 3 Cover Enrichment (batch: {batch_id})")

    # Check API key
    import os
    api_key = os.getenv("COMIC_VINE_API_KEY")
    if not api_key:
        logger.error(f"[{job_name}] COMIC_VINE_API_KEY not set - aborting")
        return {"status": "failed", "error": "No cover sources configured"}

    # Import client factory
    from app.adapters.comicvine_adapter import get_comicvine_client, ComicVineAdapter, COMICVINE_CONFIG

    stats = {
        "processed": 0,
        "enriched": 0,
        "covers_added": 0,
        "creators_added": 0,
        "creators_linked": 0,
        "not_found": 0,
        "failed": 0,
        "by_source": {"comicvine": 0},
    }

    # Use client context manager for HTTP requests
    async with get_comicvine_client() as client:
        comicvine = ComicVineAdapter(COMICVINE_CONFIG, client, api_key)
        logger.info(f"[{job_name}] ComicVine adapter initialized")

        async with AsyncSessionLocal() as db:
            # Initialize checkpoint
            await db.execute(text("""
                INSERT INTO pipeline_checkpoints (job_name, job_type, created_at, updated_at)
                VALUES (:name, 'enrichment', NOW(), NOW())
                ON CONFLICT (job_name) DO NOTHING
            """), {"name": job_name})

            # Claim job
            result = await db.execute(text("""
                UPDATE pipeline_checkpoints
                SET is_running = true,
                    last_run_started = NOW(),
                    current_batch_id = :batch_id,
                    updated_at = NOW()
                WHERE job_name = :name
                AND (is_running = false OR is_running IS NULL)
                RETURNING id, state_data
            """), {"name": job_name, "batch_id": batch_id})
            claim = result.fetchone()

            if not claim:
                logger.warning(f"[{job_name}] Job already running, skipping")
                return {"status": "skipped", "message": "Job already running"}

            await db.commit()

            # Get starting offset from checkpoint
            state_data = claim.state_data or {}
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0
            logger.info(f"[{job_name}] Resuming from last_id={last_id}")

            try:
                while True:
                    # Find comics without covers
                    result = await db.execute(text("""
                        SELECT id, series_name, issue_name, number, gcd_id
                        FROM comic_issues
                        WHERE (image IS NULL OR image = '')
                        AND id > :last_id
                        AND gcd_id IS NOT NULL
                        ORDER BY id
                        LIMIT :limit
                    """), {"last_id": last_id, "limit": batch_size})
                    comics = result.fetchall()

                    if not comics:
                        logger.info(f"[{job_name}] No more comics without covers")
                        break

                    for comic in comics:
                        comic_id = comic.id
                        last_id = comic_id
                        stats["processed"] += 1

                        try:
                            # Build search query from series name, issue name, and number
                            series = comic.series_name or ""
                            issue = comic.issue_name or ""
                            number = comic.number or ""
                            search_query = f"{series} {issue} {number}".strip()

                            if not search_query or len(search_query) < 3:
                                stats["not_found"] += 1
                                continue

                            # Rate limiting: 1 request per second
                            await aio.sleep(1.0)

                            # Search ComicVine
                            result = await comicvine.search_issues(search_query, limit=5)

                            if not result.success or not result.records:
                                stats["not_found"] += 1
                                continue

                            # Find best match (first result with image or creators)
                            cover_url = None
                            creators = []
                            for record in result.records:
                                normalized = comicvine.normalize(record)
                                if not cover_url and normalized.get("cover_url"):
                                    cover_url = sanitize_url(normalized["cover_url"])
                                if not creators and normalized.get("creators"):
                                    creators = normalized["creators"]
                                # Stop once we have both
                                if cover_url and creators:
                                    break

                            if not cover_url and not creators:
                                stats["not_found"] += 1
                                continue

                            enriched_this_record = False

                            # Update cover image
                            if cover_url:
                                await db.execute(text("""
                                    UPDATE comic_issues
                                    SET image = :image,
                                        updated_at = NOW()
                                    WHERE id = :id AND (image IS NULL OR image = '')
                                """), {"id": comic_id, "image": cover_url})
                                stats["covers_added"] += 1
                                enriched_this_record = True

                            # Process creators
                            if creators:
                                for creator_data in creators:
                                    creator_name = creator_data.get("name")
                                    creator_role = creator_data.get("role", "unknown")
                                    comicvine_id = creator_data.get("comicvine_id")

                                    if not creator_name:
                                        continue

                                    # Find or create creator
                                    existing = await db.execute(text("""
                                        SELECT id FROM comic_creators WHERE LOWER(name) = LOWER(:name) LIMIT 1
                                    """), {"name": creator_name})
                                    creator_row = existing.fetchone()

                                    if creator_row:
                                        creator_id = creator_row.id
                                    else:
                                        # Create new creator
                                        try:
                                            result_ins = await db.execute(text("""
                                                INSERT INTO comic_creators (name, raw_data, created_at, updated_at)
                                                VALUES (:name, :raw_data, NOW(), NOW())
                                                RETURNING id
                                            """), {
                                                "name": creator_name,
                                                "raw_data": json.dumps({"comicvine_id": comicvine_id}) if comicvine_id else None
                                            })
                                            row = result_ins.fetchone()
                                            creator_id = row.id if row else None
                                            if creator_id:
                                                stats["creators_added"] += 1
                                        except Exception:
                                            # Race condition - another process inserted, re-fetch
                                            existing = await db.execute(text("""
                                                SELECT id FROM comic_creators WHERE LOWER(name) = LOWER(:name) LIMIT 1
                                            """), {"name": creator_name})
                                            creator_row = existing.fetchone()
                                            creator_id = creator_row.id if creator_row else None

                                    # Link creator to issue (with role)
                                    if creator_id:
                                        await db.execute(text("""
                                        INSERT INTO issue_creators (issue_id, creator_id, role)
                                        VALUES (:issue_id, :creator_id, :role)
                                        ON CONFLICT (issue_id, creator_id) DO NOTHING
                                    """), {
                                        "issue_id": comic_id,
                                        "creator_id": creator_id,
                                        "role": creator_role[:100] if creator_role else "unknown"
                                    })
                                    stats["creators_linked"] += 1
                                    enriched_this_record = True

                            if enriched_this_record:
                                stats["enriched"] += 1
                                stats["by_source"]["comicvine"] += 1

                        except Exception as e:
                            logger.error(f"[{job_name}] Error enriching {comic_id}: {e}")
                            stats["failed"] += 1
                            await add_to_dlq(
                                db, job_name, str(e),
                                entity_type="comic_issue",
                                entity_id=str(comic_id),
                                error_type=type(e).__name__
                            )

                    # Commit batch and update checkpoint
                    await db.commit()

                    await db.execute(text("""
                        UPDATE pipeline_checkpoints
                        SET state_data = jsonb_build_object('last_id', CAST(:last_id AS integer)),
                            updated_at = NOW()
                        WHERE job_name = :name
                    """), {"name": job_name, "last_id": last_id})
                    await db.commit()

                    logger.info(
                        f"[{job_name}] Batch: {stats['processed']} processed, "
                        f"{stats['enriched']} enriched, {stats['not_found']} not found"
                    )

                    # Check max_records limit
                    if max_records and stats["processed"] >= max_records:
                        logger.info(f"[{job_name}] Reached max_records limit ({max_records})")
                        break

            except Exception as e:
                logger.error(f"[{job_name}] Job failed: {e}")
                traceback.print_exc()
                await update_checkpoint(db, job_name, last_error=str(e), errors_delta=1)

            finally:
                await update_checkpoint(
                    db, job_name,
                    is_running=False,
                    processed_delta=stats["processed"],
                    updated_delta=stats["enriched"],
                    errors_delta=stats["failed"],
                )

    logger.info(
        f"[{job_name}] COMPLETE: {stats['processed']} processed, "
        f"{stats['enriched']} records enriched, {stats['covers_added']} covers, "
        f"{stats['creators_added']} new creators, {stats['creators_linked']} links, "
        f"{stats['not_found']} not found, {stats['failed']} failed. Sources: {stats['by_source']}"
    )

    return {"status": "completed", "batch_id": batch_id, "stats": stats}


# =============================================================================
# MARVEL FANDOM ENRICHMENT JOB (v1.10.8 - Story-level credits)
# =============================================================================

async def run_marvel_fandom_job(batch_size: int = 20, max_records: int = 0):
    """
    Marvel Fandom Enrichment - Fetch story-level credits from Marvel Database.

    This job enriches Marvel comics with:
    - Per-story credits (writer, penciler, inker, colorist, letterer, editor)
    - Character appearances per story
    - Cover variants
    - Editor-in-chief, release date, Marvel Unlimited status

    Rate Limits:
    - 1 request per second (Fandom API)

    Args:
        batch_size: Comics to process per batch
        max_records: Max total to process (0 = unlimited)
    """
    import httpx
    from bs4 import BeautifulSoup
    import re

    job_name = "marvel_fandom"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting Marvel Fandom Story Enrichment (batch: {batch_id})")

    stats = {
        "processed": 0,
        "enriched": 0,
        "stories_created": 0,
        "creators_created": 0,
        "credits_linked": 0,
        "not_found": 0,
        "failed": 0,
    }

    async with AsyncSessionLocal() as db:
        # Initialize checkpoint
        await db.execute(text("""
            INSERT INTO pipeline_checkpoints (job_name, job_type, created_at, updated_at)
            VALUES (:name, 'enrichment', NOW(), NOW())
            ON CONFLICT (job_name) DO NOTHING
        """), {"name": job_name})

        # Claim job
        result = await db.execute(text("""
            UPDATE pipeline_checkpoints
            SET is_running = true,
                last_run_started = NOW(),
                current_batch_id = :batch_id,
                updated_at = NOW()
            WHERE job_name = :name
            AND (is_running = false OR is_running IS NULL)
            RETURNING id, state_data
        """), {"name": job_name, "batch_id": batch_id})
        claim = result.fetchone()

        if not claim:
            logger.warning(f"[{job_name}] Job already running, skipping")
            return {"status": "skipped", "message": "Job already running"}

        await db.commit()

        state_data = claim.state_data or {}
        last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0
        logger.info(f"[{job_name}] Resuming from last_id={last_id}")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                while True:
                    # Find Marvel comics without story data
                    # Look for publisher_name containing 'Marvel' and no stories yet
                    result = await db.execute(text("""
                        SELECT ci.id, ci.series_name, ci.number, ci.series_year_began,
                               ci.publisher_name
                        FROM comic_issues ci
                        LEFT JOIN stories s ON s.comic_issue_id = ci.id
                        WHERE ci.publisher_name ILIKE '%Marvel%'
                        AND ci.id > :last_id
                        AND s.id IS NULL
                        AND ci.series_name IS NOT NULL
                        ORDER BY ci.id
                        LIMIT :limit
                    """), {"last_id": last_id, "limit": batch_size})
                    comics = result.fetchall()

                    if not comics:
                        logger.info(f"[{job_name}] No more Marvel comics without stories")
                        break

                    for comic in comics:
                        comic_id = comic.id
                        last_id = comic_id
                        stats["processed"] += 1

                        if max_records > 0 and stats["processed"] > max_records:
                            logger.info(f"[{job_name}] Reached max_records limit ({max_records})")
                            break

                        try:
                            # Build Marvel Fandom page title
                            series = comic.series_name or ""
                            number = comic.number or ""
                            year = comic.series_year_began

                            if not series or not number:
                                stats["not_found"] += 1
                                continue

                            # Try to determine volume from year
                            volume = 1  # Default
                            if year:
                                # Common Marvel volume patterns
                                if "Amazing Spider-Man" in series:
                                    if year >= 2018:
                                        volume = 5
                                    elif year >= 2014:
                                        volume = 3
                                    elif year >= 1999:
                                        volume = 2
                                # Add more series-specific logic as needed

                            # Build page title: "Series_Name_Vol_X_Issue"
                            page_title = f"{series.replace(' ', '_')}_Vol_{volume}_{number}"

                            # Rate limiting: 1 req/sec
                            await asyncio.sleep(1.0)

                            # Fetch from Marvel Fandom API
                            response = await client.get(
                                "https://marvel.fandom.com/api.php",
                                params={
                                    "action": "parse",
                                    "page": page_title,
                                    "prop": "text",
                                    "format": "json",
                                }
                            )

                            if response.status_code != 200:
                                stats["not_found"] += 1
                                continue

                            data = response.json()
                            if "error" in data:
                                stats["not_found"] += 1
                                continue

                            html = data.get("parse", {}).get("text", {}).get("*", "")
                            if not html:
                                stats["not_found"] += 1
                                continue

                            # Parse the HTML
                            soup = BeautifulSoup(html, "html.parser")

                            # Extract stories and credits
                            stories = []
                            current_story = None

                            for elem in soup.find_all(["h2", "h3"]):
                                elem_text = elem.get_text(strip=True)

                                # Story header: '1. "Title"'
                                match = re.match(r'^(\d+)\.\s*"?(.+?)"?\s*$', elem_text)
                                if match and elem.name == "h2":
                                    if current_story:
                                        stories.append(current_story)
                                    current_story = {
                                        "number": int(match.group(1)),
                                        "title": match.group(2).strip('"'),
                                        "credits": {},
                                        "characters": [],  # Will be populated from Appearing sections
                                    }
                                    continue

                                # Credit roles
                                if current_story and elem.name == "h3":
                                    for role in ["Writer", "Penciler", "Inker", "Colorist", "Letterer", "Editor"]:
                                        if role in elem_text:
                                            next_elem = elem.find_next_sibling()
                                            if next_elem:
                                                links = next_elem.find_all("a") if hasattr(next_elem, "find_all") else []
                                                names = [a.get_text(strip=True) for a in links if a.get_text(strip=True)]
                                                if names:
                                                    current_story["credits"][role.lower()] = names
                                            break

                            if current_story:
                                stories.append(current_story)

                            # Extract character appearances from "Appearing in" sections
                            has_first_appearance = False
                            has_death = False

                            for h2 in soup.find_all("h2"):
                                h2_text = h2.get_text(strip=True)
                                if h2_text.startswith("Appearing in"):
                                    # Match to story by title
                                    story_match = re.search(r'Appearing in "(.+?)"', h2_text)
                                    if story_match:
                                        story_title = story_match.group(1)
                                        # Find matching story
                                        for story in stories:
                                            if story["title"] == story_title:
                                                # Parse character list
                                                next_elem = h2.find_next_sibling()
                                                while next_elem and next_elem.name != "h2":
                                                    if hasattr(next_elem, "find_all"):
                                                        for li in next_elem.find_all("li"):
                                                            li_text = li.get_text(strip=True)
                                                            link = li.find("a")
                                                            if link:
                                                                char_name = link.get_text(strip=True)
                                                                if char_name:
                                                                    char_data = {
                                                                        "name": char_name,
                                                                        "is_first_appearance": "(First appearance" in li_text,
                                                                        "is_death": "(Death" in li_text,
                                                                        "is_cameo": "(Cameo" in li_text,
                                                                        "is_unnamed": "unnamed" in li_text.lower(),
                                                                    }
                                                                    story["characters"].append(char_data)
                                                                    if char_data["is_first_appearance"]:
                                                                        has_first_appearance = True
                                                                    if char_data["is_death"]:
                                                                        has_death = True
                                                    next_elem = next_elem.find_next_sibling()
                                                break

                            if not stories:
                                stats["not_found"] += 1
                                continue

                            # Update comic_issues with significance flags
                            if has_first_appearance or has_death:
                                await db.execute(text("""
                                    UPDATE comic_issues
                                    SET has_first_appearance = :first,
                                        has_death = :death,
                                        updated_at = NOW()
                                    WHERE id = :comic_id
                                """), {
                                    "comic_id": comic_id,
                                    "first": has_first_appearance,
                                    "death": has_death,
                                })

                            # Save stories and credits to database
                            for story_data in stories:
                                # Create story record
                                story_result = await db.execute(text("""
                                    INSERT INTO stories (comic_issue_id, story_number, title, marvel_fandom_id, created_at, updated_at)
                                    VALUES (:comic_id, :number, :title, :fandom_id, NOW(), NOW())
                                    ON CONFLICT (comic_issue_id, story_number) DO UPDATE
                                    SET title = EXCLUDED.title, updated_at = NOW()
                                    RETURNING id
                                """), {
                                    "comic_id": comic_id,
                                    "number": story_data["number"],
                                    "title": story_data["title"][:500] if story_data["title"] else None,
                                    "fandom_id": page_title,
                                })
                                story_row = story_result.fetchone()
                                story_id = story_row.id if story_row else None
                                stats["stories_created"] += 1

                                if story_id:
                                    # Create/link creators for each role
                                    for role, creators in story_data["credits"].items():
                                        for creator_name in creators:
                                            if not creator_name:
                                                continue

                                            # Get or create creator
                                            creator_result = await db.execute(text("""
                                                INSERT INTO creators (name, created_at, updated_at)
                                                VALUES (:name, NOW(), NOW())
                                                ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
                                                RETURNING id
                                            """), {"name": creator_name[:255]})
                                            creator_row = creator_result.fetchone()
                                            creator_id = creator_row.id if creator_row else None

                                            if creator_id:
                                                stats["creators_created"] += 1

                                                # Link creator to story with role
                                                await db.execute(text("""
                                                    INSERT INTO story_creators (story_id, creator_id, role, created_at)
                                                    VALUES (:story_id, :creator_id, :role, NOW())
                                                    ON CONFLICT (story_id, creator_id, role) DO NOTHING
                                                """), {
                                                    "story_id": story_id,
                                                    "creator_id": creator_id,
                                                    "role": role,
                                                })
                                                stats["credits_linked"] += 1

                                    # Save character appearances for this story
                                    for char_data in story_data.get("characters", []):
                                        if not char_data.get("name"):
                                            continue

                                        # Get or create character
                                        char_result = await db.execute(text("""
                                            INSERT INTO characters (name, created_at, updated_at)
                                            VALUES (:name, NOW(), NOW())
                                            ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
                                            RETURNING id
                                        """), {"name": char_data["name"][:255]})
                                        char_row = char_result.fetchone()
                                        char_id = char_row.id if char_row else None

                                        if char_id:
                                            # Determine appearance type
                                            appearance_type = "featured"
                                            if char_data.get("is_cameo"):
                                                appearance_type = "cameo"
                                            elif char_data.get("is_unnamed"):
                                                appearance_type = "background"

                                            # Link character to story
                                            await db.execute(text("""
                                                INSERT INTO story_characters (
                                                    story_id, character_id, appearance_type,
                                                    is_first_appearance, is_death, created_at
                                                )
                                                VALUES (:story_id, :char_id, :appearance_type,
                                                        :is_first, :is_death, NOW())
                                                ON CONFLICT (story_id, character_id) DO UPDATE
                                                SET is_first_appearance = EXCLUDED.is_first_appearance,
                                                    is_death = EXCLUDED.is_death
                                            """), {
                                                "story_id": story_id,
                                                "char_id": char_id,
                                                "appearance_type": appearance_type,
                                                "is_first": char_data.get("is_first_appearance", False),
                                                "is_death": char_data.get("is_death", False),
                                            })

                                            # Update character's first appearance if this is it
                                            if char_data.get("is_first_appearance"):
                                                await db.execute(text("""
                                                    UPDATE characters
                                                    SET first_appearance_story_id = :story_id
                                                    WHERE id = :char_id
                                                    AND first_appearance_story_id IS NULL
                                                """), {"char_id": char_id, "story_id": story_id})

                                            # Mark character as deceased if death occurs
                                            if char_data.get("is_death"):
                                                await db.execute(text("""
                                                    UPDATE characters
                                                    SET death_story_id = :story_id,
                                                        is_deceased = true
                                                    WHERE id = :char_id
                                                """), {"char_id": char_id, "story_id": story_id})

                            await db.commit()
                            stats["enriched"] += 1

                            char_count = sum(len(s.get("characters", [])) for s in stories)
                            logger.debug(
                                f"[{job_name}] Enriched {series} #{number}: "
                                f"{len(stories)} stories, {char_count} characters"
                            )

                        except Exception as e:
                            logger.error(f"[{job_name}] Error enriching comic {comic_id}: {e}")
                            stats["failed"] += 1
                            continue

                    # Update checkpoint
                    await update_checkpoint(
                        db, job_name,
                        last_processed_id=last_id,
                        processed_delta=batch_size,
                        updated_delta=stats["enriched"],
                        errors_delta=stats["failed"],
                    )

                    if max_records > 0 and stats["processed"] >= max_records:
                        break

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            await db.execute(text("""
                UPDATE pipeline_checkpoints
                SET is_running = false, last_error = :error, updated_at = NOW()
                WHERE job_name = :name
            """), {"name": job_name, "error": str(e)[:500]})
            await db.commit()
            raise

        finally:
            # Release job lock
            await db.execute(text("""
                UPDATE pipeline_checkpoints
                SET is_running = false,
                    last_run_completed = NOW(),
                    state_data = jsonb_build_object('last_id', CAST(:last_id AS integer)),
                    updated_at = NOW()
                WHERE job_name = :name
            """), {"name": job_name, "last_id": last_id})
            await db.commit()

    logger.info(
        f"[{job_name}] COMPLETE: {stats['processed']} processed, "
        f"{stats['enriched']} enriched, {stats['stories_created']} stories, "
        f"{stats['creators_created']} creators, {stats['credits_linked']} credits, "
        f"{stats['not_found']} not found, {stats['failed']} failed"
    )

    return {"status": "completed", "batch_id": batch_id, "stats": stats}


# =============================================================================
# UPC BACKFILL JOB (v1.12.0 - Multi-Source UPC Recovery)
# =============================================================================

async def run_upc_backfill_job(batch_size: int = 100, max_records: int = 0):
    """
    Multi-Source UPC Backfill Job - Recovers missing UPCs from external sources.

    PROBLEM: Only 16% of comics have UPCs despite barcodes being standard since 1980s.
    This is a DATA GAP, not a "comics don't have barcodes" issue.

    SOURCES (in priority order):
    1. Metron API - If we have metron_id, direct lookup (fast, authoritative)
    2. ComicBookRealm - Scrape by series/issue, has UPC extraction
    3. ComicVine - May have UPC in API response

    STRATEGY:
    - Prioritize US publishers (Marvel, DC, Image, Dark Horse, etc.) - they have barcodes
    - Skip UK/European publishers (D.C. Thomson, IPC, etc.) - different barcode systems
    - Match by series_name + issue_number when no external ID available

    Args:
        batch_size: Comics to process per batch (default 100)
        max_records: Max total to process (0 = unlimited)
    """
    import httpx
    import os
    from app.adapters.metron_adapter import MetronAdapter
    from app.adapters.comicbookrealm_adapter import create_comicbookrealm_adapter

    job_name = "upc_backfill"
    batch_id = str(uuid4())

    # US publishers that definitely use UPCs (prioritize these)
    US_PUBLISHERS = [
        'Marvel', 'DC', 'DC Comics', 'Image', 'Image Comics', 'Dark Horse',
        'Dark Horse Comics', 'IDW', 'IDW Publishing', 'Dynamite', 'BOOM! Studios',
        'Valiant', 'Archie', 'Archie Comics', 'Oni Press', 'AfterShock',
        'Scout Comics', 'Vault Comics', 'AWA', 'Mad Cave', 'Ablaze',
        'Titan', 'Titan Comics', 'Antarctic Press'
    ]

    logger.info(f"[{job_name}] Starting UPC Backfill (batch: {batch_id})")

    stats = {
        "processed": 0,
        "upcs_found": 0,
        "isbns_found": 0,
        "by_source": {"metron": 0, "comicbookrealm": 0, "comicvine": 0},
        "no_match": 0,
        "errors": 0,
    }

    # Initialize adapters
    metron_adapter = None
    cbr_adapter = None

    try:
        metron_adapter = MetronAdapter()
        if await metron_adapter.health_check():
            logger.info(f"[{job_name}] Metron adapter ready")
        else:
            metron_adapter = None
            logger.warning(f"[{job_name}] Metron not available")
    except Exception as e:
        logger.warning(f"[{job_name}] Metron init failed: {e}")

    try:
        cbr_adapter = await create_comicbookrealm_adapter()
        logger.info(f"[{job_name}] ComicBookRealm adapter ready")
    except Exception as e:
        logger.warning(f"[{job_name}] ComicBookRealm init failed: {e}")

    async with AsyncSessionLocal() as db:
        claimed, checkpoint = await try_claim_job(db, job_name, "enrichment", batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running, skipping")
            return {"status": "skipped"}

        try:
            state_data = checkpoint.get("state_data") or {}
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            # Build publisher filter for SQL
            publisher_list = ", ".join([f"'{p}'" for p in US_PUBLISHERS])

            while True:
                # Find comics missing UPCs from US publishers (they should have barcodes)
                # Also include comics from 1980+ (barcode era)
                result = await db.execute(text(f"""
                    SELECT id, metron_id, comicvine_id, gcd_id,
                           series_name, number, issue_name, publisher_name,
                           cover_date, upc, isbn
                    FROM comic_issues
                    WHERE (upc IS NULL OR upc = '')
                    AND (isbn IS NULL OR isbn = '')
                    AND id > :last_id
                    AND (
                        publisher_name IN ({publisher_list})
                        OR (cover_date IS NOT NULL AND cover_date >= '1980-01-01')
                    )
                    ORDER BY
                        CASE WHEN metron_id IS NOT NULL THEN 0 ELSE 1 END,  -- Prioritize those with metron_id
                        cover_date DESC NULLS LAST,  -- Newer comics more likely to have data
                        id
                    LIMIT :limit
                """), {"last_id": last_id, "limit": batch_size})

                comics = result.fetchall()

                if not comics:
                    logger.info(f"[{job_name}] No more comics to process")
                    break

                logger.info(f"[{job_name}] Processing {len(comics)} comics")

                for comic in comics:
                    comic_id = comic.id
                    last_id = comic_id
                    stats["processed"] += 1

                    found_upc = None
                    found_isbn = None
                    source = None

                    try:
                        # =========================================================
                        # SOURCE 1: Metron API - SEARCH by series/issue, fuzzy match
                        # We don't assume metron_id exists - we SEARCH and MATCH
                        # =========================================================
                        if metron_adapter and comic.series_name and not found_upc:
                            try:
                                # Rate limit: 1 sec between Metron requests
                                await asyncio.sleep(1.0)

                                # Search Metron by series name and issue number
                                search_result = await metron_adapter.search_issues(
                                    series_name=comic.series_name,
                                    number=str(comic.number) if comic.number else None,
                                    publisher_name=comic.publisher_name,
                                )

                                if search_result.success and search_result.records:
                                    # Score each result with fuzzy matching
                                    best_match = None
                                    best_score = 0

                                    for record in search_result.records[:5]:
                                        score = 0

                                        # Series name match (+3)
                                        rec_series = (record.get("series_name") or "").lower()
                                        our_series = comic.series_name.lower()
                                        if our_series in rec_series or rec_series in our_series:
                                            score += 3
                                        else:
                                            continue  # Series must match

                                        # Issue number match (+2)
                                        if comic.number:
                                            rec_num = str(record.get("number") or "").lstrip("0") or "0"
                                            our_num = str(comic.number).lstrip("0") or "0"
                                            if rec_num == our_num:
                                                score += 2

                                        # Publisher match (+1)
                                        if comic.publisher_name:
                                            rec_pub = (record.get("publisher_name") or "").lower()
                                            our_pub = comic.publisher_name.lower()
                                            if our_pub in rec_pub or rec_pub in our_pub:
                                                score += 1

                                        # Cover year match (+2)
                                        if comic.cover_date:
                                            try:
                                                our_year = comic.cover_date.year if hasattr(comic.cover_date, 'year') else int(str(comic.cover_date)[:4])
                                                rec_year = record.get("cover_year") or record.get("cover_date", "")[:4]
                                                if str(our_year) == str(rec_year)[:4]:
                                                    score += 2
                                            except:
                                                pass

                                        if score > best_score:
                                            best_score = score
                                            best_match = record

                                    # Require minimum score of 5 (series + number at least)
                                    if best_match and best_score >= 5:
                                        # Fetch full details for UPC
                                        metron_id = best_match.get("id")
                                        if metron_id:
                                            await asyncio.sleep(1.0)
                                            full_data = await metron_adapter.fetch_by_id(str(metron_id), endpoint="issue")
                                            if full_data:
                                                if full_data.get("upc"):
                                                    found_upc = full_data["upc"]
                                                    source = "metron"
                                                if full_data.get("isbn") and not found_isbn:
                                                    found_isbn = full_data["isbn"]
                                                    if not source:
                                                        source = "metron"
                                                # Also store the metron_id for future reference
                                                if found_upc or found_isbn:
                                                    await db.execute(text("""
                                                        UPDATE comic_issues SET metron_id = :mid WHERE id = :id AND metron_id IS NULL
                                                    """), {"id": comic_id, "mid": metron_id})

                            except Exception as e:
                                logger.debug(f"[{job_name}] Metron search failed for {comic_id}: {e}")

                        # =========================================================
                        # SOURCE 2: ComicBookRealm (scrape by series/issue)
                        # =========================================================
                        if cbr_adapter and not found_upc and comic.series_name:
                            try:
                                # Rate limit: 2 sec between requests
                                await asyncio.sleep(2.0)

                                search_result = await cbr_adapter.search_issues(
                                    series_name=comic.series_name,
                                    issue_number=comic.number,
                                    limit=3
                                )

                                if search_result.success and search_result.records:
                                    # Try to get detail page for UPC
                                    for record in search_result.records[:2]:
                                        if record.get("url"):
                                            await asyncio.sleep(2.0)
                                            detail = await cbr_adapter.fetch_by_id(record["url"])
                                            if detail:
                                                if detail.get("upc"):
                                                    found_upc = detail["upc"]
                                                    source = "comicbookrealm"
                                                    break
                                                if detail.get("isbn") and not found_isbn:
                                                    found_isbn = detail["isbn"]
                                                    if not source:
                                                        source = "comicbookrealm"
                            except Exception as e:
                                logger.debug(f"[{job_name}] CBR lookup failed for {comic_id}: {e}")

                        # =========================================================
                        # Update comic if we found identifiers
                        # =========================================================
                        if found_upc or found_isbn:
                            update_parts = []
                            params = {"id": comic_id}

                            if found_upc:
                                # Clean UPC - digits only
                                clean_upc = ''.join(filter(str.isdigit, found_upc))
                                if len(clean_upc) >= 10:
                                    update_parts.append("upc = :upc")
                                    params["upc"] = clean_upc
                                    stats["upcs_found"] += 1
                                    stats["by_source"][source] = stats["by_source"].get(source, 0) + 1

                            if found_isbn:
                                # Clean ISBN - digits and X only
                                clean_isbn = ''.join(c for c in found_isbn.upper() if c.isdigit() or c == 'X')
                                if len(clean_isbn) in (10, 13):
                                    update_parts.append("isbn = :isbn")
                                    update_parts.append("isbn_normalized = :isbn_norm")
                                    params["isbn"] = clean_isbn
                                    params["isbn_norm"] = clean_isbn
                                    stats["isbns_found"] += 1

                            if update_parts:
                                update_parts.append("updated_at = NOW()")
                                await db.execute(text(f"""
                                    UPDATE comic_issues
                                    SET {', '.join(update_parts)}
                                    WHERE id = :id
                                """), params)

                                logger.debug(
                                    f"[{job_name}] Comic {comic_id}: "
                                    f"UPC={found_upc or 'N/A'}, ISBN={found_isbn or 'N/A'} "
                                    f"from {source}"
                                )
                        else:
                            stats["no_match"] += 1

                    except Exception as e:
                        stats["errors"] += 1
                        logger.warning(f"[{job_name}] Error processing comic {comic_id}: {e}")

                # Commit batch and checkpoint
                await db.commit()
                await db.execute(text("""
                    UPDATE pipeline_checkpoints
                    SET state_data = jsonb_build_object('last_id', CAST(:last_id AS integer))
                    WHERE job_name = :name
                """), {"name": job_name, "last_id": last_id})
                await db.commit()

                logger.info(
                    f"[{job_name}] Batch complete: {stats['processed']} processed, "
                    f"{stats['upcs_found']} UPCs found, {stats['isbns_found']} ISBNs found"
                )

                if max_records and stats["processed"] >= max_records:
                    break

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()
            await update_checkpoint(db, job_name, last_error=str(e), errors_delta=1)

        finally:
            await update_checkpoint(
                db, job_name,
                is_running=False,
                processed_delta=stats["processed"],
                updated_delta=stats["upcs_found"] + stats["isbns_found"],
                errors_delta=stats["errors"],
            )

    logger.info(
        f"[{job_name}] COMPLETE: {stats['processed']} processed, "
        f"{stats['upcs_found']} UPCs, {stats['isbns_found']} ISBNs, "
        f"by source: {stats['by_source']}, {stats['errors']} errors"
    )

    return {"status": "completed", "batch_id": batch_id, "stats": stats}


# =============================================================================
# DLQ RETRY JOB
# =============================================================================

async def run_dlq_retry_job():
    """
    Retry failed jobs from the dead letter queue.

    - Finds pending DLQ entries that are due for retry
    - Attempts to re-run the failed operation
    - Updates status based on result
    """
    job_name = "dlq_retry"

    logger.info(f"[{job_name}] Starting DLQ retry job")

    async with AsyncSessionLocal() as db:
        # First check if table exists and has data
        try:
            count_result = await db.execute(text("SELECT COUNT(*) FROM dead_letter_queue"))
            total = count_result.scalar() or 0
            logger.info(f"[{job_name}] DLQ has {total} total entries")
        except Exception as e:
            logger.warning(f"[{job_name}] DLQ table not ready: {e}")
            return

        # Find entries ready for retry - use enum member name (PENDING) not value
        result = await db.execute(text("""
            SELECT id, job_type, entity_type, entity_id, external_id, retry_count, max_retries
            FROM dead_letter_queue
            WHERE status = 'PENDING'
            AND retry_count < max_retries
            AND (next_retry_at IS NULL OR next_retry_at <= NOW())
            ORDER BY created_at
            LIMIT 20
        """))

        entries = result.fetchall()

        if not entries:
            logger.info(f"[{job_name}] No DLQ entries to retry")
            return

        logger.info(f"[{job_name}] Found {len(entries)} entries to retry")

        stats = {"retried": 0, "resolved": 0, "failed": 0}

        for entry in entries:
            entry_id = entry.id
            job_type = entry.job_type
            retry_count = entry.retry_count

            try:
                # Mark as retrying - use enum member name (RETRYING)
                await db.execute(text("""
                    UPDATE dead_letter_queue
                    SET status = 'RETRYING', last_retry_at = NOW(), retry_count = retry_count + 1
                    WHERE id = :id
                """), {"id": entry_id})
                await db.commit()

                # Attempt retry based on job type
                # v1.8.0: Implemented actual retry logic (was placeholder)
                success = False

                if job_type == "comic_enrichment" and entry.entity_id:
                    # Re-fetch comic from Metron and update database
                    try:
                        from app.services.metron import metron_service
                        from app.services.comic_cache import comic_cache

                        comic_data = await metron_service.get_issue(entry.entity_id)
                        if comic_data and comic_data.get('id'):
                            await comic_cache._cache_issue_batch(db, comic_data)
                            await db.commit()
                            success = True
                            logger.info(f"[{job_name}] Retry succeeded for comic {entry.entity_id}")
                    except Exception as retry_err:
                        logger.error(f"[{job_name}] Retry failed for comic {entry.entity_id}: {retry_err}")
                        success = False

                elif job_type == "funko_price_check" and entry.entity_id:
                    # Re-fetch Funko price from PriceCharting
                    try:
                        # Update the funko record to mark it needs price refresh
                        await db.execute(text("""
                            UPDATE funkos
                            SET days_stale = NULL, updated_at = NOW()
                            WHERE id = :id
                        """), {"id": entry.entity_id})
                        await db.commit()
                        success = True
                        logger.info(f"[{job_name}] Retry succeeded for funko {entry.entity_id} (marked for refresh)")
                    except Exception as retry_err:
                        logger.error(f"[{job_name}] Retry failed for funko {entry.entity_id}: {retry_err}")
                        success = False

                else:
                    # Unknown job type - log and mark as permanently failed after max retries
                    if retry_count >= 3:
                        logger.warning(f"[{job_name}] Unknown job type '{job_type}' exceeded max retries, marking permanently failed")
                        await db.execute(text("""
                            UPDATE dead_letter_queue
                            SET status = 'PERMANENT_FAILURE', resolved_at = NOW()
                            WHERE id = :id
                        """), {"id": entry_id})
                        await db.commit()
                        continue

                if success:
                    await db.execute(text("""
                        UPDATE dead_letter_queue
                        SET status = 'RESOLVED', resolved_at = NOW()
                        WHERE id = :id
                    """), {"id": entry_id})
                    stats["resolved"] += 1
                else:
                    # Calculate next retry time with exponential backoff
                    backoff_minutes = 5 * (2 ** retry_count)  # 5, 10, 20, 40...
                    await db.execute(text(f"""
                        UPDATE dead_letter_queue
                        SET status = 'PENDING', next_retry_at = NOW() + INTERVAL '{backoff_minutes} minutes'
                        WHERE id = :id
                    """), {"id": entry_id})
                    stats["failed"] += 1

                stats["retried"] += 1
                await db.commit()

            except Exception as e:
                logger.error(f"[{job_name}] Error retrying DLQ entry {entry_id}: {e}")
                await db.execute(text("""
                    UPDATE dead_letter_queue
                    SET status = 'PENDING',
                        next_retry_at = NOW() + INTERVAL '30 minutes',
                        error_message = error_message || E'\nRetry error: ' || :error
                    WHERE id = :id
                """), {"id": entry_id, "error": str(e)[:500]})
                await db.commit()
                stats["failed"] += 1

        logger.info(f"[{job_name}] Complete: {stats['retried']} retried, {stats['resolved']} resolved, {stats['failed']} failed again")


# =============================================================================
# DAILY PRICE SNAPSHOT JOB (v1.7.0 - AI Intelligence)
# =============================================================================

async def run_daily_snapshot_job():
    """
    Capture daily price snapshots for ALL entities.

    This is the core data collection job for AI/ML model training.
    Unlike price_changelog (only captures changes), this captures complete state.

    Logic:
    1. Query all funkos with pricecharting_id
    2. Query all comic_issues with pricecharting_id
    3. For each entity: INSERT snapshot with current prices
    4. Mark price_changed = TRUE if different from yesterday
    5. Calculate days_since_change from price_changelog
    6. Flag is_stale if updated_at > 7 days ago
    """
    job_name = "daily_snapshot"
    batch_id = str(uuid4())
    snapshot_date = date.today()

    logger.info(f"[{job_name}] Starting daily price snapshot (batch: {batch_id}, date: {snapshot_date})")

    async with AsyncSessionLocal() as db:
        # Atomically claim job to prevent race condition
        claimed, checkpoint = await try_claim_job(db, job_name, "snapshot", batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or claim failed, skipping")
            return

        stats = {"funkos": 0, "comics": 0, "changed": 0, "errors": 0}

        try:
            # ============================================================
            # PHASE 1: Snapshot all Funkos with pricecharting_id
            # v1.8.0: Optimized with CTE-based batch INSERT (fixes N+1 query issue)
            # ============================================================
            logger.info(f"[{job_name}] Phase 1: Snapshotting Funkos (batch mode)...")

            # Single CTE-based INSERT that eliminates N+1 queries
            # Combines: entity data, yesterday's prices, and days_since_change in one query
            funko_snapshot_result = await db.execute(text("""
                WITH yesterday_prices AS (
                    SELECT entity_id, price_loose, price_cib, price_new
                    FROM price_snapshots
                    WHERE entity_type = 'funko'
                    AND snapshot_date = :yesterday
                ),
                days_since AS (
                    SELECT entity_id,
                           EXTRACT(DAY FROM NOW() - MAX(changed_at))::INTEGER as days
                    FROM price_changelog
                    WHERE entity_type = 'funko'
                    GROUP BY entity_id
                ),
                funko_data AS (
                    SELECT
                        f.id,
                        f.pricecharting_id,
                        f.price_loose,
                        f.price_cib,
                        f.price_new,
                        f.sales_volume,
                        EXTRACT(EPOCH FROM (NOW() - f.updated_at)) / 86400 > 7 as is_stale,
                        yp.price_loose as y_loose,
                        yp.price_cib as y_cib,
                        yp.price_new as y_new,
                        dsc.days as days_since_change,
                        -- Detect price changes with tolerance
                        CASE WHEN yp.entity_id IS NOT NULL AND (
                            ABS(COALESCE(f.price_loose::numeric, 0) - COALESCE(yp.price_loose::numeric, 0)) > 0.01 OR
                            ABS(COALESCE(f.price_cib::numeric, 0) - COALESCE(yp.price_cib::numeric, 0)) > 0.01 OR
                            ABS(COALESCE(f.price_new::numeric, 0) - COALESCE(yp.price_new::numeric, 0)) > 0.01
                        ) THEN TRUE ELSE FALSE END as price_changed
                    FROM funkos f
                    LEFT JOIN yesterday_prices yp ON yp.entity_id = f.id
                    LEFT JOIN days_since dsc ON dsc.entity_id = f.id
                    WHERE f.pricecharting_id IS NOT NULL
                )
                INSERT INTO price_snapshots (
                    snapshot_date, entity_type, entity_id, pricecharting_id,
                    price_loose, price_cib, price_new,
                    sales_volume, price_changed, days_since_change,
                    data_source, is_stale, created_at
                )
                SELECT
                    :date, 'funko', fd.id, fd.pricecharting_id,
                    fd.price_loose, fd.price_cib, fd.price_new,
                    fd.sales_volume, fd.price_changed, fd.days_since_change,
                    'pricecharting', fd.is_stale, NOW()
                FROM funko_data fd
                ON CONFLICT (entity_type, entity_id, snapshot_date)
                DO UPDATE SET
                    price_loose = EXCLUDED.price_loose,
                    price_cib = EXCLUDED.price_cib,
                    price_new = EXCLUDED.price_new,
                    sales_volume = EXCLUDED.sales_volume,
                    price_changed = EXCLUDED.price_changed,
                    days_since_change = EXCLUDED.days_since_change,
                    is_stale = EXCLUDED.is_stale
                RETURNING entity_id, price_changed
            """), {"date": snapshot_date, "yesterday": snapshot_date - timedelta(days=1)})

            funko_results = funko_snapshot_result.fetchall()
            stats["funkos"] = len(funko_results)
            stats["changed"] = sum(1 for r in funko_results if r.price_changed)

            await db.commit()
            logger.info(f"[{job_name}] Phase 1 complete: {stats['funkos']} Funkos snapshotted, {stats['changed']} price changes")

            # ============================================================
            # PHASE 2: Snapshot all Comics with pricecharting_id
            # v1.8.0: Optimized with CTE-based batch INSERT (fixes N+1 query issue)
            # ============================================================
            logger.info(f"[{job_name}] Phase 2: Snapshotting Comics (batch mode)...")

            # Single CTE-based INSERT that eliminates N+1 queries for Comics
            comic_snapshot_result = await db.execute(text("""
                WITH yesterday_prices AS (
                    SELECT entity_id, price_loose, price_cib, price_new
                    FROM price_snapshots
                    WHERE entity_type = 'comic'
                    AND snapshot_date = :yesterday
                ),
                days_since AS (
                    SELECT entity_id,
                           EXTRACT(DAY FROM NOW() - MAX(changed_at))::INTEGER as days
                    FROM price_changelog
                    WHERE entity_type = 'comic'
                    GROUP BY entity_id
                ),
                comic_data AS (
                    SELECT
                        c.id,
                        c.pricecharting_id,
                        c.price_loose,
                        c.price_cib,
                        c.price_new,
                        c.price_graded,
                        c.price_bgs_10,
                        c.price_cgc_98,
                        c.price_cgc_96,
                        c.sales_volume,
                        EXTRACT(EPOCH FROM (NOW() - c.updated_at)) / 86400 > 7 as is_stale,
                        dsc.days as days_since_change,
                        -- Detect price changes with tolerance
                        CASE WHEN yp.entity_id IS NOT NULL AND (
                            ABS(COALESCE(c.price_loose::numeric, 0) - COALESCE(yp.price_loose::numeric, 0)) > 0.01 OR
                            ABS(COALESCE(c.price_cib::numeric, 0) - COALESCE(yp.price_cib::numeric, 0)) > 0.01 OR
                            ABS(COALESCE(c.price_new::numeric, 0) - COALESCE(yp.price_new::numeric, 0)) > 0.01
                        ) THEN TRUE ELSE FALSE END as price_changed
                    FROM comic_issues c
                    LEFT JOIN yesterday_prices yp ON yp.entity_id = c.id
                    LEFT JOIN days_since dsc ON dsc.entity_id = c.id
                    WHERE c.pricecharting_id IS NOT NULL
                )
                INSERT INTO price_snapshots (
                    snapshot_date, entity_type, entity_id, pricecharting_id,
                    price_loose, price_cib, price_new,
                    price_graded, price_bgs_10, price_cgc_98, price_cgc_96,
                    sales_volume, price_changed, days_since_change,
                    data_source, is_stale, created_at
                )
                SELECT
                    :date, 'comic', cd.id, cd.pricecharting_id,
                    cd.price_loose, cd.price_cib, cd.price_new,
                    cd.price_graded, cd.price_bgs_10, cd.price_cgc_98, cd.price_cgc_96,
                    cd.sales_volume, cd.price_changed, cd.days_since_change,
                    'pricecharting', cd.is_stale, NOW()
                FROM comic_data cd
                ON CONFLICT (entity_type, entity_id, snapshot_date)
                DO UPDATE SET
                    price_loose = EXCLUDED.price_loose,
                    price_cib = EXCLUDED.price_cib,
                    price_new = EXCLUDED.price_new,
                    price_graded = EXCLUDED.price_graded,
                    price_bgs_10 = EXCLUDED.price_bgs_10,
                    price_cgc_98 = EXCLUDED.price_cgc_98,
                    price_cgc_96 = EXCLUDED.price_cgc_96,
                    sales_volume = EXCLUDED.sales_volume,
                    price_changed = EXCLUDED.price_changed,
                    days_since_change = EXCLUDED.days_since_change,
                    is_stale = EXCLUDED.is_stale
                RETURNING entity_id, price_changed
            """), {"date": snapshot_date, "yesterday": snapshot_date - timedelta(days=1)})

            comic_results = comic_snapshot_result.fetchall()
            stats["comics"] = len(comic_results)
            comic_changes = sum(1 for r in comic_results if r.price_changed)
            stats["changed"] += comic_changes

            await db.commit()
            logger.info(f"[{job_name}] Phase 2 complete: {stats['comics']} Comics snapshotted, {comic_changes} price changes")

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()
            await add_to_dlq(
                db, job_name, str(e),
                error_type=type(e).__name__,
                error_trace=traceback.format_exc(),
                batch_id=batch_id
            )

        finally:
            total = stats["funkos"] + stats["comics"]
            await update_checkpoint(
                db, job_name,
                is_running=False,
                processed_delta=total,
                updated_delta=stats["changed"],
                errors_delta=stats["errors"]
            )

        logger.info(
            f"[{job_name}] Complete: {stats['funkos']} Funkos + {stats['comics']} Comics = "
            f"{stats['funkos'] + stats['comics']} snapshots, {stats['changed']} changed, {stats['errors']} errors"
        )


# =============================================================================
# SELF-HEALING JOB (v1.9.0 - Automated Recovery)
# =============================================================================

# Track auto-restart counts per job (reset daily)
_auto_restart_counts: Dict[str, list] = {}


async def run_self_healing_job():
    """
    Self-healing job that detects and restarts stalled pipeline jobs.

    v1.23.0: Enhanced with Autonomous Resilience System features:
    - Auto-unpause jobs paused > 30 minutes
    - Decay error counts on recovery
    - Audit logging with hash chain
    This job runs periodically (every 10 minutes by default) and:
    1. Checks for jobs marked as running but not making progress
    2. Clears stale checkpoints and restarts stuck jobs
    3. Auto-unpauses jobs paused too long (v1.23.0)
    4. Tracks restart counts to avoid infinite restart loops
    5. Logs all healing actions for debugging and audit

    Per constitution_defect_resolution.json:
    > "self_correct": "Automation reopens incidents lacking closure"
    """
    from app.core.config import settings
    from datetime import timezone

    job_name = "self_healing"
    logger.info(f"[{job_name}] Starting self-healing check...")

    async with AsyncSessionLocal() as db:
        healed_count = 0

        try:
            # Get all checkpoints that claim to be running
            result = await db.execute(text("""
                SELECT
                    job_name, is_running, last_run_started, updated_at,
                    total_processed, state_data
                FROM pipeline_checkpoints
                WHERE is_running = true
            """))
            running_jobs = result.fetchall()

            if not running_jobs:
                logger.info(f"[{job_name}] No running jobs found - system healthy")
                return {"healed": 0, "checked": 0}

            logger.info(f"[{job_name}] Found {len(running_jobs)} jobs claiming to be running")

            now = datetime.now(timezone.utc)
            today = now.date().isoformat()

            for job in running_jobs:
                job_name_to_check = job.job_name
                last_run_started = job.last_run_started
                updated_at = job.updated_at
                total_processed = job.total_processed or 0

                # Calculate how long since last activity
                if updated_at:
                    # Make sure we're comparing timezone-aware datetimes
                    if updated_at.tzinfo is None:
                        from datetime import timezone as tz
                        updated_at = updated_at.replace(tzinfo=tz.utc)
                    minutes_since_update = (now - updated_at).total_seconds() / 60
                else:
                    minutes_since_update = float('inf')

                logger.info(
                    f"[{job_name}] Checking {job_name_to_check}: "
                    f"minutes_since_update={minutes_since_update:.1f}, "
                    f"threshold={SELF_HEAL_STALL_THRESHOLD_MINUTES}"
                )

                # Is this job stalled?
                if minutes_since_update > SELF_HEAL_STALL_THRESHOLD_MINUTES:
                    logger.warning(
                        f"[{job_name}] STALL DETECTED: {job_name_to_check} "
                        f"has not updated in {minutes_since_update:.1f} minutes"
                    )

                    # Check restart count for today
                    if job_name_to_check not in _auto_restart_counts:
                        _auto_restart_counts[job_name_to_check] = []

                    # Clean up old restart records (keep only today's)
                    _auto_restart_counts[job_name_to_check] = [
                        ts for ts in _auto_restart_counts[job_name_to_check]
                        if ts.startswith(today)
                    ]

                    restart_count_today = len(_auto_restart_counts[job_name_to_check])

                    if restart_count_today >= SELF_HEAL_MAX_AUTO_RESTARTS:
                        logger.error(
                            f"[{job_name}] GIVING UP: {job_name_to_check} has been "
                            f"auto-restarted {restart_count_today} times today. "
                            f"Max is {SELF_HEAL_MAX_AUTO_RESTARTS}. Manual intervention required."
                        )
                        # Update checkpoint with error message
                        await db.execute(text("""
                            UPDATE pipeline_checkpoints
                            SET is_running = false,
                                last_error = 'SELF-HEAL LIMIT REACHED: Auto-restarted ' ||
                                    :count || ' times today. Manual intervention required. ' ||
                                    NOW()::text
                            WHERE job_name = :name
                        """), {"name": job_name_to_check, "count": restart_count_today})
                        await db.commit()
                        continue

                    # Clear the stale checkpoint
                    logger.info(f"[{job_name}] Clearing stale checkpoint for {job_name_to_check}")
                    await db.execute(text("""
                        UPDATE pipeline_checkpoints
                        SET is_running = false,
                            last_error = 'SELF-HEALED: Cleared stale checkpoint at ' || NOW()::text ||
                                ' (was stalled for ' || :minutes || ' minutes)'
                        WHERE job_name = :name
                    """), {"name": job_name_to_check, "minutes": str(int(minutes_since_update))})
                    await db.commit()

                    # Record this restart
                    _auto_restart_counts[job_name_to_check].append(f"{today}_{now.isoformat()}")

                    # Restart specific jobs
                    if job_name_to_check == "gcd_import":
                        logger.info(f"[{job_name}] AUTO-RESTARTING gcd_import job...")
                        try:
                            # Don't await - fire and forget to avoid blocking
                            asyncio.create_task(_restart_gcd_import())
                            healed_count += 1
                            logger.info(f"[{job_name}] GCD import restart initiated")
                        except Exception as e:
                            logger.error(f"[{job_name}] Failed to restart gcd_import: {e}")

                    # Add handlers for other jobs as needed
                    elif job_name_to_check in ("comic_enrichment", "funko_price_check"):
                        # These jobs will restart on next scheduler cycle
                        healed_count += 1
                        logger.info(
                            f"[{job_name}] {job_name_to_check} checkpoint cleared - "
                            f"will restart on next scheduler cycle"
                        )

        except Exception as e:
            logger.error(f"[{job_name}] Self-healing job failed: {e}")
            traceback.print_exc()

        # v1.23.0: Check for jobs that have been paused too long
        logger.info(f"[{job_name}] Checking for stale paused jobs...")
        try:
            paused_result = await db.execute(text("""
                SELECT job_name, control_signal, paused_at, total_errors, updated_at
                FROM pipeline_checkpoints
                WHERE control_signal = 'pause'
                  AND paused_at IS NOT NULL
            """))
            paused_jobs = paused_result.fetchall()

            for pj in paused_jobs:
                if pj.paused_at:
                    paused_at = pj.paused_at
                    if paused_at.tzinfo is None:
                        from datetime import timezone as tz
                        paused_at = paused_at.replace(tzinfo=tz.utc)

                    pause_minutes = (now - paused_at).total_seconds() / 60

                    if pause_minutes > SELF_HEAL_STALE_PAUSE_THRESHOLD_MINUTES:
                        logger.warning(
                            f"[{job_name}] AUTO-RESUMING {pj.job_name} - "
                            f"paused for {pause_minutes:.1f} minutes (threshold: {SELF_HEAL_STALE_PAUSE_THRESHOLD_MINUTES})"
                        )

                        await db.execute(text("""
                            UPDATE pipeline_checkpoints
                            SET control_signal = 'run',
                                paused_at = NULL,
                                last_error = 'AUTO-RESUMED: Cleared stale pause at ' || NOW()::text ||
                                    ' (was paused for ' || :minutes || ' minutes)',
                                updated_at = NOW()
                            WHERE job_name = :name
                        """), {"name": pj.job_name, "minutes": str(int(pause_minutes))})
                        await db.commit()
                        healed_count += 1

                        # Log to audit table
                        try:
                            await db.execute(text("""
                                INSERT INTO self_healing_audit (action, job_name, details)
                                VALUES ('JOB_AUTO_RESUMED', :job_name, :details::jsonb)
                            """), {
                                "job_name": pj.job_name,
                                "details": json.dumps({
                                    "pause_duration_minutes": int(pause_minutes),
                                    "previous_error_count": pj.total_errors or 0
                                })
                            })
                            await db.commit()
                        except Exception as audit_err:
                            logger.warning(f"[{job_name}] Failed to log audit: {audit_err}")

        except Exception as pause_err:
            logger.error(f"[{job_name}] Error checking paused jobs: {pause_err}")

        # v1.23.0: Decay error counts for jobs that have been stable
        try:
            decay_result = await db.execute(text("""
                SELECT job_name, total_errors, updated_at
                FROM pipeline_checkpoints
                WHERE total_errors > 0
                  AND is_running = FALSE
                  AND (control_signal IS NULL OR control_signal = 'run')
            """))
            decay_jobs = decay_result.fetchall()

            for dj in decay_jobs:
                if dj.updated_at:
                    updated_at = dj.updated_at
                    if updated_at.tzinfo is None:
                        from datetime import timezone as tz
                        updated_at = updated_at.replace(tzinfo=tz.utc)

                    hours_stable = (now - updated_at).total_seconds() / 3600

                    # Decay errors by 100 for every hour of stability
                    if hours_stable >= 1.0 and dj.total_errors > 0:
                        new_errors = max(0, dj.total_errors - 100)
                        await db.execute(text("""
                            UPDATE pipeline_checkpoints
                            SET total_errors = :errors,
                                updated_at = NOW()
                            WHERE job_name = :name
                        """), {"name": dj.job_name, "errors": new_errors})
                        await db.commit()
                        logger.info(
                            f"[{job_name}] Decayed errors for {dj.job_name}: "
                            f"{dj.total_errors} -> {new_errors}"
                        )

        except Exception as decay_err:
            logger.error(f"[{job_name}] Error decaying errors: {decay_err}")

        logger.info(f"[{job_name}] Complete: healed {healed_count} stalled jobs")
        return {"healed": healed_count, "checked": len(running_jobs) if 'running_jobs' in dir() else 0}


async def _restart_gcd_import():
    """
    Helper to restart GCD import job asynchronously.

    v1.9.2: The job's finally block now handles offset sync automatically,
    so this helper just needs to trigger the job. The job will:
    1. Read offset from checkpoint (already synced by previous run's finally block)
    2. Resume from correct position
    3. Sync offset again on exit (normal, error, or interrupt)
    """
    try:
        logger.info("[_restart_gcd_import] Starting GCD import (offset auto-synced by previous run)...")
        result = await run_gcd_import_job()
        logger.info(f"[_restart_gcd_import] GCD import completed: {result}")
    except Exception as e:
        logger.error(f"[_restart_gcd_import] GCD import failed: {e}")
        traceback.print_exc()


# =============================================================================
# GCD IMPORT JOB (v1.8.0 - Grand Comics Database)
# =============================================================================

# =============================================================================
# CROSS-REFERENCE MATCHING JOB (v1.8.0 - GCD-Primary)
# =============================================================================

async def run_cross_reference_job(
    batch_size: int = 100,
    max_records: int = 0,
):
    """
    Link records across data sources using cross-reference matching.

    This job finds GCD records that don't have Metron/PriceCharting IDs
    and attempts to match them using ISBN, UPC, or fuzzy title matching.

    Args:
        batch_size: Records per batch
        max_records: Limit for subset processing (0 = unlimited)
    """
    from app.services.cross_reference import cross_reference_matcher

    job_name = "cross_reference"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting cross-reference job (batch: {batch_id})")

    async with AsyncSessionLocal() as db:
        # Atomically claim job to prevent race condition
        claimed, checkpoint = await try_claim_job(db, job_name, "matching", batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or claim failed, skipping")
            return {"status": "skipped", "message": "Job already running"}

        stats = {"processed": 0, "matched": 0, "linked": 0, "errors": 0}

        try:
            # Find GCD records that are missing cross-references
            # These have gcd_id but no metron_id or pricecharting_id
            query = """
                SELECT id, gcd_id, gcd_series_id, title, issue_number,
                       isbn, upc, cover_date, release_date
                FROM comic_issues
                WHERE gcd_id IS NOT NULL
                AND (metron_id IS NULL OR pricecharting_id IS NULL)
                AND id > COALESCE(:last_id, 0)
                ORDER BY id
                LIMIT :batch_size
            """

            last_processed_id = checkpoint.get("last_processed_id") or 0

            while True:
                result = await db.execute(text(query), {
                    "last_id": last_processed_id,
                    "batch_size": batch_size,
                })
                records = result.fetchall()

                if not records:
                    logger.info(f"[{job_name}] No more records to process")
                    break

                logger.info(f"[{job_name}] Processing batch of {len(records)} records")

                for record in records:
                    try:
                        # Build record dict for matcher
                        gcd_record = {
                            "id": record.id,
                            "gcd_id": record.gcd_id,
                            "gcd_series_id": record.gcd_series_id,
                            "series_name": record.title,  # Title often includes series name
                            "issue_number": record.issue_number,
                            "isbn": record.isbn,
                            "upc": record.upc,
                            "cover_date": record.cover_date,
                            "release_date": record.release_date,
                        }

                        # Find matches
                        matches = await cross_reference_matcher.find_matches_for_gcd_record(
                            db, gcd_record
                        )

                        if matches:
                            stats["matched"] += 1

                            # Link best match (highest confidence)
                            best_match = matches[0]
                            if best_match.confidence >= 0.75:  # Minimum confidence threshold
                                linked = await cross_reference_matcher.link_records(
                                    db,
                                    source_id=record.id,
                                    target_id=best_match.target_id,
                                    match_type=best_match.match_type,
                                    confidence=best_match.confidence,
                                )
                                if linked:
                                    stats["linked"] += 1
                                    logger.debug(
                                        f"[{job_name}] Linked {record.id} -> {best_match.target_id} "
                                        f"({best_match.match_type}, {best_match.confidence:.2f})"
                                    )

                        stats["processed"] += 1
                        last_processed_id = record.id

                    except Exception as e:
                        logger.error(f"[{job_name}] Error processing record {record.id}: {e}")
                        stats["errors"] += 1
                        await add_to_dlq(
                            db, job_name, str(e),
                            entity_type="comic_issue",
                            entity_id=record.id,
                            external_id=str(record.gcd_id),
                            error_type=type(e).__name__,
                            error_trace=traceback.format_exc()[:2000],
                            batch_id=batch_id
                        )

                # Update checkpoint after each batch
                await update_checkpoint(
                    db, job_name,
                    last_processed_id=last_processed_id,
                    processed_delta=len(records),
                    updated_delta=stats["linked"],
                    errors_delta=stats["errors"],
                )
                await db.commit()

                logger.info(
                    f"[{job_name}] Batch complete: {len(records)} processed, "
                    f"{stats['matched']} matches, {stats['linked']} linked"
                )

                # Check limit
                if max_records and stats["processed"] >= max_records:
                    logger.info(f"[{job_name}] Reached max_records limit ({max_records})")
                    break

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()
            await update_checkpoint(db, job_name, last_error=str(e), errors_delta=1)

        finally:
            await update_checkpoint(db, job_name, is_running=False)

        logger.info(
            f"[{job_name}] Complete: {stats['processed']} processed, "
            f"{stats['matched']} matches found, {stats['linked']} linked, {stats['errors']} errors"
        )

        return {
            "status": "completed",
            "batch_id": batch_id,
            "stats": stats,
        }


# =============================================================================
# COVER HASH BACKFILL JOB (BE-004 - Image Search Support)
# =============================================================================

async def run_cover_hash_backfill_job(batch_size: int = 50, max_records: int = 0):
    """
    Backfill cover_hash for comics that have image URLs but no hash.

    This enables image-based comic cover search by generating perceptual
    hashes from existing cover image URLs.

    Args:
        batch_size: Number of images to process per batch (default 50)
        max_records: Limit total records processed (0 = unlimited)
    """
    from app.services.comic_cache import generate_cover_hash_from_url

    job_name = "cover_hash_backfill"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting cover hash backfill (batch: {batch_id})")

    async with AsyncSessionLocal() as db:
        # Atomically claim job
        claimed, checkpoint = await try_claim_job(db, job_name, "backfill", batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or claim failed, skipping")
            return {"status": "skipped", "message": "Job already running"}

        stats = {"processed": 0, "hashed": 0, "errors": 0, "skipped": 0}

        try:
            # Get starting offset from checkpoint
            state_data = checkpoint.get("state_data") or {}
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            while True:
                # Find comics with image URL but no cover_hash
                result = await db.execute(text("""
                    SELECT id, image
                    FROM comic_issues
                    WHERE image IS NOT NULL
                      AND image != ''
                      AND cover_hash IS NULL
                      AND id > :last_id
                    ORDER BY id
                    LIMIT :limit
                """), {"last_id": last_id, "limit": batch_size})

                comics = result.fetchall()

                if not comics:
                    logger.info(f"[{job_name}] No more comics to process")
                    break

                logger.info(f"[{job_name}] Processing batch of {len(comics)} comics")

                for comic in comics:
                    comic_id, image_url = comic.id, comic.image
                    stats["processed"] += 1
                    last_id = comic_id

                    try:
                        # Generate hash from image URL
                        cover_hash, hash_prefix, hash_bytes = await generate_cover_hash_from_url(image_url)

                        if cover_hash:
                            # Update comic with hash
                            await db.execute(text("""
                                UPDATE comic_issues
                                SET cover_hash = :hash,
                                    cover_hash_prefix = :prefix,
                                    cover_hash_bytes = :bytes,
                                    updated_at = NOW()
                                WHERE id = :id
                            """), {
                                "id": comic_id,
                                "hash": cover_hash,
                                "prefix": hash_prefix,
                                "bytes": hash_bytes
                            })
                            stats["hashed"] += 1
                        else:
                            stats["skipped"] += 1

                    except Exception as e:
                        stats["errors"] += 1
                        logger.warning(f"[{job_name}] Error hashing comic {comic_id}: {e}")

                # Commit batch and update checkpoint
                await db.commit()

                await db.execute(text("""
                    UPDATE pipeline_checkpoints
                    SET state_data = jsonb_build_object('last_id', CAST(:last_id AS integer)),
                        updated_at = NOW()
                    WHERE job_name = :name
                """), {"name": job_name, "last_id": last_id})
                await db.commit()

                logger.info(
                    f"[{job_name}] Batch complete: {stats['processed']} processed, "
                    f"{stats['hashed']} hashed, {stats['skipped']} skipped, {stats['errors']} errors"
                )

                # Check max_records limit
                if max_records and stats["processed"] >= max_records:
                    logger.info(f"[{job_name}] Reached max_records limit ({max_records})")
                    break

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()
            await update_checkpoint(db, job_name, last_error=str(e), errors_delta=1)

        finally:
            await update_checkpoint(
                db, job_name,
                is_running=False,
                processed_delta=stats["processed"],
                updated_delta=stats["hashed"],
                errors_delta=stats["errors"]
            )

        logger.info(
            f"[{job_name}] COMPLETE: {stats['processed']} processed, "
            f"{stats['hashed']} hashed, {stats['skipped']} skipped, {stats['errors']} errors"
        )

        return {"status": "completed", "batch_id": batch_id, "stats": stats}


# =============================================================================
# IMAGE ACQUISITION JOB (v1.9.5 - Download covers to S3)
# =============================================================================

async def run_image_acquisition_job(batch_size: int = 100, max_records: int = 0):
    """
    Acquire cover images from external URLs and store in S3.

    Process:
    1. Find comics with image URL but no cover_s3_key
    2. Download image, validate, generate hash
    3. Upload to S3 with checksum verification
    4. Generate thumbnail
    5. Update database with S3 keys + hash

    Args:
        batch_size: Number of images to process per batch
        max_records: Limit total records (0 = unlimited)

    Governance:
        - constitution_cyberSec.json: Checksum validation
        - constitution_data_hygiene.json: Image validation
    """
    from app.services.image_acquisition import ImageAcquisitionService, ImageAcquisitionStatus

    job_name = "image_acquisition"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting image acquisition (batch: {batch_id})")

    stats = {
        "processed": 0,
        "acquired": 0,
        "failed": 0,
        "skipped": 0,
        "bytes_downloaded": 0
    }

    async with AsyncSessionLocal() as db:
        # Atomically claim job
        claimed, checkpoint = await try_claim_job(db, job_name, "acquisition", batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or claim failed")
            return {"status": "skipped", "message": "Job already running"}

        try:
            # Get last processed ID from checkpoint
            state_data = checkpoint.get("state_data") or {}
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            async with ImageAcquisitionService() as service:
                while True:
                    # Find comics with image URL but no S3 key
                    result = await db.execute(text("""
                        SELECT id, image
                        FROM comic_issues
                        WHERE image IS NOT NULL
                          AND image != ''
                          AND cover_s3_key IS NULL
                          AND id > :last_id
                        ORDER BY id
                        LIMIT :limit
                    """), {"last_id": last_id, "limit": batch_size})

                    comics = result.fetchall()

                    if not comics:
                        logger.info(f"[{job_name}] No more comics to process")
                        break

                    logger.info(f"[{job_name}] Processing batch of {len(comics)} comics")

                    # Build batch list
                    items = [(comic.id, comic.image) for comic in comics]

                    # Process batch concurrently
                    results = await service.acquire_batch(items)

                    # Update database with results
                    for result in results:
                        stats["processed"] += 1
                        last_id = max(last_id, result.issue_id)

                        if result.status == ImageAcquisitionStatus.SUCCESS:
                            # Update comic with S3 keys and hash
                            await db.execute(text("""
                                UPDATE comic_issues
                                SET cover_s3_key = :cover_key,
                                    thumb_s3_key = :thumb_key,
                                    cover_hash = :hash,
                                    cover_hash_prefix = :hash_prefix,
                                    cover_hash_bytes = :hash_bytes,
                                    image_checksum = :checksum,
                                    image_acquired_at = NOW(),
                                    updated_at = NOW()
                                WHERE id = :id
                            """), {
                                "id": result.issue_id,
                                "cover_key": result.cover_s3_key,
                                "thumb_key": result.thumb_s3_key,
                                "hash": result.cover_hash,
                                "hash_prefix": result.cover_hash_prefix,
                                "hash_bytes": result.cover_hash_bytes,
                                "checksum": result.checksum
                            })
                            stats["acquired"] += 1
                            stats["bytes_downloaded"] += result.file_size

                        elif result.status == ImageAcquisitionStatus.SKIPPED:
                            stats["skipped"] += 1

                        else:
                            stats["failed"] += 1
                            # Add to DLQ for retry
                            await add_to_dlq(
                                db,
                                job_type="image_acquisition",
                                error_message=result.error_message or result.status.value,
                                entity_type="comic_issue",
                                entity_id=result.issue_id,
                                error_type=result.status.value,
                                raw_data={"source_url": result.source_url},
                                batch_id=batch_id
                            )

                    # Commit batch
                    await db.commit()

                    # Update checkpoint
                    await db.execute(text("""
                        UPDATE pipeline_checkpoints
                        SET state_data = jsonb_build_object('last_id', CAST(:last_id AS integer)),
                            updated_at = NOW()
                        WHERE job_name = :name
                    """), {"name": job_name, "last_id": last_id})
                    await db.commit()

                    logger.info(
                        f"[{job_name}] Batch: {stats['processed']} processed, "
                        f"{stats['acquired']} acquired, {stats['failed']} failed"
                    )

                    # Check limits
                    if max_records and stats["processed"] >= max_records:
                        logger.info(f"[{job_name}] Reached max_records limit ({max_records})")
                        break

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()
            await update_checkpoint(db, job_name, last_error=str(e), errors_delta=1)

        finally:
            await update_checkpoint(
                db, job_name,
                is_running=False,
                processed_delta=stats["processed"],
                updated_delta=stats["acquired"],
                errors_delta=stats["failed"]
            )

    mb_downloaded = stats["bytes_downloaded"] / (1024 * 1024)
    logger.info(
        f"[{job_name}] COMPLETE: {stats['processed']} processed, "
        f"{stats['acquired']} acquired, {stats['failed']} failed, "
        f"{mb_downloaded:.1f} MB downloaded"
    )

    return {"status": "completed", "batch_id": batch_id, "stats": stats}


# =============================================================================
# MULTI-SOURCE ENRICHMENT JOB (v1.10.0)
# =============================================================================

async def run_multi_source_enrichment_job(
    batch_size: int = 100,
    max_records: int = 0,
):
    """
    Enrich comics using the multi-source rotator.

    v1.10.0 - Multi-Source Enrichment:
    - Uses source rotator for intelligent failover
    - Tries Metron first, falls back to Comic Vine, then scrapers
    - Tracks quota usage across sources
    - Logs enrichment attempts for debugging
    - Collects grading examples for AI training

    Sources (by priority):
    1. Metron (API) - Primary for descriptions, creators, characters
    2. Comic Vine (API) - Secondary metadata
    3. ComicBookRealm (scraper) - Covers, pricing, CGC data
    4. MyComicShop (scraper) - Covers, retail pricing
    5. GradingTool (scraper) - AI grading training data

    Args:
        batch_size: Records to process per batch
        max_records: Max total records (0 = unlimited)
    """
    from app.services.source_rotator import source_rotator, SourceCapability
    from app.services.quota_tracker import quota_tracker
    from app.models.source_quota import EnrichmentAttempt
    from app.adapters.comicvine_adapter import create_comicvine_adapter
    from app.adapters.comicbookrealm_adapter import create_comicbookrealm_adapter
    from app.adapters.mycomicshop_adapter import create_mycomicshop_adapter
    from app.adapters.gradingtool_adapter import create_gradingtool_adapter

    job_name = "multi_source_enrichment"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting multi-source enrichment (batch: {batch_id})")

    # Initialize all adapters
    try:
        await create_comicvine_adapter()
        logger.info(f"[{job_name}] Comic Vine adapter initialized")
    except Exception as e:
        logger.warning(f"[{job_name}] Comic Vine adapter init failed: {e}")

    try:
        await create_comicbookrealm_adapter()
        logger.info(f"[{job_name}] ComicBookRealm adapter initialized")
    except Exception as e:
        logger.warning(f"[{job_name}] ComicBookRealm adapter init failed: {e}")

    try:
        await create_mycomicshop_adapter()
        logger.info(f"[{job_name}] MyComicShop adapter initialized")
    except Exception as e:
        logger.warning(f"[{job_name}] MyComicShop adapter init failed: {e}")

    try:
        await create_gradingtool_adapter()
        logger.info(f"[{job_name}] GradingTool adapter initialized")
    except Exception as e:
        logger.warning(f"[{job_name}] GradingTool adapter init failed: {e}")

    stats = {
        "processed": 0,
        "descriptions_enriched": 0,
        "covers_enriched": 0,
        "failed": 0,
        "by_source": {},
    }

    async with AsyncSessionLocal() as db:
        # Atomically claim job
        claimed, checkpoint = await try_claim_job(db, job_name, "enrichment", batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running, skipping")
            return {"status": "skipped"}

        try:
            # Log source statuses
            statuses = await source_rotator.get_all_statuses(db)
            for name, status in statuses.items():
                logger.info(
                    f"[{job_name}] Source {name}: "
                    f"remaining={status.remaining_today}, healthy={status.is_healthy}"
                )

            # Find comics needing enrichment (missing description OR cover image)
            last_id = checkpoint.get("last_processed_id", 0)
            result = await db.execute(text("""
                SELECT id, metron_id, gcd_id, comicvine_id, series_id,
                       description, image, issue_name, number
                FROM comic_issues
                WHERE (
                    description IS NULL OR description = ''
                    OR image IS NULL OR image = ''
                )
                AND id > :last_id
                ORDER BY id
                LIMIT :batch_size
            """), {"last_id": last_id, "batch_size": batch_size})

            comics = result.fetchall()

            if not comics:
                logger.info(f"[{job_name}] No comics need enrichment")
                await update_checkpoint(db, job_name, is_running=False)
                return {"status": "completed", "stats": stats}

            logger.info(f"[{job_name}] Found {len(comics)} comics to enrich")

            for comic in comics:
                comic_id = comic.id
                stats["processed"] += 1
                needs_description = not comic.description
                needs_cover = not comic.image

                try:
                    # ===========================================================
                    # PHASE A: Fetch Description (if needed)
                    # ===========================================================
                    if needs_description:
                        async def fetch_description(adapter):
                            # Try by metron_id first, then search
                            if comic.metron_id and adapter.name == "metron":
                                data = await adapter.fetch_by_id(str(comic.metron_id))
                            elif comic.comicvine_id and adapter.name == "comicvine":
                                data = await adapter.fetch_by_id(str(comic.comicvine_id))
                            else:
                                return {}
                            if data:
                                return adapter.normalize(data)
                            return {}

                        desc_result = await source_rotator.fetch_with_fallback(
                            db,
                            capability=SourceCapability.DESCRIPTIONS,
                            fetch_func=fetch_description,
                            required_fields={"description"},
                        )

                        if desc_result.success and desc_result.data.get("description"):
                            await db.execute(text("""
                                UPDATE comic_issues
                                SET description = :description,
                                    enrichment_source = :source,
                                    updated_at = NOW()
                                WHERE id = :id AND (description IS NULL OR description = '')
                            """), {
                                "id": comic_id,
                                "description": desc_result.data["description"][:5000],
                                "source": desc_result.source_name,
                            })
                            stats["descriptions_enriched"] += 1
                            stats["by_source"][desc_result.source_name] = stats["by_source"].get(desc_result.source_name, 0) + 1

                    # ===========================================================
                    # PHASE B: Fetch Cover Image (if needed)
                    # ===========================================================
                    if needs_cover:
                        async def fetch_cover(adapter):
                            # Try by existing IDs first
                            if comic.metron_id and adapter.name == "metron":
                                data = await adapter.fetch_by_id(str(comic.metron_id))
                            elif comic.comicvine_id and adapter.name == "comicvine":
                                data = await adapter.fetch_by_id(str(comic.comicvine_id))
                            elif adapter.name in ("comicbookrealm", "mycomicshop"):
                                # Scrapers need search - try by title
                                if comic.issue_name:
                                    search_query = comic.issue_name
                                    if comic.number:
                                        search_query = f"{comic.issue_name} #{comic.number}"
                                    result = await adapter.fetch_page(q=search_query)
                                    if result.success and result.records:
                                        return adapter.normalize(result.records[0])
                                return {}
                            else:
                                return {}
                            if data:
                                return adapter.normalize(data)
                            return {}

                        cover_result = await source_rotator.fetch_with_fallback(
                            db,
                            capability=SourceCapability.COVERS,
                            fetch_func=fetch_cover,
                            required_fields={"cover_image_url"},
                        )

                        if cover_result.success and cover_result.data.get("cover_image_url"):
                            await db.execute(text("""
                                UPDATE comic_issues
                                SET image = :image_url,
                                    enrichment_source = :source,
                                    updated_at = NOW()
                                WHERE id = :id AND (image IS NULL OR image = '')
                            """), {
                                "id": comic_id,
                                "image_url": cover_result.data["cover_image_url"],
                                "source": cover_result.source_name,
                            })
                            stats["covers_enriched"] += 1
                            stats["by_source"][cover_result.source_name] = stats["by_source"].get(cover_result.source_name, 0) + 1

                    # Track if we enriched anything
                    if stats["descriptions_enriched"] > 0 or stats["covers_enriched"] > 0:
                        pass  # Success tracked above
                    else:
                        stats["failed"] += 1

                except Exception as e:
                    stats["failed"] += 1
                    logger.warning(f"[{job_name}] Error enriching comic {comic_id}: {e}")

                # Commit periodically
                if stats["processed"] % 50 == 0:
                    await db.commit()

                # Update checkpoint periodically
                if stats["processed"] % 100 == 0:
                    await db.execute(text("""
                        UPDATE pipeline_checkpoints
                        SET state_data = jsonb_build_object('last_processed_id', CAST(:last_id AS integer)),
                            updated_at = NOW()
                        WHERE job_name = :name
                    """), {"name": job_name, "last_id": comic_id})
                    await db.commit()

                # Check max records
                if max_records and stats["processed"] >= max_records:
                    logger.info(f"[{job_name}] Reached max_records ({max_records})")
                    break

            await db.commit()

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()
            await update_checkpoint(db, job_name, last_error=str(e), errors_delta=1)

        finally:
            total_enriched = stats["descriptions_enriched"] + stats["covers_enriched"]
            await update_checkpoint(
                db, job_name,
                is_running=False,
                processed_delta=stats["processed"],
                updated_delta=total_enriched,
                errors_delta=stats["failed"],
            )

    # Log rotator stats
    rotator_stats = source_rotator.get_stats()
    logger.info(
        f"[{job_name}] COMPLETE: {stats['processed']} processed, "
        f"{stats['descriptions_enriched']} descriptions, {stats['covers_enriched']} covers, "
        f"{stats['failed']} failed. By source: {stats['by_source']}"
    )
    logger.info(f"[{job_name}] Rotator stats: {rotator_stats}")

    return {"status": "completed", "batch_id": batch_id, "stats": stats}


# =============================================================================
# COVER DATE BACKFILL JOB (v1.10.3 - Populate cover_date from GCD)
# =============================================================================

async def run_cover_date_backfill_job(batch_size: int = 5000, max_records: int = 0):
    """
    Backfill cover_date and price for comics imported from GCD.

    This job reads from the GCD SQLite dump and updates existing records
    that are missing cover_date values. Works for any number of records.

    Args:
        batch_size: Number of records to process per batch (default 5000)
        max_records: Limit total records (0 = unlimited, processes all)

    Constitution Compliance:
        - constitution_db.json: Batch updates with checkpointing
        - constitution_data_hygiene.json: Source validation
    """
    from app.adapters.gcd import GCDAdapter, ensure_gcd_dump_exists

    job_name = "cover_date_backfill"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting cover_date backfill (batch: {batch_id})")

    async with AsyncSessionLocal() as db:
        # Atomically claim job
        claimed, checkpoint = await try_claim_job(db, job_name, "backfill", batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or claim failed, skipping")
            return {"status": "skipped", "message": "Job already running"}

        stats = {"processed": 0, "updated": 0, "skipped": 0, "errors": 0}

        try:
            # Ensure GCD SQLite dump exists
            db_path = await ensure_gcd_dump_exists()
            if not db_path:
                raise Exception("GCD SQLite dump not available")

            # Get starting offset from checkpoint
            state_data = checkpoint.get("state_data") or {}
            start_offset = state_data.get("offset", 0) if isinstance(state_data, dict) else 0

            logger.info(f"[{job_name}] Resuming from offset {start_offset:,}")

            # Initialize GCD adapter
            adapter = GCDAdapter()

            # Process batches from GCD dump
            current_offset = start_offset
            for batch in adapter.import_from_sqlite(db_path, offset=start_offset, limit=batch_size):
                if not batch:
                    break

                batch_updated = 0
                batch_skipped = 0

                for record in batch:
                    gcd_id = record.get("gcd_id")
                    release_date = record.get("release_date")
                    cover_price = record.get("cover_price")

                    if not gcd_id:
                        stats["errors"] += 1
                        continue

                    # Skip if no date/price to update
                    if not release_date and not cover_price:
                        batch_skipped += 1
                        continue

                    try:
                        # Update only cover_date and price where they're NULL
                        result = await db.execute(text("""
                            UPDATE comic_issues
                            SET cover_date = COALESCE(cover_date, :cover_date),
                                price = COALESCE(price, :price),
                                updated_at = NOW()
                            WHERE gcd_id = :gcd_id
                              AND (cover_date IS NULL OR price IS NULL)
                        """), {
                            "gcd_id": gcd_id,
                            "cover_date": release_date,
                            "price": cover_price,
                        })

                        if result.rowcount > 0:
                            batch_updated += 1
                        else:
                            batch_skipped += 1

                    except Exception as e:
                        logger.warning(f"[{job_name}] Error updating gcd_id={gcd_id}: {e}")
                        stats["errors"] += 1

                stats["processed"] += len(batch)
                stats["updated"] += batch_updated
                stats["skipped"] += batch_skipped
                current_offset += len(batch)

                # Commit batch and update checkpoint
                await db.commit()

                await db.execute(text("""
                    UPDATE pipeline_checkpoints
                    SET state_data = jsonb_build_object('offset', CAST(:offset AS integer)),
                        processed = COALESCE(processed, 0) + :processed,
                        updated = COALESCE(updated, 0) + :updated
                    WHERE job_name = :name
                """), {
                    "name": job_name,
                    "offset": current_offset,
                    "processed": len(batch),
                    "updated": batch_updated,
                })
                await db.commit()

                logger.info(
                    f"[{job_name}] Batch complete: {len(batch)} processed, "
                    f"{batch_updated} updated, {batch_skipped} skipped. "
                    f"Total: {stats['processed']:,}"
                )

                # Check max records
                if max_records and stats["processed"] >= max_records:
                    logger.info(f"[{job_name}] Reached max_records ({max_records})")
                    break

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()
            await update_checkpoint(db, job_name, last_error=str(e), errors_delta=1)

        finally:
            await update_checkpoint(
                db, job_name,
                is_running=False,
                processed_delta=stats["processed"],
                updated_delta=stats["updated"],
                errors_delta=stats["errors"],
            )

    logger.info(
        f"[{job_name}] COMPLETE: {stats['processed']:,} processed, "
        f"{stats['updated']:,} updated, {stats['skipped']:,} skipped, "
        f"{stats['errors']} errors"
    )

    return {"status": "completed", "batch_id": batch_id, "stats": stats}


# =============================================================================
# MAIN SCHEDULER
# =============================================================================

class PipelineScheduler:
    """
    Main scheduler that runs all pipeline jobs.

    Call start() to begin background scheduling.
    """

    def __init__(self):
        self._tasks = []
        self._running = False

    async def start(self):
        """Start all scheduled jobs."""
        if self._running:
            print("[SCHEDULER] Pipeline scheduler already running")
            return

        self._running = True
        print("=" * 60)
        print("[SCHEDULER] PIPELINE SCHEDULER STARTED")
        print("=" * 60)

        # Clear any stale checkpoints from crashed runs
        print("[SCHEDULER] Checking for stale checkpoints...")
        try:
            async with AsyncSessionLocal() as db:
                cleared = await clear_stale_checkpoints(db)
                if cleared:
                    print(f"[SCHEDULER] Cleared {cleared} stale checkpoint(s)")
                else:
                    print("[SCHEDULER] No stale checkpoints found")
        except Exception as e:
            print(f"[SCHEDULER] Warning: Could not check stale checkpoints: {e}")

        # Start job loops
        self._tasks = [
            asyncio.create_task(self._run_job_loop("funko_price_check", run_funko_price_check_job, interval_minutes=60)),
            asyncio.create_task(self._run_job_loop("dlq_retry", run_dlq_retry_job, interval_minutes=15)),
            # v1.7.0: Daily price snapshot for AI/ML training (runs once per day at startup, then every 24h)
            asyncio.create_task(self._run_job_loop("daily_snapshot", run_daily_snapshot_job, interval_minutes=1440)),
            # v1.8.0: HIGH-005 - Cross-reference job for GCD-to-Metron/Primary linking
            asyncio.create_task(self._run_job_loop("cross_reference", run_cross_reference_job, interval_minutes=60)),
            # v1.9.0: Self-healing job - detects and restarts stalled jobs automatically
            asyncio.create_task(self._run_job_loop("self_healing", run_self_healing_job, interval_minutes=SELF_HEAL_CHECK_INTERVAL_MINUTES)),
            # v1.9.3: DEPRECATED - Use independent jobs instead (v1.23.0+)
            # PC-ANALYSIS-2025-12-18: Removed run_full_price_sync_job (replaced by funko_price_sync + comic_price_sync)
            # PC-ANALYSIS-2025-12-18: Removed run_pricecharting_matching_job (replaced by funko_pricecharting_match + comic_pricecharting_match)
            # v1.10.3: Comprehensive enrichment - ALL sources, ALL fields, PARALLEL
            asyncio.create_task(self._run_job_loop("comprehensive_enrichment", run_comprehensive_enrichment_job, interval_minutes=30)),
            # v1.10.4: GCD Import - runs every 60 min until complete (~170k remaining)
            asyncio.create_task(self._run_job_loop("gcd_import", run_gcd_import_job, interval_minutes=60)),
            # v1.10.5: Phase 3 Cover Enrichment - Covers + Creators from ComicVine
            asyncio.create_task(self._run_job_loop("cover_enrichment", run_cover_enrichment_job, interval_minutes=60)),
            # v1.10.8: Marvel Fandom Enrichment - Story-level credits
            asyncio.create_task(self._run_job_loop("marvel_fandom", run_marvel_fandom_job, interval_minutes=60)),
            # v1.12.0: UPC Backfill - Recover missing UPCs from Metron/CBR
            asyncio.create_task(self._run_job_loop("upc_backfill", run_upc_backfill_job, interval_minutes=60)),
            # v1.25.0: Convention refresh (GalaxyCon Columbus pages -> ML features)
            asyncio.create_task(self._run_job_loop("convention_refresh", run_convention_refresh_job, interval_minutes=1440, initial_delay_minutes=30)),
            # v1.13.0: Sequential Exhaustive Enrichment - ONE row at a time, ALL sources exhausted
            asyncio.create_task(self._run_job_loop("sequential_enrichment", run_sequential_exhaustive_enrichment_job, interval_minutes=30)),
            # v1.21.0: Inbound cover processor - watches Inbound folder, queues to Match Review
            asyncio.create_task(self._run_job_loop("inbound_processor", run_inbound_processor, interval_minutes=5)),
            # v1.21.2: Image acquisition - download external cover URLs to S3
            asyncio.create_task(self._run_job_loop("image_acquisition", run_image_acquisition_job, interval_minutes=30)),
            # v1.22.0: BCW Dropship Integration - inventory and order sync
            asyncio.create_task(self._run_job_loop("bcw_inventory_sync", run_bcw_inventory_sync_job, interval_minutes=60)),
            asyncio.create_task(self._run_job_loop("bcw_full_inventory_sync", run_bcw_full_inventory_sync_job, interval_minutes=1440)),
            asyncio.create_task(self._run_job_loop("bcw_order_status_sync", run_bcw_order_status_sync_job, interval_minutes=30)),
            asyncio.create_task(self._run_job_loop("bcw_email_processing", run_bcw_email_processing_job, interval_minutes=15)),
            asyncio.create_task(self._run_job_loop("bcw_quote_cleanup", run_bcw_quote_cleanup_job, interval_minutes=60)),
            asyncio.create_task(self._run_job_loop("bcw_selector_health", run_bcw_selector_health_job, interval_minutes=1440)),
            # v1.23.0: PriceCharting Independent Jobs (Autonomous Resilience System)
            # v1.24.0: Staggered starts (15-min offsets) to prevent combined rate limit pressure
            # Pattern: funko_match(0) -> comic_match(15) -> funko_sync(30) -> comic_sync(45)
            asyncio.create_task(self._run_job_loop("funko_pricecharting_match", run_funko_pricecharting_match_job, interval_minutes=60, initial_delay_minutes=0)),
            asyncio.create_task(self._run_job_loop("comic_pricecharting_match", run_comic_pricecharting_match_job, interval_minutes=60, initial_delay_minutes=15)),
            asyncio.create_task(self._run_job_loop("funko_price_sync", run_funko_price_sync_job, interval_minutes=60, initial_delay_minutes=30)),
            asyncio.create_task(self._run_job_loop("comic_price_sync", run_comic_price_sync_job, interval_minutes=1440, initial_delay_minutes=45)),
            # v1.24.0: Pipeline Instrumentation (Stall Detection & Metrics Retention)
            # Stall detection runs every 2 minutes for responsive stall recovery
            asyncio.create_task(self._run_job_loop("stall_detection", run_stall_detection_job, interval_minutes=2)),
            # Metrics retention runs daily at 3 AM (1440 min = 24h, delay ensures staggered start)
            asyncio.create_task(self._run_job_loop("metrics_retention", run_metrics_retention_job, interval_minutes=1440, initial_delay_minutes=180)),
        ]

        print("[SCHEDULER] Scheduled jobs:")
        print("[SCHEDULER]   - comic_enrichment: every 30 minutes")
        print("[SCHEDULER]   - funko_price_check: every 60 minutes")
        print("[SCHEDULER]   - dlq_retry: every 15 minutes")
        print("[SCHEDULER]   - daily_snapshot: every 24 hours (AI/ML data)")
        print("[SCHEDULER]   - cross_reference: every 60 minutes (GCD-Primary linking)")
        print(f"[SCHEDULER]   - self_healing: every {SELF_HEAL_CHECK_INTERVAL_MINUTES} minutes (auto-restart stalled jobs)")
        # PC-ANALYSIS-2025-12-18: Removed deprecated jobs (replaced by independent v1.23.0 jobs)
        print("[SCHEDULER]   - comprehensive_enrichment: every 30 minutes (ALL sources, parallel)")
        print("[SCHEDULER]   - gcd_import: every 60 minutes (finish remaining ~170k records)")
        print("[SCHEDULER]   - cover_enrichment: every 60 minutes (Phase 3: covers + creators from ComicVine)")
        print("[SCHEDULER]   - marvel_fandom: every 60 minutes (Story-level credits from Marvel Database)")
        print("[SCHEDULER]   - sequential_enrichment: every 30 minutes (ONE row, ALL sources exhausted)")
        print("[SCHEDULER]   - inbound_processor: every 5 minutes (watch Inbound folder, queue to Match Review)")
        print("[SCHEDULER]   - image_acquisition: every 30 minutes (download external cover URLs to S3)")
        print("[SCHEDULER]   - bcw_inventory_sync: every 60 minutes (hot items inventory sync)")
        print("[SCHEDULER]   - bcw_full_inventory_sync: every 24 hours (full inventory sync)")
        print("[SCHEDULER]   - bcw_order_status_sync: every 30 minutes (poll order status)")
        print("[SCHEDULER]   - bcw_email_processing: every 15 minutes (parse BCW emails)")
        print("[SCHEDULER]   - bcw_quote_cleanup: every 60 minutes (cleanup expired quotes)")
        print("[SCHEDULER]   - bcw_selector_health: every 24 hours (validate DOM selectors)")
        print("[SCHEDULER]   - convention_refresh: every 24 hours, delay=30min (GalaxyCon pages -> ML features)")
        print("[SCHEDULER]   - funko_pricecharting_match: every 60 minutes, delay=0min (match Funkos to PC IDs)")
        print("[SCHEDULER]   - comic_pricecharting_match: every 60 minutes, delay=15min (match Comics to PC IDs)")
        print("[SCHEDULER]   - funko_price_sync: every 60 minutes, delay=30min (sync Funko prices from PC)")
        print("[SCHEDULER]   - comic_price_sync: every 24 hours, delay=45min (sync Comic prices from PC)")
        print("[SCHEDULER]   - stall_detection: every 2 minutes (adaptive stall detection & self-healing)")
        print("[SCHEDULER]   - metrics_retention: every 24 hours, delay=3h (90-day retention cleanup)")
        print("[SCHEDULER] Jobs will start after 5 second delay...")

    async def stop(self):
        """Stop all scheduled jobs."""
        self._running = False
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks = []
        logger.info("Pipeline scheduler stopped")

    async def _run_job_loop(self, name: str, job_func, interval_minutes: int, initial_delay_minutes: int = 0):
        """
        Run a job on a schedule.

        v1.24.0: Added initial_delay_minutes for job staggering to prevent
        simultaneous API calls from multiple jobs hitting the same rate limiter.

        Args:
            name: Job name for logging
            job_func: Async function to call
            interval_minutes: Time between runs
            initial_delay_minutes: Initial delay before first run (for staggering)
        """
        interval_seconds = interval_minutes * 60

        # Initial delay to stagger job starts (v1.24.0: configurable per-job)
        initial_delay_seconds = 5 + (initial_delay_minutes * 60)
        if initial_delay_minutes > 0:
            logger.info(f"[SCHEDULER] {name} will start after {initial_delay_minutes}min delay")
        await asyncio.sleep(initial_delay_seconds)

        while self._running:
            try:
                print(f"[SCHEDULER] Running {name}...")
                await job_func()
                print(f"[SCHEDULER] Job {name} completed")
            except Exception as e:
                print(f"[SCHEDULER] Job {name} failed: {e}")

            print(f"[SCHEDULER] Next {name} run in {interval_minutes} minutes")
            await asyncio.sleep(interval_seconds)

    async def run_job_now(self, job_name: str, **kwargs):
        """
        Manually trigger a job.

        v1.20.0: Auto-clears pause/stop signals before running to support
        cron auto-resume of paused/stopped jobs.
        """
        jobs = {
            "funko_price_check": run_funko_price_check_job,
            "dlq_retry": run_dlq_retry_job,
            "daily_snapshot": run_daily_snapshot_job,  # v1.7.0
            "gcd_import": run_gcd_import_job,  # v1.8.0
            "cross_reference": run_cross_reference_job,  # v1.8.0 - GCD-Primary
            "self_healing": run_self_healing_job,  # v1.9.0 - Auto-restart stalled jobs
            # PC-ANALYSIS-2025-12-18: Removed deprecated jobs (use independent v1.23.0 jobs)
            # "full_price_sync": DEPRECATED -> use funko_price_sync + comic_price_sync
            # "pricecharting_matching": DEPRECATED -> use funko_pricecharting_match + comic_pricecharting_match
            "cover_hash_backfill": run_cover_hash_backfill_job,  # v1.9.4 - Generate cover hashes for image search
            "image_acquisition": run_image_acquisition_job,  # v1.9.5 - Download covers to S3
            "multi_source_enrichment": run_multi_source_enrichment_job,  # v1.10.0 - Multi-source with rotator
            "cover_date_backfill": run_cover_date_backfill_job,  # v1.10.3 - Backfill cover_date from GCD
            "comprehensive_enrichment": run_comprehensive_enrichment_job,  # v1.10.3 - All sources, all fields, parallel
            "cover_enrichment": run_cover_enrichment_job,  # v1.10.5 - Phase 3: covers + creators from ComicVine
            "marvel_fandom": run_marvel_fandom_job,  # v1.10.8 - Story-level credits from Marvel Database
            "upc_backfill": run_upc_backfill_job,  # v1.12.0 - Multi-source UPC recovery from Metron/CBR
            "sequential_enrichment": run_sequential_exhaustive_enrichment_job,  # v1.13.0 - Sequential exhaustive enrichment
            # v1.23.0: PriceCharting Independent Jobs (Autonomous Resilience System)
            "funko_pricecharting_match": run_funko_pricecharting_match_job,
            "comic_pricecharting_match": run_comic_pricecharting_match_job,
            "funko_price_sync": run_funko_price_sync_job,
            "comic_price_sync": run_comic_price_sync_job,
            # v1.24.0: Pipeline Instrumentation (Stall Detection & Metrics Retention)
            "stall_detection": run_stall_detection_job,
            "metrics_retention": run_metrics_retention_job,
        }

        if job_name not in jobs:
            raise ValueError(f"Unknown job: {job_name}. Available: {list(jobs.keys())}")

        # v1.20.0: Auto-clear pause/stop signals before running
        # This enables cron auto-resume of paused/stopped jobs
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(text("""
                    UPDATE pipeline_checkpoints
                    SET control_signal = 'run',
                        paused_at = NULL,
                        updated_at = NOW()
                    WHERE job_name = :name
                    AND control_signal IN ('pause', 'stop')
                    RETURNING control_signal
                """), {"name": job_name})
                cleared = result.fetchone()
                if cleared:
                    await db.commit()
                    logger.info(f"[SCHEDULER] Auto-resumed {job_name} (was paused/stopped)")
        except Exception as e:
            logger.warning(f"[SCHEDULER] Failed to clear control signal for {job_name}: {e}")

        logger.info(f"[SCHEDULER] Manual trigger: {job_name}")
        return await jobs[job_name](**kwargs)


# Global scheduler instance
pipeline_scheduler = PipelineScheduler()
