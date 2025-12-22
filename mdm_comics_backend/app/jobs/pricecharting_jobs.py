"""
PriceCharting Independent Jobs v1.3.0

Document ID: IMPL-PC-2025-12-17
Status: APPROVED

This module implements the independent PriceCharting jobs per the
Autonomous Resilience System proposal.

Jobs:
1. funko_pricecharting_match_job - Match Funkos to PriceCharting IDs
2. comic_pricecharting_match_job - Match Comics to PriceCharting IDs
3. funko_price_sync_job - Sync prices for matched Funkos
4. comic_price_sync_job - Sync prices for matched Comics

Architecture:
- Each job runs independently (no phase blocking)
- Per-job circuit breaker provides isolation (v1.3.0)
- Checkpoints are persisted per job
- Self-healer can auto-resume paused jobs

v1.3.0 Changes (PC-OPT-2024-001 Phase 5):
- Per-job circuit breaker isolation - one job failing won't affect others
- Job-specific circuit breaker configuration (match vs sync)
- Automatic DB state persistence for circuit breaker

v1.2.0 Changes (PC-OPT-2024-001 Phase 3):
- Added multi-factor match scoring to reduce false positives
- Title, year, publisher, issue number all contribute to match score
- Configurable threshold (default: 0.6) with confidence levels

v1.1.0 Changes (PC-OPT-2024-001 Phase 1-2):
- Added SearchCache integration to reduce duplicate API calls
- Title searches now cached with 1-hour TTL
- Added incremental sync (only sync stale records >24h)
- Expected 80%+ reduction in PriceCharting API calls

Per constitution_cyberSec.json Section 5:
> "All partner API clients implement retries with bounded budgets and circuit breakers"

Per constitution_devops_doctrine.json:
> "circuit_breakers": true
"""
import json
import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.core.http_client import get_pricecharting_client
from app.core.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    get_circuit_breaker,
    log_circuit_event_to_audit,
)
from app.core.job_circuit_breaker import (
    JobCircuitBreaker,
    get_job_circuit_from_db,
)
from app.core.search_cache import pricecharting_search_cache
from app.services.match_scoring import find_best_match, MATCH_THRESHOLD
from app.services.pipeline_metrics import (
    pipeline_metrics,
    PipelineType,
    BatchResult,
    ApiCallResult,
    ApiSource,
    generate_batch_id,
)

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION (Phase 2 - PC-ANALYSIS-2025-12-18)
# =============================================================================
# All values configurable via environment variables for runtime tuning

# Batch size for database queries (default: 100)
PC_BATCH_SIZE = int(os.getenv("PRICECHARTING_BATCH_SIZE", "100"))

# Staleness threshold in hours - records older than this will be re-synced (default: 24)
PC_STALENESS_HOURS = int(os.getenv("PRICECHARTING_STALENESS_HOURS", "24"))

# API quota tracking (daily limit estimate)
PC_DAILY_QUOTA_LIMIT = int(os.getenv("PRICECHARTING_DAILY_QUOTA", "10000"))

# Price change threshold for alerts (percentage, default: 20%)
PC_PRICE_ALERT_THRESHOLD = float(os.getenv("PRICECHARTING_PRICE_ALERT_PCT", "20.0"))

# PHASE 2 (IMPL-2025-12-21-PC-REFACTOR): Force sync bypasses staleness check
PC_FORCE_SYNC = os.getenv("PRICECHARTING_FORCE_SYNC", "false").lower() in ("true", "1", "yes")

logger.info(
    f"[pricecharting_jobs] Config loaded: batch_size={PC_BATCH_SIZE}, "
    f"staleness_hours={PC_STALENESS_HOURS}, daily_quota={PC_DAILY_QUOTA_LIMIT}, "
    f"force_sync={PC_FORCE_SYNC}"
)


# =============================================================================
# API QUOTA TELEMETRY (Phase 2 - PC-ANALYSIS-2025-12-18)
# =============================================================================

class APIQuotaTracker:
    """
    Simple in-memory API quota tracker with daily reset.

    Logs warnings when approaching quota limits.
    """
    def __init__(self, daily_limit: int):
        self.daily_limit = daily_limit
        self.calls_today = 0
        self.reset_date = datetime.now(timezone.utc).date()
        self._lock = None  # Will be lazily initialized

    def _check_reset(self):
        """Reset counter if it's a new day."""
        today = datetime.now(timezone.utc).date()
        if today > self.reset_date:
            logger.info(f"[API_QUOTA] New day - resetting counter (was {self.calls_today})")
            self.calls_today = 0
            self.reset_date = today

    def record_call(self, job_name: str = "unknown"):
        """Record an API call and log quota status."""
        self._check_reset()
        self.calls_today += 1

        # Calculate usage percentage
        usage_pct = (self.calls_today / self.daily_limit) * 100

        # Log warnings at thresholds
        if self.calls_today == int(self.daily_limit * 0.75):
            logger.warning(
                f"[API_QUOTA] 75% of daily quota used ({self.calls_today}/{self.daily_limit})"
            )
        elif self.calls_today == int(self.daily_limit * 0.90):
            logger.warning(
                f"[API_QUOTA] 90% of daily quota used ({self.calls_today}/{self.daily_limit}) - "
                f"Consider reducing batch sizes"
            )
        elif self.calls_today >= self.daily_limit:
            logger.error(
                f"[API_QUOTA] Daily quota EXCEEDED ({self.calls_today}/{self.daily_limit}) - "
                f"Job: {job_name}"
            )

        return self.calls_today

    def get_stats(self) -> dict:
        """Get current quota statistics."""
        self._check_reset()
        return {
            "calls_today": self.calls_today,
            "daily_limit": self.daily_limit,
            "usage_pct": round((self.calls_today / self.daily_limit) * 100, 1),
            "remaining": max(0, self.daily_limit - self.calls_today),
        }


# Global quota tracker instance
pc_quota_tracker = APIQuotaTracker(PC_DAILY_QUOTA_LIMIT)


# =============================================================================
# CACHED SEARCH HELPER (v1.1.0 - PC-OPT-2024-001 Phase 1)
# =============================================================================

async def cached_pricecharting_search(
    client,
    pc_token: str,
    query: str,
    circuit_breaker: CircuitBreaker,
    console_name: str = None,
    use_upc: bool = False,
) -> list:
    """
    Perform a cached PriceCharting API search.

    Uses the global pricecharting_search_cache to avoid duplicate API calls
    for the same search query within the TTL window.

    Args:
        client: HTTP client for API calls
        pc_token: PriceCharting API token
        query: Search query or UPC/ISBN
        circuit_breaker: Circuit breaker for API protection
        console_name: Filter by console/category (e.g., "Comics")
        use_upc: If True, search by UPC instead of query string

    Returns:
        List of product results from PriceCharting API
    """
    # Build cache key filters
    filters = {}
    if console_name:
        filters["console_name"] = console_name
    if use_upc:
        filters["search_type"] = "upc"

    async def do_search(q: str, **kwargs) -> list:
        """Execute the actual API search."""
        params = {"t": pc_token}

        if kwargs.get("search_type") == "upc":
            params["upc"] = q
        else:
            params["q"] = q
            if kwargs.get("console_name"):
                params["console-name"] = kwargs["console_name"]

        response = await circuit_breaker.execute(
            lambda: client.get(
                "https://www.pricecharting.com/api/products",
                params=params
            )
        )

        # Phase 2: Track API quota usage
        pc_quota_tracker.record_call("search")

        if response.status_code == 200:
            try:
                data = response.json()
                return data.get("products", [])
            except json.JSONDecodeError as e:
                # PC-ANALYSIS-2025-12-18: Handle malformed JSON responses
                logger.warning(
                    f"[pricecharting_search] JSON decode error for query '{q}': {e}. "
                    f"Response text (first 200 chars): {response.text[:200]}"
                )
                return []
        elif response.status_code >= 500:
            circuit_breaker._on_failure(
                Exception(f"API returned {response.status_code}")
            )
            return []
        else:
            return []

    return await pricecharting_search_cache.get_or_fetch(
        query,
        do_search,
        **filters
    )


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def try_claim_independent_job(
    db: AsyncSession,
    job_name: str,
    batch_id: str
) -> tuple[bool, dict]:
    """
    Atomically claim an independent job for execution.

    Returns (claimed, checkpoint_data) tuple.
    """
    # Ensure checkpoint exists with circuit breaker columns
    await db.execute(text("""
        INSERT INTO pipeline_checkpoints (
            job_name, job_type, is_running, control_signal,
            circuit_state, circuit_failure_count, circuit_backoff_multiplier,
            created_at, updated_at
        )
        VALUES (
            :name, 'pricecharting', FALSE, 'run',
            'CLOSED', 0, 1,
            NOW(), NOW()
        )
        ON CONFLICT (job_name) DO NOTHING
    """), {"name": job_name})

    # Atomically claim
    result = await db.execute(text("""
        UPDATE pipeline_checkpoints
        SET is_running = TRUE,
            last_run_started = NOW(),
            current_batch_id = :batch_id,
            updated_at = NOW()
        WHERE job_name = :name
          AND is_running = FALSE
          AND (control_signal IS NULL OR control_signal = 'run')
        RETURNING id, state_data, total_processed, total_errors,
                  circuit_state, circuit_failure_count,
                  circuit_last_failure, circuit_backoff_multiplier
    """), {"name": job_name, "batch_id": batch_id})

    row = result.fetchone()
    await db.commit()

    if row:
        return True, {
            "id": row.id,
            "state_data": row.state_data or {},
            "total_processed": row.total_processed or 0,
            "total_errors": row.total_errors or 0,
            "circuit_state": row.circuit_state,
            "circuit_failure_count": row.circuit_failure_count,
            "circuit_last_failure": row.circuit_last_failure,
            "circuit_backoff_multiplier": row.circuit_backoff_multiplier,
        }

    return False, {}


async def update_independent_checkpoint(
    db: AsyncSession,
    job_name: str,
    last_id: int = None,
    processed_delta: int = 0,
    errors_delta: int = 0,
    circuit_breaker: CircuitBreaker = None,
    is_running: bool = None,
    last_error: str = None,
):
    """Update checkpoint for independent job."""
    updates = ["updated_at = NOW()"]
    params = {"name": job_name}

    if last_id is not None:
        updates.append("state_data = jsonb_set(COALESCE(state_data, '{}'), '{last_id}', :last_id::text::jsonb)")
        params["last_id"] = last_id

    if processed_delta:
        updates.append("total_processed = COALESCE(total_processed, 0) + :proc_delta")
        params["proc_delta"] = processed_delta

    if errors_delta:
        updates.append("total_errors = COALESCE(total_errors, 0) + :err_delta")
        params["err_delta"] = errors_delta

    if is_running is not None:
        updates.append("is_running = :running")
        params["running"] = is_running
        if not is_running:
            updates.append("last_run_completed = NOW()")

    if last_error is not None:
        updates.append("last_error = :error")
        params["error"] = last_error[:1000]

    # Persist circuit breaker state
    if circuit_breaker:
        cb_state = circuit_breaker.to_db_state()
        updates.append("circuit_state = :cb_state")
        updates.append("circuit_failure_count = :cb_failures")
        updates.append("circuit_backoff_multiplier = :cb_backoff")
        params["cb_state"] = cb_state["circuit_state"]
        params["cb_failures"] = cb_state["circuit_failure_count"]
        params["cb_backoff"] = cb_state["circuit_backoff_multiplier"]

        if cb_state["circuit_last_failure"]:
            updates.append("circuit_last_failure = :cb_last_failure")
            params["cb_last_failure"] = cb_state["circuit_last_failure"]

    await db.execute(
        text(f"UPDATE pipeline_checkpoints SET {', '.join(updates)} WHERE job_name = :name"),
        params
    )
    await db.commit()


# =============================================================================
# FUNKO PRICECHARTING MATCH JOB (Independent)
# =============================================================================

async def run_funko_pricecharting_match_job(
    batch_size: int = None,  # Uses PC_BATCH_SIZE if not specified
    max_records: int = 0
) -> dict:
    """
    Independent job to match Funkos to PriceCharting IDs.

    Runs independently of comic matching - no phase blocking.

    Args:
        batch_size: Records per batch
        max_records: Limit (0 = unlimited)

    Returns:
        Job result statistics
    """
    # Phase 2: Use configurable defaults
    if batch_size is None:
        batch_size = PC_BATCH_SIZE

    job_name = "funko_pricecharting_match"
    batch_id = generate_batch_id("funko_pc_match")
    metrics_batch_id = None  # Will be set after we know records_in_batch

    logger.info(f"[{job_name}] Starting Funko PriceCharting matching (batch: {batch_id}, batch_size: {batch_size})")

    async with AsyncSessionLocal() as db:
        claimed, checkpoint = await try_claim_independent_job(db, job_name, batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or paused, skipping")
            return {"status": "skipped", "message": "Job already running or paused"}

        # Restore circuit breaker from checkpoint
        # v1.3.0: Use job-specific circuit breaker for isolation
        circuit_breaker = await get_job_circuit_from_db(db, job_name)

        stats = {
            "processed": 0,
            "matched": 0,
            "errors": 0,
            "circuit_opens": 0,
        }

        # Start pipeline metrics tracking
        try:
            await pipeline_metrics.start_batch(
                batch_id=batch_id,
                pipeline_type=PipelineType.FUNKO_PRICECHARTING_MATCH.value,
                records_in_batch=batch_size,  # Estimated, will update on completion
                db=db
            )
            metrics_batch_id = batch_id
        except Exception as e:
            logger.warning(f"[{job_name}] Failed to start metrics tracking: {e}")

        try:
            pc_token = os.getenv("PRICECHARTING_API_TOKEN")
            if not pc_token:
                logger.error(f"[{job_name}] PRICECHARTING_API_TOKEN not set")
                await update_independent_checkpoint(
                    db, job_name,
                    is_running=False,
                    last_error="Missing PRICECHARTING_API_TOKEN"
                )
                if metrics_batch_id:
                    await pipeline_metrics.fail_batch(batch_id, "config_error", db)
                return {"status": "error", "message": "Missing API token"}

            state_data = checkpoint.get("state_data", {})
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            async with get_pricecharting_client() as client:
                while True:
                    # Check circuit breaker before batch (v1.3.0: job-isolated)
                    if not circuit_breaker.is_call_permitted():
                        retry_after = circuit_breaker.get_retry_after_seconds()
                        logger.warning(
                            f"[{job_name}] Circuit OPEN, pausing job. "
                            f"Retry after {retry_after:.0f}s"
                        )
                        await update_independent_checkpoint(
                            db, job_name,
                            last_id=last_id,
                            circuit_breaker=circuit_breaker,
                            is_running=False,
                            last_error=f"Circuit breaker OPEN, retry after {retry_after:.0f}s"
                        )
                        stats["circuit_opens"] += 1
                        break

                    # Fetch batch of Funkos without pricecharting_id
                    result = await db.execute(text("""
                        SELECT
                          f.id,
                          f.title,
                          f.box_number,
                          f.category,
                          f.license,
                          string_agg(DISTINCT fsn.name, ', ') AS series_names
                        FROM funkos f
                        LEFT JOIN funko_series fs ON fs.funko_id = f.id
                        LEFT JOIN funko_series_names fsn ON fsn.id = fs.series_id
                        WHERE f.pricecharting_id IS NULL
                          AND f.id > :last_id
                        GROUP BY f.id, f.title, f.box_number, f.category, f.license
                        ORDER BY
                          CASE WHEN f.box_number IS NOT NULL THEN 0 ELSE 1 END,
                          f.id
                        LIMIT :limit
                    """), {"last_id": last_id, "limit": batch_size})

                    funkos = result.fetchall()

                    if not funkos:
                        logger.info(f"[{job_name}] No more Funkos to process")
                        break

                    logger.info(f"[{job_name}] Processing {len(funkos)} Funkos")

                    # Heartbeat for stall detection
                    if metrics_batch_id:
                        try:
                            await pipeline_metrics.heartbeat(batch_id, db)
                        except Exception:
                            pass  # Non-critical

                    for funko in funkos:
                        funko_id = funko.id
                        last_id = funko_id
                        stats["processed"] += 1

                        try:
                            pc_id = None

                            # Search by title (v1.1.0: cached search, v1.2.0: multi-factor scoring)
                            if funko.title:
                                search_query = f"Funko Pop {funko.title}"[:100]

                                try:
                                    # Use cached search to reduce API calls
                                    products = await cached_pricecharting_search(
                                        client=client,
                                        pc_token=pc_token,
                                        query=search_query,
                                        circuit_breaker=circuit_breaker,
                                    )

                                    # v1.2.0: Use multi-factor match scoring
                                    funko_dict = {
                                        "title": funko.title,
                                        "box_number": funko.box_number,
                                        "category": funko.category,
                                        "license": funko.license,
                                        "series_names": funko.series_names,
                                    }
                                    match_result = find_best_match(
                                        item=funko_dict,
                                        products=products,
                                        item_type="funko",
                                    )

                                    if match_result and match_result.matched:
                                        pc_id = match_result.pricecharting_id

                                        # Phase 3: Track match quality metrics
                                        if "match_scores" not in stats:
                                            stats["match_scores"] = []
                                            stats["low_confidence_matches"] = 0
                                        stats["match_scores"].append(match_result.score)

                                        # Flag low-confidence matches for potential review
                                        if match_result.confidence == "low":
                                            stats["low_confidence_matches"] += 1
                                            logger.warning(
                                                f"[{job_name}] LOW CONFIDENCE match: Funko {funko_id} "
                                                f"'{funko.title}' -> PC:{pc_id} (score={match_result.score:.2f}) "
                                                f"- may need manual review"
                                            )
                                        else:
                                            logger.info(
                                                f"[{job_name}] Matched Funko {funko_id}: "
                                                f"'{funko.title}' -> PC:{pc_id} "
                                                f"(score={match_result.score:.2f}, "
                                                f"confidence={match_result.confidence})"
                                            )

                                except CircuitOpenError:
                                    logger.warning(f"[{job_name}] Circuit opened during processing")
                                    stats["circuit_opens"] += 1
                                    break

                            if pc_id:
                                await db.execute(text("""
                                    UPDATE funkos
                                    SET pricecharting_id = :pc_id, updated_at = NOW()
                                    WHERE id = :id
                                """), {"id": funko_id, "pc_id": pc_id})
                                stats["matched"] += 1
                                logger.debug(f"[{job_name}] Matched Funko {funko_id} -> PC:{pc_id}")

                        except Exception as e:
                            stats["errors"] += 1
                            logger.warning(f"[{job_name}] Error matching Funko {funko_id}: {e}")

                    # Commit batch and checkpoint
                    await db.commit()
                    await update_independent_checkpoint(
                        db, job_name,
                        last_id=last_id,
                        processed_delta=len(funkos),
                        circuit_breaker=circuit_breaker,
                    )

                    logger.info(
                        f"[{job_name}] Batch complete: "
                        f"processed={stats['processed']}, matched={stats['matched']}"
                    )

                    if max_records and stats["processed"] >= max_records:
                        break

            # Job complete
            await update_independent_checkpoint(
                db, job_name,
                last_id=last_id,
                errors_delta=stats["errors"],
                circuit_breaker=circuit_breaker,
                is_running=False,
            )

            # Complete metrics tracking
            if metrics_batch_id:
                try:
                    await pipeline_metrics.complete_batch(
                        batch_id,
                        BatchResult(
                            records_processed=stats["processed"],
                            records_enriched=stats["matched"],
                            records_skipped=0,
                            records_failed=stats["errors"]
                        ),
                        db
                    )
                except Exception as e:
                    logger.warning(f"[{job_name}] Failed to complete metrics: {e}")

            logger.info(f"[{job_name}] Complete: {stats}")
            return {"status": "success", **stats}

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            await update_independent_checkpoint(
                db, job_name,
                errors_delta=1,
                circuit_breaker=circuit_breaker,
                is_running=False,
                last_error=str(e),
            )
            # Fail metrics tracking
            if metrics_batch_id:
                try:
                    await pipeline_metrics.fail_batch(batch_id, "unhandled_exception", db)
                except Exception:
                    pass
            return {"status": "error", "message": str(e), **stats}


# =============================================================================
# COMIC PRICECHARTING MATCH JOB (Independent)
# =============================================================================

async def run_comic_pricecharting_match_job(
    batch_size: int = None,  # Uses PC_BATCH_SIZE if not specified
    max_records: int = 0
) -> dict:
    """
    Independent job to match Comics to PriceCharting IDs.

    Runs independently of funko matching - no phase blocking.

    Args:
        batch_size: Records per batch
        max_records: Limit (0 = unlimited)

    Returns:
        Job result statistics
    """
    # Phase 2: Use configurable defaults
    if batch_size is None:
        batch_size = PC_BATCH_SIZE

    job_name = "comic_pricecharting_match"
    batch_id = generate_batch_id("comic_pc_match")
    metrics_batch_id = None

    logger.info(f"[{job_name}] Starting Comic PriceCharting matching (batch: {batch_id}, batch_size: {batch_size})")

    async with AsyncSessionLocal() as db:
        claimed, checkpoint = await try_claim_independent_job(db, job_name, batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or paused, skipping")
            return {"status": "skipped", "message": "Job already running or paused"}

        # v1.3.0: Use job-specific circuit breaker for isolation
        circuit_breaker = await get_job_circuit_from_db(db, job_name)

        stats = {
            "processed": 0,
            "matched": 0,
            "errors": 0,
            "circuit_opens": 0,
        }

        # Start pipeline metrics tracking
        try:
            await pipeline_metrics.start_batch(
                batch_id=batch_id,
                pipeline_type=PipelineType.COMIC_PRICECHARTING_MATCH.value,
                records_in_batch=batch_size,
                db=db
            )
            metrics_batch_id = batch_id
        except Exception as e:
            logger.warning(f"[{job_name}] Failed to start metrics tracking: {e}")

        try:
            pc_token = os.getenv("PRICECHARTING_API_TOKEN")
            if not pc_token:
                logger.error(f"[{job_name}] PRICECHARTING_API_TOKEN not set")
                await update_independent_checkpoint(
                    db, job_name,
                    is_running=False,
                    last_error="Missing PRICECHARTING_API_TOKEN"
                )
                if metrics_batch_id:
                    await pipeline_metrics.fail_batch(batch_id, "config_error", db)
                return {"status": "error", "message": "Missing API token"}

            state_data = checkpoint.get("state_data", {})
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            async with get_pricecharting_client() as client:
                while True:
                    # v1.3.0: Job-isolated circuit breaker check
                    if not circuit_breaker.is_call_permitted():
                        retry_after = circuit_breaker.get_retry_after_seconds()
                        logger.warning(
                            f"[{job_name}] Circuit OPEN, pausing job. "
                            f"Retry after {retry_after:.0f}s"
                        )
                        await update_independent_checkpoint(
                            db, job_name,
                            last_id=last_id,
                            circuit_breaker=circuit_breaker,
                            is_running=False,
                            last_error=f"Circuit breaker OPEN, retry after {retry_after:.0f}s"
                        )
                        stats["circuit_opens"] += 1
                        break

                    result = await db.execute(text("""
                        SELECT id, upc, isbn, isbn_normalized,
                               series_name, number
                        FROM comic_issues
                        WHERE pricecharting_id IS NULL
                          AND id > :last_id
                        ORDER BY
                          CASE
                            WHEN isbn_normalized IS NOT NULL THEN 0
                            WHEN isbn IS NOT NULL THEN 1
                            WHEN upc IS NOT NULL THEN 2
                            ELSE 3
                          END,
                          id
                        LIMIT :limit
                    """), {"last_id": last_id, "limit": batch_size})

                    comics = result.fetchall()

                    if not comics:
                        logger.info(f"[{job_name}] No more Comics to process")
                        break

                    logger.info(f"[{job_name}] Processing {len(comics)} Comics")

                    # Heartbeat for stall detection
                    if metrics_batch_id:
                        try:
                            await pipeline_metrics.heartbeat(batch_id, db)
                        except Exception:
                            pass  # Non-critical

                    for comic in comics:
                        comic_id = comic.id
                        last_id = comic_id
                        stats["processed"] += 1

                        try:
                            pc_id = None

                            # Method 1: ISBN lookup (v1.1.0: cached)
                            isbn_to_search = comic.isbn_normalized or comic.isbn
                            if not pc_id and isbn_to_search and len(isbn_to_search) >= 10:
                                clean_isbn = ''.join(filter(str.isdigit, isbn_to_search))
                                if len(clean_isbn) >= 10:
                                    try:
                                        products = await cached_pricecharting_search(
                                            client=client,
                                            pc_token=pc_token,
                                            query=clean_isbn,
                                            circuit_breaker=circuit_breaker,
                                            use_upc=True,
                                        )
                                        if products:
                                            pc_id = int(products[0].get("id"))
                                    except CircuitOpenError:
                                        stats["circuit_opens"] += 1
                                        break

                            # Method 2: UPC lookup (v1.1.0: cached)
                            if not pc_id and comic.upc and len(comic.upc) >= 10:
                                clean_upc = ''.join(filter(str.isdigit, comic.upc))
                                if len(clean_upc) >= 12:
                                    try:
                                        products = await cached_pricecharting_search(
                                            client=client,
                                            pc_token=pc_token,
                                            query=clean_upc,
                                            circuit_breaker=circuit_breaker,
                                            use_upc=True,
                                        )
                                        if products:
                                            pc_id = int(products[0].get("id"))
                                    except CircuitOpenError:
                                        stats["circuit_opens"] += 1
                                        break

                            # Method 3: Title search (v1.1.0: cached, v1.2.0: multi-factor scoring)
                            if not pc_id and comic.series_name:
                                search_parts = [comic.series_name]
                                if comic.number:
                                    search_parts.append(f"#{comic.number}")
                                search_query = " ".join(search_parts)[:100]

                                try:
                                    products = await cached_pricecharting_search(
                                        client=client,
                                        pc_token=pc_token,
                                        query=search_query,
                                        circuit_breaker=circuit_breaker,
                                        console_name="Comics",
                                    )

                                    # v1.2.0: Use multi-factor match scoring
                                    comic_dict = {
                                        "series_name": comic.series_name,
                                        "number": comic.number,
                                        # Include additional fields if available
                                    }
                                    match_result = find_best_match(
                                        item=comic_dict,
                                        products=products,
                                        item_type="comic",
                                    )

                                    if match_result and match_result.matched:
                                        pc_id = match_result.pricecharting_id

                                        # Phase 3: Track match quality metrics
                                        if "match_scores" not in stats:
                                            stats["match_scores"] = []
                                            stats["low_confidence_matches"] = 0
                                        stats["match_scores"].append(match_result.score)

                                        # Flag low-confidence matches for potential review
                                        if match_result.confidence == "low":
                                            stats["low_confidence_matches"] += 1
                                            logger.warning(
                                                f"[{job_name}] LOW CONFIDENCE match: Comic {comic_id} "
                                                f"'{comic.series_name} #{comic.number}' -> PC:{pc_id} "
                                                f"(score={match_result.score:.2f}) - may need manual review"
                                            )
                                        else:
                                            logger.info(
                                                f"[{job_name}] Matched Comic {comic_id}: "
                                                f"'{comic.series_name} #{comic.number}' -> PC:{pc_id} "
                                                f"(score={match_result.score:.2f}, "
                                                f"confidence={match_result.confidence})"
                                            )

                                except CircuitOpenError:
                                    stats["circuit_opens"] += 1
                                    break

                            if pc_id:
                                await db.execute(text("""
                                    UPDATE comic_issues
                                    SET pricecharting_id = :pc_id, updated_at = NOW()
                                    WHERE id = :id
                                """), {"id": comic_id, "pc_id": pc_id})
                                stats["matched"] += 1
                                logger.debug(f"[{job_name}] Matched Comic {comic_id} -> PC:{pc_id}")

                        except Exception as e:
                            stats["errors"] += 1
                            logger.warning(f"[{job_name}] Error matching Comic {comic_id}: {e}")

                    await db.commit()
                    await update_independent_checkpoint(
                        db, job_name,
                        last_id=last_id,
                        processed_delta=len(comics),
                        circuit_breaker=circuit_breaker,
                    )

                    logger.info(
                        f"[{job_name}] Batch complete: "
                        f"processed={stats['processed']}, matched={stats['matched']}"
                    )

                    if max_records and stats["processed"] >= max_records:
                        break

            await update_independent_checkpoint(
                db, job_name,
                last_id=last_id,
                errors_delta=stats["errors"],
                circuit_breaker=circuit_breaker,
                is_running=False,
            )

            # Complete metrics tracking
            if metrics_batch_id:
                try:
                    await pipeline_metrics.complete_batch(
                        batch_id,
                        BatchResult(
                            records_processed=stats["processed"],
                            records_enriched=stats["matched"],
                            records_skipped=0,
                            records_failed=stats["errors"]
                        ),
                        db
                    )
                except Exception as e:
                    logger.warning(f"[{job_name}] Failed to complete metrics: {e}")

            logger.info(f"[{job_name}] Complete: {stats}")
            return {"status": "success", **stats}

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            await update_independent_checkpoint(
                db, job_name,
                errors_delta=1,
                circuit_breaker=circuit_breaker,
                is_running=False,
                last_error=str(e),
            )
            # Fail metrics tracking
            if metrics_batch_id:
                try:
                    await pipeline_metrics.fail_batch(batch_id, "unhandled_exception", db)
                except Exception:
                    pass
            return {"status": "error", "message": str(e), **stats}


# =============================================================================
# FUNKO PRICE SYNC JOB (Independent)
# =============================================================================

async def run_funko_price_sync_job(
    batch_size: int = None,  # Uses PC_BATCH_SIZE if not specified
    max_records: int = 0
) -> dict:
    """
    Independent job to sync prices for Funkos with pricecharting_id.

    Fetches current prices from PriceCharting API and updates the database.

    Args:
        batch_size: Records per batch
        max_records: Limit (0 = unlimited)

    Returns:
        Job result statistics
    """
    # Phase 2: Use configurable defaults
    if batch_size is None:
        batch_size = PC_BATCH_SIZE

    job_name = "funko_price_sync"
    batch_id = generate_batch_id("funko_pc_sync")
    metrics_batch_id = None

    logger.info(f"[{job_name}] Starting Funko price sync (batch: {batch_id}, batch_size: {batch_size})")

    async with AsyncSessionLocal() as db:
        claimed, checkpoint = await try_claim_independent_job(db, job_name, batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or paused, skipping")
            return {"status": "skipped", "message": "Job already running or paused"}

        # v1.3.0: Use job-specific circuit breaker for isolation
        circuit_breaker = await get_job_circuit_from_db(db, job_name)

        stats = {
            "processed": 0,
            "updated": 0,
            "errors": 0,
            "circuit_opens": 0,
        }

        async def fuzzy_match_and_fetch(funko_row, client, pc_token: str, circuit_breaker):
            """
            Fallback path: build enriched fuzzy query using title + box_number + category + series
            to recover pricecharting_id and fetch prices in one flow.
            """
            query_parts = [funko_row.title or ""]
            if funko_row.box_number:
                query_parts.append(f"#{funko_row.box_number}")
            if funko_row.category:
                query_parts.append(funko_row.category)
            if funko_row.series_names:
                query_parts.append(funko_row.series_names)

            search_query = " ".join([part for part in query_parts if part]).strip() or (funko_row.title or "")

            products = await cached_pricecharting_search(
                client=client,
                pc_token=pc_token,
                query=search_query,
                circuit_breaker=circuit_breaker,
            )

            funko_dict = {
                "title": funko_row.title,
                "box_number": funko_row.box_number,
                "category": funko_row.category,
                "license": funko_row.license,
                "series_names": funko_row.series_names,
            }
            match_result = find_best_match(
                item=funko_dict,
                products=products,
                item_type="funko",
            )

            if not match_result or not match_result.matched:
                return None, None

            pc_id = match_result.pricecharting_id

            async def do_price_fetch_by_match():
                return await client.get(
                    "https://www.pricecharting.com/api/product",
                    params={"t": pc_token, "id": pc_id}
                )

            response = await circuit_breaker.execute(do_price_fetch_by_match)
            pc_quota_tracker.record_call(job_name)

            if response.status_code == 200:
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    data = None
            else:
                data = None

            return pc_id, data

        # Start pipeline metrics tracking
        try:
            await pipeline_metrics.start_batch(
                batch_id=batch_id,
                pipeline_type=PipelineType.FUNKO_PRICE_SYNC.value,
                records_in_batch=batch_size,
                db=db
            )
            metrics_batch_id = batch_id
        except Exception as e:
            logger.warning(f"[{job_name}] Failed to start metrics tracking: {e}")

        try:
            pc_token = os.getenv("PRICECHARTING_API_TOKEN")
            if not pc_token:
                logger.error(f"[{job_name}] PRICECHARTING_API_TOKEN not set")
                await update_independent_checkpoint(
                    db, job_name,
                    is_running=False,
                    last_error="Missing PRICECHARTING_API_TOKEN"
                )
                if metrics_batch_id:
                    await pipeline_metrics.fail_batch(batch_id, "config_error", db)
                return {"status": "error", "message": "Missing API token"}

            state_data = checkpoint.get("state_data", {})
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            async with get_pricecharting_client() as client:
                while True:
                    # v1.3.0: Job-isolated circuit breaker check
                    if not circuit_breaker.is_call_permitted():
                        retry_after = circuit_breaker.get_retry_after_seconds()
                        logger.warning(
                            f"[{job_name}] Circuit OPEN, pausing job. "
                            f"Retry after {retry_after:.0f}s"
                        )
                        await update_independent_checkpoint(
                            db, job_name,
                            last_id=last_id,
                            circuit_breaker=circuit_breaker,
                            is_running=False,
                            last_error=f"Circuit breaker OPEN, retry after {retry_after:.0f}s"
                        )
                        stats["circuit_opens"] += 1
                        break

                    # Fetch Funkos with pricecharting_id that need price update
                    # v1.1.0: Incremental sync - only fetch stale records
                    # Phase 2: Use configurable staleness threshold (PC_STALENESS_HOURS)
                    # IMPL-2025-12-21-PC-REFACTOR: PRICECHARTING_FORCE_SYNC bypasses staleness
                    if PC_FORCE_SYNC:
                        result = await db.execute(text("""
                            SELECT
                              f.id,
                              f.pricecharting_id,
                              f.price_loose,
                              f.price_cib,
                              f.price_new,
                              f.title,
                              f.box_number,
                              f.category,
                              f.license,
                              string_agg(DISTINCT fsn.name, ', ') AS series_names
                            FROM funkos f
                            LEFT JOIN funko_series fs ON fs.funko_id = f.id
                            LEFT JOIN funko_series_names fsn ON fsn.id = fs.series_id
                            WHERE f.pricecharting_id IS NOT NULL
                              AND f.id > :last_id
                            GROUP BY f.id, f.pricecharting_id, f.price_loose, f.price_cib, f.price_new,
                                     f.title, f.box_number, f.category, f.license
                            ORDER BY f.id
                            LIMIT :limit
                        """), {"last_id": last_id, "limit": batch_size})
                    else:
                        result = await db.execute(text("""
                            SELECT
                              f.id,
                              f.pricecharting_id,
                              f.price_loose,
                              f.price_cib,
                              f.price_new,
                              f.title,
                              f.box_number,
                              f.category,
                              f.license,
                              string_agg(DISTINCT fsn.name, ', ') AS series_names
                            FROM funkos f
                            LEFT JOIN funko_series fs ON fs.funko_id = f.id
                            LEFT JOIN funko_series_names fsn ON fsn.id = fs.series_id
                            WHERE f.pricecharting_id IS NOT NULL
                              AND f.id > :last_id
                              AND (f.pricecharting_synced_at IS NULL
                                   OR f.pricecharting_synced_at < NOW() - make_interval(hours => :staleness_hours))
                            GROUP BY f.id, f.pricecharting_id, f.price_loose, f.price_cib, f.price_new,
                                     f.title, f.box_number, f.category, f.license
                            ORDER BY f.id
                            LIMIT :limit
                        """), {"last_id": last_id, "limit": batch_size, "staleness_hours": PC_STALENESS_HOURS})

                    funkos = result.fetchall()

                    if not funkos:
                        logger.info(f"[{job_name}] No {'Funkos' if PC_FORCE_SYNC else 'stale Funkos'} to sync (Force Mode: {PC_FORCE_SYNC}), cycle complete")
                        last_id = 0  # Reset for next run
                        break

                    logger.info(f"[{job_name}] Processing {len(funkos)} Funkos")

                    # Heartbeat for stall detection
                    if metrics_batch_id:
                        try:
                            await pipeline_metrics.heartbeat(batch_id, db)
                        except Exception:
                            pass  # Non-critical

                    for funko in funkos:
                        funko_id = funko.id
                        last_id = funko_id
                        stats["processed"] += 1

                        try:
                            data = None
                            resolved_pc_id = funko.pricecharting_id

                            async def do_price_fetch():
                                return await client.get(
                                    "https://www.pricecharting.com/api/product",
                                    params={"t": pc_token, "id": funko.pricecharting_id}
                                )

                            try:
                                response = await circuit_breaker.execute(do_price_fetch)

                                # Phase 2: Track API quota usage
                                pc_quota_tracker.record_call(job_name)

                                if response.status_code == 200:
                                    try:
                                        data = response.json()
                                    except json.JSONDecodeError as e:
                                        # PC-ANALYSIS-2025-12-18: Handle malformed JSON responses
                                        logger.warning(
                                            f"[{job_name}] JSON decode error for Funko {funko_id}: {e}"
                                        )
                                        stats["errors"] += 1
                                elif response.status_code >= 500:
                                    circuit_breaker._on_failure(
                                        Exception(f"API returned {response.status_code}")
                                    )

                            except CircuitOpenError:
                                stats["circuit_opens"] += 1
                                break

                            # Fallback fuzzy match if we still don't have price data
                            if data is None:
                                fallback_pc_id, fallback_data = await fuzzy_match_and_fetch(funko, client, pc_token, circuit_breaker)
                                if fallback_pc_id:
                                    resolved_pc_id = fallback_pc_id
                                    if resolved_pc_id != funko.pricecharting_id:
                                        await db.execute(text("""
                                            UPDATE funkos
                                            SET pricecharting_id = :pc_id,
                                                updated_at = NOW()
                                            WHERE id = :id
                                        """), {"pc_id": resolved_pc_id, "id": funko_id})
                                data = fallback_data

                            if not data:
                                # No data to update
                                continue

                            # Extract prices (in cents, convert to dollars)
                            loose_price = data.get("loose-price")
                            cib_price = data.get("cib-price")
                            new_price = data.get("new-price")

                            # Convert from cents to dollars
                            updates = []
                            params = {"id": funko_id}

                            # Phase 3: Track significant price changes
                            if "significant_changes" not in stats:
                                stats["significant_changes"] = 0

                            def check_price_change(field_name, old_val, new_cents):
                                """Check for significant price change and log alert."""
                                if not new_cents:
                                    return None
                                new_dollars = float(new_cents) / 100
                                old_dollars = float(old_val) if old_val else 0

                                if old_dollars > 0:
                                    pct_change = ((new_dollars - old_dollars) / old_dollars) * 100
                                    if abs(pct_change) >= PC_PRICE_ALERT_THRESHOLD:
                                        stats["significant_changes"] += 1
                                        direction = "UP" if pct_change > 0 else "DOWN"
                                        logger.warning(
                                            f"[PRICE_ALERT] Funko {funko_id} {field_name}: "
                                            f"${old_dollars:.2f} -> ${new_dollars:.2f} "
                                            f"({direction} {abs(pct_change):.1f}%)"
                                        )
                                return new_dollars

                            if loose_price:
                                new_loose = check_price_change("loose", funko.price_loose, loose_price)
                                if new_loose:
                                    updates.append("price_loose = :loose")
                                    params["loose"] = new_loose

                            if cib_price:
                                new_cib = check_price_change("cib", funko.price_cib, cib_price)
                                if new_cib:
                                    updates.append("price_cib = :cib")
                                    params["cib"] = new_cib

                            if new_price:
                                new_new = check_price_change("new", funko.price_new, new_price)
                                if new_new:
                                    updates.append("price_new = :new")
                                    params["new"] = new_new

                            if updates:
                                # v1.1.0: Track sync timestamp for incremental sync
                                updates.append("pricecharting_synced_at = NOW()")
                                updates.append("updated_at = NOW()")
                                await db.execute(
                                    text(f"UPDATE funkos SET {', '.join(updates)} WHERE id = :id"),
                                    params
                                )
                                stats["updated"] += 1

                                # IMPL-2025-12-21-PC-REFACTOR PHASE 1: Record to price_changelog
                                changelog_entries = []
                                if "loose" in params and funko.price_loose != params["loose"]:
                                    old_val = float(funko.price_loose) if funko.price_loose else None
                                    new_val = params["loose"]
                                    pct = ((new_val - old_val) / old_val * 100) if old_val else None
                                    changelog_entries.append(("price_loose", old_val, new_val, pct))
                                if "cib" in params and funko.price_cib != params["cib"]:
                                    old_val = float(funko.price_cib) if funko.price_cib else None
                                    new_val = params["cib"]
                                    pct = ((new_val - old_val) / old_val * 100) if old_val else None
                                    changelog_entries.append(("price_cib", old_val, new_val, pct))
                                if "new" in params and funko.price_new != params["new"]:
                                    old_val = float(funko.price_new) if funko.price_new else None
                                    new_val = params["new"]
                                    pct = ((new_val - old_val) / old_val * 100) if old_val else None
                                    changelog_entries.append(("price_new", old_val, new_val, pct))

                                for field_name, old_val, new_val, pct in changelog_entries:
                                    await db.execute(text("""
                                        INSERT INTO price_changelog
                                        (entity_type, entity_id, entity_name, field_name,
                                         old_value, new_value, change_pct, data_source, reason, sync_batch_id)
                                        VALUES ('funko', :entity_id, :entity_name, :field_name,
                                                :old_value, :new_value, :change_pct, 'pricecharting', 'price_sync', :batch_id::uuid)
                                        ON CONFLICT (entity_type, entity_id, field_name, sync_batch_id)
                                            WHERE sync_batch_id IS NOT NULL
                                        DO NOTHING
                                    """), {
                                        "entity_id": funko_id,
                                        "entity_name": funko.title[:500] if funko.title else None,
                                        "field_name": field_name,
                                        "old_value": old_val,
                                        "new_value": new_val,
                                        "change_pct": round(pct, 2) if pct else None,
                                        "batch_id": batch_id,
                                    })

                                if changelog_entries:
                                    if "changelog_recorded" not in stats:
                                        stats["changelog_recorded"] = 0
                                    stats["changelog_recorded"] += len(changelog_entries)

                                logger.debug(
                                    f"[{job_name}] Updated Funko {funko_id} prices (pc_id={resolved_pc_id})"
                                )
                            else:
                                # No price changes but mark as synced
                                await db.execute(text("""
                                    UPDATE funkos
                                    SET pricecharting_synced_at = NOW()
                                    WHERE id = :id
                                """), {"id": funko_id})

                        except Exception as e:
                            stats["errors"] += 1
                            logger.warning(f"[{job_name}] Error syncing Funko {funko_id}: {e}")

                    await db.commit()
                    await update_independent_checkpoint(
                        db, job_name,
                        last_id=last_id,
                        processed_delta=len(funkos),
                        circuit_breaker=circuit_breaker,
                    )

                    logger.info(
                        f"[{job_name}] Batch complete: "
                        f"processed={stats['processed']}, updated={stats['updated']}"
                    )

                    if max_records and stats["processed"] >= max_records:
                        break

            await update_independent_checkpoint(
                db, job_name,
                last_id=last_id,
                errors_delta=stats["errors"],
                circuit_breaker=circuit_breaker,
                is_running=False,
            )

            # Complete metrics tracking
            if metrics_batch_id:
                try:
                    await pipeline_metrics.complete_batch(
                        batch_id,
                        BatchResult(
                            records_processed=stats["processed"],
                            records_enriched=stats["updated"],
                            records_skipped=0,
                            records_failed=stats["errors"]
                        ),
                        db
                    )
                except Exception as e:
                    logger.warning(f"[{job_name}] Failed to complete metrics: {e}")

            logger.info(f"[{job_name}] Complete: {stats}")
            return {"status": "success", **stats}

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            await update_independent_checkpoint(
                db, job_name,
                errors_delta=1,
                circuit_breaker=circuit_breaker,
                is_running=False,
                last_error=str(e),
            )
            # Fail metrics tracking
            if metrics_batch_id:
                try:
                    await pipeline_metrics.fail_batch(batch_id, "unhandled_exception", db)
                except Exception:
                    pass
            return {"status": "error", "message": str(e), **stats}


# =============================================================================
# COMIC PRICE SYNC JOB (Independent)
# =============================================================================

async def run_comic_price_sync_job(
    batch_size: int = None,  # Uses PC_BATCH_SIZE if not specified
    max_records: int = 0
) -> dict:
    """
    Independent job to sync prices for Comics with pricecharting_id.

    Fetches current prices from PriceCharting API and updates the database.

    Args:
        batch_size: Records per batch
        max_records: Limit (0 = unlimited)

    Returns:
        Job result statistics
    """
    # Phase 2: Use configurable defaults
    if batch_size is None:
        batch_size = PC_BATCH_SIZE

    job_name = "comic_price_sync"
    batch_id = generate_batch_id("comic_pc_sync")
    metrics_batch_id = None

    logger.info(f"[{job_name}] Starting Comic price sync (batch: {batch_id}, batch_size: {batch_size})")

    async with AsyncSessionLocal() as db:
        claimed, checkpoint = await try_claim_independent_job(db, job_name, batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or paused, skipping")
            return {"status": "skipped", "message": "Job already running or paused"}

        # v1.3.0: Use job-specific circuit breaker for isolation
        circuit_breaker = await get_job_circuit_from_db(db, job_name)

        stats = {
            "processed": 0,
            "updated": 0,
            "errors": 0,
            "circuit_opens": 0,
        }

        # Start pipeline metrics tracking
        try:
            await pipeline_metrics.start_batch(
                batch_id=batch_id,
                pipeline_type=PipelineType.COMIC_PRICE_SYNC.value,
                records_in_batch=batch_size,
                db=db
            )
            metrics_batch_id = batch_id
        except Exception as e:
            logger.warning(f"[{job_name}] Failed to start metrics tracking: {e}")

        try:
            pc_token = os.getenv("PRICECHARTING_API_TOKEN")
            if not pc_token:
                logger.error(f"[{job_name}] PRICECHARTING_API_TOKEN not set")
                await update_independent_checkpoint(
                    db, job_name,
                    is_running=False,
                    last_error="Missing PRICECHARTING_API_TOKEN"
                )
                if metrics_batch_id:
                    await pipeline_metrics.fail_batch(batch_id, "config_error", db)
                return {"status": "error", "message": "Missing API token"}

            state_data = checkpoint.get("state_data", {})
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            async with get_pricecharting_client() as client:
                while True:
                    # v1.3.0: Job-isolated circuit breaker check
                    if not circuit_breaker.is_call_permitted():
                        retry_after = circuit_breaker.get_retry_after_seconds()
                        logger.warning(
                            f"[{job_name}] Circuit OPEN, pausing job. "
                            f"Retry after {retry_after:.0f}s"
                        )
                        await update_independent_checkpoint(
                            db, job_name,
                            last_id=last_id,
                            circuit_breaker=circuit_breaker,
                            is_running=False,
                            last_error=f"Circuit breaker OPEN, retry after {retry_after:.0f}s"
                        )
                        stats["circuit_opens"] += 1
                        break

                    # v1.1.0: Incremental sync - only fetch stale records
                    # Phase 2: Use configurable staleness threshold (PC_STALENESS_HOURS)
                    # IMPL-2025-12-21-PC-REFACTOR PHASE 2: Force sync bypasses staleness check
                    if PC_FORCE_SYNC:
                        result = await db.execute(text("""
                            SELECT id, pricecharting_id, price_guide_value
                            FROM comic_issues
                            WHERE pricecharting_id IS NOT NULL
                              AND id > :last_id
                            ORDER BY id
                            LIMIT :limit
                        """), {"last_id": last_id, "limit": batch_size})
                        if stats["processed"] == 0:
                            logger.info(f"[{job_name}] FORCE_SYNC mode enabled - bypassing staleness check")
                    else:
                        result = await db.execute(text("""
                            SELECT id, pricecharting_id, price_guide_value
                            FROM comic_issues
                            WHERE pricecharting_id IS NOT NULL
                              AND id > :last_id
                              AND (pricecharting_synced_at IS NULL
                                   OR pricecharting_synced_at < NOW() - make_interval(hours => :staleness_hours))
                            ORDER BY id
                            LIMIT :limit
                        """), {"last_id": last_id, "limit": batch_size, "staleness_hours": PC_STALENESS_HOURS})

                    comics = result.fetchall()

                    if not comics:
                        logger.info(f"[{job_name}] No stale Comics to sync, cycle complete")
                        last_id = 0
                        break

                    logger.info(f"[{job_name}] Processing {len(comics)} Comics")

                    # Heartbeat for stall detection
                    if metrics_batch_id:
                        try:
                            await pipeline_metrics.heartbeat(batch_id, db)
                        except Exception:
                            pass  # Non-critical

                    for comic in comics:
                        comic_id = comic.id
                        last_id = comic_id
                        stats["processed"] += 1

                        try:
                            async def do_price_fetch():
                                return await client.get(
                                    "https://www.pricecharting.com/api/product",
                                    params={"t": pc_token, "id": comic.pricecharting_id}
                                )

                            try:
                                response = await circuit_breaker.execute(do_price_fetch)

                                # Phase 2: Track API quota usage
                                pc_quota_tracker.record_call(job_name)

                                if response.status_code == 200:
                                    try:
                                        data = response.json()
                                    except json.JSONDecodeError as e:
                                        # PC-ANALYSIS-2025-12-18: Handle malformed JSON responses
                                        logger.warning(
                                            f"[{job_name}] JSON decode error for Comic {comic_id}: {e}"
                                        )
                                        stats["errors"] += 1
                                        continue

                                    # For comics, use CIB price as guide value
                                    cib_price = data.get("cib-price")

                                    if cib_price:
                                        price_dollars = float(cib_price) / 100
                                        old_price = float(comic.price_guide_value) if comic.price_guide_value else 0

                                        # Phase 3: Track significant price changes
                                        if "significant_changes" not in stats:
                                            stats["significant_changes"] = 0

                                        if old_price > 0:
                                            pct_change = ((price_dollars - old_price) / old_price) * 100
                                            if abs(pct_change) >= PC_PRICE_ALERT_THRESHOLD:
                                                stats["significant_changes"] += 1
                                                direction = "UP" if pct_change > 0 else "DOWN"
                                                logger.warning(
                                                    f"[PRICE_ALERT] Comic {comic_id} guide_value: "
                                                    f"${old_price:.2f} -> ${price_dollars:.2f} "
                                                    f"({direction} {abs(pct_change):.1f}%)"
                                                )

                                        # IMPL-2025-12-21-PC-REFACTOR PHASE 1: Record to price_changelog
                                        # Only record if price actually changed
                                        if old_price != price_dollars:
                                            change_pct = ((price_dollars - old_price) / old_price * 100) if old_price > 0 else None
                                            try:
                                                await db.execute(text("""
                                                    INSERT INTO price_changelog
                                                    (entity_type, entity_id, entity_name, field_name,
                                                     old_value, new_value, change_pct, data_source, reason, sync_batch_id)
                                                    VALUES ('comic', :entity_id, :entity_name, 'price_guide_value',
                                                            :old_value, :new_value, :change_pct, 'pricecharting', 'price_sync', :batch_id::uuid)
                                                    ON CONFLICT (entity_type, entity_id, field_name, sync_batch_id)
                                                        WHERE sync_batch_id IS NOT NULL
                                                    DO NOTHING
                                                """), {
                                                    "entity_id": comic_id,
                                                    "entity_name": f"Comic #{comic_id}",
                                                    "old_value": old_price,
                                                    "new_value": price_dollars,
                                                    "change_pct": change_pct,
                                                    "batch_id": batch_id
                                                })
                                            except Exception as cl_err:
                                                logger.warning(f"[{job_name}] Failed to log price change for Comic {comic_id}: {cl_err}")

                                        # v1.1.0: Track sync timestamp for incremental sync
                                        await db.execute(text("""
                                            UPDATE comic_issues
                                            SET price_guide_value = :price,
                                                pricecharting_synced_at = NOW(),
                                                updated_at = NOW()
                                            WHERE id = :id
                                        """), {"id": comic_id, "price": price_dollars})
                                        stats["updated"] += 1
                                        logger.debug(
                                            f"[{job_name}] Updated Comic {comic_id} price: ${price_dollars:.2f}"
                                        )
                                    else:
                                        # No price but mark as synced
                                        await db.execute(text("""
                                            UPDATE comic_issues
                                            SET pricecharting_synced_at = NOW()
                                            WHERE id = :id
                                        """), {"id": comic_id})

                                elif response.status_code >= 500:
                                    circuit_breaker._on_failure(
                                        Exception(f"API returned {response.status_code}")
                                    )

                            except CircuitOpenError:
                                stats["circuit_opens"] += 1
                                break

                        except Exception as e:
                            stats["errors"] += 1
                            logger.warning(f"[{job_name}] Error syncing Comic {comic_id}: {e}")

                    await db.commit()
                    await update_independent_checkpoint(
                        db, job_name,
                        last_id=last_id,
                        processed_delta=len(comics),
                        circuit_breaker=circuit_breaker,
                    )

                    logger.info(
                        f"[{job_name}] Batch complete: "
                        f"processed={stats['processed']}, updated={stats['updated']}"
                    )

                    if max_records and stats["processed"] >= max_records:
                        break

            await update_independent_checkpoint(
                db, job_name,
                last_id=last_id,
                errors_delta=stats["errors"],
                circuit_breaker=circuit_breaker,
                is_running=False,
            )

            # Complete metrics tracking
            if metrics_batch_id:
                try:
                    await pipeline_metrics.complete_batch(
                        batch_id,
                        BatchResult(
                            records_processed=stats["processed"],
                            records_enriched=stats["updated"],
                            records_skipped=0,
                            records_failed=stats["errors"]
                        ),
                        db
                    )
                except Exception as e:
                    logger.warning(f"[{job_name}] Failed to complete metrics: {e}")

            logger.info(f"[{job_name}] Complete: {stats}")
            return {"status": "success", **stats}

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            await update_independent_checkpoint(
                db, job_name,
                errors_delta=1,
                circuit_breaker=circuit_breaker,
                is_running=False,
                last_error=str(e),
            )
            # Fail metrics tracking
            if metrics_batch_id:
                try:
                    await pipeline_metrics.fail_batch(batch_id, "unhandled_exception", db)
                except Exception:
                    pass
            return {"status": "error", "message": str(e), **stats}
