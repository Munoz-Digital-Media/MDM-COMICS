"""
PriceCharting Independent Jobs v1.0.0

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
- Circuit breaker protects against API failures
- Checkpoints are persisted per job
- Self-healer can auto-resume paused jobs

Per constitution_cyberSec.json Section 5:
> "All partner API clients implement retries with bounded budgets and circuit breakers"

Per constitution_devops_doctrine.json:
> "circuit_breakers": true
"""
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

logger = logging.getLogger(__name__)


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
    batch_size: int = 100,
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
    job_name = "funko_pricecharting_match"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting Funko PriceCharting matching (batch: {batch_id})")

    async with AsyncSessionLocal() as db:
        claimed, checkpoint = await try_claim_independent_job(db, job_name, batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or paused, skipping")
            return {"status": "skipped", "message": "Job already running or paused"}

        # Restore circuit breaker from checkpoint
        circuit_breaker = CircuitBreaker.from_db_state(job_name, checkpoint)

        stats = {
            "processed": 0,
            "matched": 0,
            "errors": 0,
            "circuit_opens": 0,
        }

        try:
            pc_token = os.getenv("PRICECHARTING_API_TOKEN")
            if not pc_token:
                logger.error(f"[{job_name}] PRICECHARTING_API_TOKEN not set")
                await update_independent_checkpoint(
                    db, job_name,
                    is_running=False,
                    last_error="Missing PRICECHARTING_API_TOKEN"
                )
                return {"status": "error", "message": "Missing API token"}

            state_data = checkpoint.get("state_data", {})
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            async with get_pricecharting_client() as client:
                while True:
                    # Check circuit breaker before batch
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
                        SELECT id, title, box_number
                        FROM funkos
                        WHERE pricecharting_id IS NULL
                          AND id > :last_id
                        ORDER BY
                          CASE WHEN box_number IS NOT NULL THEN 0 ELSE 1 END,
                          id
                        LIMIT :limit
                    """), {"last_id": last_id, "limit": batch_size})

                    funkos = result.fetchall()

                    if not funkos:
                        logger.info(f"[{job_name}] No more Funkos to process")
                        break

                    logger.info(f"[{job_name}] Processing {len(funkos)} Funkos")

                    for funko in funkos:
                        funko_id = funko.id
                        last_id = funko_id
                        stats["processed"] += 1

                        try:
                            pc_id = None

                            # Search by title
                            if funko.title:
                                search_query = f"Funko Pop {funko.title}"[:100]

                                async def do_search():
                                    return await client.get(
                                        "https://www.pricecharting.com/api/products",
                                        params={"t": pc_token, "q": search_query}
                                    )

                                try:
                                    response = await circuit_breaker.execute(do_search)

                                    if response.status_code == 200:
                                        data = response.json()
                                        products = data.get("products", [])

                                        for prod in products[:5]:
                                            prod_name = prod.get("product-name", "").lower()
                                            if funko.title and funko.title.lower() in prod_name:
                                                if "funko" in prod_name or "pop" in prod_name:
                                                    pc_id = int(prod.get("id"))
                                                    break
                                    elif response.status_code >= 500:
                                        circuit_breaker._on_failure(
                                            Exception(f"API returned {response.status_code}")
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
            return {"status": "error", "message": str(e), **stats}


# =============================================================================
# COMIC PRICECHARTING MATCH JOB (Independent)
# =============================================================================

async def run_comic_pricecharting_match_job(
    batch_size: int = 100,
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
    job_name = "comic_pricecharting_match"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting Comic PriceCharting matching (batch: {batch_id})")

    async with AsyncSessionLocal() as db:
        claimed, checkpoint = await try_claim_independent_job(db, job_name, batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or paused, skipping")
            return {"status": "skipped", "message": "Job already running or paused"}

        circuit_breaker = CircuitBreaker.from_db_state(job_name, checkpoint)

        stats = {
            "processed": 0,
            "matched": 0,
            "errors": 0,
            "circuit_opens": 0,
        }

        try:
            pc_token = os.getenv("PRICECHARTING_API_TOKEN")
            if not pc_token:
                logger.error(f"[{job_name}] PRICECHARTING_API_TOKEN not set")
                await update_independent_checkpoint(
                    db, job_name,
                    is_running=False,
                    last_error="Missing PRICECHARTING_API_TOKEN"
                )
                return {"status": "error", "message": "Missing API token"}

            state_data = checkpoint.get("state_data", {})
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            async with get_pricecharting_client() as client:
                while True:
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

                    for comic in comics:
                        comic_id = comic.id
                        last_id = comic_id
                        stats["processed"] += 1

                        try:
                            pc_id = None

                            # Method 1: ISBN lookup
                            isbn_to_search = comic.isbn_normalized or comic.isbn
                            if not pc_id and isbn_to_search and len(isbn_to_search) >= 10:
                                clean_isbn = ''.join(filter(str.isdigit, isbn_to_search))
                                if len(clean_isbn) >= 10:
                                    async def do_isbn_search():
                                        return await client.get(
                                            "https://www.pricecharting.com/api/products",
                                            params={"t": pc_token, "upc": clean_isbn}
                                        )

                                    try:
                                        response = await circuit_breaker.execute(do_isbn_search)
                                        if response.status_code == 200:
                                            data = response.json()
                                            products = data.get("products", [])
                                            if products:
                                                pc_id = int(products[0].get("id"))
                                        elif response.status_code >= 500:
                                            circuit_breaker._on_failure(
                                                Exception(f"API returned {response.status_code}")
                                            )
                                    except CircuitOpenError:
                                        stats["circuit_opens"] += 1
                                        break

                            # Method 2: UPC lookup
                            if not pc_id and comic.upc and len(comic.upc) >= 10:
                                clean_upc = ''.join(filter(str.isdigit, comic.upc))
                                if len(clean_upc) >= 12:
                                    async def do_upc_search():
                                        return await client.get(
                                            "https://www.pricecharting.com/api/products",
                                            params={"t": pc_token, "upc": clean_upc}
                                        )

                                    try:
                                        response = await circuit_breaker.execute(do_upc_search)
                                        if response.status_code == 200:
                                            data = response.json()
                                            products = data.get("products", [])
                                            if products:
                                                pc_id = int(products[0].get("id"))
                                        elif response.status_code >= 500:
                                            circuit_breaker._on_failure(
                                                Exception(f"API returned {response.status_code}")
                                            )
                                    except CircuitOpenError:
                                        stats["circuit_opens"] += 1
                                        break

                            # Method 3: Title search
                            if not pc_id and comic.series_name:
                                search_parts = [comic.series_name]
                                if comic.number:
                                    search_parts.append(f"#{comic.number}")
                                search_query = " ".join(search_parts)[:100]

                                async def do_title_search():
                                    return await client.get(
                                        "https://www.pricecharting.com/api/products",
                                        params={
                                            "t": pc_token,
                                            "q": search_query,
                                            "console-name": "Comics"
                                        }
                                    )

                                try:
                                    response = await circuit_breaker.execute(do_title_search)
                                    if response.status_code == 200:
                                        data = response.json()
                                        products = data.get("products", [])

                                        for prod in products[:5]:
                                            prod_name = prod.get("product-name", "").lower()
                                            if comic.series_name.lower() in prod_name:
                                                pc_id = int(prod.get("id"))
                                                break
                                    elif response.status_code >= 500:
                                        circuit_breaker._on_failure(
                                            Exception(f"API returned {response.status_code}")
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
            return {"status": "error", "message": str(e), **stats}


# =============================================================================
# FUNKO PRICE SYNC JOB (Independent)
# =============================================================================

async def run_funko_price_sync_job(
    batch_size: int = 100,
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
    job_name = "funko_price_sync"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting Funko price sync (batch: {batch_id})")

    async with AsyncSessionLocal() as db:
        claimed, checkpoint = await try_claim_independent_job(db, job_name, batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or paused, skipping")
            return {"status": "skipped", "message": "Job already running or paused"}

        circuit_breaker = CircuitBreaker.from_db_state(job_name, checkpoint)

        stats = {
            "processed": 0,
            "updated": 0,
            "errors": 0,
            "circuit_opens": 0,
        }

        try:
            pc_token = os.getenv("PRICECHARTING_API_TOKEN")
            if not pc_token:
                logger.error(f"[{job_name}] PRICECHARTING_API_TOKEN not set")
                await update_independent_checkpoint(
                    db, job_name,
                    is_running=False,
                    last_error="Missing PRICECHARTING_API_TOKEN"
                )
                return {"status": "error", "message": "Missing API token"}

            state_data = checkpoint.get("state_data", {})
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            async with get_pricecharting_client() as client:
                while True:
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
                    result = await db.execute(text("""
                        SELECT id, pricecharting_id, price_loose, price_cib, price_new
                        FROM funkos
                        WHERE pricecharting_id IS NOT NULL
                          AND id > :last_id
                        ORDER BY id
                        LIMIT :limit
                    """), {"last_id": last_id, "limit": batch_size})

                    funkos = result.fetchall()

                    if not funkos:
                        logger.info(f"[{job_name}] No more Funkos to process, resetting to start")
                        last_id = 0  # Reset for next run
                        break

                    logger.info(f"[{job_name}] Processing {len(funkos)} Funkos")

                    for funko in funkos:
                        funko_id = funko.id
                        last_id = funko_id
                        stats["processed"] += 1

                        try:
                            async def do_price_fetch():
                                return await client.get(
                                    "https://www.pricecharting.com/api/product",
                                    params={"t": pc_token, "id": funko.pricecharting_id}
                                )

                            try:
                                response = await circuit_breaker.execute(do_price_fetch)

                                if response.status_code == 200:
                                    data = response.json()

                                    # Extract prices (in cents, convert to dollars)
                                    loose_price = data.get("loose-price")
                                    cib_price = data.get("cib-price")
                                    new_price = data.get("new-price")

                                    # Convert from cents to dollars
                                    updates = []
                                    params = {"id": funko_id}

                                    if loose_price:
                                        updates.append("price_loose = :loose")
                                        params["loose"] = float(loose_price) / 100

                                    if cib_price:
                                        updates.append("price_cib = :cib")
                                        params["cib"] = float(cib_price) / 100

                                    if new_price:
                                        updates.append("price_new = :new")
                                        params["new"] = float(new_price) / 100

                                    if updates:
                                        updates.append("updated_at = NOW()")
                                        await db.execute(
                                            text(f"UPDATE funkos SET {', '.join(updates)} WHERE id = :id"),
                                            params
                                        )
                                        stats["updated"] += 1
                                        logger.debug(
                                            f"[{job_name}] Updated Funko {funko_id} prices"
                                        )

                                elif response.status_code >= 500:
                                    circuit_breaker._on_failure(
                                        Exception(f"API returned {response.status_code}")
                                    )

                            except CircuitOpenError:
                                stats["circuit_opens"] += 1
                                break

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
            return {"status": "error", "message": str(e), **stats}


# =============================================================================
# COMIC PRICE SYNC JOB (Independent)
# =============================================================================

async def run_comic_price_sync_job(
    batch_size: int = 100,
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
    job_name = "comic_price_sync"
    batch_id = str(uuid4())

    logger.info(f"[{job_name}] Starting Comic price sync (batch: {batch_id})")

    async with AsyncSessionLocal() as db:
        claimed, checkpoint = await try_claim_independent_job(db, job_name, batch_id)

        if not claimed:
            logger.warning(f"[{job_name}] Job already running or paused, skipping")
            return {"status": "skipped", "message": "Job already running or paused"}

        circuit_breaker = CircuitBreaker.from_db_state(job_name, checkpoint)

        stats = {
            "processed": 0,
            "updated": 0,
            "errors": 0,
            "circuit_opens": 0,
        }

        try:
            pc_token = os.getenv("PRICECHARTING_API_TOKEN")
            if not pc_token:
                logger.error(f"[{job_name}] PRICECHARTING_API_TOKEN not set")
                await update_independent_checkpoint(
                    db, job_name,
                    is_running=False,
                    last_error="Missing PRICECHARTING_API_TOKEN"
                )
                return {"status": "error", "message": "Missing API token"}

            state_data = checkpoint.get("state_data", {})
            last_id = state_data.get("last_id", 0) if isinstance(state_data, dict) else 0

            async with get_pricecharting_client() as client:
                while True:
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
                        SELECT id, pricecharting_id, price_guide_value
                        FROM comic_issues
                        WHERE pricecharting_id IS NOT NULL
                          AND id > :last_id
                        ORDER BY id
                        LIMIT :limit
                    """), {"last_id": last_id, "limit": batch_size})

                    comics = result.fetchall()

                    if not comics:
                        logger.info(f"[{job_name}] No more Comics to process, resetting to start")
                        last_id = 0
                        break

                    logger.info(f"[{job_name}] Processing {len(comics)} Comics")

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

                                if response.status_code == 200:
                                    data = response.json()

                                    # For comics, use CIB price as guide value
                                    cib_price = data.get("cib-price")

                                    if cib_price:
                                        price_dollars = float(cib_price) / 100
                                        await db.execute(text("""
                                            UPDATE comic_issues
                                            SET price_guide_value = :price,
                                                updated_at = NOW()
                                            WHERE id = :id
                                        """), {"id": comic_id, "price": price_dollars})
                                        stats["updated"] += 1
                                        logger.debug(
                                            f"[{job_name}] Updated Comic {comic_id} price: ${price_dollars:.2f}"
                                        )

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
            return {"status": "error", "message": str(e), **stats}
