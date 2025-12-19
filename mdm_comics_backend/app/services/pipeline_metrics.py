"""
Pipeline Metrics Service

Document: 20251219_pipeline_instrumentation_proposal.md
Governance: constitution_observability.json, constitution_logging.json
Classification: TIER_0

Provides:
- Batch lifecycle tracking (start, heartbeat, complete, fail)
- API call performance recording
- Adaptive stall detection thresholds
- Performance statistics retrieval
"""
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any, List
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)


class PipelineType(str, Enum):
    """Supported pipeline types."""
    SEQUENTIAL_ENRICHMENT = "sequential_enrichment"
    GCD_IMPORT = "gcd_import"
    PRICECHARTING = "pricecharting"
    METRON_SYNC = "metron_sync"
    FUNKO_PRICECHARTING_MATCH = "funko_pricecharting_match"
    COMIC_PRICECHARTING_MATCH = "comic_pricecharting_match"
    FUNKO_PRICE_SYNC = "funko_price_sync"
    COMIC_PRICE_SYNC = "comic_price_sync"
    COMIC_ENRICHMENT = "comic_enrichment"
    COVER_ENRICHMENT = "cover_enrichment"
    BCW_INVENTORY_SYNC = "bcw_inventory_sync"
    IMAGE_ACQUISITION = "image_acquisition"


class BatchStatus(str, Enum):
    """Batch status values."""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    STALLED = "stalled"
    SELF_HEALED = "self_healed"


class ApiSource(str, Enum):
    """Supported API sources."""
    METRON = "metron"
    COMICVINE = "comicvine"
    PRICECHARTING = "pricecharting"
    MARVEL_FANDOM = "marvel_fandom"
    DC_FANDOM = "dc_fandom"
    IMAGE_FANDOM = "image_fandom"
    IDW_FANDOM = "idw_fandom"
    DARK_HORSE_FANDOM = "dark_horse_fandom"
    DYNAMITE_FANDOM = "dynamite_fandom"
    MYCOMICSHOP = "mycomicshop"
    CBR = "cbr"
    GCD = "gcd"
    BCW = "bcw"


class ErrorCategory(str, Enum):
    """API error categories."""
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    SERVER_ERROR = "server_error"
    PARSE_ERROR = "parse_error"
    NETWORK_ERROR = "network_error"
    VALIDATION_ERROR = "validation_error"


@dataclass
class BatchResult:
    """Result of batch processing."""
    records_processed: int
    records_enriched: int
    records_skipped: int
    records_failed: int
    error_category: Optional[str] = None


@dataclass
class ApiCallResult:
    """Result of an API call."""
    http_status: Optional[int]
    success: bool
    error_category: Optional[str] = None
    retry_count: int = 0
    circuit_state: Optional[str] = None


@dataclass
class PerformanceStats:
    """Pipeline performance statistics."""
    pipeline_type: str
    avg_duration_ms: int
    p50_duration_ms: int
    p75_duration_ms: int
    p90_duration_ms: int
    p95_duration_ms: int
    p99_duration_ms: int
    recommended_stall_threshold_ms: int
    sample_count: int


class PipelineMetricsService:
    """
    Service for tracking pipeline batch metrics and API call performance.

    Governance Compliance:
    - TIER_0 aggregates only (no PII, no content)
    - SHA-512 hash chain for immutability
    - 90-day retention
    """

    def __init__(self, environment: Optional[str] = None):
        self.environment = environment or os.getenv("RAILWAY_ENVIRONMENT", "development")

    def _generate_record_hash(self, data: Dict[str, Any]) -> str:
        """
        Generate SHA-512 hash for record immutability.
        Governance: constitution_logging.json §7
        """
        content = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha512(content.encode()).hexdigest()

    async def _get_previous_record_hash(
        self, db: AsyncSession, pipeline_type: str
    ) -> Optional[str]:
        """Get the previous record hash for chain linking."""
        result = await db.execute(text("""
            SELECT record_hash
            FROM pipeline_batch_metrics
            WHERE pipeline_type = :pipeline_type
            ORDER BY created_at DESC
            LIMIT 1
        """), {"pipeline_type": pipeline_type})
        row = result.fetchone()
        return row.record_hash if row else None

    async def start_batch(
        self,
        batch_id: str,
        pipeline_type: str,
        records_in_batch: int,
        db: Optional[AsyncSession] = None
    ) -> str:
        """
        Start tracking a new batch.

        Args:
            batch_id: Unique batch identifier
            pipeline_type: Type of pipeline (from PipelineType enum)
            records_in_batch: Number of records to process
            db: Optional database session (creates new one if not provided)

        Returns:
            The batch_id for reference
        """
        async def _do_start(session: AsyncSession):
            prev_hash = await self._get_previous_record_hash(session, pipeline_type)

            record_data = {
                "batch_id": batch_id,
                "pipeline_type": pipeline_type,
                "records_in_batch": records_in_batch,
                "environment": self.environment,
                "batch_started_at": datetime.now(timezone.utc).isoformat(),
                "status": "running"
            }
            record_hash = self._generate_record_hash(record_data)

            await session.execute(text("""
                INSERT INTO pipeline_batch_metrics (
                    batch_id, pipeline_type, environment, batch_started_at,
                    records_in_batch, status, last_heartbeat_at,
                    record_hash, prev_record_hash
                ) VALUES (
                    :batch_id, :pipeline_type, :environment, NOW(),
                    :records_in_batch, 'running', NOW(),
                    :record_hash, :prev_hash
                )
            """), {
                "batch_id": batch_id,
                "pipeline_type": pipeline_type,
                "environment": self.environment,
                "records_in_batch": records_in_batch,
                "record_hash": record_hash,
                "prev_hash": prev_hash
            })
            await session.commit()

            logger.info(
                f"[PipelineMetrics] Started batch {batch_id} for {pipeline_type} "
                f"({records_in_batch} records)"
            )

        if db:
            await _do_start(db)
        else:
            async with AsyncSessionLocal() as session:
                await _do_start(session)

        return batch_id

    async def heartbeat(self, batch_id: str, db: Optional[AsyncSession] = None) -> None:
        """
        Update heartbeat to prevent false stall detection.

        Args:
            batch_id: The batch to update
            db: Optional database session
        """
        async def _do_heartbeat(session: AsyncSession):
            await session.execute(text("""
                UPDATE pipeline_batch_metrics
                SET last_heartbeat_at = NOW()
                WHERE batch_id = :batch_id AND status = 'running'
            """), {"batch_id": batch_id})
            await session.commit()

        if db:
            await _do_heartbeat(db)
        else:
            async with AsyncSessionLocal() as session:
                await _do_heartbeat(session)

    async def complete_batch(
        self,
        batch_id: str,
        result: BatchResult,
        db: Optional[AsyncSession] = None
    ) -> None:
        """
        Complete a batch with results.

        Args:
            batch_id: The batch to complete
            result: BatchResult with processing statistics
            db: Optional database session
        """
        async def _do_complete(session: AsyncSession):
            await session.execute(text("""
                UPDATE pipeline_batch_metrics SET
                    batch_completed_at = NOW(),
                    batch_duration_ms = EXTRACT(EPOCH FROM (NOW() - batch_started_at)) * 1000,
                    records_processed = :records_processed,
                    records_enriched = :records_enriched,
                    records_skipped = :records_skipped,
                    records_failed = :records_failed,
                    status = 'completed',
                    error_category = :error_category
                WHERE batch_id = :batch_id
            """), {
                "batch_id": batch_id,
                "records_processed": result.records_processed,
                "records_enriched": result.records_enriched,
                "records_skipped": result.records_skipped,
                "records_failed": result.records_failed,
                "error_category": result.error_category
            })
            await session.commit()

            logger.info(
                f"[PipelineMetrics] Completed batch {batch_id}: "
                f"processed={result.records_processed}, enriched={result.records_enriched}, "
                f"failed={result.records_failed}"
            )

        if db:
            await _do_complete(db)
        else:
            async with AsyncSessionLocal() as session:
                await _do_complete(session)

    async def fail_batch(
        self,
        batch_id: str,
        error_category: str,
        db: Optional[AsyncSession] = None
    ) -> None:
        """
        Mark batch as failed.

        Args:
            batch_id: The batch to fail
            error_category: Category of error that caused failure
            db: Optional database session
        """
        async def _do_fail(session: AsyncSession):
            await session.execute(text("""
                UPDATE pipeline_batch_metrics SET
                    batch_completed_at = NOW(),
                    batch_duration_ms = EXTRACT(EPOCH FROM (NOW() - batch_started_at)) * 1000,
                    status = 'failed',
                    error_category = :error_category
                WHERE batch_id = :batch_id
            """), {"batch_id": batch_id, "error_category": error_category})
            await session.commit()

            logger.warning(f"[PipelineMetrics] Failed batch {batch_id}: {error_category}")

        if db:
            await _do_fail(db)
        else:
            async with AsyncSessionLocal() as session:
                await _do_fail(session)

    async def record_api_call(
        self,
        batch_id: str,
        api_source: str,
        endpoint_category: str,
        start_time: datetime,
        result: ApiCallResult,
        db: Optional[AsyncSession] = None
    ) -> None:
        """
        Record an API call metric.

        Args:
            batch_id: The batch this call belongs to
            api_source: Source API (from ApiSource enum)
            endpoint_category: Type of endpoint (search, details, pricing, etc.)
            start_time: When the call started
            result: ApiCallResult with outcome
            db: Optional database session
        """
        response_time_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)

        async def _do_record(session: AsyncSession):
            await session.execute(text("""
                INSERT INTO api_call_metrics (
                    batch_id, api_source, endpoint_category,
                    call_started_at, call_completed_at, response_time_ms,
                    http_status, success, error_category, retry_count, circuit_state
                ) VALUES (
                    :batch_id, :api_source, :endpoint_category,
                    :start_time, NOW(), :response_time_ms,
                    :http_status, :success, :error_category, :retry_count, :circuit_state
                )
            """), {
                "batch_id": batch_id,
                "api_source": api_source,
                "endpoint_category": endpoint_category,
                "start_time": start_time,
                "response_time_ms": response_time_ms,
                "http_status": result.http_status,
                "success": result.success,
                "error_category": result.error_category,
                "retry_count": result.retry_count,
                "circuit_state": result.circuit_state
            })
            await session.commit()

            # Log slow calls (>1000ms) per constitution_db.json §7
            if response_time_ms > 1000:
                logger.warning(
                    f"[PipelineMetrics] Slow API call: {api_source}/{endpoint_category} "
                    f"took {response_time_ms}ms"
                )

        if db:
            await _do_record(db)
        else:
            async with AsyncSessionLocal() as session:
                await _do_record(session)

    async def get_performance_stats(
        self,
        pipeline_type: str,
        db: Optional[AsyncSession] = None
    ) -> Optional[PerformanceStats]:
        """
        Get performance statistics for adaptive stall detection.

        Args:
            pipeline_type: Type of pipeline
            db: Optional database session

        Returns:
            PerformanceStats or None if insufficient data
        """
        async def _do_get(session: AsyncSession) -> Optional[PerformanceStats]:
            result = await session.execute(text("""
                SELECT
                    pipeline_type,
                    avg_duration_ms,
                    p50_duration_ms,
                    p75_duration_ms,
                    p90_duration_ms,
                    p95_duration_ms,
                    p99_duration_ms,
                    recommended_stall_threshold_ms,
                    sample_count
                FROM pipeline_performance_stats
                WHERE pipeline_type = :pipeline_type AND environment = :environment
            """), {"pipeline_type": pipeline_type, "environment": self.environment})
            row = result.fetchone()

            if not row:
                return None

            return PerformanceStats(
                pipeline_type=row.pipeline_type,
                avg_duration_ms=int(row.avg_duration_ms or 0),
                p50_duration_ms=int(row.p50_duration_ms or 0),
                p75_duration_ms=int(row.p75_duration_ms or 0),
                p90_duration_ms=int(row.p90_duration_ms or 0),
                p95_duration_ms=int(row.p95_duration_ms or 0),
                p99_duration_ms=int(row.p99_duration_ms or 0),
                recommended_stall_threshold_ms=int(row.recommended_stall_threshold_ms or 0),
                sample_count=int(row.sample_count or 0)
            )

        if db:
            return await _do_get(db)
        else:
            async with AsyncSessionLocal() as session:
                return await _do_get(session)

    async def get_stall_threshold_ms(
        self,
        pipeline_type: str,
        db: Optional[AsyncSession] = None
    ) -> int:
        """
        Get current stall threshold (adaptive or fallback).

        Returns milliseconds.
        """
        stats = await self.get_performance_stats(pipeline_type, db)

        # Minimum sample size for statistical confidence
        MIN_SAMPLES = 50

        # Fallback: 8 minutes
        FALLBACK_THRESHOLD_MS = 8 * 60 * 1000

        # Absolute minimum: 3 minutes (sanity floor)
        MIN_THRESHOLD_MS = 3 * 60 * 1000

        # Absolute maximum: 15 minutes (sanity ceiling)
        MAX_THRESHOLD_MS = 15 * 60 * 1000

        if not stats or stats.sample_count < MIN_SAMPLES:
            logger.info(
                f"[StallDetector] Insufficient samples ({stats.sample_count if stats else 0}/{MIN_SAMPLES}), "
                f"using fallback: {FALLBACK_THRESHOLD_MS}ms"
            )
            return FALLBACK_THRESHOLD_MS

        # Clamp to sanity bounds
        threshold = max(MIN_THRESHOLD_MS, min(MAX_THRESHOLD_MS, stats.recommended_stall_threshold_ms))

        logger.info(
            f"[StallDetector] Data-driven threshold for {pipeline_type}: {threshold}ms "
            f"(P95: {stats.p95_duration_ms}ms, samples: {stats.sample_count})"
        )

        return threshold

    async def detect_and_handle_stalls(
        self,
        pipeline_type: str,
        db: Optional[AsyncSession] = None
    ) -> List[str]:
        """
        Detect and mark stalled batches.

        Args:
            pipeline_type: Type of pipeline to check
            db: Optional database session

        Returns:
            List of stalled batch_ids
        """
        threshold_ms = await self.get_stall_threshold_ms(pipeline_type, db)
        threshold_seconds = threshold_ms // 1000

        async def _do_detect(session: AsyncSession) -> List[str]:
            result = await session.execute(text(f"""
                UPDATE pipeline_batch_metrics SET
                    status = 'stalled',
                    stall_detected_at = NOW()
                WHERE
                    pipeline_type = :pipeline_type
                    AND status = 'running'
                    AND last_heartbeat_at < NOW() - INTERVAL '{threshold_seconds} seconds'
                RETURNING batch_id
            """), {"pipeline_type": pipeline_type})
            await session.commit()

            stalled = [row.batch_id for row in result.fetchall()]
            if stalled:
                logger.warning(
                    f"[StallDetector] Detected {len(stalled)} stalled batches for {pipeline_type}: {stalled}"
                )
            return stalled

        if db:
            return await _do_detect(db)
        else:
            async with AsyncSessionLocal() as session:
                return await _do_detect(session)

    async def mark_self_healed(
        self,
        batch_id: str,
        db: Optional[AsyncSession] = None
    ) -> None:
        """Mark a stalled batch as self-healed."""
        async def _do_mark(session: AsyncSession):
            await session.execute(text("""
                UPDATE pipeline_batch_metrics SET
                    status = 'self_healed',
                    self_healed_at = NOW()
                WHERE batch_id = :batch_id
            """), {"batch_id": batch_id})
            await session.commit()

            logger.info(f"[StallDetector] Self-healed batch {batch_id}")

        if db:
            await _do_mark(db)
        else:
            async with AsyncSessionLocal() as session:
                await _do_mark(session)

    async def refresh_performance_stats(self, db: Optional[AsyncSession] = None) -> None:
        """Refresh the performance stats materialized view."""
        async def _do_refresh(session: AsyncSession):
            await session.execute(text("SELECT refresh_pipeline_performance_stats()"))
            await session.commit()
            logger.info("[PipelineMetrics] Refreshed performance stats")

        if db:
            await _do_refresh(db)
        else:
            async with AsyncSessionLocal() as session:
                await _do_refresh(session)


# Singleton instance
pipeline_metrics = PipelineMetricsService()


def generate_batch_id(prefix: str = "batch") -> str:
    """Generate a unique batch ID."""
    return f"{prefix}_{uuid4().hex[:12]}"
