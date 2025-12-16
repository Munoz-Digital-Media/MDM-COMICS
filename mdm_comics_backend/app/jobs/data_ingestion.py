"""
Data Ingestion Jobs v1.0.0

Background jobs for data ingestion operations using arq.

These jobs wrap the DataIngestionService for async execution,
providing progress tracking, checkpointing, and error handling.

Usage:
    # Queue a job via API
    POST /api/admin/ingest-data
    {
        "source": "pricecharting",
        "file_path": "/path/to/data.csv",
        "options": {"batch_size": 1000}
    }

    # Or trigger directly
    from app.jobs.data_ingestion import run_csv_ingestion_job
    await run_csv_ingestion_job(ctx, source="pricecharting", file_path="...", table="comic_issues")
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.core.database import get_db_session
from app.core.utils import utcnow
from app.services.data_ingestion import (
    DataIngestionService,
    IngestionOptions,
    IngestionStats,
    parse_date,
    parse_decimal,
    parse_int,
    clean_string,
)

logger = logging.getLogger(__name__)


# =============================================================================
# JOB: CSV INGESTION
# =============================================================================


async def run_csv_ingestion_job(
    ctx: dict,
    source: str,
    file_path: str,
    table_name: str,
    batch_size: int = 1000,
    skip_existing: bool = True,
    update_existing: bool = False,
    field_mapping: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Background job for CSV ingestion.

    Args:
        ctx: arq job context
        source: Data source identifier (e.g., "pricecharting", "gcd")
        file_path: Path to CSV file
        table_name: Target database table
        batch_size: Records per batch
        skip_existing: Skip duplicate records
        update_existing: Update existing records on conflict
        field_mapping: Source-to-DB field mapping

    Returns:
        Dict with job results
    """
    job_name = f"csv_ingest_{source}"

    logger.info(f"[{job_name}] Starting CSV ingestion job")
    logger.info(f"[{job_name}] Source: {source}, File: {file_path}, Table: {table_name}")

    async with get_db_session() as db:
        try:
            # Mark job as running
            await _update_checkpoint(db, job_name, is_running=True)

            # Configure options
            options = IngestionOptions(
                batch_size=batch_size,
                skip_existing=skip_existing,
                update_existing=update_existing,
                field_mapping=field_mapping or {},
            )

            # Add common transformers based on source
            options.transformers = _get_source_transformers(source)

            # Run ingestion
            service = DataIngestionService(db)
            stats = await service.ingest_csv(
                source=source,
                file_path=file_path,
                table_name=table_name,
                options=options,
            )

            # Update checkpoint with results
            await _update_checkpoint(
                db,
                job_name,
                is_running=False,
                total_processed=stats.processed,
                total_updated=stats.inserted + stats.updated,
                total_errors=stats.errors,
            )

            result = {
                "status": "completed",
                "source": source,
                "stats": stats.to_dict(),
            }

            logger.info(f"[{job_name}] Job completed: {stats.to_dict()}")
            return result

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")

            # Update checkpoint with error
            await _update_checkpoint(
                db,
                job_name,
                is_running=False,
                last_error=str(e),
            )

            return {
                "status": "failed",
                "source": source,
                "error": str(e),
            }


# =============================================================================
# JOB: JSON INGESTION
# =============================================================================


async def run_json_ingestion_job(
    ctx: dict,
    source: str,
    file_path: str,
    table_name: str,
    json_path: Optional[str] = None,
    batch_size: int = 1000,
    skip_existing: bool = True,
    update_existing: bool = False,
    field_mapping: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Background job for JSON ingestion.

    Args:
        ctx: arq job context
        source: Data source identifier
        file_path: Path to JSON file
        table_name: Target database table
        json_path: JSONPath to data array (e.g., "data.items")
        batch_size: Records per batch
        skip_existing: Skip duplicate records
        update_existing: Update existing records on conflict
        field_mapping: Source-to-DB field mapping

    Returns:
        Dict with job results
    """
    job_name = f"json_ingest_{source}"

    logger.info(f"[{job_name}] Starting JSON ingestion job")

    async with get_db_session() as db:
        try:
            await _update_checkpoint(db, job_name, is_running=True)

            options = IngestionOptions(
                batch_size=batch_size,
                skip_existing=skip_existing,
                update_existing=update_existing,
                field_mapping=field_mapping or {},
            )
            options.transformers = _get_source_transformers(source)

            service = DataIngestionService(db)
            stats = await service.ingest_json(
                source=source,
                file_path=file_path,
                table_name=table_name,
                options=options,
                json_path=json_path,
            )

            await _update_checkpoint(
                db,
                job_name,
                is_running=False,
                total_processed=stats.processed,
                total_updated=stats.inserted + stats.updated,
                total_errors=stats.errors,
            )

            result = {
                "status": "completed",
                "source": source,
                "stats": stats.to_dict(),
            }

            logger.info(f"[{job_name}] Job completed: {stats.to_dict()}")
            return result

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")

            await _update_checkpoint(
                db,
                job_name,
                is_running=False,
                last_error=str(e),
            )

            return {
                "status": "failed",
                "source": source,
                "error": str(e),
            }


# =============================================================================
# JOB: COVER FOLDER INGESTION
# =============================================================================


async def run_cover_folder_ingestion_job(
    ctx: dict,
    folder_path: str,
    user_id: int,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Background job for cover folder ingestion.

    Scans a folder for cover images, matches to comic_issues,
    and queues all items to Match Review for approval.

    Args:
        ctx: arq job context
        folder_path: Path to folder containing cover images
        user_id: User ID performing ingestion
        limit: Max files to process (for testing)

    Returns:
        Dict with job results
    """
    job_name = "cover_folder_ingest"

    logger.info(f"[{job_name}] Starting cover folder ingestion")
    logger.info(f"[{job_name}] Folder: {folder_path}, Limit: {limit}")

    async with get_db_session() as db:
        try:
            await _update_checkpoint(db, job_name, is_running=True)

            from app.services.cover_ingestion import CoverIngestionService

            service = CoverIngestionService(db)
            result = await service.ingest_folder(
                folder_path=folder_path,
                user_id=user_id,
                limit=limit,
            )

            await _update_checkpoint(
                db,
                job_name,
                is_running=False,
                total_processed=result.processed,
                total_updated=result.queued_for_review,
                total_errors=result.errors,
            )

            job_result = {
                "status": "completed",
                "total_files": result.total_files,
                "processed": result.processed,
                "queued_for_review": result.queued_for_review,
                "high_confidence": result.high_confidence,
                "medium_confidence": result.medium_confidence,
                "low_confidence": result.low_confidence,
                "skipped": result.skipped,
                "errors": result.errors,
            }

            logger.info(f"[{job_name}] Job completed: {job_result}")
            return job_result

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")

            await _update_checkpoint(
                db,
                job_name,
                is_running=False,
                last_error=str(e),
            )

            return {
                "status": "failed",
                "error": str(e),
            }


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


async def _update_checkpoint(
    db,
    job_name: str,
    is_running: bool = False,
    total_processed: int = 0,
    total_updated: int = 0,
    total_errors: int = 0,
    last_error: Optional[str] = None,
) -> None:
    """Update or create pipeline checkpoint for job tracking."""
    await db.execute(text("""
        INSERT INTO pipeline_checkpoints (
            job_name, job_type, is_running,
            total_processed, total_updated, total_errors,
            last_error, last_run_started, updated_at, created_at
        )
        VALUES (
            :job_name, 'data_ingestion', :is_running,
            :total_processed, :total_updated, :total_errors,
            :last_error,
            CASE WHEN :is_running THEN NOW() ELSE NULL END,
            NOW(), NOW()
        )
        ON CONFLICT (job_name) DO UPDATE SET
            is_running = :is_running,
            total_processed = COALESCE(pipeline_checkpoints.total_processed, 0) + :total_processed,
            total_updated = COALESCE(pipeline_checkpoints.total_updated, 0) + :total_updated,
            total_errors = COALESCE(pipeline_checkpoints.total_errors, 0) + :total_errors,
            last_error = COALESCE(:last_error, pipeline_checkpoints.last_error),
            last_run_started = CASE WHEN :is_running THEN NOW() ELSE pipeline_checkpoints.last_run_started END,
            last_run_completed = CASE WHEN NOT :is_running THEN NOW() ELSE pipeline_checkpoints.last_run_completed END,
            updated_at = NOW()
    """), {
        "job_name": job_name,
        "is_running": is_running,
        "total_processed": total_processed,
        "total_updated": total_updated,
        "total_errors": total_errors,
        "last_error": last_error,
    })
    await db.commit()


def _get_source_transformers(source: str) -> Dict[str, Any]:
    """Get field transformers for a specific data source."""
    transformers = {
        "pricecharting": {
            "price_loose": parse_decimal,
            "price_cib": parse_decimal,
            "price_new": parse_decimal,
            "release_date": parse_date,
        },
        "gcd": {
            "publication_date": parse_date,
            "page_count": parse_int,
            "price": clean_string,
        },
        "metron": {
            "cover_date": parse_date,
            "store_date": parse_date,
            "page_count": parse_int,
        },
        "comicvine": {
            "cover_date": parse_date,
            "store_date": parse_date,
        },
    }

    return transformers.get(source, {})


# =============================================================================
# ARQ JOB REGISTRY
# =============================================================================


# Export jobs for arq worker registration
__all__ = [
    "run_csv_ingestion_job",
    "run_json_ingestion_job",
    "run_cover_folder_ingestion_job",
]
