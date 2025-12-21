"""
Jobs Package

v1.23.0: Data Ingestion Jobs - Unified bulk data import
v1.6.0: Data Acquisition Pipeline - Automated enrichment jobs
v1.5.0: Outreach System - Background job definitions for ARQ
"""
from app.jobs.newsletter import (
    generate_weekly_newsletter,
    send_newsletter_batch,
    process_confirmation_email,
)
from app.jobs.analytics import (
    sync_price_changelog,
    generate_weekly_report,
)
from app.jobs.content import (
    generate_content_batch,
)
from app.jobs.social import (
    post_to_bluesky,
    process_content_queue,
)
# Data Acquisition Pipeline v1.6.0
from app.jobs.pipeline_scheduler import (
    pipeline_scheduler,
    run_gcd_import_job,
    run_funko_price_check_job,
    run_dlq_retry_job,
    run_daily_snapshot_job,  # v1.7.0
)
# Data Ingestion Jobs v1.23.0
from app.jobs.data_ingestion import (
    run_csv_ingestion_job,
    run_json_ingestion_job,
    run_cover_folder_ingestion_job,
)
# BCW Dropship Integration v1.22.0
from app.jobs.bcw_sync import (
    run_bcw_inventory_sync_job,
    run_bcw_full_inventory_sync_job,
    run_bcw_order_status_sync_job,
    run_bcw_email_processing_job,
    run_bcw_quote_cleanup_job,
    run_bcw_selector_health_job,
)

__all__ = [
    "generate_weekly_newsletter",
    "send_newsletter_batch",
    "process_confirmation_email",
    "sync_price_changelog",
    "generate_weekly_report",
    "generate_content_batch",
    "post_to_bluesky",
    "process_content_queue",
    # Data Acquisition Pipeline v1.6.0
    "pipeline_scheduler",
    "run_gcd_import_job",
    "run_funko_price_check_job",
    "run_dlq_retry_job",
    # Price Snapshots for AI Intelligence v1.7.0
    "run_daily_snapshot_job",
    # Data Ingestion Jobs v1.23.0
    "run_csv_ingestion_job",
    "run_json_ingestion_job",
    "run_cover_folder_ingestion_job",
    # BCW Dropship Integration v1.22.0
    "run_bcw_inventory_sync_job",
    "run_bcw_full_inventory_sync_job",
    "run_bcw_order_status_sync_job",
    "run_bcw_email_processing_job",
    "run_bcw_quote_cleanup_job",
    "run_bcw_selector_health_job",
]
