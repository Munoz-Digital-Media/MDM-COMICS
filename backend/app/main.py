"""
MDM Comics Backend
FastAPI application entry point

Risk Mitigation Implementation:
- P0-1: Stock cleanup scheduler with heartbeat metrics
- P1-3: Rate limiting with SlowAPI
- P2-4: Error sanitization middleware
- P2-7: Security headers (CSP, X-Frame-Options, etc.)
- P2-8: Enhanced health endpoint with DB ping
- P2-10: Request size limits
- P2-11: HTTP client lifecycle management
- P2-5: Request metrics collection and monitoring
- P3-14: Dead Funko scraper code removed
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from sqlalchemy import select, func, text

from app.api.routes import products, users, auth, cart, orders, grading, comics, checkout, funkos, analytics, coupons, admin, contact
from app.api.routes import shipping
# User Management System v1.0.0
from app.api.routes import admin_users, admin_roles, admin_dsar
# Outreach System v1.5.0 - optional imports for graceful degradation
try:
    from app.api.routes import newsletter, webhooks
    OUTREACH_ROUTES_AVAILABLE = True
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"Could not import outreach routes: {e}")
    newsletter = None
    webhooks = None
    OUTREACH_ROUTES_AVAILABLE = False

# Data Acquisition Pipeline v1.0.0 - optional import for graceful degradation
try:
    from app.api.routes import data_health
    DATA_HEALTH_ROUTES_AVAILABLE = True
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"Could not import data_health routes: {e}")
    data_health = None
    DATA_HEALTH_ROUTES_AVAILABLE = False

# Pipeline Scheduler v1.6.0 - automated data acquisition jobs
try:
    from app.jobs.pipeline_scheduler import pipeline_scheduler
    PIPELINE_SCHEDULER_AVAILABLE = True
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"Could not import pipeline_scheduler: {e}")
    pipeline_scheduler = None
    PIPELINE_SCHEDULER_AVAILABLE = False
from app.core.config import settings
from app.core.database import init_db, AsyncSessionLocal, engine
from app.core.rate_limit import limiter, rate_limit_exceeded_handler
from app.core.error_handler import ErrorSanitizationMiddleware
from app.core.security_headers import SecurityHeadersMiddleware
from app.core.monitoring import RequestMetricsMiddleware, metrics, record_db_metrics, get_prometheus_metrics
from app.core.backup import get_backup_status, get_restore_instructions
from app.services.metron import metron_service

# Import models to register them with SQLAlchemy
from app.models import (
    User, Product, CartItem, Order, OrderItem, GradeRequest,
    ComicPublisher, ComicSeries, ComicIssue, ComicCharacter,
    ComicCreator, ComicArc, MetronAPILog,
    Funko, FunkoSeriesName
)

logger = logging.getLogger(__name__)

# P0-1: Background task references
_stock_cleanup_task: Optional[asyncio.Task] = None
_cleanup_heartbeat: dict = {
    "last_run": None,
    "last_success": None,
    "records_processed": 0,
    "errors": 0,
}


async def import_funkos_if_needed():
    """
    Import Funko data using handle-based reconciliation (BE-007 fix).

    This function now uses handle-based comparison instead of count-based,
    ensuring that new Funkos in the JSON file are always imported even if
    the total count is similar. This is more robust for incremental updates.
    """
    async with AsyncSessionLocal() as db:
        # Find the JSON file
        json_path = Path(__file__).parent.parent / "funko_data.json"
        if not json_path.exists():
            logger.warning(f"Funko data file not found at {json_path}")
            return

        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        total_in_file = len(data)
        if total_in_file == 0:
            logger.info("Funko data file is empty, nothing to import.")
            return

        # BE-007 FIX: Get all existing handles for content-based reconciliation
        # This replaces the count-based check that could miss new items
        existing_result = await db.execute(select(Funko.handle))
        existing_handles = set(row[0] for row in existing_result.fetchall())
        logger.info(f"Found {len(existing_handles)} existing Funko handles in database")

        # Calculate how many new items we need to import
        file_handles = set(item.get('handle', '') for item in data if item.get('handle'))
        new_handles = file_handles - existing_handles

        if not new_handles:
            logger.info(f"Funko database is in sync with file ({len(existing_handles)} entries). No import needed.")
            return

        logger.info(f"BE-007: Handle-based sync found {len(new_handles)} new Funkos to import")

        # Get existing series names for efficient lookup
        series_result = await db.execute(select(FunkoSeriesName))
        series_cache = {s.name: s for s in series_result.scalars().all()}
        logger.info(f"Loaded {len(series_cache)} existing series names")

        imported = 0
        skipped = 0
        batch_size = 500

        for item in data:
            handle = item.get('handle', '')
            if not handle:
                continue

            # Skip if already exists (handle-based check)
            if handle in existing_handles:
                skipped += 1
                continue

            # Get or create series
            series_list = item.get('series', [])
            funko_series = []

            for series_name in series_list:
                if series_name not in series_cache:
                    new_series = FunkoSeriesName(name=series_name)
                    db.add(new_series)
                    await db.flush()
                    series_cache[series_name] = new_series
                funko_series.append(series_cache[series_name])

            # Create Funko
            funko = Funko(
                handle=handle,
                title=item.get('title', ''),
                image_url=item.get('image', ''),
                series=funko_series
            )
            db.add(funko)
            existing_handles.add(handle)  # Add to set so we skip if duplicated in file
            imported += 1

            # Commit in batches
            if imported % batch_size == 0:
                await db.commit()
                logger.info(f"Imported {imported} Funkos (skipped {skipped})...")

        await db.commit()
        logger.info(f"BE-007: Funko import complete! {imported} new entries added, {skipped} already existed.")


# ============== P0-1: STOCK CLEANUP SCHEDULER ==============

async def run_stock_cleanup():
    """
    P0-1: Run stock cleanup to release expired reservations.
    Updates heartbeat metrics for health monitoring.
    """
    global _cleanup_heartbeat
    from app.services.stock_cleanup import release_expired_reservations

    _cleanup_heartbeat["last_run"] = datetime.now(timezone.utc).isoformat()

    try:
        stats = await release_expired_reservations()
        _cleanup_heartbeat["last_success"] = datetime.now(timezone.utc).isoformat()
        _cleanup_heartbeat["records_processed"] += stats.get("reservations_released", 0)

        if stats.get("reservations_released", 0) > 0:
            logger.info(
                f"Stock cleanup: released {stats['reservations_released']} reservations, "
                f"restored {stats['stock_restored']} units"
            )
    except Exception as e:
        _cleanup_heartbeat["errors"] += 1
        logger.error(f"Stock cleanup failed: {e}")


async def stock_cleanup_scheduler():
    """
    P0-1: Background scheduler that runs stock cleanup at configured intervals.
    Runs until cancelled during shutdown.
    """
    interval_seconds = settings.STOCK_CLEANUP_INTERVAL_MINUTES * 60
    logger.info(f"Stock cleanup scheduler started (interval: {settings.STOCK_CLEANUP_INTERVAL_MINUTES} minutes)")

    while True:
        try:
            await run_stock_cleanup()
        except Exception as e:
            logger.error(f"Stock cleanup scheduler error: {e}")

        await asyncio.sleep(interval_seconds)


async def migrate_funko_columns():
    """Add enrichment columns to funkos table if they don't exist."""
    from sqlalchemy import text
    from app.core.database import engine

    columns_to_add = [
        ("category", "VARCHAR(255)"),
        ("license", "VARCHAR(255)"),
        ("product_type", "VARCHAR(100)"),
        ("box_number", "VARCHAR(50)"),
        ("funko_url", "TEXT"),
    ]

    async with engine.begin() as conn:
        for col_name, col_type in columns_to_add:
            try:
                await conn.execute(text(
                    f"ALTER TABLE funkos ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
                logger.info(f"Added column: {col_name}")
            except Exception as e:
                # Column already exists or other issue
                pass

        # Add indexes
        for col in ["category", "license", "product_type", "box_number"]:
            try:
                await conn.execute(text(
                    f"CREATE INDEX IF NOT EXISTS ix_funkos_{col} ON funkos({col})"
                ))
            except Exception:
                pass

    logger.info("Funko columns migration complete")


async def migrate_shipping_tables():
    """
    UPS Shipping Integration v1.28.0: Create shipping tables on startup.
    This is an idempotent migration - safe to run on every startup.
    """
    try:
        from app.migrations.shipping_tables import migrate_shipping_tables as run_migration
        await run_migration(engine)
        logger.info("Shipping tables migration complete")
    except Exception as e:
        logger.error(f"Shipping tables migration failed: {e}")
        # Don't block startup - tables may already exist or DB issue
        pass


async def migrate_outreach_tables():
    """
    Outreach System v1.5.0: Create outreach tables on startup.
    This is an idempotent migration - safe to run on every startup.
    """
    try:
        async with engine.begin() as conn:
            # newsletter_subscribers
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS newsletter_subscribers (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) NOT NULL,
                    email_hash VARCHAR(64) NOT NULL UNIQUE,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    content_types TEXT[] DEFAULT ARRAY['newsletter'],
                    confirmation_token VARCHAR(64),
                    unsubscribe_token VARCHAR(64) NOT NULL,
                    confirmed_at TIMESTAMP WITH TIME ZONE,
                    unsubscribed_at TIMESTAMP WITH TIME ZONE,
                    unsubscribe_reason TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_newsletter_subscribers_email_hash
                ON newsletter_subscribers(email_hash)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_newsletter_subscribers_status
                ON newsletter_subscribers(status)
            """))

            # email_events
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS email_events (
                    id SERIAL PRIMARY KEY,
                    subscriber_id INTEGER REFERENCES newsletter_subscribers(id) ON DELETE SET NULL,
                    event_type VARCHAR(50) NOT NULL,
                    message_id VARCHAR(255),
                    email_hash VARCHAR(64),
                    campaign_id VARCHAR(100),
                    metadata JSONB DEFAULT '{}',
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_email_events_subscriber_id
                ON email_events(subscriber_id)
            """))

            # content_queue
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS content_queue (
                    id SERIAL PRIMARY KEY,
                    content_type VARCHAR(50) NOT NULL,
                    platform VARCHAR(50) NOT NULL DEFAULT 'bluesky',
                    source_type VARCHAR(50),
                    source_id INTEGER,
                    content TEXT,
                    image_url TEXT,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending_review',
                    scheduled_for TIMESTAMP WITH TIME ZONE,
                    posted_at TIMESTAMP WITH TIME ZONE,
                    post_url TEXT,
                    approved_by INTEGER,
                    approved_at TIMESTAMP WITH TIME ZONE,
                    rejected_reason TEXT,
                    retry_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    ai_generated BOOLEAN DEFAULT FALSE,
                    idempotency_key VARCHAR(100) UNIQUE,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_content_queue_status
                ON content_queue(status)
            """))

            # price_changelog
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS price_changelog (
                    id SERIAL PRIMARY KEY,
                    entity_type VARCHAR(50) NOT NULL,
                    entity_id INTEGER NOT NULL,
                    entity_name VARCHAR(255),
                    field_name VARCHAR(50) NOT NULL,
                    old_value NUMERIC(10, 2),
                    new_value NUMERIC(10, 2),
                    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    source VARCHAR(50) DEFAULT 'sync'
                )
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_price_changelog_entity
                ON price_changelog(entity_type, entity_id)
            """))

        logger.info("Outreach tables migration complete")
    except Exception as e:
        logger.error(f"Outreach tables migration failed: {e}")
        pass


async def migrate_user_management_tables():
    """
    User Management System v2.0.0: Create user management tables on startup.
    This is an idempotent migration - safe to run on every startup.

    Creates (each in its own transaction to prevent cascading failures):
    - users table columns (email_verified_at, lockout fields, etc.)
    - roles table
    - user_roles junction table
    - user_sessions table
    - user_audit_log table
    - dsar_requests table
    - email_verifications table
    - password_resets table
    """
    # 1. Add missing columns to users table (separate transaction)
    try:
        async with engine.begin() as conn:
            logger.info("User Management: Adding missing columns to users table...")
            user_columns = [
                ("email_verified_at", "TIMESTAMP WITH TIME ZONE"),
                ("failed_login_attempts", "INTEGER DEFAULT 0"),
                ("locked_until", "TIMESTAMP WITH TIME ZONE"),
                ("lockout_count", "INTEGER DEFAULT 0"),
                ("password_changed_at", "TIMESTAMP WITH TIME ZONE DEFAULT NOW()"),
                ("last_login_at", "TIMESTAMP WITH TIME ZONE"),
                ("last_login_ip_hash", "VARCHAR(64)"),
                ("deleted_at", "TIMESTAMP WITH TIME ZONE"),
            ]
            for col_name, col_type in user_columns:
                try:
                    await conn.execute(text(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
                except Exception:
                    pass  # Column may already exist
            logger.info("User Management: users columns complete")
    except Exception as e:
        logger.warning(f"User Management: users columns migration warning: {e}")

    # 2. Create roles table (separate transaction)
    try:
        async with engine.begin() as conn:
            logger.info("User Management: Creating roles table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS roles (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) UNIQUE NOT NULL,
                    description TEXT,
                    permissions JSONB NOT NULL DEFAULT '[]',
                    is_system BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_roles_name ON roles(name)"))
            logger.info("User Management: roles table complete")
    except Exception as e:
        logger.warning(f"User Management: roles table warning: {e}")

    # 3. Seed system roles (separate transaction)
    try:
        async with engine.begin() as conn:
            logger.info("User Management: Seeding system roles...")
            system_roles = [
                ("customer", "Default customer role", '["orders:read", "orders:create", "profile:read", "profile:update"]'),
                ("admin", "Full administrative access", '["*"]'),
                ("support", "Customer support role", '["users:read", "orders:read", "orders:update"]'),
                ("inventory", "Inventory management", '["products:*", "inventory:*"]'),
            ]
            for name, desc, perms in system_roles:
                try:
                    await conn.execute(text("""
                        INSERT INTO roles (name, description, permissions, is_system)
                        VALUES (:name, :desc, :perms::jsonb, TRUE)
                        ON CONFLICT (name) DO NOTHING
                    """), {"name": name, "desc": desc, "perms": perms})
                except Exception:
                    pass
            logger.info("User Management: roles seeding complete")
    except Exception as e:
        logger.warning(f"User Management: roles seeding warning: {e}")

    # 4. Create user_roles junction table (separate transaction)
    try:
        async with engine.begin() as conn:
            logger.info("User Management: Creating user_roles table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_roles (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE RESTRICT,
                    assigned_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    expires_at TIMESTAMP WITH TIME ZONE,
                    UNIQUE(user_id, role_id)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_roles_user_id ON user_roles(user_id)"))
            logger.info("User Management: user_roles table complete")
    except Exception as e:
        logger.warning(f"User Management: user_roles table warning: {e}")

    # 5. Create user_sessions table (separate transaction)
    try:
        async with engine.begin() as conn:
            logger.info("User Management: Creating user_sessions table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_sessions (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    token_jti VARCHAR(36) UNIQUE NOT NULL,
                    refresh_jti VARCHAR(36),
                    device_fingerprint_hash VARCHAR(64),
                    user_agent_hash VARCHAR(64),
                    ip_address_hash VARCHAR(64),
                    device_type VARCHAR(50),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    last_activity_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    revoked_at TIMESTAMP WITH TIME ZONE,
                    revoke_reason VARCHAR(50)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_sessions_token_jti ON user_sessions(token_jti)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_sessions_user_id ON user_sessions(user_id)"))
            logger.info("User Management: user_sessions table complete")
    except Exception as e:
        logger.warning(f"User Management: user_sessions table warning: {e}")

    # 6. Create user_audit_log table (separate transaction)
    try:
        async with engine.begin() as conn:
            logger.info("User Management: Creating user_audit_log table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_audit_log (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    actor_type VARCHAR(20) NOT NULL,
                    actor_id_hash VARCHAR(64) NOT NULL,
                    action VARCHAR(100) NOT NULL,
                    resource_type VARCHAR(50) NOT NULL,
                    resource_id_hash VARCHAR(64),
                    before_hash VARCHAR(128),
                    after_hash VARCHAR(128),
                    outcome VARCHAR(20) NOT NULL,
                    ip_hash VARCHAR(64),
                    event_metadata JSONB DEFAULT '{}',
                    prev_hash VARCHAR(128),
                    entry_hash VARCHAR(128) NOT NULL
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_ts ON user_audit_log(ts)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_actor ON user_audit_log(actor_id_hash, ts)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_resource ON user_audit_log(resource_type, resource_id_hash)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_action ON user_audit_log(action)"))
            logger.info("User Management: user_audit_log table complete")
    except Exception as e:
        logger.warning(f"User Management: user_audit_log table warning: {e}")

    # 7. Create dsar_requests table (separate transaction)
    try:
        async with engine.begin() as conn:
            logger.info("User Management: Creating dsar_requests table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dsar_requests (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    request_type VARCHAR(20) NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    completed_at TIMESTAMP WITH TIME ZONE,
                    export_url_hash VARCHAR(128),
                    processed_by INTEGER REFERENCES users(id),
                    notes TEXT,
                    ledger_tx_id VARCHAR(128)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_dsar_user ON dsar_requests(user_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_dsar_status ON dsar_requests(status)"))
            logger.info("User Management: dsar_requests table complete")
    except Exception as e:
        logger.warning(f"User Management: dsar_requests table warning: {e}")

    # 8. Create email_verifications table (separate transaction)
    try:
        async with engine.begin() as conn:
            logger.info("User Management: Creating email_verifications table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS email_verifications (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    token_hash VARCHAR(64) UNIQUE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    verified_at TIMESTAMP WITH TIME ZONE
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_email_verifications_user ON email_verifications(user_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_email_verifications_token ON email_verifications(token_hash)"))
            logger.info("User Management: email_verifications table complete")
    except Exception as e:
        logger.warning(f"User Management: email_verifications table warning: {e}")

    # 9. Create password_resets table (separate transaction)
    try:
        async with engine.begin() as conn:
            logger.info("User Management: Creating password_resets table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS password_resets (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    token_hash VARCHAR(64) UNIQUE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    used_at TIMESTAMP WITH TIME ZONE
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_password_resets_user ON password_resets(user_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_password_resets_token ON password_resets(token_hash)"))
            logger.info("User Management: password_resets table complete")
    except Exception as e:
        logger.warning(f"User Management: password_resets table warning: {e}")

    logger.info("User Management tables migration complete")


async def migrate_pipeline_tables():
    """
    Data Acquisition Pipeline v1.0.0: Create pipeline tables on startup.
    This is an idempotent migration - safe to run on every startup.

    Creates:
    - field_changelog (generalized change tracking)
    - dead_letter_queue (failed job storage)
    - pipeline_checkpoints (job resume state)
    - data_quarantine (low-confidence data review)
    - field_provenance (source tracking per field)
    """
    # 1. field_changelog table
    try:
        async with engine.begin() as conn:
            logger.info("Pipeline: Creating field_changelog table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS field_changelog (
                    id SERIAL PRIMARY KEY,
                    entity_type VARCHAR(50) NOT NULL,
                    entity_id INTEGER NOT NULL,
                    entity_name VARCHAR(500),
                    field_name VARCHAR(100) NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    value_type VARCHAR(50) DEFAULT 'string',
                    change_pct NUMERIC(8, 2),
                    data_source VARCHAR(50) NOT NULL,
                    reason VARCHAR(50) NOT NULL DEFAULT 'sync',
                    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    sync_batch_id UUID,
                    changed_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL
                )
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_field_changelog_entity
                ON field_changelog(entity_type, entity_id)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_field_changelog_changed_at
                ON field_changelog(changed_at)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_field_changelog_batch
                ON field_changelog(sync_batch_id)
            """))
            await conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ix_field_changelog_idempotent
                ON field_changelog(entity_type, entity_id, field_name, sync_batch_id)
                WHERE sync_batch_id IS NOT NULL
            """))
            logger.info("Pipeline: field_changelog table complete")
    except Exception as e:
        logger.warning(f"Pipeline: field_changelog table warning: {e}")

    # 2. dead_letter_queue table
    try:
        async with engine.begin() as conn:
            logger.info("Pipeline: Creating dead_letter_queue table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dead_letter_queue (
                    id SERIAL PRIMARY KEY,
                    job_type VARCHAR(100) NOT NULL,
                    batch_id UUID,
                    entity_type VARCHAR(50),
                    entity_id INTEGER,
                    external_id VARCHAR(100),
                    error_message TEXT NOT NULL,
                    error_type VARCHAR(100),
                    error_trace TEXT,
                    request_data JSONB,
                    response_data JSONB,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    next_retry_at TIMESTAMP WITH TIME ZONE,
                    last_retry_at TIMESTAMP WITH TIME ZONE,
                    resolved_at TIMESTAMP WITH TIME ZONE,
                    resolved_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    resolution_notes TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_dlq_status ON dead_letter_queue(status)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_dlq_job ON dead_letter_queue(job_type, status)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_dlq_retry ON dead_letter_queue(status, next_retry_at)
            """))
            logger.info("Pipeline: dead_letter_queue table complete")
    except Exception as e:
        logger.warning(f"Pipeline: dead_letter_queue table warning: {e}")

    # 3. pipeline_checkpoints table
    try:
        async with engine.begin() as conn:
            logger.info("Pipeline: Creating pipeline_checkpoints table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
                    id SERIAL PRIMARY KEY,
                    job_name VARCHAR(100) NOT NULL UNIQUE,
                    job_type VARCHAR(50) NOT NULL,
                    last_processed_id INTEGER,
                    last_page INTEGER,
                    cursor VARCHAR(500),
                    total_processed INTEGER DEFAULT 0,
                    total_updated INTEGER DEFAULT 0,
                    total_errors INTEGER DEFAULT 0,
                    state_data JSONB,
                    current_batch_id UUID,
                    is_running BOOLEAN DEFAULT FALSE,
                    last_run_started TIMESTAMP WITH TIME ZONE,
                    last_run_completed TIMESTAMP WITH TIME ZONE,
                    last_error TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_checkpoint_running ON pipeline_checkpoints(is_running)
            """))
            logger.info("Pipeline: pipeline_checkpoints table complete")
    except Exception as e:
        logger.warning(f"Pipeline: pipeline_checkpoints table warning: {e}")

    # 4. data_quarantine table
    try:
        async with engine.begin() as conn:
            logger.info("Pipeline: Creating data_quarantine table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS data_quarantine (
                    id SERIAL PRIMARY KEY,
                    entity_type VARCHAR(50) NOT NULL,
                    entity_id INTEGER,
                    reason VARCHAR(50) NOT NULL,
                    confidence_score NUMERIC(5, 4),
                    quarantined_data JSONB NOT NULL,
                    conflict_data JSONB,
                    potential_match_ids JSONB,
                    match_scores JSONB,
                    data_source VARCHAR(50) NOT NULL,
                    batch_id UUID,
                    is_resolved BOOLEAN DEFAULT FALSE,
                    resolved_at TIMESTAMP WITH TIME ZONE,
                    resolved_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    resolution_action VARCHAR(50),
                    resolution_notes TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_quarantine_status ON data_quarantine(is_resolved)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_quarantine_entity ON data_quarantine(entity_type, entity_id)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_quarantine_reason ON data_quarantine(reason, is_resolved)
            """))
            logger.info("Pipeline: data_quarantine table complete")
    except Exception as e:
        logger.warning(f"Pipeline: data_quarantine table warning: {e}")

    # 5. field_provenance table
    try:
        async with engine.begin() as conn:
            logger.info("Pipeline: Creating field_provenance table...")
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS field_provenance (
                    id SERIAL PRIMARY KEY,
                    entity_type VARCHAR(50) NOT NULL,
                    entity_id INTEGER NOT NULL,
                    field_name VARCHAR(100) NOT NULL,
                    data_source VARCHAR(50) NOT NULL,
                    source_id VARCHAR(100),
                    source_url TEXT,
                    confidence_score NUMERIC(5, 4) DEFAULT 1.0,
                    trust_weight NUMERIC(3, 2) DEFAULT 1.0,
                    license_type VARCHAR(100),
                    requires_attribution BOOLEAN DEFAULT FALSE,
                    attribution_text TEXT,
                    is_locked BOOLEAN DEFAULT FALSE,
                    locked_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    locked_at TIMESTAMP WITH TIME ZONE,
                    lock_reason TEXT,
                    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """))
            await conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ix_provenance_unique
                ON field_provenance(entity_type, entity_id, field_name)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_provenance_source ON field_provenance(data_source)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_provenance_locked ON field_provenance(is_locked)
            """))
            logger.info("Pipeline: field_provenance table complete")
    except Exception as e:
        logger.warning(f"Pipeline: field_provenance table warning: {e}")

    logger.info("Data Acquisition Pipeline tables migration complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialize database and background tasks on startup.

    P0-1: Now includes stock cleanup scheduler
    P2-11: HTTP client lifecycle management (metron_service cleanup)
    P3-14: Funko scraper code removed (blocked by Funko.com)
    """
    global _stock_cleanup_task

    await init_db()
    # Migrate funko columns (add new enrichment fields)
    await migrate_funko_columns()
    # UPS Shipping Integration v1.28.0: Create shipping tables
    await migrate_shipping_tables()
    # Outreach System v1.5.0: Create outreach tables
    await migrate_outreach_tables()
    # User Management System v2.0.0: Create user management tables
    await migrate_user_management_tables()
    # Data Acquisition Pipeline v1.0.0: Create pipeline tables
    await migrate_pipeline_tables()
    # Import Funkos if database is empty
    await import_funkos_if_needed()

    # P0-1: Start stock cleanup scheduler if enabled
    if settings.STOCK_CLEANUP_ENABLED:
        _stock_cleanup_task = asyncio.create_task(stock_cleanup_scheduler())
        logger.info("Stock cleanup scheduler ENABLED")
    else:
        logger.info("Stock cleanup scheduler DISABLED via config")

    # v1.6.0: Start pipeline scheduler for automated data acquisition
    if PIPELINE_SCHEDULER_AVAILABLE and settings.PIPELINE_SCHEDULER_ENABLED:
        await pipeline_scheduler.start()
        logger.info("Pipeline scheduler ENABLED - jobs will run automatically")
    else:
        logger.info("Pipeline scheduler DISABLED via config or import failed")

    yield

    # Cleanup on shutdown
    if _stock_cleanup_task and not _stock_cleanup_task.done():
        _stock_cleanup_task.cancel()
        try:
            await _stock_cleanup_task
        except asyncio.CancelledError:
            logger.info("Stock cleanup scheduler cancelled")

    # v1.6.0: Stop pipeline scheduler
    if PIPELINE_SCHEDULER_AVAILABLE and pipeline_scheduler:
        await pipeline_scheduler.stop()
        logger.info("Pipeline scheduler stopped")

    # P2-11: Close HTTP clients to prevent connection leaks
    await metron_service.close()
    logger.info("Metron HTTP client closed")


app = FastAPI(
    lifespan=lifespan,
    title="MDM Comics API",
    description="""
## MDM Comics E-Commerce API

AI-powered comic book grading and e-commerce platform for collectors.

### Features
- **Authentication**: JWT-based auth with HttpOnly cookies and CSRF protection
- **Products**: Browse, search, and manage comic inventory
- **Cart & Checkout**: Full shopping cart with Stripe payment integration
- **Comic Database**: Search comics via Metron API with local caching
- **Funko Database**: Browse and search Funko Pop! collectibles
- **AI Grading**: Coming soon - ML-powered comic grade estimation

### Authentication
Most endpoints require authentication. Use `/api/auth/login` to get tokens.
Tokens are set as HttpOnly cookies for web clients, or returned in the response body for API clients.

### Rate Limits
- Auth endpoints: 5 requests/minute
- Checkout: 10 requests/minute
- General: 100 requests/minute

### Security
- All mutations require CSRF token (for cookie-based auth)
- Passwords must be 8+ chars with uppercase, lowercase, and digit
- Tokens expire in 30 minutes (access) / 7 days (refresh)
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {"name": "Health", "description": "Health check and monitoring endpoints"},
        {"name": "Authentication", "description": "User registration, login, and token management"},
        {"name": "Users", "description": "User profile management"},
        {"name": "Products", "description": "Product catalog and inventory management"},
        {"name": "Cart", "description": "Shopping cart operations"},
        {"name": "Checkout", "description": "Payment and order processing"},
        {"name": "Orders", "description": "Order history and management"},
        {"name": "Comics Database", "description": "Comic book search via Metron API"},
        {"name": "Funko Database", "description": "Funko Pop! collectibles database"},
        {"name": "AI Grading", "description": "AI-powered comic grade estimation (coming soon)"},
        {"name": "Config", "description": "Public configuration endpoints"},
    ],
    contact={
        "name": "MDM Comics Support",
        "url": "https://mdmcomics.com",
        "email": "support@mdmcomics.com",
    },
    license_info={
        "name": "Proprietary",
    },
)

# P1-3: Rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)


# P2-10: Request size limit middleware (10MB max)
MAX_REQUEST_SIZE = 10 * 1024 * 1024  # 10MB


class RequestSizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject requests that exceed the size limit."""

    async def dispatch(self, request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > MAX_REQUEST_SIZE:
            logger.warning(
                f"Request size limit exceeded: {content_length} bytes from {request.client.host}"
            )
            return JSONResponse(
                status_code=413,
                content={
                    "error": "request_too_large",
                    "message": f"Request body exceeds maximum size of {MAX_REQUEST_SIZE // (1024*1024)}MB",
                },
            )
        return await call_next(request)


app.add_middleware(RequestSizeLimitMiddleware)

# P2-5: Request metrics collection
app.add_middleware(RequestMetricsMiddleware)

# P2-4: Error sanitization (catches unhandled exceptions)
app.add_middleware(ErrorSanitizationMiddleware)

# P2-7: Security headers (CSP, X-Frame-Options, etc.)
app.add_middleware(SecurityHeadersMiddleware)

# CORS - adjust origins for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(users.router, prefix="/api/users", tags=["Users"])
app.include_router(products.router, prefix="/api/products", tags=["Products"])
app.include_router(cart.router, prefix="/api/cart", tags=["Cart"])
app.include_router(orders.router, prefix="/api/orders", tags=["Orders"])
app.include_router(grading.router, prefix="/api/grading", tags=["AI Grading"])
app.include_router(comics.router, prefix="/api", tags=["Comics Database"])
app.include_router(checkout.router, prefix="/api", tags=["Checkout"])
app.include_router(funkos.router, prefix="/api", tags=["Funko Database"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])
app.include_router(coupons.router, prefix="/api/coupons", tags=["Coupons"])
# Admin Console Inventory System v1.3.0
app.include_router(admin.router, prefix="/api", tags=["Admin"])
# UPS Shipping Integration v1.28.0
app.include_router(shipping.router, prefix="/api", tags=["Shipping"])
# IMPL-001: Contact Form
app.include_router(contact.router, prefix="/api", tags=["Contact"])
# User Management System v1.0.0
app.include_router(admin_users.router, prefix="/api/admin/users", tags=["Admin - Users"])
app.include_router(admin_roles.router, prefix="/api/admin/roles", tags=["Admin - Roles"])
app.include_router(admin_dsar.router, prefix="/api/admin/dsar", tags=["Admin - DSAR/Compliance"])
# Outreach System v1.5.0
if OUTREACH_ROUTES_AVAILABLE:
    app.include_router(newsletter.router, prefix="/api", tags=["Newsletter"])
    app.include_router(webhooks.router, prefix="/api", tags=["Webhooks"])
else:
    logger.warning("Outreach routes disabled - import failed")

# Data Acquisition Pipeline v1.0.0
if DATA_HEALTH_ROUTES_AVAILABLE:
    app.include_router(data_health.router, prefix="/api/admin", tags=["Admin - Data Health"])
else:
    logger.warning("Data health routes disabled - import failed")


@app.get("/", tags=["Health"])
async def root():
    return {
        "message": "MDM Comics API",
        "version": "0.1.0",
        "status": "operational"
    }


@app.get("/api/config", tags=["Config"])
async def get_config():
    """
    P3-12: Public configuration endpoint for frontend feature flags.
    Returns non-sensitive configuration values.
    """
    return {
        "under_construction": settings.UNDER_CONSTRUCTION,
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """
    P2-8: Enhanced health check with actual DB ping and cleanup heartbeat.
    Returns 503 if database is unreachable.
    """
    from fastapi import Response
    from fastapi.responses import JSONResponse

    health_status = {
        "status": "healthy",
        "database": "unknown",
        "stock_cleanup": _cleanup_heartbeat,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # P2-8: Actually ping the database
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("SELECT 1"))
        health_status["database"] = "connected"
    except Exception as e:
        health_status["database"] = f"error: {str(e)[:50]}"
        health_status["status"] = "unhealthy"
        return JSONResponse(status_code=503, content=health_status)

    return health_status


@app.get("/health/detailed", tags=["Health"])
async def detailed_health_check():
    """
    Detailed health check with pool statistics.
    Useful for debugging connection issues.
    """
    health = await health_check()

    # Add pool stats
    pool = engine.pool
    health["pool"] = {
        "size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
    }

    # P2-5: Record DB metrics for monitoring
    await record_db_metrics(pool)

    return health


@app.get("/metrics", tags=["Health"])
async def prometheus_metrics():
    """
    P2-5: Prometheus-compatible metrics endpoint.

    Returns application metrics in text format for monitoring systems.
    Includes request latency, error rates, and database pool stats.
    """
    from starlette.responses import Response

    # Update DB pool metrics
    await record_db_metrics(engine.pool)

    return Response(
        content=get_prometheus_metrics(),
        media_type="text/plain; charset=utf-8"
    )


@app.get("/metrics/json", tags=["Health"])
async def json_metrics():
    """
    P2-5: JSON metrics endpoint for dashboards.

    Returns all collected metrics in JSON format for easier integration
    with custom dashboards or alerting systems.
    """
    # Update DB pool metrics
    await record_db_metrics(engine.pool)

    return metrics.get_all_metrics()


@app.get("/health/backup", tags=["Health"])
async def backup_status():
    """
    P2-3: Database backup status and configuration.

    Returns current backup configuration, most recent backup info,
    and Railway managed backup details.

    Note: Railway PostgreSQL includes automatic point-in-time recovery.
    """
    return get_backup_status()


@app.get("/health/backup/restore-guide", tags=["Health"])
async def restore_guide():
    """
    P2-3: Database restore instructions.

    Returns step-by-step instructions for restoring from various backup sources.
    """
    from starlette.responses import Response
    return Response(
        content=get_restore_instructions(),
        media_type="text/plain; charset=utf-8"
    )
