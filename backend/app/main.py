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
# Brand Asset Management v1.0.0
from app.api.routes import admin_assets, admin_settings
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

# Price Intelligence v1.7.0 - AI/ML price analysis endpoints
try:
    from app.api.routes import price_intelligence
    PRICE_INTELLIGENCE_ROUTES_AVAILABLE = True
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"Could not import price_intelligence routes: {e}")
    price_intelligence = None
    PRICE_INTELLIGENCE_ROUTES_AVAILABLE = False

# Pipeline Scheduler v1.6.0 - automated data acquisition jobs
try:
    from app.jobs.pipeline_scheduler import pipeline_scheduler
    PIPELINE_SCHEDULER_AVAILABLE = True
    print("[IMPORT] pipeline_scheduler imported successfully")
except Exception as e:
    import traceback
    print(f"[IMPORT] Failed to import pipeline_scheduler: {e}")
    print(traceback.format_exc())
    pipeline_scheduler = None
    PIPELINE_SCHEDULER_AVAILABLE = False
from app.core.config import settings
from app.core.database import AsyncSessionLocal, engine
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








@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialize database and background tasks on startup.

    P0-1: Now includes stock cleanup scheduler
    P2-11: HTTP client lifecycle management (metron_service cleanup)
    P3-14: Funko scraper code removed (blocked by Funko.com)
    v1.7.0: Auto-create price_snapshots table for ML/AI training
    """
    global _stock_cleanup_task

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
# Brand Asset Management v1.0.0
app.include_router(admin_assets.router, prefix="/api", tags=["Admin - Assets"])
app.include_router(admin_settings.router, prefix="/api", tags=["Admin - Settings"])
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

# Price Intelligence v1.7.0
if PRICE_INTELLIGENCE_ROUTES_AVAILABLE:
    app.include_router(price_intelligence.router, prefix="/api/prices", tags=["Price Intelligence"])
else:
    logger.warning("Price intelligence routes disabled - import failed")


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
        health_status["database"] = f"error: {type(e).__name__}: {str(e)[:100]}"
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
