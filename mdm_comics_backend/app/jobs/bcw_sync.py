"""
BCW Sync Jobs

Background jobs for BCW dropship integration:
1. Inventory Sync - Sync product availability from BCW
2. Order Status Sync - Poll BCW for order status updates
3. Email Processing - Parse BCW emails for tracking info
4. Quote Cache Cleanup - Remove expired shipping quotes

Per 20251216_mdm_comics_bcw_initial_integration.json v1.2.0.
"""
import logging
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.core.utils import utcnow
from app.models.bcw import BCWOrder, BCWOrderState, BCWConfig

logger = logging.getLogger(__name__)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def get_bcw_client_with_session(db: AsyncSession):
    """
    Get a BCW browser client with active session.

    Handles login if no valid session exists.
    """
    from app.services.bcw.browser_client import BCWBrowserClient
    from app.services.bcw.session_manager import BCWSessionManager

    session_mgr = BCWSessionManager(db)

    # Check for valid session
    cookies = await session_mgr.get_valid_session()

    client = BCWBrowserClient()
    await client.init()

    if cookies:
        # Restore existing session
        await client.set_cookies(cookies)
        logger.info("[BCW] Restored existing session")
    else:
        # Need to login
        creds = await session_mgr.get_credentials()
        if not creds:
            logger.error("[BCW] No credentials configured")
            await client.close()
            return None

        login_success = await client.login(creds["username"], creds["password"])
        if not login_success:
            logger.error("[BCW] Login failed")
            await client.close()
            return None

        # Save new session
        new_cookies = await client.get_cookies()
        await session_mgr.save_session(new_cookies)
        logger.info("[BCW] New session created and saved")

    return client


async def check_bcw_enabled(db: AsyncSession) -> bool:
    """Check if BCW integration is enabled and circuit breaker is closed."""
    result = await db.execute(
        select(BCWConfig).where(BCWConfig.vendor_code == "BCW")
    )
    config = result.scalar_one_or_none()

    if not config:
        logger.warning("[BCW] No BCW configuration found")
        return False

    if not config.is_enabled:
        logger.info("[BCW] BCW integration is disabled")
        return False

    if config.circuit_state == "OPEN":
        # Check if enough time has passed for half-open
        if config.circuit_opened_at:
            reset_time = config.circuit_opened_at + timedelta(seconds=30)
            if datetime.now(timezone.utc) < reset_time:
                logger.info("[BCW] Circuit breaker is OPEN, skipping")
                return False

    return True


# =============================================================================
# INVENTORY SYNC JOB
# =============================================================================

async def run_bcw_inventory_sync_job():
    """
    Sync inventory data from BCW.

    Schedule: Hourly for hot items, daily full sync.

    Metrics (TIER_0):
    - inventory_sync_duration_ms
    - sku_count
    - delta_count
    - error_count
    """
    job_name = "bcw_inventory_sync"
    logger.info(f"[{job_name}] Starting BCW inventory sync")

    async with AsyncSessionLocal() as db:
        # Check if BCW is enabled
        if not await check_bcw_enabled(db):
            return

        client = None
        try:
            client = await get_bcw_client_with_session(db)
            if not client:
                logger.error(f"[{job_name}] Could not get BCW client")
                return

            from app.services.dropship.inventory_sync import BCWInventorySyncService

            sync_service = BCWInventorySyncService(client, db)

            # Run hot items sync (hourly)
            result = await sync_service.sync_hot_items(limit=100)

            logger.info(
                f"[{job_name}] Inventory sync complete: "
                f"{result.total_checked} checked, "
                f"{result.updated_count} updated, "
                f"{result.out_of_stock_count} OOS, "
                f"{result.error_count} errors, "
                f"{result.duration_ms}ms"
            )

            # Log metrics for monitoring
            await _log_sync_metrics(db, job_name, {
                "sku_count": result.total_checked,
                "delta_count": result.updated_count,
                "oos_count": result.out_of_stock_count,
                "error_count": result.error_count,
                "duration_ms": result.duration_ms,
            })

            await db.commit()

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()

        finally:
            if client:
                await client.close()


async def run_bcw_full_inventory_sync_job():
    """
    Full inventory sync for all active BCW products.

    Schedule: Daily (off-peak hours).
    """
    job_name = "bcw_full_inventory_sync"
    logger.info(f"[{job_name}] Starting full BCW inventory sync")

    async with AsyncSessionLocal() as db:
        if not await check_bcw_enabled(db):
            return

        client = None
        try:
            client = await get_bcw_client_with_session(db)
            if not client:
                logger.error(f"[{job_name}] Could not get BCW client")
                return

            from app.services.dropship.inventory_sync import BCWInventorySyncService

            sync_service = BCWInventorySyncService(client, db)

            # Full sync with checkpointing
            result = await sync_service.sync_all_active_products(
                batch_size=50,
                checkpoint_callback=lambda current, total: logger.info(
                    f"[{job_name}] Progress: {current}/{total}"
                ),
            )

            logger.info(
                f"[{job_name}] Full sync complete: "
                f"{result.total_checked} checked, "
                f"{result.updated_count} updated, "
                f"{result.out_of_stock_count} OOS, "
                f"{result.error_count} errors, "
                f"{result.duration_ms}ms"
            )

            await db.commit()

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()

        finally:
            if client:
                await client.close()


# =============================================================================
# ORDER STATUS SYNC JOB
# =============================================================================

async def run_bcw_order_status_sync_job():
    """
    Poll BCW for order status updates.

    Schedule:
    - Every 30 min for orders pending shipment
    - Daily for shipped orders until delivered
    - Stop polling DELIVERED or CANCELLED orders

    Metrics (TIER_0):
    - orders_checked
    - status_changes
    - tracking_extracted
    """
    job_name = "bcw_order_status_sync"
    logger.info(f"[{job_name}] Starting BCW order status sync")

    async with AsyncSessionLocal() as db:
        if not await check_bcw_enabled(db):
            return

        # Find orders to poll
        # Active orders: VENDOR_SUBMITTED, BACKORDERED, SHIPPED
        # (but not shipped for > 7 days - they might be lost)
        active_states = [
            BCWOrderState.VENDOR_SUBMITTED.value,
            BCWOrderState.BACKORDERED.value,
        ]

        result = await db.execute(
            select(BCWOrder)
            .where(BCWOrder.state.in_(active_states))
            .where(BCWOrder.bcw_order_id.isnot(None))
            .order_by(BCWOrder.updated_at.asc())
            .limit(20)  # Batch size to avoid overloading BCW
        )
        orders_to_poll = result.scalars().all()

        # Also poll recently shipped orders (< 7 days)
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        result = await db.execute(
            select(BCWOrder)
            .where(BCWOrder.state == BCWOrderState.SHIPPED.value)
            .where(BCWOrder.shipped_at >= week_ago)
            .where(BCWOrder.bcw_order_id.isnot(None))
            .order_by(BCWOrder.updated_at.asc())
            .limit(10)
        )
        shipped_orders = result.scalars().all()
        orders_to_poll.extend(shipped_orders)

        if not orders_to_poll:
            logger.info(f"[{job_name}] No orders to poll")
            return

        logger.info(f"[{job_name}] Found {len(orders_to_poll)} orders to poll")

        client = None
        stats = {"checked": 0, "updated": 0, "tracking": 0, "errors": 0}

        try:
            client = await get_bcw_client_with_session(db)
            if not client:
                logger.error(f"[{job_name}] Could not get BCW client")
                return

            from app.services.bcw.status_poller import BCWStatusPoller
            from app.services.dropship.orchestrator import DropshipOrchestrator

            poller = BCWStatusPoller(client)
            orchestrator = DropshipOrchestrator(client, db)

            for order in orders_to_poll:
                try:
                    status_info = await poller.get_order_status(order.bcw_order_id)
                    stats["checked"] += 1

                    if not status_info:
                        logger.warning(
                            f"[{job_name}] Could not get status for {order.bcw_order_id}"
                        )
                        continue

                    # Check for tracking info
                    if status_info.tracking_number and not order.tracking_number:
                        await orchestrator.handle_tracking_update(
                            order.bcw_order_id,
                            status_info.tracking_number,
                            status_info.carrier,
                            status_info.tracking_url,
                        )
                        stats["tracking"] += 1
                        stats["updated"] += 1
                        logger.info(
                            f"[{job_name}] Tracking found for {order.bcw_order_id}: "
                            f"{status_info.tracking_number}"
                        )

                    # Check for delivery
                    if status_info.status == "DELIVERED" and order.state != BCWOrderState.DELIVERED:
                        await orchestrator.handle_delivery_confirmation(
                            order.bcw_order_id,
                            status_info.delivered_date,
                        )
                        stats["updated"] += 1
                        logger.info(
                            f"[{job_name}] Order {order.bcw_order_id} marked DELIVERED"
                        )

                except Exception as e:
                    logger.error(f"[{job_name}] Error polling {order.bcw_order_id}: {e}")
                    stats["errors"] += 1

            logger.info(
                f"[{job_name}] Status sync complete: "
                f"{stats['checked']} checked, "
                f"{stats['updated']} updated, "
                f"{stats['tracking']} tracking found, "
                f"{stats['errors']} errors"
            )

            await db.commit()

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()

        finally:
            if client:
                await client.close()


# =============================================================================
# EMAIL PROCESSING JOB
# =============================================================================

async def run_bcw_email_processing_job():
    """
    Process BCW notification emails.

    Parses emails for:
    - Order confirmations
    - Shipping notifications with tracking
    - Delivery confirmations

    Schedule: Every 15 minutes.
    """
    job_name = "bcw_email_processing"
    logger.info(f"[{job_name}] Starting BCW email processing")

    async with AsyncSessionLocal() as db:
        if not await check_bcw_enabled(db):
            return

        try:
            from app.services.bcw.email_parser import process_bcw_emails

            updates = await process_bcw_emails(db)

            if updates > 0:
                logger.info(f"[{job_name}] Updated {updates} orders from emails")

            await db.commit()

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()


# =============================================================================
# QUOTE CACHE CLEANUP JOB
# =============================================================================

async def run_bcw_quote_cleanup_job():
    """
    Clean up expired shipping quotes from cache.

    Schedule: Hourly.
    """
    job_name = "bcw_quote_cleanup"
    logger.info(f"[{job_name}] Starting quote cache cleanup")

    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(text("""
                DELETE FROM bcw_shipping_quotes
                WHERE expires_at <= NOW()
                RETURNING id
            """))
            deleted = len(result.fetchall())

            if deleted > 0:
                logger.info(f"[{job_name}] Deleted {deleted} expired quotes")

            await db.commit()

        except Exception as e:
            logger.error(f"[{job_name}] Job failed: {e}")
            traceback.print_exc()


# =============================================================================
# SELECTOR HEALTH CHECK JOB
# =============================================================================

async def run_bcw_selector_health_job():
    """
    Validate BCW DOM selectors are still working.

    Schedule: Daily.

    P1 alert if selectors fail 3 consecutive times.
    """
    job_name = "bcw_selector_health"
    logger.info(f"[{job_name}] Starting selector health check")

    async with AsyncSessionLocal() as db:
        if not await check_bcw_enabled(db):
            return

        client = None
        try:
            client = await get_bcw_client_with_session(db)
            if not client:
                logger.error(f"[{job_name}] Could not get BCW client")
                return

            from app.services.bcw.selectors import SELECTOR_VERSION
            from app.services.bcw.session_manager import BCWSessionManager

            session_mgr = BCWSessionManager(db)

            # Try a basic search to validate selectors
            test_result = await client.search_product("TEST123")

            # If we got here without exceptions, selectors are working
            await session_mgr.update_selector_health("HEALTHY", SELECTOR_VERSION)
            logger.info(f"[{job_name}] Selectors healthy (v{SELECTOR_VERSION})")

            await db.commit()

        except Exception as e:
            logger.error(f"[{job_name}] Selector health check failed: {e}")

            # Update health status
            from app.services.bcw.session_manager import BCWSessionManager
            session_mgr = BCWSessionManager(db)
            await session_mgr.update_selector_health("DEGRADED")
            await db.commit()

            # TODO: Check consecutive failures and send P1 alert

        finally:
            if client:
                await client.close()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def _log_sync_metrics(db: AsyncSession, job_name: str, metrics: dict):
    """Log sync metrics to database for monitoring."""
    try:
        await db.execute(text("""
            INSERT INTO pipeline_metrics (job_name, metrics, created_at)
            VALUES (:job_name, :metrics::jsonb, NOW())
        """), {
            "job_name": job_name,
            "metrics": str(metrics).replace("'", '"'),
        })
    except Exception as e:
        # Table might not exist yet, that's ok
        logger.debug(f"Could not log metrics: {e}")
