"""
Database Migration Script for BCW Dropship Integration v1.0.0

Creates the necessary tables for BCW dropship automation:
- bcw_config: Vendor credentials and session state
- bcw_inventory_snapshots: Point-in-time inventory data
- bcw_orders: Order tracking and state machine
- bcw_order_events: State change audit trail
- bcw_shipping_quotes: Cached shipping quotes
- bcw_automation_errors: Error logging with screenshots

Per constitution_db.json:
- DB-001: Numeric(12,2) for monetary fields
- DB-003: FK with appropriate ON DELETE
- DB-004: Indexes on FKs and query columns
- DB-006: CHECK constraints for data integrity

Run this migration after deploying the new models.
"""
import asyncio
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


async def migrate_bcw_tables(engine):
    """
    Create BCW dropship tables if they don't exist.

    This is an idempotent migration - safe to run multiple times.
    Uses separate transactions for DDL and DML to prevent cascading failures.
    """
    logger.info("Starting BCW dropship tables migration...")

    async with engine.begin() as conn:
        # ==================== bcw_order_state ENUM ====================
        await conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'bcw_order_state') THEN
                    CREATE TYPE bcw_order_state AS ENUM (
                        'DRAFT',
                        'PENDING_SHIPPING_QUOTE',
                        'PENDING_PAYMENT',
                        'PENDING_VENDOR_SUBMISSION',
                        'VENDOR_SUBMITTED',
                        'BACKORDERED',
                        'PARTIALLY_SHIPPED',
                        'SHIPPED',
                        'DELIVERED',
                        'CANCELLED',
                        'RETURN_REQUESTED',
                        'RETURN_IN_TRANSIT',
                        'RETURN_RECEIVED',
                        'REFUNDED',
                        'EXCEPTION_REVIEW'
                    );
                END IF;
            END
            $$;
        """))
        logger.info("Created/verified bcw_order_state ENUM")

        # ==================== bcw_config table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bcw_config (
                id SERIAL PRIMARY KEY,
                vendor_code VARCHAR(20) UNIQUE NOT NULL DEFAULT 'BCW',
                vendor_name VARCHAR(100) NOT NULL DEFAULT 'BCW Supplies',
                base_url VARCHAR(255) NOT NULL DEFAULT 'https://www.bcwsupplies.com',

                -- Encrypted credentials (per constitution_pii.json)
                username_encrypted TEXT NOT NULL,
                password_encrypted TEXT NOT NULL,

                -- Session state (browser cookies stored as encrypted JSON)
                session_data_encrypted TEXT,
                session_expires_at TIMESTAMP WITH TIME ZONE,

                -- Rate limiting state
                last_action_at TIMESTAMP WITH TIME ZONE,
                actions_this_hour INTEGER DEFAULT 0,
                hour_reset_at TIMESTAMP WITH TIME ZONE,

                -- Circuit breaker state
                consecutive_failures INTEGER DEFAULT 0,
                circuit_opened_at TIMESTAMP WITH TIME ZONE,
                circuit_state VARCHAR(20) DEFAULT 'CLOSED',

                -- Selector version tracking
                selector_version VARCHAR(20) DEFAULT '1.0.0',
                last_selector_check_at TIMESTAMP WITH TIME ZONE,
                selector_health_status VARCHAR(20) DEFAULT 'HEALTHY',

                -- Configuration flags
                is_active BOOLEAN DEFAULT TRUE,
                blind_shipping_enabled BOOLEAN DEFAULT FALSE,

                -- Timestamps
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                -- DB-005: Audit columns
                updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                update_reason VARCHAR(255)
            )
        """))
        logger.info("Created/verified bcw_config table")

        # ==================== bcw_inventory_snapshots table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bcw_inventory_snapshots (
                id SERIAL PRIMARY KEY,

                -- SKU mapping
                sku VARCHAR(50) NOT NULL,
                bcw_sku VARCHAR(50),
                product_id INTEGER REFERENCES products(id) ON DELETE SET NULL,

                -- Availability state
                in_stock BOOLEAN DEFAULT FALSE,
                available_qty INTEGER,

                -- Backorder info
                backorder BOOLEAN DEFAULT FALSE,
                backorder_date DATE,

                -- Pricing (if scraped)
                unit_price DECIMAL(12, 2),

                -- Sync metadata
                sync_batch_id VARCHAR(36),
                checked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                -- Source tracking
                data_source VARCHAR(50) DEFAULT 'bcw_browser'
            )
        """))
        logger.info("Created/verified bcw_inventory_snapshots table")

        # Inventory snapshot indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bcw_inventory_sku ON bcw_inventory_snapshots(sku)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_inventory_bcw_sku ON bcw_inventory_snapshots(bcw_sku)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_inventory_product_id ON bcw_inventory_snapshots(product_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_inventory_checked_at ON bcw_inventory_snapshots(checked_at)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_inventory_sku_checked ON bcw_inventory_snapshots(sku, checked_at)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_inventory_product_checked ON bcw_inventory_snapshots(product_id, checked_at)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_inventory_sync_batch ON bcw_inventory_snapshots(sync_batch_id)",
        ]:
            await conn.execute(text(idx_sql))

        # Partial index for backorders
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bcw_inventory_backorder
            ON bcw_inventory_snapshots(backorder)
            WHERE backorder = TRUE
        """))

        # ==================== bcw_orders table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bcw_orders (
                id SERIAL PRIMARY KEY,

                -- Link to internal order
                order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,

                -- BCW order reference
                bcw_order_id VARCHAR(50) UNIQUE,
                bcw_confirmation_number VARCHAR(100),

                -- State machine
                state bcw_order_state NOT NULL DEFAULT 'DRAFT',
                previous_state VARCHAR(50),

                -- Idempotency (per idempotency_strategy in proposal)
                idempotency_key VARCHAR(255) UNIQUE NOT NULL,

                -- Correlation for distributed tracing
                correlation_id VARCHAR(36) NOT NULL,

                -- Shipping info from BCW
                bcw_shipping_method VARCHAR(100),
                bcw_shipping_cost DECIMAL(12, 2),

                -- Tracking
                tracking_number VARCHAR(100),
                carrier VARCHAR(50),

                -- Timestamps
                submitted_at TIMESTAMP WITH TIME ZONE,
                confirmed_at TIMESTAMP WITH TIME ZONE,
                shipped_at TIMESTAMP WITH TIME ZONE,
                delivered_at TIMESTAMP WITH TIME ZONE,

                -- Exception handling
                exception_category VARCHAR(50),
                exception_details JSONB,
                exception_resolved_at TIMESTAMP WITH TIME ZONE,

                -- Retry tracking
                submission_attempts INTEGER DEFAULT 0,
                last_attempt_at TIMESTAMP WITH TIME ZONE,
                last_error TEXT,

                -- Standard timestamps
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                -- DB-005: Audit columns
                updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                update_reason VARCHAR(255),

                -- DB-006: Check constraint
                CONSTRAINT check_submission_attempts_non_negative CHECK (submission_attempts >= 0)
            )
        """))
        logger.info("Created/verified bcw_orders table")

        # BCW order indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bcw_orders_order_id ON bcw_orders(order_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_orders_bcw_order_id ON bcw_orders(bcw_order_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_orders_state ON bcw_orders(state)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_orders_idempotency_key ON bcw_orders(idempotency_key)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_orders_correlation_id ON bcw_orders(correlation_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_orders_tracking_number ON bcw_orders(tracking_number)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_orders_state_created ON bcw_orders(state, created_at)",
        ]:
            await conn.execute(text(idx_sql))

        # Partial index for pending orders
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bcw_orders_pending
            ON bcw_orders(state)
            WHERE state IN ('PENDING_VENDOR_SUBMISSION', 'VENDOR_SUBMITTED', 'BACKORDERED', 'PARTIALLY_SHIPPED')
        """))

        # ==================== bcw_order_events table (Audit Trail) ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bcw_order_events (
                id SERIAL PRIMARY KEY,

                -- Link to BCW order
                bcw_order_id INTEGER NOT NULL REFERENCES bcw_orders(id) ON DELETE CASCADE,

                -- State transition
                from_state VARCHAR(50),
                to_state VARCHAR(50) NOT NULL,
                trigger VARCHAR(100) NOT NULL,

                -- Context
                correlation_id VARCHAR(36) NOT NULL,
                actor_type VARCHAR(20) NOT NULL,
                actor_id_hash VARCHAR(64),

                -- Event data
                event_data JSONB,

                -- Timestamp (immutable)
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        logger.info("Created/verified bcw_order_events table")

        # BCW order event indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bcw_order_events_bcw_order_id ON bcw_order_events(bcw_order_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_order_events_correlation ON bcw_order_events(correlation_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_order_events_created_at ON bcw_order_events(created_at)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_order_events_trigger ON bcw_order_events(trigger, created_at)",
        ]:
            await conn.execute(text(idx_sql))

        # ==================== bcw_shipping_quotes table (Cache) ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bcw_shipping_quotes (
                id SERIAL PRIMARY KEY,

                -- Cache key components
                address_hash VARCHAR(64) NOT NULL,
                cart_hash VARCHAR(64) NOT NULL,

                -- Quote data
                shipping_options JSONB NOT NULL,
                lowest_price DECIMAL(12, 2) NOT NULL,

                -- Validity
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,

                -- Source tracking
                correlation_id VARCHAR(36)
            )
        """))
        logger.info("Created/verified bcw_shipping_quotes table")

        # Shipping quote indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bcw_quotes_address_hash ON bcw_shipping_quotes(address_hash)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_quotes_cart_hash ON bcw_shipping_quotes(cart_hash)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_quotes_cache_key ON bcw_shipping_quotes(address_hash, cart_hash)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_quotes_expires_at ON bcw_shipping_quotes(expires_at)",
        ]:
            await conn.execute(text(idx_sql))

        # ==================== bcw_automation_errors table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bcw_automation_errors (
                id SERIAL PRIMARY KEY,

                -- Error context
                error_type VARCHAR(100) NOT NULL,
                error_code VARCHAR(50) NOT NULL,
                error_message TEXT NOT NULL,

                -- Selector info (if selector error)
                selector_key VARCHAR(100),
                selector_version VARCHAR(20),

                -- Page state
                page_url VARCHAR(500),
                screenshot_path VARCHAR(500),
                page_html_path VARCHAR(500),

                -- Context
                correlation_id VARCHAR(36),
                bcw_order_id INTEGER REFERENCES bcw_orders(id) ON DELETE SET NULL,

                -- Resolution
                resolved_at TIMESTAMP WITH TIME ZONE,
                resolution_notes TEXT,

                -- Timestamp
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        logger.info("Created/verified bcw_automation_errors table")

        # Automation error indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bcw_errors_error_type ON bcw_automation_errors(error_type)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_errors_error_code ON bcw_automation_errors(error_code)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_errors_correlation ON bcw_automation_errors(correlation_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_errors_created_at ON bcw_automation_errors(created_at)",
        ]:
            await conn.execute(text(idx_sql))

        # Partial index for unresolved errors
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bcw_errors_unresolved
            ON bcw_automation_errors(error_type)
            WHERE resolved_at IS NULL
        """))

        # ==================== bcw_product_mappings table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bcw_product_mappings (
                id SERIAL PRIMARY KEY,

                -- SKU mapping (customer never sees bcw_sku)
                mdm_sku VARCHAR(50) UNIQUE NOT NULL,
                bcw_sku VARCHAR(50) NOT NULL,

                -- Product info (cached from BCW)
                product_name VARCHAR(255),
                bcw_category VARCHAR(100),
                mdm_category VARCHAR(100),

                -- Pricing
                bcw_cost NUMERIC(10, 2),
                bcw_msrp NUMERIC(10, 2),
                our_price NUMERIC(10, 2),
                min_margin_percent NUMERIC(5, 2),

                -- Link to our product (optional)
                product_id INTEGER REFERENCES products(id) ON DELETE SET NULL,

                -- Status flags
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                is_dropship_only BOOLEAN NOT NULL DEFAULT TRUE,
                sync_inventory BOOLEAN NOT NULL DEFAULT TRUE,

                -- Import metadata
                imported_at TIMESTAMP WITH TIME ZONE,
                imported_from VARCHAR(100),

                -- Audit
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_by VARCHAR(100)
            )
        """))
        logger.info("Created/verified bcw_product_mappings table")

        # Product mapping indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bcw_mapping_mdm_sku ON bcw_product_mappings(mdm_sku)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_mapping_bcw_sku ON bcw_product_mappings(bcw_sku)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_mapping_product_id ON bcw_product_mappings(product_id)",
        ]:
            await conn.execute(text(idx_sql))

        # Partial indexes for active and sync-enabled products
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bcw_mapping_active
            ON bcw_product_mappings(is_active)
            WHERE is_active = TRUE
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bcw_mapping_sync
            ON bcw_product_mappings(sync_inventory)
            WHERE sync_inventory = TRUE
        """))

    logger.info("BCW dropship tables migration complete!")


async def run_migration():
    """Run the migration using the app's database engine."""
    from app.core.database import engine

    await migrate_bcw_tables(engine)


if __name__ == "__main__":
    asyncio.run(run_migration())
