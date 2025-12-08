"""
Database Migration Script for UPS Shipping Integration v1.28.0

Creates the necessary tables for shipping functionality:
- addresses: Normalized address storage with encrypted PII
- carriers: Carrier configuration and API credentials
- shipments: Shipment records with tracking
- shipment_rates: Rate quotes with TTL
- tracking_events: Tracking history

Run this migration after deploying the new models.
"""
import asyncio
import logging
from datetime import datetime, timezone
from sqlalchemy import text

logger = logging.getLogger(__name__)


async def migrate_shipping_tables(engine):
    """
    Create shipping tables if they don't exist.

    This is an idempotent migration - safe to run multiple times.
    Uses separate transactions for DDL and DML to prevent cascading failures.
    """
    logger.info("Starting shipping tables migration...")

    # Transaction 1: Create all tables (DDL)
    async with engine.begin() as conn:
        # ==================== addresses table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS addresses (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                address_type VARCHAR(20) NOT NULL DEFAULT 'shipping',
                recipient_name_encrypted TEXT,
                company_name_encrypted TEXT,
                phone_hash VARCHAR(100),
                phone_encrypted TEXT,
                phone_last4 VARCHAR(4),
                email_encrypted TEXT,
                address_line1_encrypted TEXT NOT NULL,
                address_line2_encrypted TEXT,
                address_line3_encrypted TEXT,
                city VARCHAR(100) NOT NULL,
                state_province VARCHAR(50) NOT NULL,
                postal_code VARCHAR(20) NOT NULL,
                country_code VARCHAR(2) NOT NULL DEFAULT 'US',
                residential BOOLEAN DEFAULT TRUE,
                validation_status VARCHAR(20) DEFAULT 'pending',
                validated_at TIMESTAMP WITH TIME ZONE,
                validation_messages TEXT,
                is_default BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                deleted_at TIMESTAMP WITH TIME ZONE
            )
        """))
        logger.info("Created/verified addresses table")

        # Address indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_addresses_user_id ON addresses(user_id)",
            "CREATE INDEX IF NOT EXISTS ix_addresses_phone_hash ON addresses(phone_hash)",
            "CREATE INDEX IF NOT EXISTS ix_addresses_validation_status ON addresses(validation_status)",
            "CREATE INDEX IF NOT EXISTS ix_addresses_postal_code ON addresses(postal_code)",
        ]:
            await conn.execute(text(idx_sql))

        # ==================== carriers table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS carriers (
                id SERIAL PRIMARY KEY,
                code VARCHAR(20) NOT NULL UNIQUE,
                name VARCHAR(100) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                client_id_encrypted TEXT,
                client_secret_encrypted TEXT,
                account_number_encrypted TEXT,
                use_sandbox BOOLEAN DEFAULT FALSE,
                origin_name VARCHAR(100),
                origin_address_line1 VARCHAR(100),
                origin_city VARCHAR(100),
                origin_state_province VARCHAR(50),
                origin_postal_code VARCHAR(20),
                origin_country_code VARCHAR(2) DEFAULT 'US',
                origin_phone VARCHAR(20),
                service_levels JSONB,
                default_package_type VARCHAR(10),
                markup_percentage DECIMAL(5,2) DEFAULT 0.00,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        logger.info("Created/verified carriers table")

        # ==================== shipments table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS shipments (
                id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES orders(id) NOT NULL,
                carrier_id INTEGER REFERENCES carriers(id),
                destination_address_id INTEGER REFERENCES addresses(id),
                tracking_number VARCHAR(50),
                tracking_url TEXT,
                ups_shipment_id VARCHAR(100),
                service_code VARCHAR(20) NOT NULL,
                service_name VARCHAR(100),
                status VARCHAR(30) NOT NULL DEFAULT 'draft',
                status_detail TEXT,
                weight DECIMAL(10,2) NOT NULL DEFAULT 0,
                package_count INTEGER NOT NULL DEFAULT 1,
                dimensions JSONB,
                declared_value DECIMAL(10,2),
                shipping_cost DECIMAL(10,2),
                carrier_cost DECIMAL(10,2),
                label_data TEXT,
                label_format VARCHAR(10),
                label_created_at TIMESTAMP WITH TIME ZONE,
                signature_required BOOLEAN DEFAULT FALSE,
                delivery_confirmation TEXT,
                estimated_delivery_date TIMESTAMP WITH TIME ZONE,
                actual_delivery_date TIMESTAMP WITH TIME ZONE,
                tracking_events JSONB,
                last_tracking_update TIMESTAMP WITH TIME ZONE,
                voided_at TIMESTAMP WITH TIME ZONE,
                void_reason TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        logger.info("Created/verified shipments table")

        # Shipment indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_shipments_order_id ON shipments(order_id)",
            "CREATE INDEX IF NOT EXISTS ix_shipments_tracking_number ON shipments(tracking_number)",
            "CREATE INDEX IF NOT EXISTS ix_shipments_status ON shipments(status)",
            "CREATE INDEX IF NOT EXISTS ix_shipments_carrier_id ON shipments(carrier_id)",
            "CREATE INDEX IF NOT EXISTS ix_shipments_created_at ON shipments(created_at)",
        ]:
            await conn.execute(text(idx_sql))

        # ==================== shipment_rates table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS shipment_rates (
                id SERIAL PRIMARY KEY,
                quote_id VARCHAR(100) NOT NULL UNIQUE,
                order_id INTEGER REFERENCES orders(id),
                destination_address_id INTEGER REFERENCES addresses(id),
                carrier_id INTEGER REFERENCES carriers(id),
                service_code VARCHAR(20) NOT NULL,
                service_name VARCHAR(100),
                total_rate DECIMAL(10,2) NOT NULL,
                carrier_rate DECIMAL(10,2),
                markup_amount DECIMAL(10,2),
                currency VARCHAR(3) DEFAULT 'USD',
                estimated_delivery_date TIMESTAMP WITH TIME ZONE,
                estimated_transit_days INTEGER,
                guaranteed_delivery BOOLEAN DEFAULT FALSE,
                rate_details JSONB,
                selected BOOLEAN DEFAULT FALSE,
                selected_at TIMESTAMP WITH TIME ZONE,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        logger.info("Created/verified shipment_rates table")

        # Rate indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_shipment_rates_quote_id ON shipment_rates(quote_id)",
            "CREATE INDEX IF NOT EXISTS ix_shipment_rates_order_id ON shipment_rates(order_id)",
            "CREATE INDEX IF NOT EXISTS ix_shipment_rates_expires_at ON shipment_rates(expires_at)",
        ]:
            await conn.execute(text(idx_sql))

        # ==================== Add FK to orders table ====================
        try:
            await conn.execute(text("""
                ALTER TABLE orders
                ADD COLUMN IF NOT EXISTS normalized_address_id INTEGER REFERENCES addresses(id)
            """))
            logger.info("Added normalized_address_id to orders table")
        except Exception as e:
            logger.debug(f"Column may already exist: {e}")

    logger.info("Shipping tables DDL migration complete!")

    # Transaction 2: Insert seed data (DML) - separate transaction to prevent DDL rollback on failure
    try:
        async with engine.begin() as conn:
            # ==================== Insert default UPS carrier if not exists ====================
            result = await conn.execute(text("""
                SELECT id FROM carriers WHERE code = 'UPS'
            """))
            if result.fetchone() is None:
                await conn.execute(text("""
                    INSERT INTO carriers (code, name, display_name, is_active, service_levels, default_package_type)
                    VALUES (
                        'UPS',
                        'United Parcel Service',
                        'UPS',
                        FALSE,
                        '["03", "02", "01", "13", "14"]',
                        '02'
                    )
                """))
                logger.info("Created default UPS carrier record (inactive - configure credentials)")
    except Exception as e:
        # DML failures shouldn't block startup - carrier may already exist with different schema
        logger.warning(f"Could not seed UPS carrier (may already exist or schema mismatch): {e}")

    logger.info("Shipping tables migration complete!")


async def run_migration():
    """Run the migration using the app's database engine."""
    from app.core.database import engine

    await migrate_shipping_tables(engine)


if __name__ == "__main__":
    asyncio.run(run_migration())
