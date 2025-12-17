"""
Database Migration Script for BCW Refund Request Module v1.0.0

Creates the necessary tables for BCW supply refunds:
- refund_eligibility_policies: Product type refund rules
- bcw_refund_requests: Refund tracking with vendor credit blocking
- bcw_refund_events: Immutable audit trail with hash chain

BUSINESS RULE: Only BCW Supplies are refundable. Collectibles are FINAL SALE.

Per constitution_db.json:
- DB-001: Numeric(12,2) for monetary fields
- DB-003: FK with appropriate ON DELETE
- DB-004: Indexes on FKs and query columns
- DB-005: Audit columns
- DB-006: CHECK constraints for data integrity

Per constitution_pii.json:
- SHA-512 hash for actor identification in audit trail

Run this migration after deploying the new models.
"""
import asyncio
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


async def migrate_refund_tables(engine):
    """
    Create BCW refund tables if they don't exist.

    This is an idempotent migration - safe to run multiple times.
    """
    logger.info("Starting BCW refund tables migration...")

    async with engine.begin() as conn:
        # ==================== bcw_refund_state ENUM ====================
        await conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'bcw_refund_state') THEN
                    CREATE TYPE bcw_refund_state AS ENUM (
                        'REQUESTED',
                        'UNDER_REVIEW',
                        'APPROVED',
                        'DENIED',
                        'VENDOR_RETURN_INITIATED',
                        'VENDOR_RETURN_IN_TRANSIT',
                        'VENDOR_RETURN_RECEIVED',
                        'VENDOR_CREDIT_PENDING',
                        'VENDOR_CREDIT_RECEIVED',
                        'CUSTOMER_REFUND_PROCESSING',
                        'CUSTOMER_REFUND_ISSUED',
                        'COMPLETED',
                        'CANCELLED',
                        'EXCEPTION'
                    );
                END IF;
            END
            $$;
        """))
        logger.info("Created/verified bcw_refund_state ENUM")

        # ==================== refund_eligibility_policies table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS refund_eligibility_policies (
                id SERIAL PRIMARY KEY,

                -- Policy identification
                product_type VARCHAR(50) UNIQUE NOT NULL,

                -- Eligibility
                is_refundable BOOLEAN NOT NULL DEFAULT FALSE,
                requires_vendor_approval BOOLEAN NOT NULL DEFAULT TRUE,

                -- Time limits
                return_window_days INTEGER,
                restocking_fee_percent NUMERIC(5, 2) DEFAULT 0,

                -- Legal text
                policy_summary VARCHAR(500) NOT NULL,
                full_policy_text TEXT NOT NULL,

                -- Display
                display_on_product_page BOOLEAN NOT NULL DEFAULT TRUE,
                display_on_checkout BOOLEAN NOT NULL DEFAULT TRUE,

                -- Timestamps
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                -- DB-005: Audit columns
                updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                update_reason VARCHAR(255)
            )
        """))
        logger.info("Created/verified refund_eligibility_policies table")

        # Policy indexes
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_refund_policies_product_type
            ON refund_eligibility_policies(product_type)
        """))

        # ==================== bcw_refund_requests table ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bcw_refund_requests (
                id SERIAL PRIMARY KEY,

                -- Request identification
                refund_number VARCHAR(50) UNIQUE NOT NULL,
                idempotency_key VARCHAR(255) UNIQUE NOT NULL,
                correlation_id VARCHAR(36) NOT NULL,

                -- Relationships
                order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
                bcw_order_id INTEGER REFERENCES bcw_orders(id) ON DELETE SET NULL,
                user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,

                -- State machine
                state bcw_refund_state NOT NULL DEFAULT 'REQUESTED',
                previous_state VARCHAR(50),

                -- Request details
                reason_code VARCHAR(50) NOT NULL,
                reason_description TEXT,

                -- Items being refunded (JSON array)
                refund_items JSONB NOT NULL,

                -- Monetary values (DB-001: Numeric(12,2))
                original_amount NUMERIC(12, 2) NOT NULL,
                restocking_fee NUMERIC(12, 2) DEFAULT 0,
                refund_amount NUMERIC(12, 2) NOT NULL,

                -- Vendor credit tracking (BLOCKING GATE)
                vendor_credit_amount NUMERIC(12, 2),
                vendor_credit_reference VARCHAR(100),
                vendor_credit_received_at TIMESTAMP WITH TIME ZONE,

                -- Customer refund tracking (GATED)
                stripe_refund_id VARCHAR(100),
                customer_refund_issued_at TIMESTAMP WITH TIME ZONE,

                -- Return shipping
                return_tracking_number VARCHAR(100),
                return_carrier VARCHAR(50),
                return_label_url VARCHAR(500),

                -- Review
                reviewed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                reviewed_at TIMESTAMP WITH TIME ZONE,
                denial_reason TEXT,

                -- Exception handling
                exception_category VARCHAR(50),
                exception_details JSONB,

                -- Timestamps
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                -- DB-005: Audit columns
                updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                update_reason VARCHAR(255),

                -- DB-006: Check constraints
                CONSTRAINT check_refund_amount_non_negative CHECK (refund_amount >= 0),
                CONSTRAINT check_restocking_fee_non_negative CHECK (restocking_fee >= 0),
                CONSTRAINT check_original_amount_non_negative CHECK (original_amount >= 0)
            )
        """))
        logger.info("Created/verified bcw_refund_requests table")

        # Refund request indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bcw_refunds_refund_number ON bcw_refund_requests(refund_number)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refunds_idempotency_key ON bcw_refund_requests(idempotency_key)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refunds_correlation_id ON bcw_refund_requests(correlation_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refunds_order_id ON bcw_refund_requests(order_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refunds_bcw_order_id ON bcw_refund_requests(bcw_order_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refunds_user_id ON bcw_refund_requests(user_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refunds_state ON bcw_refund_requests(state)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refunds_created_at ON bcw_refund_requests(created_at)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refunds_state_created ON bcw_refund_requests(state, created_at)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refunds_user_created ON bcw_refund_requests(user_id, created_at)",
        ]:
            await conn.execute(text(idx_sql))

        # Partial index for pending vendor actions
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bcw_refunds_pending_vendor
            ON bcw_refund_requests(state)
            WHERE state IN (
                'VENDOR_RETURN_INITIATED',
                'VENDOR_RETURN_IN_TRANSIT',
                'VENDOR_CREDIT_PENDING'
            )
        """))

        # ==================== bcw_refund_events table (Audit Trail) ====================
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bcw_refund_events (
                id SERIAL PRIMARY KEY,

                -- Link to refund request
                refund_request_id INTEGER NOT NULL REFERENCES bcw_refund_requests(id) ON DELETE CASCADE,

                -- State transition
                from_state VARCHAR(50),
                to_state VARCHAR(50) NOT NULL,
                trigger VARCHAR(100) NOT NULL,

                -- Correlation
                correlation_id VARCHAR(36) NOT NULL,

                -- Actor identification (PII-compliant: SHA-512 hash)
                actor_type VARCHAR(20) NOT NULL,
                actor_id_hash VARCHAR(128),

                -- Event data (no PII)
                event_data JSONB,

                -- Hash chain for tamper evidence
                prev_event_hash VARCHAR(128),
                event_hash VARCHAR(128) NOT NULL,

                -- Timestamp (immutable)
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        logger.info("Created/verified bcw_refund_events table")

        # Refund event indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS ix_bcw_refund_events_request_id ON bcw_refund_events(refund_request_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refund_events_correlation ON bcw_refund_events(correlation_id)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refund_events_created_at ON bcw_refund_events(created_at)",
            "CREATE INDEX IF NOT EXISTS ix_bcw_refund_events_trigger ON bcw_refund_events(trigger, created_at)",
        ]:
            await conn.execute(text(idx_sql))

    logger.info("BCW refund tables migration complete!")


async def seed_refund_policies(engine):
    """
    Seed refund eligibility policies.

    Per business decision:
    - BCW Supplies: Refundable (30 days, 15% restocking)
    - Comics, Funkos, Graded: FINAL SALE (not refundable)
    """
    logger.info("Seeding refund eligibility policies...")

    policies = [
        {
            "product_type": "bcw_supply",
            "is_refundable": True,
            "requires_vendor_approval": True,
            "return_window_days": 30,
            "restocking_fee_percent": 15.00,
            "policy_summary": "30-day returns; 15% restocking fee; refund after vendor credit confirmation",
            "full_policy_text": """BCW supply products (bags, boards, boxes, toploaders, etc.) may be returned within 30 days of delivery for a refund, subject to the following conditions:

1. Items must be unopened and in original packaging
2. A 15% restocking fee applies to all returns
3. Customer is responsible for return shipping costs
4. Refunds are processed after vendor credit confirmation (typically 5-10 business days after return receipt)

To initiate a return, please contact customer service with your order number and reason for return.""",
            "display_on_product_page": True,
            "display_on_checkout": True,
        },
        {
            "product_type": "comic",
            "is_refundable": False,
            "requires_vendor_approval": False,
            "return_window_days": None,
            "restocking_fee_percent": 0,
            "policy_summary": "ALL SALES FINAL - Collectible item not eligible for returns",
            "full_policy_text": """Due to the condition-sensitive nature of comic books as collectibles, ALL SALES ARE FINAL.

We carefully grade and photograph all comics before listing. Please review all product photos, descriptions, and condition notes before purchasing.

If you receive a comic that is significantly different from its description, please contact us within 48 hours of delivery for resolution options. Claims for shipping damage must include photos and be reported within 48 hours.""",
            "display_on_product_page": True,
            "display_on_checkout": True,
        },
        {
            "product_type": "funko",
            "is_refundable": False,
            "requires_vendor_approval": False,
            "return_window_days": None,
            "restocking_fee_percent": 0,
            "policy_summary": "ALL SALES FINAL - Collectible item not eligible for returns",
            "full_policy_text": """Due to the collectible nature of Funko Pop! figures and their condition-based value, ALL SALES ARE FINAL.

Box condition is carefully noted in listings. Please review all product photos and descriptions before purchasing.

If you receive an item that is significantly different from its description, please contact us within 48 hours of delivery for resolution options. Claims for shipping damage must include photos and be reported within 48 hours.""",
            "display_on_product_page": True,
            "display_on_checkout": True,
        },
        {
            "product_type": "graded",
            "is_refundable": False,
            "requires_vendor_approval": False,
            "return_window_days": None,
            "restocking_fee_percent": 0,
            "policy_summary": "ALL SALES FINAL - Professionally graded collectible",
            "full_policy_text": """Professionally graded items (CGC, CBCS, PSA, etc.) have been evaluated by third-party grading services. ALL SALES ARE FINAL.

The grade assigned by the grading company is the definitive assessment of the item's condition. We do not accept returns or offer refunds for graded items.

If the case is damaged during shipping, please contact us within 48 hours of delivery with photos for resolution options.""",
            "display_on_product_page": True,
            "display_on_checkout": True,
        },
    ]

    async with engine.begin() as conn:
        for policy in policies:
            # Use upsert pattern for idempotency
            await conn.execute(text("""
                INSERT INTO refund_eligibility_policies (
                    product_type,
                    is_refundable,
                    requires_vendor_approval,
                    return_window_days,
                    restocking_fee_percent,
                    policy_summary,
                    full_policy_text,
                    display_on_product_page,
                    display_on_checkout
                ) VALUES (
                    :product_type,
                    :is_refundable,
                    :requires_vendor_approval,
                    :return_window_days,
                    :restocking_fee_percent,
                    :policy_summary,
                    :full_policy_text,
                    :display_on_product_page,
                    :display_on_checkout
                )
                ON CONFLICT (product_type) DO UPDATE SET
                    is_refundable = EXCLUDED.is_refundable,
                    requires_vendor_approval = EXCLUDED.requires_vendor_approval,
                    return_window_days = EXCLUDED.return_window_days,
                    restocking_fee_percent = EXCLUDED.restocking_fee_percent,
                    policy_summary = EXCLUDED.policy_summary,
                    full_policy_text = EXCLUDED.full_policy_text,
                    display_on_product_page = EXCLUDED.display_on_product_page,
                    display_on_checkout = EXCLUDED.display_on_checkout,
                    updated_at = NOW()
            """), policy)

    logger.info(f"Seeded {len(policies)} refund eligibility policies")


async def run_migration():
    """Run the migration using the app's database engine."""
    from app.core.database import engine

    await migrate_refund_tables(engine)
    await seed_refund_policies(engine)


if __name__ == "__main__":
    asyncio.run(run_migration())
