#!/usr/bin/env python3
"""
Outreach System v1.5.0 - Database Migration

This migration adds:
1. newsletter_subscribers - Email list with double opt-in
2. email_events - SendGrid webhook tracking
3. content_queue - Social content approval workflow
4. price_changelog - Price movement tracking

Run with: python -m migrations.outreach_system_v1_5_0
"""
import asyncio
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)


async def run_migration():
    """Apply Outreach System v1.5.0 schema changes."""
    engine = create_async_engine(DATABASE_URL, echo=True)

    async with engine.begin() as conn:
        print("=" * 70)
        print("OUTREACH SYSTEM v1.5.0 - MIGRATION")
        print("=" * 70)

        # -------------------------------------------------------------------
        # 1. Create newsletter_subscribers table
        # -------------------------------------------------------------------
        print("\n[1/5] Creating newsletter_subscribers table...")

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

        # Create indexes
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_newsletter_subscribers_email_hash
            ON newsletter_subscribers(email_hash)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_newsletter_subscribers_confirmation_token
            ON newsletter_subscribers(confirmation_token)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_newsletter_subscribers_unsubscribe_token
            ON newsletter_subscribers(unsubscribe_token)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_newsletter_subscribers_status
            ON newsletter_subscribers(status)
        """))

        print("    [OK] newsletter_subscribers table created")

        # -------------------------------------------------------------------
        # 2. Create email_events table
        # -------------------------------------------------------------------
        print("\n[2/5] Creating email_events table...")

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
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_email_events_event_type
            ON email_events(event_type)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_email_events_message_id
            ON email_events(message_id)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_email_events_created_at
            ON email_events(created_at)
        """))

        print("    [OK] email_events table created")

        # -------------------------------------------------------------------
        # 3. Create content_queue table
        # -------------------------------------------------------------------
        print("\n[3/5] Creating content_queue table...")

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
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_content_queue_scheduled_for
            ON content_queue(scheduled_for)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_content_queue_platform
            ON content_queue(platform)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_content_queue_content_type
            ON content_queue(content_type)
        """))

        print("    [OK] content_queue table created")

        # -------------------------------------------------------------------
        # 4. Create price_changelog table
        # -------------------------------------------------------------------
        print("\n[4/5] Creating price_changelog table...")

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
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_price_changelog_changed_at
            ON price_changelog(changed_at)
        """))
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_price_changelog_field_name
            ON price_changelog(field_name)
        """))

        print("    [OK] price_changelog table created")

        # -------------------------------------------------------------------
        # 5. Create marketing templates directory reference (no DB change)
        # -------------------------------------------------------------------
        print("\n[5/5] Verifying templates directory setup...")
        print("    [INFO] Marketing templates should be in app/templates/marketing/")
        print("    [INFO] Social templates: social/bluesky_price_winner.txt, etc.")
        print("    [INFO] Newsletter templates: newsletter/price_winners.html, etc.")

        print("\n" + "=" * 70)
        print("MIGRATION COMPLETE - OUTREACH SYSTEM v1.5.0")
        print("=" * 70)
        print("\nNew tables created:")
        print("  - newsletter_subscribers (double opt-in email list)")
        print("  - email_events (SendGrid webhook tracking)")
        print("  - content_queue (social content workflow)")
        print("  - price_changelog (price movement analytics)")
        print("\nNext steps:")
        print("  1. Configure environment variables:")
        print("     - SENDGRID_API_KEY, SENDGRID_WEBHOOK_SIGNING_KEY")
        print("     - BLUESKY_HANDLE, BLUESKY_APP_PASSWORD")
        print("     - OPENAI_API_KEY (optional, for AI content)")
        print("     - ARQ_REDIS_URL (for background jobs)")
        print("  2. Set feature flags:")
        print("     - MARKETING_NEWSLETTER_ENABLED=true")
        print("     - MARKETING_SOCIAL_ENABLED=true")
        print("  3. Create marketing templates in app/templates/marketing/")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run_migration())
