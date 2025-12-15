#!/usr/bin/env python3
"""
Migration script for Match Review Queue tables
Run this script locally to apply migrations to Railway database.

Usage:
    python scripts/migrate_match_review_v1.py --env development
    python scripts/migrate_match_review_v1.py --env production --dry-run
"""

import argparse
import sys

# Railway database URLs
RAILWAY_URLS = {
    "development": "postgresql://postgres:figmwrDYQFzYjGYItivpQewcNKNfzXWv@gondola.proxy.rlwy.net:38453/railway",
    "production": "postgresql://postgres:figmwrDYQFzYjGYItivpQewcNKNfzXWv@gondola.proxy.rlwy.net:38453/railway",
}

MIGRATION_SQL = """
-- ============================================================
-- Match Review Queue Migration v1
-- Creates tables for Match Review feature
-- ============================================================

-- Check if table exists first
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = 'match_review_queue') THEN

        -- ============================================================
        -- match_review_queue: Pending matches for human review
        -- ============================================================
        CREATE TABLE match_review_queue (
            id SERIAL PRIMARY KEY,

            -- Source record
            entity_type VARCHAR(20) NOT NULL,
            entity_id INTEGER NOT NULL,

            -- Match candidate
            candidate_source VARCHAR(50) NOT NULL,
            candidate_id VARCHAR(100) NOT NULL,
            candidate_name VARCHAR(500),
            candidate_data JSONB,

            -- Matching metadata
            match_method VARCHAR(50) NOT NULL,
            match_score INTEGER,
            match_details JSONB,

            -- Queue status
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            is_escalated BOOLEAN DEFAULT FALSE,

            -- Resolution
            reviewed_by INTEGER REFERENCES users(id),
            reviewed_at TIMESTAMP WITH TIME ZONE,
            resolution_notes TEXT,

            -- Optimistic locking
            locked_by INTEGER REFERENCES users(id),
            locked_at TIMESTAMP WITH TIME ZONE,

            -- Timestamps
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            expires_at TIMESTAMP WITH TIME ZONE,

            -- Constraints
            CONSTRAINT ck_entity_type CHECK (entity_type IN ('comic', 'funko', 'cover_ingestion', 'cover_upload')),
            CONSTRAINT ck_status CHECK (status IN ('pending', 'approved', 'rejected', 'skipped', 'expired')),
            CONSTRAINT uq_match_candidate UNIQUE (entity_type, entity_id, candidate_source, candidate_id)
        );

        -- Indexes
        CREATE INDEX idx_match_queue_status ON match_review_queue(status) WHERE status = 'pending';
        CREATE INDEX idx_match_queue_entity ON match_review_queue(entity_type, entity_id);
        CREATE INDEX idx_match_queue_escalated ON match_review_queue(is_escalated, created_at) WHERE status = 'pending';
        CREATE INDEX idx_match_queue_candidate_source ON match_review_queue(candidate_source);

        RAISE NOTICE 'Created match_review_queue table';
    ELSE
        RAISE NOTICE 'match_review_queue already exists';
    END IF;
END $$;

-- ============================================================
-- match_audit_log: Immutable audit trail (hash-chained)
-- ============================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = 'match_audit_log') THEN
        CREATE TABLE match_audit_log (
            id SERIAL PRIMARY KEY,

            -- Action details
            action VARCHAR(50) NOT NULL,
            entity_type VARCHAR(20) NOT NULL,
            entity_id INTEGER NOT NULL,

            -- Before/after state (hashed)
            before_state_hash VARCHAR(128),
            after_state_hash VARCHAR(128),

            -- Actor (pseudonymized)
            actor_type VARCHAR(20) NOT NULL,
            actor_id_hash VARCHAR(128),

            -- Match details
            match_source VARCHAR(50),
            match_id VARCHAR(100),
            match_method VARCHAR(50),
            match_score INTEGER,

            -- Immutability - hash chain
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            log_hash VARCHAR(128),
            previous_hash VARCHAR(128)
        );

        CREATE INDEX idx_audit_log_entity ON match_audit_log(entity_type, entity_id);
        CREATE INDEX idx_audit_log_created ON match_audit_log(created_at);

        RAISE NOTICE 'Created match_audit_log table';
    ELSE
        RAISE NOTICE 'match_audit_log already exists';
    END IF;
END $$;

-- ============================================================
-- isbn_sources: Track ISBN provenance from multiple sources
-- ============================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = 'isbn_sources') THEN
        CREATE TABLE isbn_sources (
            id SERIAL PRIMARY KEY,
            comic_issue_id INTEGER NOT NULL REFERENCES comic_issues(id) ON DELETE CASCADE,

            source_name VARCHAR(50) NOT NULL,
            source_id VARCHAR(100),

            isbn_raw VARCHAR(50),
            isbn_normalized VARCHAR(13),

            confidence NUMERIC(3, 2) DEFAULT 1.00,
            fetched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

            CONSTRAINT uq_isbn_source UNIQUE (comic_issue_id, source_name)
        );

        CREATE INDEX idx_isbn_sources_normalized ON isbn_sources(isbn_normalized) WHERE isbn_normalized IS NOT NULL;

        RAISE NOTICE 'Created isbn_sources table';
    ELSE
        RAISE NOTICE 'isbn_sources already exists';
    END IF;
END $$;

-- ============================================================
-- Add match tracking columns to comic_issues (if not exists)
-- ============================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'comic_issues' AND column_name = 'pricecharting_match_method') THEN
        ALTER TABLE comic_issues ADD COLUMN pricecharting_match_method VARCHAR(50);
        ALTER TABLE comic_issues ADD COLUMN pricecharting_match_score INTEGER;
        ALTER TABLE comic_issues ADD COLUMN pricecharting_matched_at TIMESTAMP WITH TIME ZONE;
        ALTER TABLE comic_issues ADD COLUMN pricecharting_matched_by INTEGER REFERENCES users(id);
        RAISE NOTICE 'Added pricecharting match columns to comic_issues';
    ELSE
        RAISE NOTICE 'pricecharting match columns already exist on comic_issues';
    END IF;
END $$;

-- ============================================================
-- Add match tracking columns to funkos (if not exists)
-- ============================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'funkos' AND column_name = 'pricecharting_match_method') THEN
        ALTER TABLE funkos ADD COLUMN pricecharting_match_method VARCHAR(50);
        ALTER TABLE funkos ADD COLUMN pricecharting_match_score INTEGER;
        ALTER TABLE funkos ADD COLUMN pricecharting_matched_at TIMESTAMP WITH TIME ZONE;
        ALTER TABLE funkos ADD COLUMN pricecharting_matched_by INTEGER REFERENCES users(id);
        RAISE NOTICE 'Added pricecharting match columns to funkos';
    ELSE
        RAISE NOTICE 'pricecharting match columns already exist on funkos';
    END IF;
END $$;

-- Done
SELECT 'Migration complete' as status;
"""


def run_migration(database_url: str, dry_run: bool = False):
    """Run the migration SQL against the database."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        print("ERROR: sqlalchemy not installed. Run: pip install sqlalchemy psycopg2-binary")
        sys.exit(1)

    print(f"Connecting to database...")
    print(f"URL: {database_url[:50]}...")

    engine = create_engine(database_url)

    if dry_run:
        print("\n=== DRY RUN - SQL that would be executed ===")
        print(MIGRATION_SQL)
        print("=== END DRY RUN ===")
        return

    print("\nRunning migration...")

    with engine.connect() as conn:
        # Execute the migration
        result = conn.execute(text(MIGRATION_SQL))
        conn.commit()

        # Verify tables were created
        verify = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN ('match_review_queue', 'match_audit_log', 'isbn_sources')
            ORDER BY table_name
        """))

        tables = [row[0] for row in verify]

        print("\n=== Migration Results ===")
        print(f"Tables present: {', '.join(tables)}")

        # Check row counts
        for table in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()[0]
            print(f"  {table}: {count} rows")

    print("\n✓ Migration complete!")


def main():
    parser = argparse.ArgumentParser(description="Run Match Review migration on Railway")
    parser.add_argument("--env", choices=["development", "production"], required=True,
                       help="Target environment")
    parser.add_argument("--dry-run", action="store_true",
                       help="Print SQL without executing")

    args = parser.parse_args()

    database_url = RAILWAY_URLS.get(args.env)
    if not database_url:
        print(f"ERROR: No database URL configured for {args.env}")
        sys.exit(1)

    print(f"=== Match Review Migration ===")
    print(f"Environment: {args.env}")
    print(f"Dry run: {args.dry_run}")

    run_migration(database_url, args.dry_run)


if __name__ == "__main__":
    main()
