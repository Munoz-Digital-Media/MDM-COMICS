#!/usr/bin/env python3
"""
Migration: Multi-Source Enrichment System v1.10.0

Creates tables for:
- source_quotas: Rate limit quota tracking per source
- enrichment_attempts: Track enrichment attempts per entity
- grading_training_examples: AI grading training data

Also adds enrichment tracking columns to comic_issues.

Usage:
    python scripts/migrate_multi_source_enrichment.py
    python scripts/migrate_multi_source_enrichment.py --dry-run
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.core.database import AsyncSessionLocal


# Split into individual statements for asyncpg compatibility
MIGRATION_STATEMENTS = [
    # 1. SOURCE_QUOTAS TABLE
    """
    CREATE TABLE IF NOT EXISTS source_quotas (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(50) NOT NULL UNIQUE,
        requests_today INTEGER NOT NULL DEFAULT 0,
        daily_limit INTEGER NOT NULL,
        requests_per_second NUMERIC(4,2) NOT NULL,
        last_request_at TIMESTAMPTZ,
        last_reset_at TIMESTAMPTZ DEFAULT NOW(),
        is_healthy BOOLEAN NOT NULL DEFAULT TRUE,
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        last_failure_at TIMESTAMPTZ,
        last_success_at TIMESTAMPTZ,
        circuit_state VARCHAR(20) NOT NULL DEFAULT 'closed',
        circuit_opened_at TIMESTAMPTZ,
        extra_config JSONB DEFAULT '{}',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_source_quotas_name ON source_quotas(source_name)",
    "CREATE INDEX IF NOT EXISTS idx_source_quotas_health ON source_quotas(is_healthy)",

    # 2. ENRICHMENT_ATTEMPTS TABLE
    """
    CREATE TABLE IF NOT EXISTS enrichment_attempts (
        id SERIAL PRIMARY KEY,
        entity_type VARCHAR(50) NOT NULL,
        entity_id INTEGER NOT NULL,
        source_name VARCHAR(50) NOT NULL,
        attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        success BOOLEAN NOT NULL,
        error_message VARCHAR(500),
        error_type VARCHAR(100),
        data_fields_returned JSONB,
        response_time_ms INTEGER,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_enrichment_attempts_entity ON enrichment_attempts(entity_type, entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_enrichment_attempts_source ON enrichment_attempts(source_name)",
    "CREATE INDEX IF NOT EXISTS idx_enrichment_attempts_failed ON enrichment_attempts(source_name, success) WHERE success = false",
    "CREATE INDEX IF NOT EXISTS idx_enrichment_attempts_recent ON enrichment_attempts(entity_type, entity_id, source_name, attempt_at)",

    # 3. GRADING_TRAINING_EXAMPLES TABLE
    """
    CREATE TABLE IF NOT EXISTS grading_training_examples (
        id SERIAL PRIMARY KEY,
        source VARCHAR(50) NOT NULL,
        source_url VARCHAR(500),
        defects JSONB NOT NULL DEFAULT '[]'::jsonb,
        defect_severity JSONB NOT NULL DEFAULT '{}'::jsonb,
        cover_condition VARCHAR(500),
        page_quality VARCHAR(50),
        spine_condition VARCHAR(500),
        raw_description VARCHAR(2000),
        grade_numeric NUMERIC(3,1) NOT NULL,
        grade_label VARCHAR(10) NOT NULL,
        confidence NUMERIC(3,2) NOT NULL DEFAULT 0.5,
        is_validated BOOLEAN NOT NULL DEFAULT FALSE,
        validated_by VARCHAR(100),
        validated_at TIMESTAMPTZ,
        validation_notes VARCHAR(1000),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_grading_examples_grade ON grading_training_examples(grade_numeric)",
    "CREATE INDEX IF NOT EXISTS idx_grading_examples_validated ON grading_training_examples(is_validated)",
    "CREATE INDEX IF NOT EXISTS idx_grading_examples_confidence ON grading_training_examples(confidence)",
    "CREATE INDEX IF NOT EXISTS idx_grading_examples_defects ON grading_training_examples USING gin(defects)",

    # 4. ADD ENRICHMENT COLUMNS TO COMIC_ISSUES (with error handling for existing columns)
    "ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS enrichment_source VARCHAR(50)",
    "ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS enrichment_confidence NUMERIC(3,2)",
    "ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS last_enrichment_attempt TIMESTAMPTZ",
    "ALTER TABLE comic_issues ADD COLUMN IF NOT EXISTS comicvine_id INTEGER",
    "CREATE INDEX IF NOT EXISTS idx_comic_issues_comicvine_id ON comic_issues(comicvine_id)",

    # 5. SEED DEFAULT SOURCE QUOTAS
    """
    INSERT INTO source_quotas (source_name, daily_limit, requests_per_second)
    VALUES ('metron', 172800, 2.0)
    ON CONFLICT (source_name) DO NOTHING
    """,
    """
    INSERT INTO source_quotas (source_name, daily_limit, requests_per_second)
    VALUES ('comicvine', 4800, 0.055)
    ON CONFLICT (source_name) DO NOTHING
    """,
    """
    INSERT INTO source_quotas (source_name, daily_limit, requests_per_second)
    VALUES ('comicbookrealm', 43200, 0.5)
    ON CONFLICT (source_name) DO NOTHING
    """,
    """
    INSERT INTO source_quotas (source_name, daily_limit, requests_per_second)
    VALUES ('mycomicshop', 25920, 0.3)
    ON CONFLICT (source_name) DO NOTHING
    """,
    """
    INSERT INTO source_quotas (source_name, daily_limit, requests_per_second)
    VALUES ('gradingtool', 8640, 0.1)
    ON CONFLICT (source_name) DO NOTHING
    """,
]


async def run_migration(dry_run: bool = False):
    """Run the migration."""
    print("=" * 60)
    print("Migration: Multi-Source Enrichment System (v1.10.0)")
    print("=" * 60)

    if dry_run:
        print("\n[DRY RUN] Would execute these statements:\n")
        for i, stmt in enumerate(MIGRATION_STATEMENTS, 1):
            print(f"-- Statement {i}:")
            print(stmt.strip())
            print()
        return

    async with AsyncSessionLocal() as db:
        try:
            # Check current state
            result = await db.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name IN ('source_quotas', 'enrichment_attempts', 'grading_training_examples')
            """))
            existing_tables = [row[0] for row in result.fetchall()]
            print(f"\nExisting tables: {existing_tables or 'None'}")

            result = await db.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'comic_issues'
                AND column_name IN ('enrichment_source', 'enrichment_confidence', 'last_enrichment_attempt', 'comicvine_id')
            """))
            existing_columns = [row[0] for row in result.fetchall()]
            print(f"Existing comic_issues columns: {existing_columns or 'None'}")

            # Run migration statements one by one
            print("\nRunning migration...")
            for i, stmt in enumerate(MIGRATION_STATEMENTS, 1):
                try:
                    await db.execute(text(stmt))
                    await db.commit()
                    print(f"  [{i}/{len(MIGRATION_STATEMENTS)}] OK")
                except Exception as e:
                    # Log but continue on non-fatal errors (like column already exists)
                    err_str = str(e).lower()
                    if 'already exists' in err_str or 'duplicate' in err_str:
                        print(f"  [{i}/{len(MIGRATION_STATEMENTS)}] SKIP (already exists)")
                        await db.rollback()
                    else:
                        print(f"  [{i}/{len(MIGRATION_STATEMENTS)}] FAIL: {e}")
                        raise

            # Verify tables
            result = await db.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name IN ('source_quotas', 'enrichment_attempts', 'grading_training_examples')
            """))
            final_tables = [row[0] for row in result.fetchall()]
            print(f"\nFinal tables: {final_tables}")

            # Verify columns
            result = await db.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'comic_issues'
                AND column_name IN ('enrichment_source', 'enrichment_confidence', 'last_enrichment_attempt', 'comicvine_id')
            """))
            final_columns = [row[0] for row in result.fetchall()]
            print(f"Final comic_issues columns: {final_columns}")

            # Verify seed data
            result = await db.execute(text("SELECT source_name, daily_limit FROM source_quotas ORDER BY source_name"))
            quotas = [(row[0], row[1]) for row in result.fetchall()]
            print(f"\nSeeded quotas: {quotas}")

            print("\nMigration completed successfully!")

        except Exception as e:
            print(f"\nMigration failed: {e}")
            raise


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    asyncio.run(run_migration(dry_run))
