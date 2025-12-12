#!/usr/bin/env python3
"""
Check ML/Newsletter/Social Data Pipeline Status

Verifies that pricing data is being properly persisted for:
- ML pricing model training
- Social media content generation
- Newsletter engine

Run: railway run python scripts/check_ml_data.py
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from app.core.database import AsyncSessionLocal


async def check_database():
    print("=" * 60)
    print("ML/Newsletter/Social Data Pipeline Status Check")
    print("=" * 60)
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print()

    async with AsyncSessionLocal() as db:
        # 1. Check required tables exist
        print("[1] Checking required tables...")
        result = await db.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN (
                'price_snapshots',
                'price_changelog',
                'pipeline_checkpoints',
                'dead_letter_queue',
                'funkos',
                'newsletter_subscribers',
                'content_queue'
            )
            ORDER BY table_name
        """))
        tables = [r[0] for r in result.fetchall()]
        print(f"    Tables found: {tables}")

        missing = {'price_snapshots', 'price_changelog', 'pipeline_checkpoints', 'funkos'} - set(tables)
        if missing:
            print(f"    ⚠️  MISSING TABLES: {missing}")
        else:
            print("    ✓ All core tables exist")
        print()

        # 2. Check price_changelog (price change history)
        print("[2] Price Changelog (price change history)...")
        result = await db.execute(text("SELECT COUNT(*) FROM price_changelog"))
        total = result.scalar()
        print(f"    Total records: {total}")

        if total > 0:
            result = await db.execute(text("""
                SELECT entity_type, COUNT(*), MAX(changed_at) as latest
                FROM price_changelog
                GROUP BY entity_type
            """))
            for row in result.fetchall():
                print(f"    - {row[0]}: {row[1]} records, latest: {row[2]}")
        print()

        # 3. Check price_snapshots (ML training data)
        print("[3] Price Snapshots (ML training data)...")
        if 'price_snapshots' in tables:
            result = await db.execute(text("SELECT COUNT(*) FROM price_snapshots"))
            total = result.scalar()
            print(f"    Total snapshots: {total}")

            if total > 0:
                result = await db.execute(text("""
                    SELECT entity_type, COUNT(*), MIN(snapshot_date), MAX(snapshot_date)
                    FROM price_snapshots
                    GROUP BY entity_type
                """))
                for row in result.fetchall():
                    print(f"    - {row[0]}: {row[1]} snapshots, {row[2]} to {row[3]}")

                # Check today's snapshots
                result = await db.execute(text("""
                    SELECT COUNT(*) FROM price_snapshots
                    WHERE snapshot_date = CURRENT_DATE
                """))
                today = result.scalar()
                print(f"    Today's snapshots: {today}")
            else:
                print("    ⚠️  NO SNAPSHOTS - Need to run migration and backfill")
        else:
            print("    ❌ TABLE DOES NOT EXIST - Need to run migration")
        print()

        # 4. Check pipeline checkpoints (job status)
        print("[4] Pipeline Checkpoints (job execution status)...")
        if 'pipeline_checkpoints' in tables:
            result = await db.execute(text("""
                SELECT job_name, is_running, last_run_started, last_run_completed,
                       total_processed, total_updated, total_errors
                FROM pipeline_checkpoints
                ORDER BY updated_at DESC
            """))
            checkpoints = result.fetchall()
            if checkpoints:
                for cp in checkpoints:
                    status = "🏃 RUNNING" if cp.is_running else "⏸️  idle"
                    print(f"    {cp.job_name}: {status}")
                    print(f"       Started: {cp.last_run_started}")
                    print(f"       Completed: {cp.last_run_completed}")
                    print(f"       Stats: {cp.total_processed} processed, {cp.total_updated} updated, {cp.total_errors} errors")
            else:
                print("    No checkpoints found - jobs haven't run yet")
        else:
            print("    Table doesn't exist")
        print()

        # 5. Check Funko pricing data
        print("[5] Funko Pricing Data...")
        result = await db.execute(text("SELECT COUNT(*) FROM funkos"))
        total = result.scalar()
        print(f"    Total Funkos: {total}")

        result = await db.execute(text("SELECT COUNT(*) FROM funkos WHERE pricecharting_id IS NOT NULL"))
        with_pc = result.scalar()
        print(f"    With PriceCharting ID: {with_pc}")

        result = await db.execute(text("SELECT COUNT(*) FROM funkos WHERE price_loose IS NOT NULL"))
        with_price = result.scalar()
        print(f"    With price_loose: {with_price}")

        result = await db.execute(text("""
            SELECT COUNT(*) FROM funkos
            WHERE updated_at > NOW() - INTERVAL '24 hours'
            AND price_loose IS NOT NULL
        """))
        recent = result.scalar()
        print(f"    Updated in last 24h: {recent}")
        print()

        # 6. Check Newsletter/Social infrastructure
        print("[6] Newsletter & Social Infrastructure...")
        if 'newsletter_subscribers' in tables:
            result = await db.execute(text("SELECT COUNT(*), SUM(CASE WHEN is_confirmed THEN 1 ELSE 0 END) FROM newsletter_subscribers"))
            row = result.fetchone()
            print(f"    Newsletter subscribers: {row[0]} total, {row[1]} confirmed")
        else:
            print("    Newsletter subscribers table: not found")

        if 'content_queue' in tables:
            result = await db.execute(text("SELECT status, COUNT(*) FROM content_queue GROUP BY status"))
            rows = result.fetchall()
            if rows:
                for row in rows:
                    print(f"    Content queue ({row[0]}): {row[1]}")
            else:
                print("    Content queue: empty")
        else:
            print("    Content queue table: not found")
        print()

        # 7. Summary
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)

        issues = []

        if 'price_snapshots' not in tables:
            issues.append("❌ price_snapshots table missing - run migration")
        elif total == 0:
            issues.append("⚠️  price_snapshots empty - run backfill script")

        if with_pc == 0:
            issues.append("⚠️  No Funkos have PriceCharting IDs - need to match")

        if with_price == 0:
            issues.append("⚠️  No Funkos have prices - price sync not running")

        if not checkpoints or all(not cp.last_run_completed for cp in checkpoints):
            issues.append("⚠️  Pipeline jobs haven't completed any runs")

        if issues:
            print("Issues found:")
            for issue in issues:
                print(f"  {issue}")
        else:
            print("✅ All systems operational!")

        print()


if __name__ == "__main__":
    asyncio.run(check_database())
