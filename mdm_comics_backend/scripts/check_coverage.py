#!/usr/bin/env python
"""Quick coverage check for pipeline monitoring."""
import asyncio
import os

import asyncpg

DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    raise EnvironmentError("DATABASE_URL environment variable is required.")

async def quick_check():
    conn = await asyncpg.connect(DB_URL)

    row = await conn.fetchrow("""
        SELECT
          COUNT(*) as total,
          COUNT(CASE WHEN upc IS NOT NULL AND upc <> '' THEN 1 END) as with_upc,
          COUNT(CASE WHEN isbn IS NOT NULL AND isbn <> '' THEN 1 END) as with_isbn,
          COUNT(CASE WHEN pricecharting_id IS NOT NULL THEN 1 END) as pc_matched,
          COUNT(CASE WHEN metron_id IS NOT NULL THEN 1 END) as with_metron,
          COUNT(CASE WHEN comicvine_id IS NOT NULL THEN 1 END) as with_cv
        FROM comic_issues
    """)

    total = row['total']

    print('=== PIPELINE COVERAGE CHECK ===')
    print(f"Total:      {total:>12,}")
    print(f"With UPC:   {row['with_upc']:>12,} ({row['with_upc']/total*100:5.1f}%)")
    print(f"With ISBN:  {row['with_isbn']:>12,} ({row['with_isbn']/total*100:5.1f}%)")
    print(f"PC Matched: {row['pc_matched']:>12,} ({row['pc_matched']/total*100:5.3f}%)")
    print(f"Metron ID:  {row['with_metron']:>12,} ({row['with_metron']/total*100:5.1f}%)")
    print(f"CV ID:      {row['with_cv']:>12,} ({row['with_cv']/total*100:5.1f}%)")

    # Job status
    jobs = await conn.fetch("SELECT job_name, is_running, total_processed FROM pipeline_checkpoints WHERE is_running = true")
    if jobs:
        print('\n=== RUNNING JOBS ===')
        for j in jobs:
            print(f"  {j['job_name']}: processed={j['total_processed'] or 0}")

    await conn.close()

if __name__ == '__main__':
    asyncio.run(quick_check())
