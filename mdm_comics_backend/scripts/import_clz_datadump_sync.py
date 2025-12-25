#!/usr/bin/env python3
"""
Sync version of CLZ import using psycopg2 (more stable network).

Run on Railway: railway run python mdm_comics_backend/scripts/import_clz_datadump_sync.py --limit 100 --verbose
"""
import argparse
import csv
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import psycopg2
from psycopg2.extras import DictCursor


# Get DATABASE_URL from environment (Railway provides this)
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:UAzxIlGnYJEeIZnrsPGWdkLNYWoEEIAx@monorail.proxy.rlwy.net:58855/railway"
)

# Default CSV path relative to project root
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_CSV = PROJECT_ROOT / "data" / "20251224_MDM_COMICS_DATADUMP.csv"


def parse_args():
    parser = argparse.ArgumentParser(description="Import CLZ CSV into comic_issues (sync)")
    parser.add_argument(
        "--csv",
        default=str(DEFAULT_CSV),
        help="Path to the CLZ CSV export",
    )
    parser.add_argument("--limit", type=int, default=0, help="Row limit (0 = all)")
    parser.add_argument("--execute", action="store_true", help="Commit changes")
    parser.add_argument("--verbose", action="store_true", help="Debug output")
    return parser.parse_args()


def get_connection(retries=5, delay=2):
    """Get connection with retry."""
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=30)
            return conn
        except Exception as e:
            print(f"  [RETRY {attempt+1}/{retries}] Connection error: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    raise Exception("Failed to connect after retries")


def normalize_upc(barcode: str) -> Optional[str]:
    if not barcode:
        return None
    digits = "".join(ch for ch in barcode if ch.isdigit())
    return digits or None


def parse_month_year(value: str) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%b %Y", "%B %Y"):
        try:
            dt = datetime.strptime(value, fmt)
            return datetime(dt.year, dt.month, 1).isoformat()
        except ValueError:
            continue
    return None


def parse_cover_price(value: str) -> Optional[float]:
    if not value:
        return None
    try:
        return float(str(value).replace("$", "").strip())
    except ValueError:
        return None


def build_description(key: str, key_category: str, key_reason: str) -> Optional[str]:
    parts = []
    if key and key.lower() == "yes":
        parts.append("Key issue")
    if key_category:
        parts.append(f"Category: {key_category}")
    if key_reason:
        parts.append(f"Reason: {key_reason}")
    return " | ".join(parts) if parts else None


def match_issue(cur, series: str, issue: str, publisher: Optional[str],
                year: Optional[int], upc: Optional[str], verbose: bool = False) -> Optional[int]:
    """Match issue - UPC first, then series+issue."""

    # UPC match
    if upc:
        cur.execute("SELECT id FROM comic_issues WHERE upc = %s LIMIT 1", (upc,))
        row = cur.fetchone()
        if row:
            if verbose:
                print(f"  [UPC MATCH] Found id={row[0]} via UPC={upc}")
            return row[0]
        elif verbose:
            print(f"  [UPC] No match for UPC={upc}")

    # Series + issue match
    series_lower = series.lower()

    # Build query with JOIN to comic_series
    query = """
        SELECT ci.id
        FROM comic_issues ci
        LEFT JOIN comic_series cs ON ci.series_id = cs.id
        WHERE (
            LOWER(ci.series_sort_name) = %s
            OR LOWER(cs.name) = %s
            OR LOWER(cs.sort_name) = %s
        )
        AND ci.number = %s
    """
    params = [series_lower, series_lower, series_lower, issue]

    if publisher:
        query += " AND ci.publisher_name ILIKE %s"
        params.append(f"%{publisher}%")
    if year:
        query += " AND (ci.year = %s OR ci.series_year_began = %s)"
        params.extend([year, year])

    query += " ORDER BY ci.id ASC LIMIT 1"

    if verbose:
        print(f"  [SERIES] Searching: series={repr(series_lower)} issue={repr(issue)} pub={repr(publisher)} year={year}")

    cur.execute(query, params)
    row = cur.fetchone()

    if row:
        if verbose:
            print(f"  [SERIES MATCH] Found id={row[0]}")
        return row[0]
    elif verbose:
        # Find similar series
        cur.execute("""
            SELECT DISTINCT cs.name
            FROM comic_issues ci
            LEFT JOIN comic_series cs ON ci.series_id = cs.id
            WHERE LOWER(cs.name) LIKE %s
            LIMIT 3
        """, (f"%{series_lower[:10]}%",))
        similar = cur.fetchall()
        if similar:
            print(f"  [DEBUG] Similar series in DB: {[s[0] for s in similar]}")
        else:
            print(f"  [DEBUG] No similar series found for '{series[:20]}...'")

    return None


def main():
    args = parse_args()

    print("Connecting to database...")
    conn = get_connection()
    cur = conn.cursor()

    stats = {"rows": 0, "matched": 0, "updated": 0, "not_found": 0}

    print(f"Reading CSV: {args.csv}")
    with open(args.csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            if args.limit and idx > args.limit:
                break
            stats["rows"] += 1

            series = (row.get("Series") or "").strip()
            issue = (row.get("Issue") or "").strip()
            publisher = (row.get("Publisher") or "").strip() or None
            cover_year = row.get("Cover Year") or row.get("Release Year") or ""
            upc = normalize_upc(row.get("Barcode") or "")

            year_int = None
            try:
                year_int = int(cover_year) if cover_year else None
            except ValueError:
                pass

            if args.verbose:
                print(f"\n[ROW {idx}] {series} #{issue} ({publisher}, {year_int}) UPC={upc}")

            try:
                issue_id = match_issue(cur, series, issue, publisher, year_int, upc, args.verbose)
            except psycopg2.OperationalError as e:
                print(f"  [ERROR] Connection lost, reconnecting...")
                conn = get_connection()
                cur = conn.cursor()
                issue_id = match_issue(cur, series, issue, publisher, year_int, upc, args.verbose)

            if not issue_id:
                stats["not_found"] += 1
                continue

            stats["matched"] += 1

            # Build update
            title = (row.get("Title") or row.get("Full Title") or "").strip()
            variant = (row.get("Variant Description") or row.get("Variant") or "").strip()
            cover_price = parse_cover_price(row.get("Cover Price") or "")
            pub_date = (row.get("Cover Date") or "").strip() or None
            store_date = parse_month_year(row.get("Release Date") or "")
            desc = build_description(
                row.get("Key") or "",
                row.get("Key Category") or "",
                row.get("Key Reason") or "",
            )

            updates = []
            params = []
            if title:
                updates.append("issue_name = %s")
                params.append(title)
            if variant:
                updates.append("variant_name = %s")
                params.append(variant)
            if cover_price:
                updates.append("price = %s")
                params.append(cover_price)
            if pub_date:
                updates.append("publication_date = %s")
                params.append(pub_date)
            if store_date:
                updates.append("store_date = %s")
                params.append(store_date)
            if year_int:
                updates.append("year = %s")
                params.append(year_int)
            if upc:
                updates.append("upc = %s")
                params.append(upc)
            if desc:
                updates.append("description = %s")
                params.append(desc)

            if updates:
                updates.append("updated_at = NOW()")
                params.append(issue_id)
                stmt = f"UPDATE comic_issues SET {', '.join(updates)} WHERE id = %s"

                if args.execute:
                    cur.execute(stmt, params)
                    stats["updated"] += 1
                else:
                    print(f"[DRY-RUN] Would update issue {issue_id}")

            # Progress indicator every 100 rows
            if idx % 100 == 0:
                print(f"  Progress: {idx} rows, {stats['matched']} matched, {stats['not_found']} not found")

    if args.execute:
        conn.commit()

    cur.close()
    conn.close()

    print(f"\nProcessed rows: {stats['rows']}")
    print(f"Matched issues: {stats['matched']}")
    print(f"Updated issues: {stats['updated']}")
    print(f"Not found: {stats['not_found']}")


if __name__ == "__main__":
    main()
