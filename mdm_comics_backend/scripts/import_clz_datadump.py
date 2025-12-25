#!/usr/bin/env python3
"""
Import CLZ/MDM CSV metadata and attach to existing comic_issues.

Mapping priority:
1) Barcode -> comic_issues.upc (exact)
2) Series + issue number (+ optional publisher + year) fuzzy-ish exact match

Fields updated (when present):
- issue_name (Title)
- variant_name (Variant / Variant Description)
- price (Cover Price)
- publication_date (Cover Date, raw string)
- store_date (Release Date -> first day of month/year)
- year (Cover Year)
- upc (from Barcode) if missing
- description (appends Key info)

Dry-run by default; pass --execute to commit.
"""
import argparse
import asyncio
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

# Optional dotenv (for offline env loading)
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # noqa: BLE001
    load_dotenv = None

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Default CSV path relative to project root
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_CSV = PROJECT_ROOT / "data" / "20251224_MDM_COMICS_DATADUMP.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import CLZ CSV into comic_issues")
    parser.add_argument(
        "--csv",
        default=str(DEFAULT_CSV),
        help="Path to the CLZ/MDM CSV export",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional row limit for testing (0 = all rows)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually commit changes (omit for dry-run)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print debug info for each row",
    )
    return parser.parse_args()


def normalize_upc(barcode: str) -> Optional[str]:
    if not barcode:
        return None
    digits = "".join(ch for ch in barcode if ch.isdigit())
    return digits or None


def parse_month_year(value: str) -> Optional[datetime]:
    """Parse strings like 'Jan 1986' into a date (first of month)."""
    if not value:
        return None
    value = value.strip()
    for fmt in ("%b %Y", "%B %Y"):
        try:
            dt = datetime.strptime(value, fmt)
            return datetime(dt.year, dt.month, 1)
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


async def match_issue(
    db: AsyncSession,
    series: str,
    issue: str,
    publisher: Optional[str],
    year: Optional[int],
    upc: Optional[str],
    verbose: bool = False,
) -> Optional[int]:
    """
    Return comic_issues.id for best match.
    Priority 1: upc exact.
    Priority 2: series + number (+ publisher + year if provided).
    """
    # UPC match
    if upc:
        res = await db.execute(
            text("SELECT id FROM comic_issues WHERE upc = :upc LIMIT 1"),
            {"upc": upc},
        )
        row = res.fetchone()
        if row:
            if verbose:
                print(f"  [UPC MATCH] Found id={row[0]} via UPC={upc}")
            return row[0]
        elif verbose:
            print(f"  [UPC] No match for UPC={upc}")

    # Series + issue match (exact text compare, case-insensitive)
    # comic_issues.series_sort_name OR comic_series.name (via join)
    params = {
        "series": series.lower(),
        "issue": issue,
    }

    # Build WHERE clauses
    series_match = "(LOWER(ci.series_sort_name) = :series OR LOWER(cs.name) = :series OR LOWER(cs.sort_name) = :series)"
    issue_match = "ci.number = :issue"

    extra_clauses = []
    if publisher:
        extra_clauses.append("ci.publisher_name ILIKE :pub")
        params["pub"] = f"%{publisher}%"
    if year:
        extra_clauses.append("(ci.year = :year OR ci.series_year_began = :year)")
        params["year"] = year

    where_clause = f"{series_match} AND {issue_match}"
    if extra_clauses:
        where_clause += " AND " + " AND ".join(extra_clauses)

    query = f"""
        SELECT ci.id
        FROM comic_issues ci
        LEFT JOIN comic_series cs ON ci.series_id = cs.id
        WHERE {where_clause}
        ORDER BY ci.id ASC
        LIMIT 1
    """

    if verbose:
        print(f"  [SERIES] Searching: series={repr(series.lower())} issue={repr(issue)} pub={repr(publisher)} year={year}")

    res = await db.execute(text(query), params)
    row = res.fetchone()

    if row:
        if verbose:
            print(f"  [SERIES MATCH] Found id={row[0]}")
        return row[0]
    elif verbose:
        # Try to find similar series names to help debug
        fuzzy_res = await db.execute(
            text("""SELECT DISTINCT cs.name, ci.series_sort_name
                    FROM comic_issues ci
                    LEFT JOIN comic_series cs ON ci.series_id = cs.id
                    WHERE LOWER(cs.name) LIKE :pattern
                       OR LOWER(ci.series_sort_name) LIKE :pattern
                    LIMIT 3"""),
            {"pattern": f"%{series.lower()[:10]}%"},
        )
        similar = fuzzy_res.fetchall()
        if similar:
            print(f"  [DEBUG] Similar series in DB: {[s[0] for s in similar]}")
        else:
            print(f"  [DEBUG] No similar series found for '{series[:20]}...'")

    return None


async def main():
    args = parse_args()
    if load_dotenv:
        try:
            load_dotenv()
        except Exception:
            pass

    db_url = os.getenv("DATABASE_URL", "")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(db_url, echo=False)
    SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    stats = {"rows": 0, "matched": 0, "updated": 0, "not_found": 0}

    async with SessionLocal() as db:
        with open(args.csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader, start=1):
                if args.limit and idx > args.limit:
                    break
                stats["rows"] += 1

                series = (row.get("Series") or "").strip()
                issue = (row.get("Issue") or "").strip()
                publisher = (row.get("Publisher") or "").strip() or None
                cover_date_raw = row.get("Cover Date") or ""
                release_date_raw = row.get("Release Date") or ""
                cover_year = row.get("Cover Year") or row.get("Release Year") or ""
                upc = normalize_upc(row.get("Barcode") or "")
                title = (row.get("Title") or row.get("Full Title") or "").strip()
                variant = (row.get("Variant Description") or row.get("Variant") or "").strip()
                cover_price = parse_cover_price(row.get("Cover Price") or "")
                desc = build_description(
                    row.get("Key") or "",
                    row.get("Key Category") or "",
                    row.get("Key Reason") or "",
                )

                store_date = parse_month_year(release_date_raw)
                pub_date = cover_date_raw.strip() or None
                year_int = None
                try:
                    year_int = int(cover_year) if cover_year else None
                except ValueError:
                    year_int = None

                if args.verbose:
                    print(f"\n[ROW {idx}] {series} #{issue} ({publisher}, {year_int}) UPC={upc}")

                issue_id = await match_issue(db, series, issue, publisher, year_int, upc, verbose=args.verbose)
                if not issue_id:
                    stats["not_found"] += 1
                    continue

                stats["matched"] += 1

                updates = {
                    "issue_name": title if title else None,
                    "variant_name": variant if variant else None,
                    "price": cover_price,
                    "publication_date": pub_date,
                    "store_date": store_date.isoformat() if store_date else None,
                    "year": year_int,
                    "upc": upc,
                    "description": desc,
                    "id": issue_id,
                }

                # Build dynamic update set only for non-empty fields
                set_parts = []
                params = {"id": issue_id}
                for field, value in updates.items():
                    if field == "id":
                        continue
                    if value is None:
                        continue
                    set_parts.append(f"{field} = :{field}")
                    params[field] = value

                if set_parts:
                    stmt = f"UPDATE comic_issues SET {', '.join(set_parts)}, updated_at = NOW() WHERE id = :id"
                    if args.execute:
                        await db.execute(text(stmt), params)
                        stats["updated"] += 1
                    else:
                        # Dry-run: just log planned update
                        print(f"[DRY-RUN] Would update issue {issue_id}: {params}")

            if args.execute:
                await db.commit()

    print(f"Processed rows: {stats['rows']}")
    print(f"Matched issues: {stats['matched']}")
    print(f"Updated issues: {stats['updated']}")
    print(f"Not found: {stats['not_found']}")


if __name__ == "__main__":
    asyncio.run(main())
