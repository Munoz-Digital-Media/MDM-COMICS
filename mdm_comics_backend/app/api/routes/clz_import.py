"""
CLZ Data Import API endpoint.

Runs the CLZ import job on the Railway container where database access is reliable.
"""
import asyncio
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.auth import get_current_admin_user

router = APIRouter(prefix="/clz-import", tags=["CLZ Import"])

# Path to CSV - relative to project root in container
CSV_PATH = Path("/app/mdm_comics_backend/data/20251224_MDM_COMICS_DATADUMP.csv")

# Global status tracking
import_status: Dict[str, Any] = {
    "running": False,
    "last_run": None,
    "stats": {},
    "errors": [],
}


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


async def match_issue(
    db: AsyncSession,
    series: str,
    issue: str,
    publisher: Optional[str],
    year: Optional[int],
    upc: Optional[str],
) -> Optional[int]:
    """Match issue by UPC first, then series+issue."""
    # UPC match
    if upc:
        res = await db.execute(
            text("SELECT id FROM comic_issues WHERE upc = :upc LIMIT 1"),
            {"upc": upc},
        )
        row = res.fetchone()
        if row:
            return row[0]

    # Series + issue match with JOIN to comic_series
    params = {
        "series": series.lower(),
        "issue": issue,
    }

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

    res = await db.execute(text(query), params)
    row = res.fetchone()
    return row[0] if row else None


async def run_import_job(db: AsyncSession, limit: int = 0, execute: bool = False):
    """Run the CLZ import job."""
    global import_status

    import_status["running"] = True
    import_status["last_run"] = datetime.utcnow().isoformat()
    import_status["errors"] = []

    stats = {"rows": 0, "matched": 0, "updated": 0, "not_found": 0}

    try:
        if not CSV_PATH.exists():
            import_status["errors"].append(f"CSV not found: {CSV_PATH}")
            import_status["running"] = False
            return

        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader, start=1):
                if limit and idx > limit:
                    break
                stats["rows"] += 1

                try:
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

                    issue_id = await match_issue(db, series, issue, publisher, year_int, upc)

                    if not issue_id:
                        stats["not_found"] += 1
                        continue

                    stats["matched"] += 1

                    if execute:
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
                            updates.append("issue_name = :title")
                            params.append(("title", title))
                        if variant:
                            updates.append("variant_name = :variant")
                            params.append(("variant", variant))
                        if cover_price:
                            updates.append("price = :price")
                            params.append(("price", cover_price))
                        if pub_date:
                            updates.append("publication_date = :pub_date")
                            params.append(("pub_date", pub_date))
                        if store_date:
                            updates.append("store_date = :store_date")
                            params.append(("store_date", store_date))
                        if year_int:
                            updates.append("year = :year")
                            params.append(("year", year_int))
                        if upc:
                            updates.append("upc = :upc")
                            params.append(("upc", upc))
                        if desc:
                            updates.append("description = :desc")
                            params.append(("desc", desc))

                        if updates:
                            updates.append("updated_at = NOW()")
                            stmt = f"UPDATE comic_issues SET {', '.join(updates)} WHERE id = :id"
                            param_dict = dict(params)
                            param_dict["id"] = issue_id
                            await db.execute(text(stmt), param_dict)
                            stats["updated"] += 1

                except Exception as e:
                    import_status["errors"].append(f"Row {idx}: {str(e)}")

                # Progress every 500 rows
                if idx % 500 == 0:
                    import_status["stats"] = stats.copy()

        if execute:
            await db.commit()

    except Exception as e:
        import_status["errors"].append(f"Fatal error: {str(e)}")

    import_status["stats"] = stats
    import_status["running"] = False


@router.get("/status")
async def get_import_status(
    _admin=Depends(get_current_admin_user),
):
    """Get current import status."""
    return import_status


@router.post("/run")
async def trigger_import(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    limit: int = Query(0, description="Row limit (0 = all)"),
    execute: bool = Query(False, description="Actually commit changes"),
    _admin=Depends(get_current_admin_user),
):
    """
    Trigger CLZ import job.

    - limit: Number of rows to process (0 = all)
    - execute: If False, dry-run only (no database changes)
    """
    global import_status

    if import_status["running"]:
        raise HTTPException(status_code=409, detail="Import already running")

    # Run in background
    background_tasks.add_task(run_import_job, db, limit, execute)

    return {
        "message": "Import started",
        "limit": limit,
        "execute": execute,
        "check_status_at": "/api/clz-import/status",
    }
