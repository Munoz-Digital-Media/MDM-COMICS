"""
CLZ Data Import API endpoint.

Runs the CLZ import job on the Railway container where database access is reliable.

Modes:
- update: Match existing records and update metadata (default)
- create: Create new comic_issues for unmatched rows
- full: Both update existing and create new records
"""
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.api.deps import get_current_admin

router = APIRouter(prefix="/clz-import", tags=["CLZ Import"])

# Path to CSV - relative to project root in container
CSV_PATH = Path("/app/mdm_comics_backend/data/20251224_MDM_COMICS_DATADUMP.csv")

# Global status tracking
import_status: Dict[str, Any] = {
    "running": False,
    "last_run": None,
    "mode": None,
    "stats": {},
    "errors": [],
}

# CSV column to DB column mapping
CSV_TO_DB_MAPPING = {
    # Core fields
    "Series": "series_sort_name",
    "Issue": "number",
    "Full Title": "issue_name",
    "Title": "story_title",
    "Publisher": "publisher_name",
    "Cover Date": "publication_date",
    "Release Year": "series_year_began",
    "Cover Year": "year",
    "Cover Price": "price",
    "Barcode": "upc",
    # CLZ-specific fields
    "Genre": "genre",
    "Storage Box": "storage_box",
    "Story Arc": "story_arc",
    "Subtitle": "subtitle",
    "Variant Description": "variant_name",
    "Variant": "variant_name",  # Fallback
    "Key": "is_key_issue",
    "Key Category": "key_category",
    "Key Reason": "key_reason",
    # Creator credits
    "Artist": "clz_artist",
    "Characters": "clz_characters",
    "Colorist": "colorist",
    "Cover Artist": "cover_artist",
    "Cover Colorist": "cover_colorist",
    "Cover Inker": "cover_inker",
    "Cover Painter": "cover_painter",
    "Cover Penciller": "cover_penciller",
    "Cover Separator": "cover_separator",
    "Creators": "clz_creators",
    "Editor": "editor",
    "Editor in Chief": "editor_in_chief",
    "Inker": "inker",
    "Layouts": "layouts",
    "Letterer": "letterer",
    "Painter": "painter",
    "Penciller": "penciller",
    "Plotter": "plotter",
    "Scripter": "scripter",
    "Separator": "separator",
    "Translator": "translator",
    "Writer": "writer",
}


def normalize_upc(barcode: str) -> Optional[str]:
    if not barcode:
        return None
    digits = "".join(ch for ch in barcode if ch.isdigit())
    return digits or None


def parse_cover_price(value: str) -> Optional[float]:
    if not value:
        return None
    try:
        return float(str(value).replace("$", "").strip())
    except ValueError:
        return None


def parse_year(value: str) -> Optional[int]:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_key_issue(value: str) -> bool:
    return value.lower() == "yes" if value else False


def extract_row_data(row: Dict[str, str]) -> Dict[str, Any]:
    """Extract all fields from a CSV row into database format."""
    data = {}

    for csv_col, db_col in CSV_TO_DB_MAPPING.items():
        value = (row.get(csv_col) or "").strip()
        if not value:
            continue

        # Skip if we already have this db column set (handles fallbacks)
        if db_col in data and data[db_col]:
            continue

        # Special handling for certain fields
        if db_col == "price":
            data[db_col] = parse_cover_price(value)
        elif db_col in ("year", "series_year_began"):
            data[db_col] = parse_year(value)
        elif db_col == "upc":
            data[db_col] = normalize_upc(value)
        elif db_col == "is_key_issue":
            data[db_col] = parse_key_issue(value)
        else:
            data[db_col] = value

    # Store the complete raw row as JSON
    data["clz_raw_data"] = json.dumps({k: v for k, v in row.items() if v})

    return data


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


async def update_issue(db: AsyncSession, issue_id: int, data: Dict[str, Any]) -> bool:
    """Update an existing issue with new data."""
    if not data:
        return False

    # Filter out None values and id
    updates = {k: v for k, v in data.items() if v is not None and k != "id"}
    if not updates:
        return False

    set_clauses = [f"{k} = :{k}" for k in updates.keys()]
    set_clauses.append("updated_at = NOW()")

    stmt = f"UPDATE comic_issues SET {', '.join(set_clauses)} WHERE id = :id"
    updates["id"] = issue_id

    await db.execute(text(stmt), updates)
    return True


async def create_issue(db: AsyncSession, data: Dict[str, Any]) -> Optional[int]:
    """Create a new comic_issue record."""
    if not data:
        return None

    # Filter out None values
    fields = {k: v for k, v in data.items() if v is not None}

    # Add metadata
    fields["data_source"] = "clz"
    fields["created_at"] = datetime.utcnow()
    fields["updated_at"] = datetime.utcnow()

    columns = list(fields.keys())
    placeholders = [f":{k}" for k in columns]

    stmt = f"INSERT INTO comic_issues ({', '.join(columns)}) VALUES ({', '.join(placeholders)}) RETURNING id"

    result = await db.execute(text(stmt), fields)
    row = result.fetchone()
    return row[0] if row else None


async def run_import_job(db: AsyncSession, limit: int = 0, execute: bool = False, mode: str = "full"):
    """Run the CLZ import job.

    Args:
        db: Database session
        limit: Row limit (0 = all)
        execute: If True, commit changes; if False, dry-run
        mode: 'update' (only update), 'create' (only create), 'full' (both)
    """
    global import_status

    import_status["running"] = True
    import_status["last_run"] = datetime.utcnow().isoformat()
    import_status["mode"] = mode
    import_status["errors"] = []

    stats = {
        "rows": 0,
        "matched": 0,
        "updated": 0,
        "created": 0,
        "not_found": 0,
        "skipped": 0,
    }

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
                    # Extract all data from row
                    data = extract_row_data(row)

                    series = data.get("series_sort_name", "")
                    issue_num = data.get("number", "")
                    publisher = data.get("publisher_name")
                    year = data.get("year")
                    upc = data.get("upc")

                    if not series or not issue_num:
                        stats["skipped"] += 1
                        continue

                    # Try to match existing issue
                    issue_id = await match_issue(db, series, issue_num, publisher, year, upc)

                    if issue_id:
                        stats["matched"] += 1

                        if mode in ("update", "full") and execute:
                            if await update_issue(db, issue_id, data):
                                stats["updated"] += 1
                    else:
                        stats["not_found"] += 1

                        if mode in ("create", "full") and execute:
                            new_id = await create_issue(db, data)
                            if new_id:
                                stats["created"] += 1

                except Exception as e:
                    import_status["errors"].append(f"Row {idx}: {str(e)[:100]}")

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
    _admin=Depends(get_current_admin),
):
    """Get current import status."""
    return import_status


@router.post("/run")
async def trigger_import(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    limit: int = Query(0, description="Row limit (0 = all)"),
    execute: bool = Query(False, description="Actually commit changes"),
    mode: str = Query("full", description="Mode: 'update', 'create', or 'full'"),
    _admin=Depends(get_current_admin),
):
    """
    Trigger CLZ import job.

    - limit: Number of rows to process (0 = all)
    - execute: If False, dry-run only (no database changes)
    - mode:
        - 'update': Only update existing matched records
        - 'create': Only create new records for unmatched
        - 'full': Both update and create (default)
    """
    global import_status

    if import_status["running"]:
        raise HTTPException(status_code=409, detail="Import already running")

    if mode not in ("update", "create", "full"):
        raise HTTPException(status_code=400, detail="Mode must be 'update', 'create', or 'full'")

    # Run in background
    background_tasks.add_task(run_import_job, db, limit, execute, mode)

    return {
        "message": "Import started",
        "limit": limit,
        "execute": execute,
        "mode": mode,
        "check_status_at": "/api/clz-import/status",
    }
