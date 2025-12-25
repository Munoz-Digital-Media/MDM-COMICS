#!/usr/bin/env python3
"""
Cover Upload Script - Upload local cover images to S3 and link to comic_issues.

Structure: covers/{comic_issue_id}/{front|back}.jpg

Usage:
    # Dry-run (see what would happen)
    python scripts/upload_covers_to_s3.py --dir "path/to/covers" --dry-run

    # Execute upload
    python scripts/upload_covers_to_s3.py --dir "path/to/covers" --execute

    # With verbose output
    python scripts/upload_covers_to_s3.py --dir "path/to/covers" --execute --verbose
"""
import argparse
import hashlib
import mimetypes
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

# Add project root to path
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def parse_args():
    parser = argparse.ArgumentParser(description="Upload cover images to S3")
    parser.add_argument(
        "--dir",
        required=True,
        help="Directory containing cover images",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without making changes",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually upload to S3 and update database",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed output",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of files to process (0 = all)",
    )
    return parser.parse_args()


def parse_filename(filename: str) -> Optional[Dict[str, Any]]:
    """
    Parse cover filename to extract series, volume, issue, and type.

    Expected formats:
        spawn_vol_1_10_front.jpg
        spawn_vol_1_100E_back.jpg
        spawn_vol_1_1_info.png

    Returns dict with: series, volume, issue, cover_type, variant
    """
    # Remove extension
    name = Path(filename).stem.lower()

    # Pattern: {series}_vol_{volume}_{issue}[{variant}]_{type}
    # Examples: spawn_vol_1_10_front, spawn_vol_1_100E_back
    pattern = r'^([a-z_]+)_vol_(\d+)_(\d+)([a-z])?_(front|back|info)$'
    match = re.match(pattern, name)

    if not match:
        return None

    series_raw, volume, issue, variant, cover_type = match.groups()

    # Convert series name: spawn -> Spawn, amazing_spider_man -> Amazing Spider-Man
    series = series_raw.replace('_', ' ').title()

    return {
        'series': series,
        'volume': int(volume),
        'issue': issue,
        'variant': variant.upper() if variant else None,
        'cover_type': cover_type,  # front, back, info
        'original_filename': filename,
    }


def get_file_checksum(filepath: Path) -> str:
    """Calculate SHA-256 checksum of file."""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def match_to_database(
    conn,
    series: str,
    volume: int,
    issue: str,
    variant: Optional[str],
    verbose: bool = False,
) -> Optional[int]:
    """
    Match parsed cover info to a comic_issues record.

    Returns comic_issue_id if found, None otherwise.
    """
    cur = conn.cursor()

    # Try exact match first: series name + issue number
    # Look for series with matching name and volume/year
    query = """
        SELECT ci.id, ci.series_name, ci.number, ci.variant_name, ci.series_year_began
        FROM comic_issues ci
        WHERE (
            LOWER(ci.series_name) = %s
            OR LOWER(ci.series_sort_name) = %s
        )
        AND ci.number = %s
    """
    params = [series.lower(), series.lower(), issue]

    cur.execute(query, params)
    rows = cur.fetchall()

    if not rows:
        if verbose:
            print(f"    No match for {series} #{issue}")
        return None

    # If we have a variant letter (e.g., 100E), try to match variant
    if variant and len(rows) > 1:
        for row in rows:
            issue_id, db_series, db_number, db_variant, db_year = row
            if db_variant and variant.lower() in db_variant.lower():
                if verbose:
                    print(f"    Matched variant: {db_series} #{db_number} ({db_variant}) -> id={issue_id}")
                return issue_id

    # Return first match (or only match)
    issue_id, db_series, db_number, db_variant, db_year = rows[0]
    if verbose:
        print(f"    Matched: {db_series} #{db_number} -> id={issue_id}")
    return issue_id


def upload_to_s3(
    storage_service,
    filepath: Path,
    issue_id: int,
    cover_type: str,
) -> Optional[str]:
    """
    Upload cover to S3 with key: covers/{issue_id}/{cover_type}.jpg

    Returns S3 key on success, None on failure.
    """
    import asyncio

    # Determine content type
    content_type, _ = mimetypes.guess_type(str(filepath))
    if not content_type:
        content_type = 'image/jpeg'

    # Determine extension
    ext = filepath.suffix.lower()
    if ext not in ['.jpg', '.jpeg', '.png', '.webp']:
        ext = '.jpg'

    # Build S3 key
    s3_key = f"covers/{issue_id}/{cover_type}{ext}"

    # Read file
    with open(filepath, 'rb') as f:
        content = f.read()

    # Upload using storage service
    async def do_upload():
        return await storage_service.upload_bytes(
            content=content,
            key=s3_key,
            content_type=content_type,
            cache_control='public, max-age=31536000',  # 1 year
        )

    url = asyncio.run(do_upload())
    return s3_key if url else None


def update_database(
    conn,
    issue_id: int,
    s3_key: str,
    checksum: str,
    cover_type: str,
):
    """Update comic_issues record with S3 cover info."""
    cur = conn.cursor()

    if cover_type == 'front':
        cur.execute("""
            UPDATE comic_issues
            SET cover_s3_key = %s,
                image_acquired_at = %s,
                image_checksum = %s,
                updated_at = NOW()
            WHERE id = %s
        """, (s3_key, datetime.now(timezone.utc), checksum, issue_id))
    elif cover_type == 'back':
        cur.execute("""
            UPDATE comic_issues
            SET back_cover_s3_key = %s,
                updated_at = NOW()
            WHERE id = %s
        """, (s3_key, issue_id))
    # 'info' type covers are uploaded to S3 but not linked to a specific DB field

    conn.commit()


def main():
    args = parse_args()

    if not args.dry_run and not args.execute:
        print("ERROR: Must specify either --dry-run or --execute")
        sys.exit(1)

    if args.dry_run and args.execute:
        print("ERROR: Cannot specify both --dry-run and --execute")
        sys.exit(1)

    # Check directory exists
    cover_dir = Path(args.dir)
    if not cover_dir.exists():
        print(f"ERROR: Directory not found: {cover_dir}")
        sys.exit(1)

    # Get list of image files
    image_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
    image_files = [
        f for f in cover_dir.iterdir()
        if f.is_file() and f.suffix.lower() in image_extensions
    ]

    print(f"Found {len(image_files)} image files in {cover_dir}")

    if args.limit:
        image_files = image_files[:args.limit]
        print(f"Limited to {args.limit} files")

    # Connect to database
    import psycopg2
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL environment variable required")
        sys.exit(1)

    # Convert asyncpg URL to psycopg2 format if needed
    db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")

    print("Connecting to database...")
    conn = psycopg2.connect(db_url)

    # Initialize storage service if executing
    storage_service = None
    if args.execute:
        from app.services.storage import StorageService
        storage_service = StorageService()
        if not storage_service.is_configured():
            print("ERROR: S3 storage not configured")
            sys.exit(1)
        print(f"S3 bucket: {storage_service._bucket}")

    # Process files
    stats = {
        'total': len(image_files),
        'parsed': 0,
        'matched': 0,
        'uploaded': 0,
        'skipped': 0,
        'errors': 0,
    }

    print("\nProcessing covers...")
    print("-" * 60)

    for filepath in image_files:
        filename = filepath.name

        # Parse filename
        parsed = parse_filename(filename)
        if not parsed:
            if args.verbose:
                print(f"[SKIP] Cannot parse: {filename}")
            stats['skipped'] += 1
            continue

        stats['parsed'] += 1

        if args.verbose:
            print(f"\n[FILE] {filename}")
            print(f"  Series: {parsed['series']}, Vol: {parsed['volume']}, Issue: {parsed['issue']}, Type: {parsed['cover_type']}")

        # Match to database
        issue_id = match_to_database(
            conn,
            parsed['series'],
            parsed['volume'],
            parsed['issue'],
            parsed['variant'],
            args.verbose,
        )

        if not issue_id:
            stats['errors'] += 1
            if not args.verbose:
                print(f"[NO MATCH] {filename} -> {parsed['series']} #{parsed['issue']}")
            continue

        stats['matched'] += 1

        # Calculate checksum
        checksum = get_file_checksum(filepath)

        # Determine S3 key
        ext = filepath.suffix.lower()
        s3_key = f"covers/{issue_id}/{parsed['cover_type']}{ext}"

        if args.dry_run:
            print(f"[DRY-RUN] {filename} -> s3://{s3_key} (issue_id={issue_id})")
        else:
            # Upload to S3
            result_key = upload_to_s3(
                storage_service,
                filepath,
                issue_id,
                parsed['cover_type'],
            )

            if result_key:
                # Update database for front and back covers
                if parsed['cover_type'] in ('front', 'back'):
                    update_database(conn, issue_id, result_key, checksum, parsed['cover_type'])

                stats['uploaded'] += 1
                print(f"[UPLOADED] {filename} -> s3://{result_key}")
            else:
                stats['errors'] += 1
                print(f"[ERROR] Failed to upload: {filename}")

    conn.close()

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total files:     {stats['total']}")
    print(f"Parsed:          {stats['parsed']}")
    print(f"Matched to DB:   {stats['matched']}")
    if args.execute:
        print(f"Uploaded to S3:  {stats['uploaded']}")
    print(f"Skipped:         {stats['skipped']}")
    print(f"Errors/No match: {stats['errors']}")

    if args.dry_run:
        print("\n[DRY-RUN MODE] No changes made. Use --execute to upload.")


if __name__ == "__main__":
    main()
