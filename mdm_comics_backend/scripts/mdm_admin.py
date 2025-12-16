#!/usr/bin/env python3
"""
MDM Admin CLI v1.0.0

Unified command-line interface for MDM Comics admin operations.

This CLI consolidates functionality from individual trigger scripts into
a single, consistent interface for all admin jobs.

Per constitution_db.json - Use this CLI instead of direct database scripts.

Usage:
    # Check status of all pipelines
    python scripts/mdm_admin.py status

    # Trigger GCD import
    python scripts/mdm_admin.py gcd import --max-records 10000

    # Trigger MSE enrichment
    python scripts/mdm_admin.py mse run --batch-size 100

    # Cover ingestion
    python scripts/mdm_admin.py covers ingest --folder "/path/to/covers"

    # Data ingestion from CSV
    python scripts/mdm_admin.py ingest csv --source pricecharting --file data.csv --table comic_issues

Environment:
    MDM_API_URL - Backend API URL (default: development)
    MDM_ADMIN_EMAIL - Admin email
    MDM_ADMIN_PASSWORD - Admin password (or prompt if not set)
"""

import argparse
import getpass
import json
import os
import sys
from datetime import datetime
from typing import Optional

try:
    import httpx
except ImportError:
    print("Error: httpx not installed. Run: pip install httpx")
    sys.exit(1)

# =============================================================================
# CONFIGURATION
# =============================================================================

ENVIRONMENTS = {
    "development": "https://mdm-comics-backend-development.up.railway.app",
    "production": "https://api.mdmcomics.com",
    "local": "http://localhost:8000",
}

DEFAULT_EMAIL = "munozdigitalmedia@gmail.com"
DEFAULT_ENV = "development"

# Global state
_token: Optional[str] = None
_base_url: Optional[str] = None


# =============================================================================
# AUTHENTICATION
# =============================================================================


def get_base_url(env: str) -> str:
    """Get base URL for environment."""
    url = os.environ.get("MDM_API_URL")
    if url:
        return url.rstrip("/")
    return ENVIRONMENTS.get(env, ENVIRONMENTS[DEFAULT_ENV])


def authenticate(base_url: str, email: str, password: str) -> str:
    """Login and return access token."""
    response = httpx.post(
        f"{base_url}/api/auth/login",
        json={"email": email, "password": password},
        headers={"Content-Type": "application/json"},
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_token(args) -> str:
    """Get or create auth token."""
    global _token, _base_url

    if _token:
        return _token

    _base_url = get_base_url(args.env)
    email = args.email or os.environ.get("MDM_ADMIN_EMAIL", DEFAULT_EMAIL)
    password = os.environ.get("MDM_ADMIN_PASSWORD")

    if not password:
        password = getpass.getpass(f"Password for {email}: ")

    print(f"Authenticating with {_base_url}...")
    try:
        _token = authenticate(_base_url, email, password)
        print("Authenticated successfully")
        return _token
    except httpx.HTTPStatusError as e:
        print(f"Login failed: {e.response.status_code} - {e.response.text}")
        sys.exit(1)


def api_request(method: str, path: str, token: str, **kwargs) -> dict:
    """Make authenticated API request."""
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers.setdefault("Content-Type", "application/json")

    timeout = kwargs.pop("timeout", 60.0)

    response = httpx.request(
        method,
        f"{_base_url}{path}",
        headers=headers,
        timeout=timeout,
        **kwargs
    )
    response.raise_for_status()
    return response.json()


# =============================================================================
# STATUS COMMAND
# =============================================================================


def cmd_status(args):
    """Show status of all pipelines."""
    token = get_token(args)

    print("\n" + "=" * 60)
    print("PIPELINE STATUS")
    print("=" * 60)

    # Get all pipeline checkpoints
    try:
        data = api_request("GET", "/api/admin/pipeline/checkpoints", token)
        checkpoints = data.get("checkpoints", [])

        if not checkpoints:
            print("No pipeline checkpoints found")
            return

        for cp in checkpoints:
            status = "RUNNING" if cp.get("is_running") else "IDLE"
            processed = cp.get("total_processed") or 0
            errors = cp.get("total_errors") or 0

            print(f"\n{cp['job_name']}: [{status}]")
            print(f"  Processed: {processed:,}")
            print(f"  Errors: {errors:,}")
            if cp.get("last_error"):
                print(f"  Last Error: {cp['last_error'][:100]}...")
            if cp.get("updated_at"):
                print(f"  Updated: {cp['updated_at']}")

    except httpx.HTTPStatusError as e:
        print(f"Failed to get status: {e.response.status_code}")


# =============================================================================
# GCD COMMANDS
# =============================================================================


def cmd_gcd_status(args):
    """Check GCD import status."""
    token = get_token(args)

    try:
        data = api_request("GET", "/api/admin/pipeline/gcd/status", token)

        settings = data.get("settings", {})
        checkpoint = data.get("checkpoint", {})

        print("\nGCD Import Status:")
        print(f"  Enabled: {settings.get('enabled')}")
        print(f"  Dump path: {settings.get('dump_path')}")
        print(f"  Dump exists: {settings.get('dump_exists')}")
        print(f"  Imported count: {data.get('imported_count', 0):,}")

        if checkpoint:
            print(f"\nCheckpoint:")
            print(f"  Is running: {checkpoint.get('is_running')}")
            print(f"  Current offset: {checkpoint.get('current_offset') or 0:,}")
            print(f"  Total processed: {checkpoint.get('total_processed') or 0:,}")
            if checkpoint.get("last_error"):
                print(f"  Last error: {checkpoint['last_error']}")

    except httpx.HTTPStatusError as e:
        print(f"Failed: {e.response.status_code} - {e.response.text}")


def cmd_gcd_import(args):
    """Trigger GCD import."""
    token = get_token(args)

    print(f"\nTriggering GCD import (max_records={args.max_records}, batch_size={args.batch_size})...")

    try:
        data = api_request(
            "POST",
            "/api/admin/pipeline/gcd/import",
            token,
            json={"max_records": args.max_records, "batch_size": args.batch_size},
            timeout=600.0,
        )
        print(f"Result: {data.get('message', data)}")
        if data.get("check_status"):
            print(f"Check status at: {data['check_status']}")

    except httpx.HTTPStatusError as e:
        print(f"Failed: {e.response.status_code} - {e.response.text}")
        sys.exit(1)


def cmd_gcd_reset(args):
    """Reset GCD import checkpoint."""
    token = get_token(args)

    if not args.force:
        confirm = input("This will reset the GCD import to start from scratch. Continue? [y/N]: ")
        if confirm.lower() != "y":
            print("Cancelled")
            return

    try:
        data = api_request("POST", "/api/admin/pipeline/gcd/reset-checkpoint", token)
        print(f"Result: {data.get('message', data)}")

    except httpx.HTTPStatusError as e:
        print(f"Failed: {e.response.status_code} - {e.response.text}")
        sys.exit(1)


# =============================================================================
# MSE COMMANDS
# =============================================================================


def cmd_mse_status(args):
    """Check MSE status."""
    token = get_token(args)

    try:
        data = api_request("GET", "/api/admin/pipeline/mse/status", token)

        checkpoint = data.get("checkpoint")
        quotas = data.get("source_quotas", [])
        needs_enrichment = data.get("comics_needing_enrichment", 0)

        print("\nMSE Status:")
        print(f"  Comics needing enrichment: {needs_enrichment:,}")

        if checkpoint:
            print(f"\nCheckpoint:")
            print(f"  Is running: {checkpoint.get('is_running')}")
            print(f"  Total processed: {checkpoint.get('total_processed') or 0:,}")
            print(f"  Total enriched: {checkpoint.get('total_enriched') or 0:,}")
            print(f"  Total errors: {checkpoint.get('total_errors') or 0:,}")

        if quotas:
            print("\nSource Quotas:")
            for q in quotas:
                health = "OK" if q["is_healthy"] else "UNHEALTHY"
                circuit = f"[{q['circuit_state']}]" if q.get("circuit_state") != "closed" else ""
                print(f"  {q['source']}: {q['requests_today']}/{q['daily_limit']} remaining={q['remaining']} {health} {circuit}")

    except httpx.HTTPStatusError as e:
        print(f"Failed: {e.response.status_code} - {e.response.text}")


def cmd_mse_run(args):
    """Trigger MSE enrichment."""
    token = get_token(args)

    print(f"\nTriggering MSE (batch_size={args.batch_size}, max_records={args.max_records})...")

    try:
        data = api_request(
            "POST",
            "/api/admin/pipeline/mse/run",
            token,
            json={"batch_size": args.batch_size, "max_records": args.max_records},
            timeout=120.0,
        )
        print(f"Result: {data.get('message', data)}")

    except httpx.HTTPStatusError as e:
        print(f"Failed: {e.response.status_code} - {e.response.text}")
        sys.exit(1)


# =============================================================================
# COVER COMMANDS
# =============================================================================


def cmd_covers_status(args):
    """Check cover ingestion status."""
    token = get_token(args)

    try:
        data = api_request("GET", "/api/admin/cover-ingestion/stats", token)

        print("\nCover Ingestion Stats:")
        print(f"  Total in queue: {data.get('total_in_queue', 0):,}")
        print(f"  Pending review: {data.get('pending_review', 0):,}")
        print(f"  Products created: {data.get('products_created', 0):,}")

        by_status = data.get("by_status", {})
        if by_status:
            print("\nBy Status:")
            for status, count in by_status.items():
                print(f"  {status}: {count:,}")

    except httpx.HTTPStatusError as e:
        print(f"Failed: {e.response.status_code} - {e.response.text}")


def cmd_covers_preview(args):
    """Preview cover ingestion from folder."""
    token = get_token(args)

    print(f"\nPreviewing covers from: {args.folder}")

    try:
        data = api_request(
            "POST",
            "/api/admin/cover-ingestion/preview",
            token,
            json={"folder_path": args.folder, "limit": args.limit},
            timeout=120.0,
        )

        items = data.get("items", [])
        print(f"Found {len(items)} items")

        for item in items[:10]:  # Show first 10
            score = item.get("match_score", 0)
            disposition = item.get("disposition", "unknown")
            print(f"\n  {item.get('product_name', 'Unknown')}")
            print(f"    Publisher: {item.get('publisher')}, Series: {item.get('series')}")
            print(f"    Match score: {score}/10 ({disposition})")

        if len(items) > 10:
            print(f"\n  ... and {len(items) - 10} more")

    except httpx.HTTPStatusError as e:
        print(f"Failed: {e.response.status_code} - {e.response.text}")


def cmd_covers_ingest(args):
    """Run cover ingestion from folder."""
    token = get_token(args)

    print(f"\nIngesting covers from: {args.folder}")

    try:
        data = api_request(
            "POST",
            "/api/admin/cover-ingestion/ingest",
            token,
            json={
                "folder_path": args.folder,
                "limit": args.limit if args.limit > 0 else None,
            },
            timeout=600.0,
        )

        print("\nResults:")
        print(f"  Total files: {data.get('total_files', 0):,}")
        print(f"  Processed: {data.get('processed', 0):,}")
        print(f"  Queued for review: {data.get('queued_for_review', 0):,}")
        print(f"  High confidence: {data.get('high_confidence', 0):,}")
        print(f"  Medium confidence: {data.get('medium_confidence', 0):,}")
        print(f"  Low confidence: {data.get('low_confidence', 0):,}")
        print(f"  Skipped: {data.get('skipped', 0):,}")
        print(f"  Errors: {data.get('errors', 0):,}")

    except httpx.HTTPStatusError as e:
        print(f"Failed: {e.response.status_code} - {e.response.text}")
        sys.exit(1)


# =============================================================================
# DATA INGESTION COMMANDS
# =============================================================================


def cmd_ingest_csv(args):
    """Ingest data from CSV file."""
    token = get_token(args)

    print(f"\nIngesting CSV: {args.file}")
    print(f"  Source: {args.source}")
    print(f"  Table: {args.table}")
    print(f"  Batch size: {args.batch_size}")

    try:
        data = api_request(
            "POST",
            "/api/admin/ingest-data",
            token,
            json={
                "source": args.source,
                "file_path": args.file,
                "table_name": args.table,
                "format": "csv",
                "options": {
                    "batch_size": args.batch_size,
                    "skip_existing": not args.update,
                    "update_existing": args.update,
                    "dry_run": args.dry_run,
                },
            },
            timeout=600.0,
        )

        print("\nResults:")
        stats = data.get("stats", {})
        print(f"  Total rows: {stats.get('total_rows', 0):,}")
        print(f"  Processed: {stats.get('processed', 0):,}")
        print(f"  Inserted: {stats.get('inserted', 0):,}")
        print(f"  Updated: {stats.get('updated', 0):,}")
        print(f"  Errors: {stats.get('errors', 0):,}")
        print(f"  Duration: {stats.get('duration_seconds', 0):.2f}s")
        print(f"  Rate: {stats.get('rows_per_second', 0):.1f} rows/sec")

    except httpx.HTTPStatusError as e:
        print(f"Failed: {e.response.status_code} - {e.response.text}")
        sys.exit(1)


# =============================================================================
# MAIN
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="MDM Comics Admin CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s status                           # Show all pipeline status
  %(prog)s gcd status                       # Check GCD import status
  %(prog)s gcd import --max-records 10000   # Run GCD import
  %(prog)s mse run --batch-size 100         # Run MSE enrichment
  %(prog)s covers ingest --folder /path     # Ingest cover images
        """,
    )

    # Global options
    parser.add_argument("--env", choices=["development", "production", "local"],
                        default=DEFAULT_ENV, help="Target environment")
    parser.add_argument("--email", help=f"Admin email (default: {DEFAULT_EMAIL})")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show pipeline status")
    status_parser.set_defaults(func=cmd_status)

    # GCD commands
    gcd_parser = subparsers.add_parser("gcd", help="GCD import commands")
    gcd_sub = gcd_parser.add_subparsers(dest="gcd_cmd")

    gcd_status = gcd_sub.add_parser("status", help="Check GCD import status")
    gcd_status.set_defaults(func=cmd_gcd_status)

    gcd_import = gcd_sub.add_parser("import", help="Trigger GCD import")
    gcd_import.add_argument("--max-records", type=int, default=0, help="Max records (0=unlimited)")
    gcd_import.add_argument("--batch-size", type=int, default=1000, help="Batch size")
    gcd_import.set_defaults(func=cmd_gcd_import)

    gcd_reset = gcd_sub.add_parser("reset", help="Reset GCD checkpoint")
    gcd_reset.add_argument("--force", action="store_true", help="Skip confirmation")
    gcd_reset.set_defaults(func=cmd_gcd_reset)

    # MSE commands
    mse_parser = subparsers.add_parser("mse", help="Multi-source enrichment commands")
    mse_sub = mse_parser.add_subparsers(dest="mse_cmd")

    mse_status = mse_sub.add_parser("status", help="Check MSE status")
    mse_status.set_defaults(func=cmd_mse_status)

    mse_run = mse_sub.add_parser("run", help="Trigger MSE enrichment")
    mse_run.add_argument("--batch-size", type=int, default=100, help="Batch size")
    mse_run.add_argument("--max-records", type=int, default=0, help="Max records (0=unlimited)")
    mse_run.set_defaults(func=cmd_mse_run)

    # Cover commands
    covers_parser = subparsers.add_parser("covers", help="Cover ingestion commands")
    covers_sub = covers_parser.add_subparsers(dest="covers_cmd")

    covers_status = covers_sub.add_parser("status", help="Check cover ingestion status")
    covers_status.set_defaults(func=cmd_covers_status)

    covers_preview = covers_sub.add_parser("preview", help="Preview folder ingestion")
    covers_preview.add_argument("--folder", required=True, help="Folder path")
    covers_preview.add_argument("--limit", type=int, default=100, help="Preview limit")
    covers_preview.set_defaults(func=cmd_covers_preview)

    covers_ingest = covers_sub.add_parser("ingest", help="Run folder ingestion")
    covers_ingest.add_argument("--folder", required=True, help="Folder path")
    covers_ingest.add_argument("--limit", type=int, default=0, help="Max files (0=unlimited)")
    covers_ingest.set_defaults(func=cmd_covers_ingest)

    # Data ingestion commands
    ingest_parser = subparsers.add_parser("ingest", help="Data ingestion commands")
    ingest_sub = ingest_parser.add_subparsers(dest="ingest_cmd")

    ingest_csv = ingest_sub.add_parser("csv", help="Ingest CSV file")
    ingest_csv.add_argument("--source", required=True, help="Source identifier (e.g., pricecharting)")
    ingest_csv.add_argument("--file", required=True, help="CSV file path")
    ingest_csv.add_argument("--table", required=True, help="Target table name")
    ingest_csv.add_argument("--batch-size", type=int, default=1000, help="Batch size")
    ingest_csv.add_argument("--update", action="store_true", help="Update existing records")
    ingest_csv.add_argument("--dry-run", action="store_true", help="Preview without inserting")
    ingest_csv.set_defaults(func=cmd_ingest_csv)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
