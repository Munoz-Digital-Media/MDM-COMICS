#!/usr/bin/env python3
"""
GCD Import Trigger Script v1.0.0

Authenticates and triggers the GCD import job on Railway.
Usage:
    python scripts/trigger_gcd_import.py [--env development|production] [--status]
"""
# DEPRECATED - See _deprecated.py for migration instructions
from scripts._deprecated import deprecated_script
deprecated_script(
    script_name="trigger_gcd_import.py",
    replacement="python scripts/mdm_admin.py gcd import",
    reason="Use unified MDM Admin CLI"
)


import argparse
import getpass
import os
import sys

import httpx

ENVIRONMENTS = {
    "development": "https://mdm-comics-backend-development.up.railway.app",
    "production": "https://api.mdmcomics.com",
}

DEFAULT_EMAIL = "munozdigitalmedia@gmail.com"


def get_token(base_url: str, email: str, password: str) -> str:
    """Login and return access token."""
    response = httpx.post(
        f"{base_url}/api/auth/login",
        json={"email": email, "password": password},
        headers={"Content-Type": "application/json"},
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_status(base_url: str, token: str) -> dict:
    """Get GCD import status."""
    response = httpx.get(
        f"{base_url}/api/admin/pipeline/gcd/status",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()


def trigger_import(base_url: str, token: str, max_records: int = 0, batch_size: int = 1000) -> dict:
    """Trigger GCD import job."""
    # Use longer timeout - imports can take several minutes
    response = httpx.post(
        f"{base_url}/api/admin/pipeline/gcd/import",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"max_records": max_records, "batch_size": batch_size},
        timeout=600.0,  # 10 minutes for large imports
    )
    response.raise_for_status()
    return response.json()


def main():
    parser = argparse.ArgumentParser(description="Trigger GCD import on Railway")
    parser.add_argument("--env", choices=["development", "production"], default="development",
                        help="Target environment (default: development)")
    parser.add_argument("--status", action="store_true", help="Only check status, don't trigger import")
    parser.add_argument("--email", default=DEFAULT_EMAIL, help=f"Admin email (default: {DEFAULT_EMAIL})")
    parser.add_argument("--max-records", type=int, default=0, help="Max records to import (0=unlimited)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size (default: 1000)")
    args = parser.parse_args()

    base_url = ENVIRONMENTS[args.env]
    print(f"Target: {args.env} ({base_url})")

    # Get password from env or prompt
    password = os.environ.get("MDM_ADMIN_PASSWORD")
    if not password:
        password = getpass.getpass(f"Password for {args.email}: ")

    # Login
    print("Authenticating...")
    try:
        token = get_token(base_url, args.email, password)
        print("Authenticated successfully")
    except httpx.HTTPStatusError as e:
        print(f"Login failed: {e.response.status_code} - {e.response.text}")
        sys.exit(1)

    # Get status
    print("\nChecking GCD import status...")
    try:
        status = get_status(base_url, token)
        settings = status.get("settings", {})
        checkpoint = status.get("checkpoint", {})

        print(f"  Enabled: {settings.get('enabled')}")
        print(f"  Dump path: {settings.get('dump_path')}")
        print(f"  Dump exists: {settings.get('dump_exists')}")
        print(f"  Imported count: {status.get('imported_count', 0):,}")

        if checkpoint:
            print(f"  Is running: {checkpoint.get('is_running')}")
            print(f"  Current offset: {checkpoint.get('current_offset') or 0:,}")
            print(f"  Total processed: {checkpoint.get('total_processed') or 0:,}")
            if checkpoint.get('last_error'):
                print(f"  Last error: {checkpoint.get('last_error')}")
    except httpx.HTTPStatusError as e:
        print(f"Status check failed: {e.response.status_code} - {e.response.text}")

    # Trigger import if not --status only
    if not args.status:
        print(f"\nTriggering GCD import (max_records={args.max_records}, batch_size={args.batch_size})...")
        try:
            result = trigger_import(base_url, token, args.max_records, args.batch_size)
            print(f"Result: {result}")
        except httpx.HTTPStatusError as e:
            print(f"Import trigger failed: {e.response.status_code} - {e.response.text}")
            sys.exit(1)

    print("\nDone!")


if __name__ == "__main__":
    main()
