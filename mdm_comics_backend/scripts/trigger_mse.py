#!/usr/bin/env python3
"""
Multi-Source Enrichment Trigger Script v1.0.0

Authenticates and triggers the MSE job on Railway.
Usage:
    $env:MDM_ADMIN_PASSWORD='password'; python scripts/trigger_mse.py --max-records 100
    python scripts/trigger_mse.py --status
"""
# DEPRECATED - See _deprecated.py for migration instructions
from scripts._deprecated import deprecated_script
deprecated_script(
    script_name="trigger_mse.py",
    replacement="python scripts/mdm_admin.py mse run",
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
    """Get MSE job status."""
    response = httpx.get(
        f"{base_url}/api/admin/pipeline/mse/status",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()


def trigger_mse(base_url: str, token: str, batch_size: int = 100, max_records: int = 0) -> dict:
    """Trigger MSE job."""
    response = httpx.post(
        f"{base_url}/api/admin/pipeline/mse/run",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"batch_size": batch_size, "max_records": max_records},
        timeout=60.0,
    )
    response.raise_for_status()
    return response.json()


def main():
    parser = argparse.ArgumentParser(description="Trigger Multi-Source Enrichment on Railway")
    parser.add_argument("--env", choices=["development", "production"], default="development",
                        help="Target environment (default: development)")
    parser.add_argument("--status", action="store_true", help="Only check status, don't trigger job")
    parser.add_argument("--email", default=DEFAULT_EMAIL, help=f"Admin email (default: {DEFAULT_EMAIL})")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size (default: 100)")
    parser.add_argument("--max-records", type=int, default=0, help="Max records to process (0=unlimited)")
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
    print("\nChecking MSE status...")
    try:
        status = get_status(base_url, token)
        checkpoint = status.get("checkpoint")
        quotas = status.get("source_quotas", [])
        needs_enrichment = status.get("comics_needing_enrichment", 0)

        print(f"  Comics needing enrichment: {needs_enrichment:,}")

        if checkpoint:
            print(f"  Is running: {checkpoint.get('is_running')}")
            print(f"  Total processed: {checkpoint.get('total_processed') or 0:,}")
            print(f"  Total enriched: {checkpoint.get('total_enriched') or 0:,}")
            print(f"  Total errors: {checkpoint.get('total_errors') or 0:,}")
            if checkpoint.get('last_error'):
                print(f"  Last error: {checkpoint.get('last_error')}")

        print("\n  Source Quotas:")
        for q in quotas:
            health = "OK" if q['is_healthy'] else "UNHEALTHY"
            circuit = f"[{q['circuit_state']}]" if q['circuit_state'] != 'closed' else ""
            print(f"    {q['source']}: {q['requests_today']}/{q['daily_limit']} ({q['remaining']} remaining) {health} {circuit}")

    except httpx.HTTPStatusError as e:
        print(f"Status check failed: {e.response.status_code} - {e.response.text}")

    # Trigger job if not --status only
    if not args.status:
        print(f"\nTriggering MSE job (batch_size={args.batch_size}, max_records={args.max_records})...")
        try:
            result = trigger_mse(base_url, token, args.batch_size, args.max_records)
            print(f"Result: {result.get('message', result)}")
        except httpx.HTTPStatusError as e:
            print(f"Trigger failed: {e.response.status_code} - {e.response.text}")
            sys.exit(1)

    print("\nDone!")


if __name__ == "__main__":
    main()
