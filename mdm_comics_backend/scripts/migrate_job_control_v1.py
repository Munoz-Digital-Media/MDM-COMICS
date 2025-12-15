#!/usr/bin/env python3
"""
Job Control Migration Script v1.20.0

Adds control_signal, paused_at, paused_by_user_id columns to pipeline_checkpoints.
Enables Start/Pause/Stop control over pipeline jobs.

Usage:
    $env:MDM_ADMIN_PASSWORD='password'; python scripts/migrate_job_control_v1.py
    python scripts/migrate_job_control_v1.py --env production
"""
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


def run_migration(base_url: str, token: str) -> dict:
    """Run the job-control migration."""
    response = httpx.post(
        f"{base_url}/api/data-health/migrations/job-control",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        timeout=60.0,
    )
    response.raise_for_status()
    return response.json()


def main():
    parser = argparse.ArgumentParser(description="Run job-control migration on Railway")
    parser.add_argument("--env", choices=["development", "production"], default="development",
                        help="Target environment (default: development)")
    parser.add_argument("--email", default=DEFAULT_EMAIL, help=f"Admin email (default: {DEFAULT_EMAIL})")
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

    # Run migration
    print("\nRunning job-control migration...")
    try:
        result = run_migration(base_url, token)
        print(f"Success: {result.get('message', result)}")
        if result.get('columns_added'):
            print(f"Columns added: {result['columns_added']}")
    except httpx.HTTPStatusError as e:
        print(f"Migration failed: {e.response.status_code} - {e.response.text}")
        sys.exit(1)

    print("\nDone! Job control is now available.")
    print("Use /api/data-health/jobs/{job_name}/start|pause|stop endpoints.")


if __name__ == "__main__":
    main()
