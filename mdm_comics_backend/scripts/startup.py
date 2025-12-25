#!/usr/bin/env python3
"""
Startup script for Railway deployment.

Runs standard Alembic migrations then starts the API server.
"""
import os
import sys
import subprocess
from pathlib import Path

# Ensure the project root is importable
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def run_migrations():
    """Run standard Alembic migrations."""
    print("Running database migrations (alembic upgrade head)...")
    try:
        # Run alembic upgrade head using subprocess to ensure environment isolation
        # and standard execution path
        result = subprocess.run(
            ["alembic", "upgrade", "head"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print("Migrations successful!")
            print(result.stdout)
        else:
            print("Migrations failed!")
            print(result.stdout)
            print(result.stderr)
            # In a strict environment, we might want to exit here.
            # For now, we'll log loudly but allow startup to attempt (it might fail if DB is broken)

    except Exception as e:
        print(f"Migration error: {e}")


def run_custom_migrations():
    """Run custom app migrations (non-Alembic)."""
    print("Running custom migrations...")

    migrations = [
        ("app.migrations.add_bcw_selectors_column", "BCW selectors"),
        ("app.migrations.add_product_case_columns", "Product case columns"),
    ]

    for module_name, description in migrations:
        try:
            result = subprocess.run(
                [sys.executable, "-m", module_name],
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
            )
            if result.returncode == 0:
                print(f"{description} migration: OK")
                if result.stdout:
                    print(result.stdout)
            else:
                print(f"{description} migration failed: {result.stderr}")
        except Exception as e:
            print(f"{description} migration error: {e}")


def start_api():
    """Start the API server."""
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    print(f"Starting API server on port {port}")
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)


def main():
    # Run standard Alembic migrations
    run_migrations()

    # Run custom migrations
    run_custom_migrations()

    # Start API
    start_api()


if __name__ == "__main__":
    main()
