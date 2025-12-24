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


def start_api():
    """Start the API server."""
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    print(f"Starting API server on port {port}")
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)


def main():
    # Run migrations
    run_migrations()

    # Start API
    start_api()


if __name__ == "__main__":
    main()