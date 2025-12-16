#!/usr/bin/env python3
"""
Startup script for Railway deployment.

Runs migrations then starts the API server.
"""
import asyncio
import os
import sys
from pathlib import Path

# Ensure the project root is importable
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


async def run_migrations():
    """Run all pending migrations."""
    from migrations.create_stock_reservations import create_stock_reservations_table

    print("Running migrations...")
    try:
        await create_stock_reservations_table()
        print("Migrations complete")
    except Exception as e:
        print(f"Migration warning (non-fatal): {e}")
        # Don't fail startup if migration fails - table might already exist


def start_api():
    """Start the API server."""
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    print(f"Starting API server on port {port}")
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)


def main():
    # Run migrations
    asyncio.run(run_migrations())

    # Start API
    start_api()


if __name__ == "__main__":
    main()
