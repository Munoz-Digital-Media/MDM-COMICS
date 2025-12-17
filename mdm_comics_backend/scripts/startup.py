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
        print("Stock reservations migration complete")
    except Exception as e:
        print(f"Stock reservations migration warning (non-fatal): {e}")

    # Feature flags migration (shipping compartmentalization)
    try:
        from migrations.m_2025_12_16_feature_flags import main as feature_flags_main
        await feature_flags_main()
        print("Feature flags migration complete")
    except Exception as e:
        print(f"Feature flags migration warning (non-fatal): {e}")

    # Carrier credentials_json migration
    try:
        from migrations.m_2025_12_16_carrier_credentials_json import run_migration as credentials_migration
        await credentials_migration()
        print("Carrier credentials migration complete")
    except Exception as e:
        print(f"Carrier credentials migration warning (non-fatal): {e}")


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
