#!/usr/bin/env python3
"""
MDM Comics - Standalone Cron Runner v1.7.0

This script runs the pipeline scheduler as a standalone service.
Used by Railway's data-pipeline-cron service.

Jobs managed:
1. comic_enrichment - Enrich comics from Metron (every 30 min)
2. funko_price_check - Check Funko prices from PriceCharting (every 60 min)
3. dlq_retry - Retry failed jobs from DLQ (every 15 min)
4. daily_snapshot - AI/ML training data capture (every 24 hours)

Per constitution_db.json: Uses same database config as main backend.
Per constitution_cyberSec.json: Requires SECRET_KEY, DATABASE_URL env vars.
"""
import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timezone

# Setup logging first
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Validate required environment variables
REQUIRED_VARS = ["DATABASE_URL", "SECRET_KEY"]
missing = [var for var in REQUIRED_VARS if not os.getenv(var)]
if missing:
    logger.error(f"Missing required environment variables: {missing}")
    logger.error("Please set these variables in Railway service settings.")
    sys.exit(1)

# Import after env validation
from app.jobs.pipeline_scheduler import pipeline_scheduler

# Graceful shutdown flag
_shutdown = False


def handle_shutdown(signum, frame):
    """Handle shutdown signals gracefully."""
    global _shutdown
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    _shutdown = True


async def main():
    """Main entry point for cron service."""
    global _shutdown

    logger.info("=" * 60)
    logger.info("MDM Comics Cron Service v1.7.0")
    logger.info("=" * 60)
    logger.info(f"Started at: {datetime.now(timezone.utc).isoformat()}")
    logger.info(f"DATABASE_URL: {os.getenv('DATABASE_URL', '')[:50]}...")
    logger.info(f"PRICECHARTING_API_TOKEN: {'set' if os.getenv('PRICECHARTING_API_TOKEN') else 'NOT SET'}")

    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
        # Start the pipeline scheduler
        await pipeline_scheduler.start()

        # Keep running until shutdown
        logger.info("Cron service running. Press Ctrl+C to stop.")
        while not _shutdown:
            await asyncio.sleep(10)  # Check every 10 seconds

    except Exception as e:
        logger.error(f"Cron service error: {e}")
        raise
    finally:
        logger.info("Stopping pipeline scheduler...")
        await pipeline_scheduler.stop()
        logger.info("Cron service stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
