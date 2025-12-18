"""
One-time cleanup script for rejected match images.

Deletes S3 objects and local files for all rejected matches.
Database records are preserved for ML training.

Usage:
    python -m app.scripts.cleanup_rejected_matches
"""

import asyncio
import os
import logging
from sqlalchemy import select, text
from app.core.database import AsyncSessionLocal
from app.services.storage import StorageService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def cleanup_rejected_matches():
    """Clean up images from all rejected matches."""

    storage = StorageService()
    s3_configured = storage.is_configured()

    stats = {
        "total_rejected": 0,
        "s3_deleted": 0,
        "s3_failed": 0,
        "local_deleted": 0,
        "local_failed": 0,
        "no_images": 0,
    }

    async with AsyncSessionLocal() as db:
        # Get all rejected matches with candidate_data
        result = await db.execute(
            text("""
                SELECT id, candidate_data
                FROM match_review_queue
                WHERE status = 'rejected'
                AND candidate_data IS NOT NULL
            """)
        )
        rejected = result.fetchall()
        stats["total_rejected"] = len(rejected)

        logger.info(f"Found {len(rejected)} rejected matches to clean up")

        for row in rejected:
            match_id = row[0]
            candidate_data = row[1] or {}

            has_image = False

            # Delete S3 object
            s3_key = candidate_data.get('s3_key')
            if s3_key:
                has_image = True
                if s3_configured:
                    try:
                        deleted = await storage.delete_object(s3_key)
                        if deleted:
                            stats["s3_deleted"] += 1
                            logger.info(f"[{match_id}] Deleted S3: {s3_key}")
                        else:
                            stats["s3_failed"] += 1
                            logger.warning(f"[{match_id}] Failed to delete S3: {s3_key}")
                    except Exception as e:
                        stats["s3_failed"] += 1
                        logger.error(f"[{match_id}] S3 delete error: {e}")
                else:
                    logger.warning(f"[{match_id}] S3 not configured, skipping: {s3_key}")
                    stats["s3_failed"] += 1

            # Delete local file
            file_path = candidate_data.get('file_path')
            if file_path and os.path.exists(file_path):
                has_image = True
                try:
                    os.remove(file_path)
                    stats["local_deleted"] += 1
                    logger.info(f"[{match_id}] Deleted local: {file_path}")
                except Exception as e:
                    stats["local_failed"] += 1
                    logger.error(f"[{match_id}] Local delete error: {e}")

            if not has_image:
                stats["no_images"] += 1

    # Summary
    logger.info("=" * 50)
    logger.info("CLEANUP COMPLETE")
    logger.info("=" * 50)
    logger.info(f"Total rejected matches: {stats['total_rejected']}")
    logger.info(f"S3 objects deleted:     {stats['s3_deleted']}")
    logger.info(f"S3 delete failures:     {stats['s3_failed']}")
    logger.info(f"Local files deleted:    {stats['local_deleted']}")
    logger.info(f"Local delete failures:  {stats['local_failed']}")
    logger.info(f"No images to clean:     {stats['no_images']}")

    return stats


if __name__ == "__main__":
    asyncio.run(cleanup_rejected_matches())
