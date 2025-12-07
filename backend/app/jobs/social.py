"""
Social Media Jobs

v1.5.0: Outreach System - Bluesky posting
"""
import logging
from datetime import timedelta

from sqlalchemy import select, update

from app.core.database import get_db_session
from app.core.config import settings
from app.core.utils import utcnow
from app.models.content_queue import ContentQueueItem, ContentStatus
from app.services.bluesky_client import BlueskyClient, RateLimitError

logger = logging.getLogger(__name__)


async def post_to_bluesky(
    ctx: dict,
    content_id: int,
) -> dict:
    """
    Post a specific content queue item to Bluesky.
    """
    if not settings.MARKETING_SOCIAL_ENABLED:
        return {"status": "skipped", "reason": "social_disabled"}

    async with get_db_session() as db:
        result = await db.execute(
            select(ContentQueueItem)
            .where(ContentQueueItem.id == content_id)
        )
        item = result.scalar_one_or_none()

        if not item:
            logger.warning(f"Content item {content_id} not found")
            return {"status": "error", "reason": "not_found"}

        if item.status != ContentStatus.SCHEDULED:
            logger.info(f"Content {content_id} not scheduled, status={item.status}")
            return {"status": "skipped", "reason": f"status_{item.status.value}"}

        if item.platform != "bluesky":
            logger.info(f"Content {content_id} not for Bluesky")
            return {"status": "skipped", "reason": "wrong_platform"}

        if not item.content:
            logger.warning(f"Content {content_id} has no content")
            return {"status": "error", "reason": "no_content"}

        # Update status to posting
        item.status = ContentStatus.POSTING
        item.updated_at = utcnow()
        await db.commit()

        # Create Bluesky post
        client = BlueskyClient()

        try:
            # Fetch image if available
            images = None
            if item.image_url:
                images = await _fetch_image(item.image_url)

            result = await client.create_post(
                text=item.content,
                images=images,
                idempotency_key=f"content_{item.id}",
            )

            if result.success:
                item.status = ContentStatus.POSTED
                item.posted_at = utcnow()
                item.post_url = result.post_url
                item.error_message = None

                logger.info(f"Posted content {content_id} to Bluesky: {result.post_url}")

                return {
                    "status": "posted",
                    "post_url": result.post_url,
                }
            else:
                item.status = ContentStatus.FAILED
                item.error_message = result.error
                item.retry_count = (item.retry_count or 0) + 1

                logger.error(f"Failed to post {content_id}: {result.error}")

                return {
                    "status": "failed",
                    "error": result.error,
                }

        except RateLimitError as e:
            item.status = ContentStatus.SCHEDULED  # Retry later
            item.error_message = "Rate limited, will retry"
            item.scheduled_for = utcnow() + timedelta(minutes=15)

            logger.warning(f"Rate limited posting {content_id}, rescheduled")

            return {
                "status": "rate_limited",
                "retry_at": item.scheduled_for.isoformat(),
            }

        except Exception as e:
            item.status = ContentStatus.FAILED
            item.error_message = str(e)
            item.retry_count = (item.retry_count or 0) + 1

            logger.error(f"Error posting {content_id}: {e}")

            return {
                "status": "error",
                "error": str(e),
            }

        finally:
            item.updated_at = utcnow()
            await client.close()


async def process_content_queue(ctx: dict) -> dict:
    """
    Process scheduled content items ready for posting.

    Runs periodically to post content at scheduled times.
    """
    if not settings.MARKETING_SOCIAL_ENABLED:
        return {"status": "skipped", "reason": "social_disabled"}

    async with get_db_session() as db:
        now = utcnow()

        # Get items scheduled for now or past
        result = await db.execute(
            select(ContentQueueItem)
            .where(ContentQueueItem.status == ContentStatus.SCHEDULED)
            .where(ContentQueueItem.scheduled_for <= now)
            .where(ContentQueueItem.platform == "bluesky")
            .order_by(ContentQueueItem.scheduled_for)
            .limit(5)  # Process max 5 at a time
        )
        items = result.scalars().all()

        if not items:
            logger.debug("No scheduled content ready for posting")
            return {"status": "complete", "processed": 0}

        posted = 0
        failed = 0

        client = BlueskyClient()

        try:
            for item in items:
                try:
                    # Fetch image if available
                    images = None
                    if item.image_url:
                        images = await _fetch_image(item.image_url)

                    item.status = ContentStatus.POSTING
                    item.updated_at = now
                    await db.commit()

                    result = await client.create_post(
                        text=item.content,
                        images=images,
                        idempotency_key=f"content_{item.id}",
                    )

                    if result.success:
                        item.status = ContentStatus.POSTED
                        item.posted_at = utcnow()
                        item.post_url = result.post_url
                        posted += 1
                    else:
                        item.status = ContentStatus.FAILED
                        item.error_message = result.error
                        item.retry_count = (item.retry_count or 0) + 1
                        failed += 1

                except RateLimitError:
                    # Stop processing, reschedule remaining
                    item.status = ContentStatus.SCHEDULED
                    item.scheduled_for = utcnow() + timedelta(minutes=15)
                    logger.warning("Rate limited, stopping queue processing")
                    break

                except Exception as e:
                    item.status = ContentStatus.FAILED
                    item.error_message = str(e)
                    item.retry_count = (item.retry_count or 0) + 1
                    failed += 1
                    logger.error(f"Error posting {item.id}: {e}")

                item.updated_at = utcnow()

        finally:
            await client.close()

        logger.info(f"Queue processed: {posted} posted, {failed} failed")

        return {
            "status": "complete",
            "posted": posted,
            "failed": failed,
        }


async def _fetch_image(url: str) -> list[bytes] | None:
    """Fetch image from URL for embedding."""
    import httpx

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                return [resp.content]
    except Exception as e:
        logger.warning(f"Failed to fetch image {url}: {e}")

    return None
