"""
Social Media Jobs

v1.5.0: Outreach System - Bluesky posting
v1.5.1: Phase 1 - Added Facebook posting via Meta Graph API
v1.5.2: The Rack Factor - Daily social post generation (4:30 PM EST)
"""
import logging
from datetime import timedelta, time

from sqlalchemy import select, update

from app.core.database import get_db_session
from app.core.config import settings
from app.core.utils import utcnow
from app.models.content_queue import ContentQueueItem, ContentStatus
from app.services.bluesky_client import BlueskyClient, RateLimitError
from app.services.meta_client import MetaClient, RateLimitError as MetaRateLimitError
from app.services.rack_report import RackReportService

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


# ============================================================
# v1.5.1: Facebook Posting via Meta Graph API (Phase 1)
# ============================================================

async def post_to_facebook(
    ctx: dict,
    content_id: int,
) -> dict:
    """
    Post a specific content queue item to Facebook.

    Phase 1: Text-only posts with optional link attachment.
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

        if item.platform != "facebook":
            logger.info(f"Content {content_id} not for Facebook")
            return {"status": "skipped", "reason": "wrong_platform"}

        # Get content (prefer final_content > ai_enhanced > original)
        content = item.final_content or item.ai_enhanced_content or item.original_content
        if not content:
            logger.warning(f"Content {content_id} has no content")
            return {"status": "error", "reason": "no_content"}

        # Update status to posting
        item.status = ContentStatus.POSTING
        item.updated_at = utcnow()
        await db.commit()

        # Create Facebook post
        client = MetaClient()

        try:
            # Phase 1: Text post with optional link
            result = await client.create_facebook_post(
                text=content,
                link=item.image_url if item.image_url and item.image_url.startswith("http") else None,
                idempotency_key=f"content_{item.id}",
            )

            if result.success:
                item.status = ContentStatus.POSTED
                item.posted_at = utcnow()
                item.post_url = result.post_url
                item.external_post_id = result.post_id
                item.error_message = None

                logger.info(f"Posted content {content_id} to Facebook: {result.post_url}")

                return {
                    "status": "posted",
                    "post_url": result.post_url,
                    "platform": "facebook",
                }
            else:
                item.status = ContentStatus.FAILED
                item.error_message = result.error
                item.retry_count = (item.retry_count or 0) + 1

                logger.error(f"Failed to post {content_id} to Facebook: {result.error}")

                return {
                    "status": "failed",
                    "error": result.error,
                    "platform": "facebook",
                }

        except MetaRateLimitError as e:
            item.status = ContentStatus.SCHEDULED  # Retry later
            item.error_message = "Rate limited, will retry"
            item.scheduled_for = utcnow() + timedelta(minutes=30)

            logger.warning(f"Rate limited posting {content_id} to Facebook, rescheduled")

            return {
                "status": "rate_limited",
                "retry_at": item.scheduled_for.isoformat(),
                "platform": "facebook",
            }

        except Exception as e:
            item.status = ContentStatus.FAILED
            item.error_message = str(e)
            item.retry_count = (item.retry_count or 0) + 1

            logger.error(f"Error posting {content_id} to Facebook: {e}")

            return {
                "status": "error",
                "error": str(e),
                "platform": "facebook",
            }

        finally:
            item.updated_at = utcnow()
            await client.close()


async def process_all_social_queues(ctx: dict) -> dict:
    """
    Process content queue for all social platforms.

    v1.5.1: Processes Bluesky and Facebook queues.
    Future phases will add Instagram (Phase 2) and TikTok (Phase 3).
    """
    if not settings.MARKETING_SOCIAL_ENABLED:
        return {"status": "skipped", "reason": "social_disabled"}

    results = {
        "bluesky": {"posted": 0, "failed": 0},
        "facebook": {"posted": 0, "failed": 0},
    }

    async with get_db_session() as db:
        now = utcnow()

        # Get items scheduled for all supported platforms
        result = await db.execute(
            select(ContentQueueItem)
            .where(ContentQueueItem.status == ContentStatus.SCHEDULED)
            .where(ContentQueueItem.scheduled_for <= now)
            .where(ContentQueueItem.platform.in_(["bluesky", "facebook"]))
            .order_by(ContentQueueItem.scheduled_for)
            .limit(10)  # Process max 10 at a time
        )
        items = result.scalars().all()

        if not items:
            logger.debug("No scheduled content ready for posting")
            return {"status": "complete", "results": results}

        # Group by platform
        bluesky_items = [i for i in items if i.platform == "bluesky"]
        facebook_items = [i for i in items if i.platform == "facebook"]

        # Process Bluesky
        if bluesky_items:
            client = BlueskyClient()
            try:
                for item in bluesky_items:
                    try:
                        content = item.final_content or item.ai_enhanced_content or item.original_content

                        images = None
                        if item.image_url:
                            images = await _fetch_image(item.image_url)

                        item.status = ContentStatus.POSTING
                        item.updated_at = now
                        await db.commit()

                        post_result = await client.create_post(
                            text=content,
                            images=images,
                            idempotency_key=f"content_{item.id}",
                        )

                        if post_result.success:
                            item.status = ContentStatus.POSTED
                            item.posted_at = utcnow()
                            item.post_url = post_result.post_url
                            results["bluesky"]["posted"] += 1
                        else:
                            item.status = ContentStatus.FAILED
                            item.error_message = post_result.error
                            item.retry_count = (item.retry_count or 0) + 1
                            results["bluesky"]["failed"] += 1

                    except RateLimitError:
                        item.status = ContentStatus.SCHEDULED
                        item.scheduled_for = utcnow() + timedelta(minutes=15)
                        break

                    except Exception as e:
                        item.status = ContentStatus.FAILED
                        item.error_message = str(e)
                        results["bluesky"]["failed"] += 1

                    item.updated_at = utcnow()

            finally:
                await client.close()

        # Process Facebook
        if facebook_items:
            client = MetaClient()
            try:
                for item in facebook_items:
                    try:
                        content = item.final_content or item.ai_enhanced_content or item.original_content

                        item.status = ContentStatus.POSTING
                        item.updated_at = now
                        await db.commit()

                        post_result = await client.create_facebook_post(
                            text=content,
                            link=item.image_url if item.image_url and item.image_url.startswith("http") else None,
                            idempotency_key=f"content_{item.id}",
                        )

                        if post_result.success:
                            item.status = ContentStatus.POSTED
                            item.posted_at = utcnow()
                            item.post_url = post_result.post_url
                            item.external_post_id = post_result.post_id
                            results["facebook"]["posted"] += 1
                        else:
                            item.status = ContentStatus.FAILED
                            item.error_message = post_result.error
                            item.retry_count = (item.retry_count or 0) + 1
                            results["facebook"]["failed"] += 1

                    except MetaRateLimitError:
                        item.status = ContentStatus.SCHEDULED
                        item.scheduled_for = utcnow() + timedelta(minutes=30)
                        break

                    except Exception as e:
                        item.status = ContentStatus.FAILED
                        item.error_message = str(e)
                        results["facebook"]["failed"] += 1

                    item.updated_at = utcnow()

            finally:
                await client.close()

        await db.commit()

    logger.info(
        f"Social queues processed: "
        f"Bluesky {results['bluesky']['posted']}/{results['bluesky']['posted'] + results['bluesky']['failed']}, "
        f"Facebook {results['facebook']['posted']}/{results['facebook']['posted'] + results['facebook']['failed']}"
    )

    return {
        "status": "complete",
        "results": results,
    }


# ============================================================
# v1.5.2: The Rack Factor - Daily Social Post Generation
# Schedule: 4:30 PM EST (21:30 UTC)
# ============================================================

async def generate_daily_rack_factor_posts(ctx: dict) -> dict:
    """
    Generate daily Rack Factor social media posts.

    Schedule: Runs at 4:30 PM EST daily (21:30 UTC)
    Content: Top 5 price gainers + Top 5 price losers

    Posts are queued for approval/auto-posting based on settings.
    """
    if not settings.MARKETING_SOCIAL_ENABLED:
        return {"status": "skipped", "reason": "social_disabled"}

    async with get_db_session() as db:
        rack_service = RackReportService(db)

        try:
            # Generate daily social posts (Top 5 up + Top 5 down)
            platforms = ["bluesky", "facebook"]
            posts = await rack_service.generate_daily_social_posts(platforms=platforms)

            if not posts:
                logger.info("No daily price movers to post about")
                return {
                    "status": "complete",
                    "queued": 0,
                    "reason": "no_movers",
                }

            # Queue posts for approval/posting
            queued = await rack_service.queue_social_posts(
                posts,
                schedule_offset_minutes=5,  # 5 min between posts
            )

            logger.info(f"Generated {len(posts)} daily Rack Factor posts, queued {queued}")

            return {
                "status": "complete",
                "generated": len(posts),
                "queued": queued,
                "platforms": platforms,
            }

        except Exception as e:
            logger.error(f"Failed to generate daily Rack Factor posts: {e}")
            return {
                "status": "error",
                "error": str(e),
            }


async def generate_weekly_rack_factor_newsletter(ctx: dict) -> dict:
    """
    Generate weekly Rack Factor newsletter and social posts.

    Schedule: Runs every Friday
    Content: The full Rack Factor Recap newsletter
    """
    if not settings.MARKETING_NEWSLETTER_ENABLED:
        return {"status": "skipped", "reason": "newsletter_disabled"}

    async with get_db_session() as db:
        rack_service = RackReportService(db)

        try:
            # Generate weekly report data
            report = await rack_service.generate_weekly_report()

            # Generate social posts from weekly data
            if settings.MARKETING_SOCIAL_ENABLED:
                platforms = ["bluesky", "facebook"]
                posts = await rack_service.generate_weekly_social_posts(
                    report,
                    platforms=platforms,
                )
                queued = await rack_service.queue_social_posts(posts)
            else:
                posts = []
                queued = 0

            logger.info(
                f"Generated Rack Factor Report Week {report.week_number}: "
                f"Portfolio ${report.portfolio.total_value:,.2f}, "
                f"{report.total_price_increases} up, {report.total_price_decreases} down, "
                f"{queued} social posts queued"
            )

            return {
                "status": "complete",
                "week_number": report.week_number,
                "year": report.year,
                "portfolio_value": float(report.portfolio.total_value),
                "flagrant": report.flagrant_winner.name if report.flagrant_winner else None,
                "total_increases": report.total_price_increases,
                "total_decreases": report.total_price_decreases,
                "social_posts_queued": queued,
            }

        except Exception as e:
            logger.error(f"Failed to generate weekly Rack Factor: {e}")
            return {
                "status": "error",
                "error": str(e),
            }
