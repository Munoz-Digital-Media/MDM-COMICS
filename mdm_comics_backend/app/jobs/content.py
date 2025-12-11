"""
Content Generation Jobs

v1.5.0: Outreach System - AI-powered content generation
"""
import logging
from datetime import timedelta

from sqlalchemy import select, update

from app.core.database import get_db_session
from app.core.config import settings
from app.core.utils import utcnow
from app.models.content_queue import ContentQueueItem, ContentStatus
from app.services.content_ai import ContentAIService
from app.services.content_templates import ContentTemplateService
from app.services.price_analytics import PriceAnalyticsService
from app.services.new_arrivals import NewArrivalsService

logger = logging.getLogger(__name__)

BATCH_SIZE = 10


async def generate_content_batch(ctx: dict) -> dict:
    """
    Generate content for pending queue items.

    Uses AI enhancement with template fallback.
    """
    async with get_db_session() as db:
        # Get pending items needing content generation
        result = await db.execute(
            select(ContentQueueItem)
            .where(ContentQueueItem.status == ContentStatus.PENDING_REVIEW)
            .where(ContentQueueItem.content.is_(None))
            .order_by(ContentQueueItem.created_at)
            .limit(BATCH_SIZE)
        )
        items = result.scalars().all()

        if not items:
            logger.debug("No pending content items")
            return {"status": "complete", "processed": 0}

        template_service = ContentTemplateService()
        ai_service = ContentAIService()

        processed = 0
        for item in items:
            try:
                content = await _generate_item_content(
                    db, item, template_service, ai_service
                )

                item.content = content.content
                item.ai_generated = content.source == "ai"
                item.updated_at = utcnow()

                processed += 1

            except Exception as e:
                logger.error(f"Content generation failed for {item.id}: {e}")

        logger.info(f"Generated content for {processed} items")

        return {
            "status": "complete",
            "processed": processed,
        }


async def _generate_item_content(
    db,
    item: ContentQueueItem,
    template_service: ContentTemplateService,
    ai_service: ContentAIService,
):
    """Generate content for a single queue item."""

    if item.content_type == "price_winner":
        # Get the price mover data
        price_service = PriceAnalyticsService(db)
        movers = await price_service.get_weekly_movers(limit=5)

        # Find matching mover
        mover = None
        for m in movers.winners:
            if m.entity_id == item.source_id and m.entity_type == item.source_type:
                mover = m
                break

        if not mover:
            # Generate fallback
            base = f"Check out this week's top performer!"
        else:
            base = await template_service.render_price_mover_post(
                mover, platform=item.platform
            )

        # Enhance with AI if enabled
        return await ai_service.enhance_blurb(
            base_content=base,
            tone="exciting",
            max_length=280 if item.platform == "bluesky" else 500,
        )

    elif item.content_type == "new_arrival":
        arrivals_service = NewArrivalsService(db)
        arrivals = await arrivals_service.get_new_arrivals(limit=20)

        # Find matching arrival
        arrival = None
        for a in arrivals.items:
            if a.entity_id == item.source_id and a.entity_type == item.source_type:
                arrival = a
                break

        if not arrival:
            base = "New items just arrived at MDM Comics!"
        else:
            base = await template_service.render_new_arrival_post(
                arrival, platform=item.platform
            )

        return await ai_service.enhance_blurb(
            base_content=base,
            tone="engaging",
            max_length=280 if item.platform == "bluesky" else 500,
        )

    elif item.content_type == "editors_choice":
        arrivals_service = NewArrivalsService(db)
        arrivals = await arrivals_service.get_new_arrivals(limit=20)

        choice = await arrivals_service.pick_editors_choice(
            arrivals.items, strategy="hybrid"
        )

        if choice:
            base = f"Editor's Choice: {choice.name}"
            if choice.price:
                base += f" - ${choice.price:.2f}"
        else:
            base = "Check out our editor's picks this week!"

        return await ai_service.enhance_blurb(
            base_content=base,
            tone="curated",
            max_length=280 if item.platform == "bluesky" else 500,
        )

    else:
        # Generic content
        return await ai_service.enhance_blurb(
            base_content=f"New update from MDM Comics!",
            tone="friendly",
            max_length=280,
        )
