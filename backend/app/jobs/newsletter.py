"""
Newsletter Jobs

v1.5.0: Outreach System - Newsletter generation and sending
"""
import logging
from typing import List, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.core.config import settings
from app.core.utils import utcnow
from app.models.newsletter import NewsletterSubscriber, SubscriberStatus
from app.services.email_provider import SendGridProvider
from app.services.price_analytics import PriceAnalyticsService
from app.services.new_arrivals import NewArrivalsService
from app.services.content_templates import ContentTemplateService

logger = logging.getLogger(__name__)

BATCH_SIZE = 100


async def generate_weekly_newsletter(ctx: dict) -> dict:
    """
    Generate weekly newsletter content.

    Aggregates price movers, new arrivals, and renders HTML.
    """
    if not settings.MARKETING_NEWSLETTER_ENABLED:
        logger.info("Newsletter disabled, skipping generation")
        return {"status": "skipped", "reason": "disabled"}

    async with get_db_session() as db:
        # Get price movers
        price_service = PriceAnalyticsService(db)
        movers = await price_service.get_weekly_movers(limit=5)

        # Get new arrivals
        arrivals_service = NewArrivalsService(db)
        arrivals = await arrivals_service.get_new_arrivals(limit=10)

        # Pick editor's choice
        editors_choice = await arrivals_service.pick_editors_choice(
            arrivals.items,
            strategy="hybrid",
        )

        # Render templates
        template_service = ContentTemplateService()

        sections = {
            "price_winners": await template_service.render_newsletter_section(
                "price_winners",
                {"items": movers.winners},
            ),
            "price_losers": await template_service.render_newsletter_section(
                "price_losers",
                {"items": movers.losers},
            ),
            "new_arrivals": await template_service.render_newsletter_section(
                "new_arrivals",
                {"items": arrivals.items},
            ),
            "editors_choice": await template_service.render_newsletter_section(
                "editors_choice",
                {"item": editors_choice},
            ) if editors_choice else "",
        }

        # Store newsletter content for batch sending
        newsletter_data = {
            "generated_at": utcnow().isoformat(),
            "sections": sections,
            "stats": {
                "winners_count": len(movers.winners),
                "losers_count": len(movers.losers),
                "arrivals_count": arrivals.total_count,
            },
        }

        logger.info(
            f"Newsletter generated: {len(movers.winners)} winners, "
            f"{len(movers.losers)} losers, {arrivals.total_count} arrivals"
        )

        return {
            "status": "generated",
            "data": newsletter_data,
        }


async def send_newsletter_batch(
    ctx: dict,
    newsletter_data: dict,
    offset: int = 0,
) -> dict:
    """
    Send newsletter to a batch of subscribers.

    Uses offset pagination to process all subscribers.
    """
    if not settings.MARKETING_NEWSLETTER_ENABLED:
        return {"status": "skipped", "reason": "disabled"}

    async with get_db_session() as db:
        # Get confirmed subscribers
        result = await db.execute(
            select(NewsletterSubscriber)
            .where(NewsletterSubscriber.status == SubscriberStatus.CONFIRMED)
            .where(NewsletterSubscriber.content_types.contains(["newsletter"]))
            .order_by(NewsletterSubscriber.id)
            .offset(offset)
            .limit(BATCH_SIZE)
        )
        subscribers = result.scalars().all()

        if not subscribers:
            logger.info(f"No more subscribers at offset {offset}")
            return {"status": "complete", "sent": 0, "offset": offset}

        # Send via SendGrid
        provider = SendGridProvider()
        sent_count = 0
        failed_count = 0

        for sub in subscribers:
            try:
                success = await provider.send_newsletter(
                    to_email=sub.email,
                    subscriber_id=sub.id,
                    template_data=newsletter_data,
                )
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.error(f"Failed to send to {sub.email}: {e}")
                failed_count += 1

        # Schedule next batch if we got a full batch
        next_offset = offset + BATCH_SIZE
        has_more = len(subscribers) == BATCH_SIZE

        logger.info(
            f"Newsletter batch sent: {sent_count} success, {failed_count} failed, "
            f"offset {offset}, has_more={has_more}"
        )

        return {
            "status": "batch_complete",
            "sent": sent_count,
            "failed": failed_count,
            "offset": offset,
            "next_offset": next_offset if has_more else None,
        }


async def process_confirmation_email(
    ctx: dict,
    subscriber_id: int,
) -> dict:
    """
    Send confirmation email to new subscriber.
    """
    async with get_db_session() as db:
        result = await db.execute(
            select(NewsletterSubscriber)
            .where(NewsletterSubscriber.id == subscriber_id)
        )
        subscriber = result.scalar_one_or_none()

        if not subscriber:
            logger.warning(f"Subscriber {subscriber_id} not found")
            return {"status": "error", "reason": "not_found"}

        if subscriber.status != SubscriberStatus.PENDING:
            logger.info(f"Subscriber {subscriber_id} already confirmed")
            return {"status": "skipped", "reason": "already_confirmed"}

        # Build confirmation URL
        confirm_url = (
            f"{settings.APP_URL}/newsletter/confirm"
            f"?token={subscriber.confirmation_token}"
        )

        provider = SendGridProvider()
        success = await provider.send_transactional(
            to_email=subscriber.email,
            subject="Confirm your MDM Comics subscription",
            template_data={
                "confirm_url": confirm_url,
                "email": subscriber.email,
            },
        )

        if success:
            logger.info(f"Confirmation email sent to {subscriber.email}")
            return {"status": "sent"}
        else:
            logger.error(f"Failed to send confirmation to {subscriber.email}")
            return {"status": "error", "reason": "send_failed"}
