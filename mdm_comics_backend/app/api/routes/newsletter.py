"""
Newsletter Routes

v1.5.0: Outreach System - Newsletter subscription management
"""
import logging
import secrets
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, EmailStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.config import settings
from app.core.utils import utcnow
from app.models.newsletter import NewsletterSubscriber, SubscriberStatus
from app.services.email_utils import compute_email_hash

logger = logging.getLogger(__name__)

router = APIRouter()


class SubscribeRequest(BaseModel):
    email: EmailStr
    content_types: list[str] = ["newsletter", "price_alerts"]


class SubscribeResponse(BaseModel):
    success: bool
    message: str
    requires_confirmation: bool = True


class UnsubscribeRequest(BaseModel):
    email: EmailStr
    reason: Optional[str] = None


@router.post("/newsletter/subscribe", response_model=SubscribeResponse)
async def subscribe(
    request: SubscribeRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """
    Subscribe to newsletter.

    Double opt-in flow:
    1. Create pending subscriber
    2. Send confirmation email
    3. User clicks confirmation link
    4. Subscriber becomes confirmed
    """
    if not settings.MARKETING_NEWSLETTER_ENABLED:
        raise HTTPException(
            status_code=503,
            detail="Newsletter subscriptions are currently disabled",
        )

    # Compute email hash for deduplication and GDPR compliance
    email_hash = compute_email_hash(request.email)

    # Check for existing subscriber
    result = await db.execute(
        select(NewsletterSubscriber).where(
            NewsletterSubscriber.email_hash == email_hash
        )
    )
    existing = result.scalar_one_or_none()

    if existing:
        if existing.status == SubscriberStatus.CONFIRMED:
            return SubscribeResponse(
                success=True,
                message="You're already subscribed!",
                requires_confirmation=False,
            )
        elif existing.status == SubscriberStatus.PENDING:
            # Resend confirmation
            await _queue_confirmation_email(background_tasks, existing.id)
            return SubscribeResponse(
                success=True,
                message="Please check your email to confirm your subscription.",
                requires_confirmation=True,
            )
        elif existing.status == SubscriberStatus.UNSUBSCRIBED:
            # Reactivate
            existing.status = SubscriberStatus.PENDING
            existing.confirmation_token = secrets.token_urlsafe(32)
            existing.content_types = request.content_types
            existing.updated_at = utcnow()
            await db.commit()
            await _queue_confirmation_email(background_tasks, existing.id)
            return SubscribeResponse(
                success=True,
                message="Please check your email to reconfirm your subscription.",
                requires_confirmation=True,
            )

    # Create new subscriber
    subscriber = NewsletterSubscriber(
        email=request.email,
        email_hash=email_hash,
        content_types=request.content_types,
        status=SubscriberStatus.PENDING,
        confirmation_token=secrets.token_urlsafe(32),
        unsubscribe_token=secrets.token_urlsafe(32),
    )
    db.add(subscriber)
    await db.commit()
    await db.refresh(subscriber)

    # Queue confirmation email
    await _queue_confirmation_email(background_tasks, subscriber.id)

    logger.info(f"New newsletter subscriber: {email_hash[:16]}...")

    return SubscribeResponse(
        success=True,
        message="Please check your email to confirm your subscription.",
        requires_confirmation=True,
    )


@router.get("/newsletter/confirm")
async def confirm_subscription(
    token: str = Query(..., description="Confirmation token from email"),
    db: AsyncSession = Depends(get_db),
):
    """
    Confirm newsletter subscription.

    Validates the confirmation token and activates the subscription.
    """
    result = await db.execute(
        select(NewsletterSubscriber).where(
            NewsletterSubscriber.confirmation_token == token
        )
    )
    subscriber = result.scalar_one_or_none()

    if not subscriber:
        raise HTTPException(status_code=404, detail="Invalid confirmation token")

    if subscriber.status == SubscriberStatus.CONFIRMED:
        return {"success": True, "message": "Already confirmed!"}

    subscriber.status = SubscriberStatus.CONFIRMED
    subscriber.confirmed_at = utcnow()
    subscriber.updated_at = utcnow()
    await db.commit()

    logger.info(f"Newsletter subscription confirmed: {subscriber.email_hash[:16]}...")

    return {
        "success": True,
        "message": "Your subscription is now active. Welcome!",
    }


@router.get("/newsletter/unsubscribe")
async def unsubscribe_via_link(
    token: str = Query(..., description="Unsubscribe token from email"),
    db: AsyncSession = Depends(get_db),
):
    """
    Unsubscribe via one-click link in email.

    Uses dedicated unsubscribe token (separate from confirmation token).
    """
    result = await db.execute(
        select(NewsletterSubscriber).where(
            NewsletterSubscriber.unsubscribe_token == token
        )
    )
    subscriber = result.scalar_one_or_none()

    if not subscriber:
        raise HTTPException(status_code=404, detail="Invalid unsubscribe token")

    if subscriber.status == SubscriberStatus.UNSUBSCRIBED:
        return {"success": True, "message": "Already unsubscribed"}

    subscriber.status = SubscriberStatus.UNSUBSCRIBED
    subscriber.unsubscribed_at = utcnow()
    subscriber.updated_at = utcnow()
    await db.commit()

    logger.info(f"Newsletter unsubscribed: {subscriber.email_hash[:16]}...")

    return {
        "success": True,
        "message": "You have been unsubscribed. Sorry to see you go!",
    }


@router.post("/newsletter/unsubscribe")
async def unsubscribe_via_form(
    request: UnsubscribeRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Unsubscribe via email form.

    Alternative to one-click link for users who don't have the token.
    """
    email_hash = compute_email_hash(request.email)

    result = await db.execute(
        select(NewsletterSubscriber).where(
            NewsletterSubscriber.email_hash == email_hash
        )
    )
    subscriber = result.scalar_one_or_none()

    if not subscriber:
        # Don't reveal if email exists
        return {
            "success": True,
            "message": "If this email was subscribed, it has been removed.",
        }

    if subscriber.status == SubscriberStatus.UNSUBSCRIBED:
        return {"success": True, "message": "Already unsubscribed"}

    subscriber.status = SubscriberStatus.UNSUBSCRIBED
    subscriber.unsubscribed_at = utcnow()
    subscriber.unsubscribe_reason = request.reason
    subscriber.updated_at = utcnow()
    await db.commit()

    logger.info(f"Newsletter unsubscribed via form: {email_hash[:16]}...")

    return {
        "success": True,
        "message": "You have been unsubscribed. Sorry to see you go!",
    }


@router.get("/newsletter/status")
async def check_subscription_status(
    email: EmailStr = Query(..., description="Email to check"),
    db: AsyncSession = Depends(get_db),
):
    """
    Check subscription status for an email.

    Note: Does not reveal if email exists in system for privacy.
    """
    email_hash = compute_email_hash(email)

    result = await db.execute(
        select(NewsletterSubscriber).where(
            NewsletterSubscriber.email_hash == email_hash
        )
    )
    subscriber = result.scalar_one_or_none()

    if not subscriber:
        return {"subscribed": False}

    return {
        "subscribed": subscriber.status == SubscriberStatus.CONFIRMED,
        "status": subscriber.status.value,
        "content_types": subscriber.content_types if subscriber.status == SubscriberStatus.CONFIRMED else [],
    }


async def _queue_confirmation_email(
    background_tasks: BackgroundTasks,
    subscriber_id: int,
):
    """Queue confirmation email via ARQ or fallback to direct send."""
    try:
        # Try ARQ queue first
        from arq import create_pool
        from app.core.job_queue import parse_redis_url
        from app.core.config import settings

        if settings.ARQ_REDIS_URL:
            redis = await create_pool(parse_redis_url(settings.ARQ_REDIS_URL))
            await redis.enqueue_job(
                "process_confirmation_email",
                subscriber_id,
            )
            logger.debug(f"Queued confirmation email for subscriber {subscriber_id}")
            return
    except Exception as e:
        logger.warning(f"ARQ queue unavailable, using background task: {e}")

    # Fallback to FastAPI background task
    from app.jobs.newsletter import process_confirmation_email

    async def send_confirmation():
        await process_confirmation_email({}, subscriber_id)

    background_tasks.add_task(send_confirmation)
