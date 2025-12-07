"""
Webhook Routes

v1.5.0: Outreach System - SendGrid webhook handling
"""
import hashlib
import hmac
import logging
from typing import List

from fastapi import APIRouter, Request, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.services.email_provider import SendGridProvider

logger = logging.getLogger(__name__)

router = APIRouter()


def verify_sendgrid_signature(request: Request, body: bytes) -> bool:
    """
    Verify SendGrid webhook signature.

    FIX-002: Use correct SendGrid webhook header format
    """
    if not settings.SENDGRID_WEBHOOK_SIGNING_KEY:
        logger.warning("SENDGRID_WEBHOOK_SIGNING_KEY not set, skipping verification")
        return True  # Allow in dev mode

    # SendGrid uses X-Twilio-Email-Event-Webhook-Signature header
    signature = request.headers.get("X-Twilio-Email-Event-Webhook-Signature")
    timestamp = request.headers.get("X-Twilio-Email-Event-Webhook-Timestamp")

    if not signature or not timestamp:
        logger.warning("Missing SendGrid webhook signature headers")
        return False

    # Build the signed payload
    signed_payload = f"{timestamp}{body.decode('utf-8')}"

    # Compute expected signature
    expected = hmac.new(
        settings.SENDGRID_WEBHOOK_SIGNING_KEY.encode(),
        signed_payload.encode(),
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(signature, expected)


@router.post("/webhooks/sendgrid")
async def handle_sendgrid_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Handle SendGrid Event Webhook.

    Processes email delivery events:
    - delivered, open, click
    - bounce, dropped, spam_report
    - unsubscribe

    Returns 200 even for processing errors (to prevent retries).
    """
    body = await request.body()

    # Verify signature
    if not verify_sendgrid_signature(request, body):
        logger.warning("Invalid SendGrid webhook signature")
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        events = await request.json()
    except Exception as e:
        logger.error(f"Failed to parse webhook payload: {e}")
        return {"status": "error", "message": "Invalid JSON"}

    if not isinstance(events, list):
        events = [events]

    provider = SendGridProvider()
    processed = 0
    errors = 0

    for event in events:
        try:
            await provider.handle_webhook_event(event, db)
            processed += 1
        except Exception as e:
            logger.error(f"Failed to process event: {e}")
            errors += 1

    logger.info(f"SendGrid webhook: processed {processed}, errors {errors}")

    return {
        "status": "ok",
        "processed": processed,
        "errors": errors,
    }


@router.get("/webhooks/sendgrid/verify")
async def verify_webhook_setup():
    """
    Verification endpoint for SendGrid webhook setup.

    Returns configuration status for debugging.
    """
    return {
        "status": "configured" if settings.SENDGRID_WEBHOOK_SIGNING_KEY else "unconfigured",
        "newsletter_enabled": settings.MARKETING_NEWSLETTER_ENABLED,
    }
