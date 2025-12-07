"""
Email Provider (SendGrid)

v1.5.0: Outreach System - Newsletter and transactional email sending
"""
import hmac
import hashlib
import json
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

import httpx
from sqlalchemy import select

from app.core.config import settings
from app.core.database import get_db_session
from app.core.utils import utcnow

logger = logging.getLogger(__name__)


@dataclass
class SendResult:
    success: bool
    sent_count: int = 0
    failed_count: int = 0
    message_id: Optional[str] = None
    error: Optional[str] = None


class SendGridProvider:
    """SendGrid email provider with full event tracking."""

    BASE_URL = "https://api.sendgrid.com/v3"

    def __init__(self):
        self.api_key = settings.SENDGRID_API_KEY
        self.from_email = settings.SENDGRID_FROM_EMAIL
        self.from_name = settings.SENDGRID_FROM_NAME
        self.webhook_key = settings.SENDGRID_WEBHOOK_SIGNING_KEY
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                timeout=30.0,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._http_client

    async def close(self):
        """Explicit cleanup method."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    async def send_newsletter(
        self,
        template_id: str,
        subscribers: List,
        dynamic_data: Dict[str, Any],
        campaign_id: str,
    ) -> SendResult:
        """Send newsletter with proper unsubscribe links."""
        if not settings.MARKETING_NEWSLETTER_ENABLED:
            logger.warning("Newsletter disabled, skipping send")
            return SendResult(success=False, error="Newsletter disabled")

        if not subscribers:
            return SendResult(success=True, sent_count=0)

        sent = 0
        failed = 0
        batch_size = 1000

        http = await self._get_http_client()

        for i in range(0, len(subscribers), batch_size):
            batch = subscribers[i:i + batch_size]

            personalizations = []
            for sub in batch:
                personalizations.append({
                    "to": [{"email": sub.email, "name": sub.name or ""}],
                    "dynamic_template_data": {
                        **dynamic_data,
                        "subscriber_id": sub.id,
                        "unsubscribe_url": f"{settings.APP_URL}/newsletter/unsubscribe?token={sub.unsubscribe_token}",
                    },
                })

            payload = {
                "personalizations": personalizations,
                "from": {"email": self.from_email, "name": self.from_name},
                "template_id": template_id,
                "headers": {
                    "List-Unsubscribe": f"<{settings.APP_URL}/api/newsletter/unsubscribe>",
                    "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
                },
                "tracking_settings": {
                    "click_tracking": {"enable": True},
                    "open_tracking": {"enable": True},
                },
                "categories": [campaign_id],
            }

            try:
                resp = await http.post(f"{self.BASE_URL}/mail/send", json=payload)

                if resp.status_code in (200, 202):
                    sent += len(batch)
                else:
                    failed += len(batch)
                    logger.error(f"SendGrid batch failed: {resp.status_code} - {resp.text}")
            except Exception as e:
                failed += len(batch)
                logger.error(f"SendGrid batch exception: {e}")

        return SendResult(
            success=failed == 0,
            sent_count=sent,
            failed_count=failed,
        )

    async def send_transactional(
        self,
        to_email: str,
        template_id: str,
        dynamic_data: Dict[str, Any],
    ) -> SendResult:
        """Send single transactional email (confirmation, etc.)."""
        if not settings.SENDGRID_API_KEY:
            logger.warning("SendGrid API key not configured")
            return SendResult(success=False, error="Email not configured")

        http = await self._get_http_client()

        payload = {
            "personalizations": [{
                "to": [{"email": to_email}],
                "dynamic_template_data": dynamic_data,
            }],
            "from": {"email": self.from_email, "name": self.from_name},
            "template_id": template_id,
        }

        try:
            resp = await http.post(f"{self.BASE_URL}/mail/send", json=payload)

            if resp.status_code in (200, 202):
                return SendResult(
                    success=True,
                    sent_count=1,
                    message_id=resp.headers.get("X-Message-Id"),
                )
            else:
                return SendResult(success=False, error=resp.text)
        except Exception as e:
            return SendResult(success=False, error=str(e))

    def verify_webhook_signature(
        self,
        payload: bytes,
        signature: str,
        timestamp: str,
    ) -> bool:
        """Verify SendGrid webhook signature."""
        if not self.webhook_key:
            logger.warning("SENDGRID_WEBHOOK_SIGNING_KEY not set - skipping validation")
            return True

        timestamped_payload = timestamp.encode() + payload
        expected = hmac.new(
            self.webhook_key.encode(),
            timestamped_payload,
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature, expected)

    async def handle_webhook(
        self,
        payload: Dict,
        signature: str,
        timestamp: str,
        raw_body: bytes,
    ) -> None:
        """Process SendGrid webhooks with event persistence."""
        if not self.verify_webhook_signature(raw_body, signature, timestamp):
            raise ValueError("Invalid webhook signature")

        for event in payload:
            event_type = event.get("event")
            email = event.get("email")

            if not email:
                continue

            try:
                if event_type == "bounce":
                    await self._handle_bounce(email, event)
                elif event_type == "spamreport":
                    await self._handle_complaint(email)
                elif event_type == "open":
                    await self._handle_open(email, event)
                elif event_type == "click":
                    await self._handle_click(email, event)
                elif event_type == "delivered":
                    await self._handle_delivered(email, event)
            except Exception as e:
                logger.error(f"Failed to process {event_type} event for {email}: {e}")

    async def _handle_bounce(self, email: str, event: Dict) -> None:
        """Handle bounce - update subscriber and log event."""
        from app.models.newsletter import NewsletterSubscriber, EmailEvent

        bounce_type = "hard" if event.get("type") == "bounce" else "soft"

        try:
            async with get_db_session() as db:
                result = await db.execute(
                    select(NewsletterSubscriber).where(NewsletterSubscriber.email == email)
                )
                sub = result.scalar_one_or_none()

                if sub:
                    sub.last_bounce_at = utcnow()
                    sub.last_bounce_type = bounce_type

                    if bounce_type == "hard":
                        sub.hard_bounce_count += 1
                    else:
                        sub.soft_bounce_count += 1

                email_event = EmailEvent(
                    subscriber_id=sub.id if sub else None,
                    email=email,
                    event_type=f"bounce_{bounce_type}",
                    event_data=json.dumps({"reason": event.get("reason")}),
                    campaign_id=self._extract_campaign_id(event),
                )
                db.add(email_event)
                await db.commit()

                logger.info(f"Recorded {bounce_type} bounce for {email}")
        except Exception as e:
            logger.error(f"Failed to handle bounce for {email}: {e}")
            raise

    async def _handle_complaint(self, email: str) -> None:
        """Handle spam complaint - unsubscribe and log."""
        from app.models.newsletter import NewsletterSubscriber, EmailEvent

        try:
            async with get_db_session() as db:
                result = await db.execute(
                    select(NewsletterSubscriber).where(NewsletterSubscriber.email == email)
                )
                sub = result.scalar_one_or_none()

                if sub:
                    sub.complaint_at = utcnow()
                    sub.status = "unsubscribed"
                    sub.unsubscribe_reason = "spam_complaint"

                email_event = EmailEvent(
                    subscriber_id=sub.id if sub else None,
                    email=email,
                    event_type="complaint",
                )
                db.add(email_event)
                await db.commit()

                logger.warning(f"Spam complaint from {email}")
        except Exception as e:
            logger.error(f"Failed to handle complaint for {email}: {e}")
            raise

    async def _handle_open(self, email: str, event: Dict) -> None:
        """Handle email open - log event."""
        from app.models.newsletter import NewsletterSubscriber, EmailEvent

        try:
            async with get_db_session() as db:
                result = await db.execute(
                    select(NewsletterSubscriber).where(NewsletterSubscriber.email == email)
                )
                sub = result.scalar_one_or_none()

                email_event = EmailEvent(
                    subscriber_id=sub.id if sub else None,
                    email=email,
                    event_type="open",
                    campaign_id=self._extract_campaign_id(event),
                    message_id=event.get("sg_message_id"),
                )
                db.add(email_event)
                await db.commit()
        except Exception as e:
            logger.error(f"Failed to handle open for {email}: {e}")

    async def _handle_click(self, email: str, event: Dict) -> None:
        """Handle link click - log event with URL."""
        from app.models.newsletter import NewsletterSubscriber, EmailEvent

        try:
            async with get_db_session() as db:
                result = await db.execute(
                    select(NewsletterSubscriber).where(NewsletterSubscriber.email == email)
                )
                sub = result.scalar_one_or_none()

                email_event = EmailEvent(
                    subscriber_id=sub.id if sub else None,
                    email=email,
                    event_type="click",
                    event_data=json.dumps({"url": event.get("url")}),
                    campaign_id=self._extract_campaign_id(event),
                )
                db.add(email_event)
                await db.commit()
        except Exception as e:
            logger.error(f"Failed to handle click for {email}: {e}")

    async def _handle_delivered(self, email: str, event: Dict) -> None:
        """Handle successful delivery - log event."""
        from app.models.newsletter import EmailEvent

        try:
            async with get_db_session() as db:
                email_event = EmailEvent(
                    email=email,
                    event_type="delivered",
                    campaign_id=self._extract_campaign_id(event),
                    message_id=event.get("sg_message_id"),
                )
                db.add(email_event)
                await db.commit()
        except Exception as e:
            logger.error(f"Failed to handle delivered for {email}: {e}")

    def _extract_campaign_id(self, event: Dict) -> Optional[str]:
        """Helper to safely extract campaign_id from categories."""
        categories = event.get("category", [])
        if categories and isinstance(categories, list):
            return categories[0]
        return None
