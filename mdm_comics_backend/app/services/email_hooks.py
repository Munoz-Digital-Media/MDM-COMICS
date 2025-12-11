"""
Email Integration Hooks

Defines interface for email service integration.
Actual email sending to be implemented with chosen provider.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional, Protocol

logger = logging.getLogger(__name__)


class EmailProvider(Protocol):
    """Protocol for email providers."""

    async def send_abandonment_email(
        self,
        to_email: str,
        to_name: str,
        cart_items: list,
        cart_value: float,
        coupon_code: Optional[str],
        coupon_discount: Optional[float],
    ) -> bool:
        """Send cart abandonment recovery email."""
        ...

    async def send_contact_notification(
        self,
        name: str,
        email: str,
        subject: str,
        message: str,
        reference_id: str,
    ) -> bool:
        """Send contact form notification to admin."""
        ...


class MockEmailProvider:
    """Mock provider for development/testing."""

    async def send_abandonment_email(
        self,
        to_email: str,
        to_name: str,
        cart_items: list,
        cart_value: float,
        coupon_code: Optional[str],
        coupon_discount: Optional[float],
    ) -> bool:
        coupon_msg = f"  Coupon: {coupon_code} ({coupon_discount}% off)" if coupon_code else ""
        logger.info(
            f"[MOCK EMAIL] Abandonment recovery to {to_email}\n"
            f"  Name: {to_name}\n"
            f"  Items: {len(cart_items)}\n"
            f"  Value: ${cart_value:.2f}\n"
            f"{coupon_msg}"
        )
        return True

    async def send_contact_notification(
        self,
        name: str,
        email: str,
        subject: str,
        message: str,
        reference_id: str,
    ) -> bool:
        """Mock contact notification - logs instead of sending email."""
        logger.info(
            f"[MOCK EMAIL] Contact form notification\n"
            f"  Reference: {reference_id}\n"
            f"  From: {name} <{email}>\n"
            f"  Subject: {subject}\n"
            f"  Message preview: {message[:100]}..."
        )
        return True


class EmailService:
    """
    Email service wrapper.

    Replace MockEmailProvider with actual provider (SendGrid, Postmark, etc.)
    """

    def __init__(self, provider: Optional[EmailProvider] = None):
        self.provider = provider or MockEmailProvider()

    async def send_cart_recovery(
        self,
        abandonment: Any,  # CartAbandonmentQueue
        coupon: Optional[Any] = None,  # Coupon
    ) -> bool:
        """Send cart recovery email."""
        try:
            success = await self.provider.send_abandonment_email(
                to_email=abandonment.user_email,
                to_name=abandonment.user_name or "Valued Customer",
                cart_items=abandonment.cart_snapshot,
                cart_value=float(abandonment.cart_value),
                coupon_code=coupon.code if coupon else None,
                coupon_discount=float(coupon.discount_value) if coupon else None,
            )
            return success
        except Exception as e:
            logger.error(f"Failed to send recovery email: {e}")
            return False

    async def send_contact_notification(
        self,
        name: str,
        email: str,
        subject: str,
        message: str,
        reference_id: str,
    ) -> bool:
        """
        Send contact form notification to admin.

        IMPL-001: Contact form notification wrapper.
        """
        try:
            success = await self.provider.send_contact_notification(
                name=name,
                email=email,
                subject=subject,
                message=message,
                reference_id=reference_id,
            )
            return success
        except Exception as e:
            logger.error(f"Failed to send contact notification: {e}")
            return False


# Singleton
_service: Optional[EmailService] = None


def get_email_service() -> EmailService:
    global _service
    if _service is None:
        _service = EmailService()
    return _service
