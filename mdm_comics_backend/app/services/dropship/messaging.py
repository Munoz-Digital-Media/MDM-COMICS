"""
Dropship Messaging Templates

Customer-facing messages for dropship orders.
Designed to preemptively address BCW visibility issues.

Key Principles:
1. Proactive transparency about fulfillment partner
2. Set accurate expectations for shipping times
3. Prepare customers for BCW-branded tracking emails
4. Maintain MDM branding throughout communication
"""
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum


class MessageType(str, Enum):
    """Types of dropship messages."""
    ORDER_CONFIRMATION = "order_confirmation"
    SHIPPING_NOTIFICATION = "shipping_notification"
    DELIVERY_CONFIRMATION = "delivery_confirmation"
    BACKORDER_NOTICE = "backorder_notice"
    TRACKING_PAGE = "tracking_page"
    ERROR = "error"


@dataclass
class DropshipMessage:
    """A formatted customer message."""
    subject: str
    body_text: str
    body_html: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class DropshipMessaging:
    """
    Generates customer-facing messages for dropship orders.

    All messages are designed to:
    - Preemptively explain fulfillment partner involvement
    - Set accurate expectations
    - Maintain brand consistency
    """

    # Company name for branding
    COMPANY_NAME = "MDM Comics"
    FULFILLMENT_PARTNER_NAME = "BCW Supplies"

    # Standard processing time
    PROCESSING_DAYS = "1-2 business days"

    @classmethod
    def order_confirmation(
        cls,
        order_number: str,
        customer_name: str,
        items: list,
        shipping_address: Dict[str, str],
        estimated_ship_date: Optional[datetime] = None,
    ) -> DropshipMessage:
        """
        Generate order confirmation message.

        Key: Preemptively tells customer about fulfillment partner emails.
        """
        if not estimated_ship_date:
            estimated_ship_date = datetime.now() + timedelta(days=2)

        ship_date_str = estimated_ship_date.strftime("%B %d, %Y")

        items_text = "\n".join([
            f"  - {item.get('name', item.get('sku', 'Item'))} (Qty: {item.get('quantity', 1)})"
            for item in items
        ])

        address_text = cls._format_address(shipping_address)

        subject = f"Order Confirmed - {cls.COMPANY_NAME} #{order_number}"

        body_text = f"""Hi {customer_name},

Thank you for your order! We've received your order #{order_number} and it's being prepared for shipment.

ORDER DETAILS:
{items_text}

SHIPPING TO:
{address_text}

WHAT TO EXPECT:
Your order will ship within {cls.PROCESSING_DAYS}. We partner with a specialized fulfillment center to ensure your items are carefully packed and shipped.

IMPORTANT: You'll receive a shipping confirmation email from "{cls.FULFILLMENT_PARTNER_NAME}" - don't worry, that's our fulfillment partner handling your shipment! The tracking information in that email is legitimate and will show your package's progress.

Estimated ship date: {ship_date_str}

If you have any questions, just reply to this email.

Thanks for shopping with {cls.COMPANY_NAME}!
"""

        body_html = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
<h2>Order Confirmed!</h2>
<p>Hi {customer_name},</p>
<p>Thank you for your order! We've received your order <strong>#{order_number}</strong> and it's being prepared for shipment.</p>

<h3>Order Details</h3>
<ul>
{"".join([f'<li>{item.get("name", item.get("sku", "Item"))} (Qty: {item.get("quantity", 1)})</li>' for item in items])}
</ul>

<h3>Shipping To</h3>
<p>{address_text.replace(chr(10), '<br>')}</p>

<div style="background: #f5f5f5; padding: 15px; border-radius: 5px; margin: 20px 0;">
<h4 style="margin-top: 0;">What to Expect</h4>
<p>Your order will ship within {cls.PROCESSING_DAYS}. We partner with a specialized fulfillment center to ensure your items are carefully packed and shipped.</p>
<p><strong>Important:</strong> You'll receive a shipping confirmation email from "<strong>{cls.FULFILLMENT_PARTNER_NAME}</strong>" - don't worry, that's our fulfillment partner handling your shipment! The tracking information in that email is legitimate.</p>
</div>

<p><strong>Estimated ship date:</strong> {ship_date_str}</p>

<p>If you have any questions, just reply to this email.</p>

<p>Thanks for shopping with {cls.COMPANY_NAME}!</p>
</body>
</html>
"""

        return DropshipMessage(
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            metadata={"order_number": order_number, "type": MessageType.ORDER_CONFIRMATION},
        )

    @classmethod
    def shipping_notification(
        cls,
        order_number: str,
        customer_name: str,
        tracking_number: str,
        carrier: Optional[str] = None,
        tracking_url: Optional[str] = None,
        estimated_delivery: Optional[datetime] = None,
    ) -> DropshipMessage:
        """
        Generate shipping notification message.

        Sent when we capture tracking from BCW.
        """
        carrier_name = carrier or "the carrier"

        delivery_text = ""
        if estimated_delivery:
            delivery_text = f"\n\nEstimated delivery: {estimated_delivery.strftime('%B %d, %Y')}"

        tracking_link = ""
        if tracking_url:
            tracking_link = f"\n\nTrack your package: {tracking_url}"
        elif tracking_number:
            tracking_link = f"\n\nTracking number: {tracking_number}"

        subject = f"Your Order Has Shipped - {cls.COMPANY_NAME} #{order_number}"

        body_text = f"""Hi {customer_name},

Great news! Your order #{order_number} is on its way!

TRACKING INFORMATION:
Carrier: {carrier_name}
Tracking Number: {tracking_number}{tracking_link}{delivery_text}

NOTE: The package will show "{cls.FULFILLMENT_PARTNER_NAME}" as the shipper - that's our fulfillment partner. Your {cls.COMPANY_NAME} order is inside!

You may also receive a separate tracking email directly from {carrier_name}. Both are legitimate and will show the same tracking information.

Thanks for your order!
{cls.COMPANY_NAME}
"""

        return DropshipMessage(
            subject=subject,
            body_text=body_text,
            metadata={
                "order_number": order_number,
                "tracking_number": tracking_number,
                "carrier": carrier,
                "type": MessageType.SHIPPING_NOTIFICATION,
            },
        )

    @classmethod
    def backorder_notice(
        cls,
        order_number: str,
        customer_name: str,
        item_name: str,
        expected_date: Optional[datetime] = None,
    ) -> DropshipMessage:
        """
        Generate backorder notification.
        """
        date_text = "as soon as it becomes available"
        if expected_date:
            date_text = f"around {expected_date.strftime('%B %d, %Y')}"

        subject = f"Backorder Update - {cls.COMPANY_NAME} #{order_number}"

        body_text = f"""Hi {customer_name},

We wanted to let you know about a delay with your order #{order_number}.

The item "{item_name}" is currently on backorder with our supplier. We expect it to ship {date_text}.

Your order will ship as soon as the item is back in stock. We'll send you a shipping confirmation with tracking information at that time.

If you'd prefer not to wait, please reply to this email and we'll process a full refund.

We apologize for any inconvenience and appreciate your patience!

{cls.COMPANY_NAME}
"""

        return DropshipMessage(
            subject=subject,
            body_text=body_text,
            metadata={
                "order_number": order_number,
                "item_name": item_name,
                "expected_date": expected_date.isoformat() if expected_date else None,
                "type": MessageType.BACKORDER_NOTICE,
            },
        )

    @classmethod
    def tracking_page_content(
        cls,
        order_number: str,
        status: str,
        tracking_number: Optional[str] = None,
        carrier: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Generate content for order tracking page on your website.

        Returns dict of content blocks for the tracking page.
        """
        fulfillment_note = (
            f"Your order is being fulfilled by our partner warehouse. "
            f"The shipping label will show '{cls.FULFILLMENT_PARTNER_NAME}' as the origin - "
            f"this is normal and your {cls.COMPANY_NAME} order is inside!"
        )

        return {
            "title": f"Order #{order_number}",
            "status": status,
            "tracking_number": tracking_number or "Awaiting shipment",
            "carrier": carrier or "TBD",
            "fulfillment_note": fulfillment_note,
            "support_text": f"Questions? Contact {cls.COMPANY_NAME} support.",
        }

    @classmethod
    def error_messages(cls) -> Dict[str, str]:
        """
        Standard error messages for checkout/cart.

        Returns dict of error codes to customer-friendly messages.
        """
        return {
            "INVALID_ADDRESS": (
                "We couldn't validate your shipping address. "
                "Please check the address and try again."
            ),
            "PO_BOX_NOT_ALLOWED": (
                "We're sorry, but we cannot ship these items to PO Boxes. "
                "Please provide a street address."
            ),
            "MILITARY_ADDRESS_NOT_ALLOWED": (
                "We're sorry, but we cannot ship these items to military addresses (APO/FPO/DPO). "
                "Please provide a US street address."
            ),
            "INTERNATIONAL_NOT_SUPPORTED": (
                "These items can only be shipped within the continental United States. "
                "International shipping is not available for this product line."
            ),
            "NON_CONTINENTAL_US": (
                "We're sorry, but we cannot ship these items to Alaska, Hawaii, or US territories. "
                "Only continental US addresses are supported."
            ),
            "OUT_OF_STOCK": (
                "One or more items in your cart are no longer available. "
                "Please update your cart and try again."
            ),
            "INSUFFICIENT_STOCK": (
                "We don't have enough stock to fulfill your requested quantity. "
                "Please reduce the quantity or remove the item."
            ),
            "SHIPPING_QUOTE_FAILED": (
                "We couldn't calculate shipping for your order. "
                "Please try again or contact support."
            ),
            "ORDER_SUBMISSION_FAILED": (
                "We encountered an issue processing your order. "
                "Your card has not been charged. Please try again."
            ),
        }

    @staticmethod
    def _format_address(address: Dict[str, str]) -> str:
        """Format address for display."""
        lines = [address.get("name", "")]
        lines.append(address.get("address1", ""))
        if address.get("address2"):
            lines.append(address["address2"])
        lines.append(
            f"{address.get('city', '')}, {address.get('state', '')} {address.get('zip', '')}"
        )
        return "\n".join(lines)
