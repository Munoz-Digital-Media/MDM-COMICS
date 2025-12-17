"""
BCW Status Poller

Polls BCW order history for status updates and tracking info.
Per order_status_polling in proposal doc.

Polling schedule:
- Every 30 min until shipped
- Daily until delivered
- Stop when DELIVERED or CANCELLED
"""
import logging
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict
from dataclasses import dataclass

from app.services.bcw.browser_client import BCWBrowserClient
from app.services.bcw.selectors import get_selector
from app.core.exceptions import BCWError

logger = logging.getLogger(__name__)


@dataclass
class OrderStatusInfo:
    """Order status information from BCW."""
    bcw_order_id: str
    status: str  # e.g., "Processing", "Shipped", "Delivered"
    tracking_number: Optional[str] = None
    carrier: Optional[str] = None
    tracking_url: Optional[str] = None
    shipped_date: Optional[datetime] = None
    delivered_date: Optional[datetime] = None
    raw_status: Optional[str] = None


class BCWStatusPoller:
    """
    Polls BCW for order status updates.

    Usage:
        async with BCWBrowserClient() as client:
            await client.login(username, password)

            poller = BCWStatusPoller(client)
            status = await poller.get_order_status("BCW123456")
    """

    def __init__(self, browser_client: BCWBrowserClient):
        self.client = browser_client

    async def get_order_status(self, bcw_order_id: str) -> Optional[OrderStatusInfo]:
        """
        Get status of a specific BCW order.

        Args:
            bcw_order_id: BCW order number

        Returns:
            OrderStatusInfo or None if not found
        """
        logger.info(f"Polling status for BCW order: {bcw_order_id}")

        try:
            # Navigate to order history
            await self.client._click_element("order_status", "order_history_link")
            await self.client._page.wait_for_load_state("domcontentloaded")
            await self.client._human_delay()

            # Find the order in the list
            order_rows = await self.client._page.query_selector_all(
                get_selector("order_status", "order_row").primary
            )

            for row in order_rows:
                # Check if this is our order
                row_text = await row.text_content()
                if bcw_order_id in (row_text or ""):
                    return await self._extract_order_status(row, bcw_order_id)

            logger.warning(f"Order {bcw_order_id} not found in order history")
            return None

        except Exception as e:
            logger.error(f"Failed to poll order status: {e}")
            return None

    async def _extract_order_status(self, row_element, bcw_order_id: str) -> OrderStatusInfo:
        """Extract status info from an order row."""
        status = "Unknown"
        tracking_number = None
        carrier = None
        tracking_url = None

        try:
            # Get status
            status_el = await row_element.query_selector(
                get_selector("order_status", "order_status").primary
            )
            if status_el:
                raw_status = await status_el.text_content()
                status = self._normalize_status(raw_status)

            # Get tracking info
            tracking_el = await row_element.query_selector(
                get_selector("order_status", "tracking_number").primary
            )
            if tracking_el:
                tracking_text = await tracking_el.text_content()
                tracking_number = self._extract_tracking_number(tracking_text)

                # Try to get tracking link
                tracking_link = await row_element.query_selector(
                    get_selector("order_status", "tracking_link").primary
                )
                if tracking_link:
                    tracking_url = await tracking_link.get_attribute("href")
                    carrier = self._detect_carrier_from_url(tracking_url)

        except Exception as e:
            logger.error(f"Error extracting order details: {e}")

        return OrderStatusInfo(
            bcw_order_id=bcw_order_id,
            status=status,
            tracking_number=tracking_number,
            carrier=carrier,
            tracking_url=tracking_url,
            raw_status=status,
        )

    def _normalize_status(self, raw_status: str) -> str:
        """Normalize BCW status to our state machine states."""
        if not raw_status:
            return "Unknown"

        status_lower = raw_status.lower().strip()

        # Map BCW statuses to our states
        status_mapping = {
            "processing": "VENDOR_SUBMITTED",
            "pending": "VENDOR_SUBMITTED",
            "on hold": "EXCEPTION_REVIEW",
            "backordered": "BACKORDERED",
            "back order": "BACKORDERED",
            "partial": "PARTIALLY_SHIPPED",
            "shipped": "SHIPPED",
            "in transit": "SHIPPED",
            "out for delivery": "SHIPPED",
            "delivered": "DELIVERED",
            "cancelled": "CANCELLED",
            "canceled": "CANCELLED",
            "refunded": "REFUNDED",
        }

        for key, value in status_mapping.items():
            if key in status_lower:
                return value

        return "VENDOR_SUBMITTED"

    def _extract_tracking_number(self, text: str) -> Optional[str]:
        """Extract tracking number from text."""
        if not text:
            return None

        # Common tracking number patterns
        patterns = [
            r"1Z[A-Z0-9]{16}",  # UPS
            r"\d{20,22}",  # USPS
            r"\d{12,15}",  # FedEx
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group()

        # Just return cleaned text if no pattern matches
        return text.strip()

    def _detect_carrier_from_url(self, url: str) -> Optional[str]:
        """Detect carrier from tracking URL."""
        if not url:
            return None

        url_lower = url.lower()
        if "ups.com" in url_lower:
            return "UPS"
        elif "usps.com" in url_lower:
            return "USPS"
        elif "fedex.com" in url_lower:
            return "FedEx"
        elif "dhl.com" in url_lower:
            return "DHL"

        return None

    async def get_all_pending_orders(self) -> List[OrderStatusInfo]:
        """
        Get status of all orders in history.

        Useful for bulk status sync.
        """
        logger.info("Getting all orders from BCW history")
        orders = []

        try:
            # Navigate to order history
            await self.client._click_element("order_status", "order_history_link")
            await self.client._page.wait_for_load_state("domcontentloaded")
            await self.client._human_delay()

            # Get all order rows
            order_rows = await self.client._page.query_selector_all(
                get_selector("order_status", "order_row").primary
            )

            for row in order_rows:
                try:
                    # Try to extract order ID from row
                    row_text = await row.text_content()
                    # Look for order ID pattern
                    match = re.search(r"(BCW\d+|\d{6,})", row_text or "")
                    if match:
                        bcw_order_id = match.group(1)
                        status_info = await self._extract_order_status(row, bcw_order_id)
                        orders.append(status_info)
                except Exception as e:
                    logger.warning(f"Failed to parse order row: {e}")

            logger.info(f"Found {len(orders)} orders in BCW history")

        except Exception as e:
            logger.error(f"Failed to get order history: {e}")

        return orders
