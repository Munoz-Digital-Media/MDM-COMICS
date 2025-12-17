"""
BCW Email Parser

Parses BCW notification emails to extract:
- Order confirmations
- Shipping notifications with tracking numbers
- Delivery confirmations

Per proposal doc: Email parsing as fallback for tracking extraction.
"""
import imaplib
import email
import logging
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from email.header import decode_header
from email.utils import parsedate_to_datetime

from bs4 import BeautifulSoup

from app.core.config import settings
from app.core.exceptions import BCWError

logger = logging.getLogger(__name__)


@dataclass
class ParsedEmail:
    """Parsed email content."""
    message_id: str
    subject: str
    from_address: str
    date: datetime
    email_type: str  # "order_confirmation", "shipping", "delivery"
    bcw_order_id: Optional[str] = None
    tracking_number: Optional[str] = None
    carrier: Optional[str] = None
    tracking_url: Optional[str] = None
    estimated_delivery: Optional[datetime] = None
    delivered_date: Optional[datetime] = None
    raw_body: Optional[str] = None


class BCWEmailParser:
    """
    Parses BCW notification emails via IMAP.

    Usage:
        parser = BCWEmailParser()
        emails = await parser.fetch_unread_bcw_emails()
        for email in emails:
            if email.email_type == "shipping":
                # Update order with tracking info
                ...
    """

    # BCW email patterns
    SUBJECT_PATTERNS = {
        "order_confirmation": [
            r"order\s+confirmation",
            r"order\s+#?\d+\s+confirmed",
            r"your\s+order\s+has\s+been\s+received",
        ],
        "shipping": [
            r"your\s+order\s+has\s+shipped",
            r"shipment\s+notification",
            r"tracking\s+information",
            r"order\s+#?\d+\s+shipped",
        ],
        "delivery": [
            r"your\s+order\s+has\s+been\s+delivered",
            r"delivery\s+confirmation",
            r"package\s+delivered",
        ],
    }

    # Tracking number patterns
    TRACKING_PATTERNS = {
        "UPS": r"1Z[A-Z0-9]{16}",
        "USPS": r"(?:94\d{20}|92\d{20}|\d{20,22})",
        "FedEx": r"(?:\d{12,15}|\d{20,22})",
        "DHL": r"\d{10,11}",
    }

    # Tracking URL patterns
    TRACKING_URL_PATTERNS = {
        "UPS": r"https?://[^\s]*ups\.com[^\s]*track[^\s]*",
        "USPS": r"https?://[^\s]*usps\.com[^\s]*track[^\s]*",
        "FedEx": r"https?://[^\s]*fedex\.com[^\s]*track[^\s]*",
        "DHL": r"https?://[^\s]*dhl\.com[^\s]*track[^\s]*",
    }

    def __init__(
        self,
        imap_host: Optional[str] = None,
        imap_port: Optional[int] = None,
        email_address: Optional[str] = None,
        email_password: Optional[str] = None,
    ):
        self.imap_host = imap_host or getattr(settings, "BCW_EMAIL_IMAP_HOST", "imap.gmail.com")
        self.imap_port = imap_port or getattr(settings, "BCW_EMAIL_IMAP_PORT", 993)
        self.email_address = email_address or getattr(settings, "BCW_EMAIL_ADDRESS", "")
        self.email_password = email_password or getattr(settings, "BCW_EMAIL_PASSWORD", "")
        self._connection: Optional[imaplib.IMAP4_SSL] = None

    def connect(self) -> bool:
        """Connect to IMAP server."""
        try:
            self._connection = imaplib.IMAP4_SSL(self.imap_host, self.imap_port)
            self._connection.login(self.email_address, self.email_password)
            logger.info(f"Connected to IMAP server: {self.imap_host}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to IMAP: {e}")
            return False

    def disconnect(self):
        """Disconnect from IMAP server."""
        if self._connection:
            try:
                self._connection.logout()
            except Exception:
                pass
            self._connection = None

    def fetch_unread_bcw_emails(
        self,
        from_address: str = "bcwsupplies.com",
        limit: int = 50,
    ) -> List[ParsedEmail]:
        """
        Fetch and parse unread BCW emails.

        Args:
            from_address: Filter emails from this domain
            limit: Maximum emails to fetch

        Returns:
            List of parsed emails
        """
        if not self._connection:
            if not self.connect():
                return []

        parsed_emails = []

        try:
            # Select inbox
            self._connection.select("INBOX")

            # Search for unread emails from BCW
            search_criteria = f'(UNSEEN FROM "{from_address}")'
            _, message_ids = self._connection.search(None, search_criteria)

            if not message_ids[0]:
                logger.info("No unread BCW emails found")
                return []

            ids = message_ids[0].split()[:limit]
            logger.info(f"Found {len(ids)} unread BCW emails")

            for msg_id in ids:
                try:
                    parsed = self._fetch_and_parse_email(msg_id)
                    if parsed:
                        parsed_emails.append(parsed)
                except Exception as e:
                    logger.warning(f"Failed to parse email {msg_id}: {e}")

        except Exception as e:
            logger.error(f"Error fetching emails: {e}")

        return parsed_emails

    def _fetch_and_parse_email(self, msg_id: bytes) -> Optional[ParsedEmail]:
        """Fetch and parse a single email."""
        _, msg_data = self._connection.fetch(msg_id, "(RFC822)")

        if not msg_data or not msg_data[0]:
            return None

        email_body = msg_data[0][1]
        msg = email.message_from_bytes(email_body)

        # Extract headers
        message_id = msg.get("Message-ID", "")
        subject = self._decode_header(msg.get("Subject", ""))
        from_addr = self._decode_header(msg.get("From", ""))
        date_str = msg.get("Date", "")

        try:
            date = parsedate_to_datetime(date_str)
        except Exception:
            date = datetime.now(timezone.utc)

        # Determine email type
        email_type = self._classify_email(subject)

        # Extract body
        body = self._extract_body(msg)

        # Parse content based on type
        parsed = ParsedEmail(
            message_id=message_id,
            subject=subject,
            from_address=from_addr,
            date=date,
            email_type=email_type,
            raw_body=body,
        )

        # Extract order ID
        parsed.bcw_order_id = self._extract_order_id(subject, body)

        # Extract tracking info for shipping emails
        if email_type == "shipping":
            tracking_info = self._extract_tracking_info(body)
            parsed.tracking_number = tracking_info.get("tracking_number")
            parsed.carrier = tracking_info.get("carrier")
            parsed.tracking_url = tracking_info.get("tracking_url")

        # Extract delivery date for delivery emails
        if email_type == "delivery":
            parsed.delivered_date = self._extract_delivery_date(body)

        return parsed

    def _decode_header(self, header: str) -> str:
        """Decode email header."""
        if not header:
            return ""

        decoded_parts = decode_header(header)
        result = []
        for content, charset in decoded_parts:
            if isinstance(content, bytes):
                result.append(content.decode(charset or "utf-8", errors="replace"))
            else:
                result.append(content)
        return "".join(result)

    def _classify_email(self, subject: str) -> str:
        """Classify email type based on subject."""
        subject_lower = subject.lower()

        for email_type, patterns in self.SUBJECT_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, subject_lower):
                    return email_type

        return "unknown"

    def _extract_body(self, msg: email.message.Message) -> str:
        """Extract email body (prefer HTML, fallback to text)."""
        body = ""

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))

                if "attachment" in content_disposition:
                    continue

                if content_type == "text/html":
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode("utf-8", errors="replace")
                        break
                elif content_type == "text/plain" and not body:
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode("utf-8", errors="replace")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode("utf-8", errors="replace")

        return body

    def _extract_order_id(self, subject: str, body: str) -> Optional[str]:
        """Extract BCW order ID from email content."""
        # Common order ID patterns
        patterns = [
            r"order\s*#?\s*:?\s*(BCW\d+)",
            r"order\s*#?\s*:?\s*(\d{6,})",
            r"order\s+number\s*:?\s*(BCW\d+)",
            r"order\s+number\s*:?\s*(\d{6,})",
            r"confirmation\s*#?\s*:?\s*(BCW\d+)",
            r"confirmation\s*#?\s*:?\s*(\d{6,})",
        ]

        # Check subject first
        for pattern in patterns:
            match = re.search(pattern, subject, re.IGNORECASE)
            if match:
                return match.group(1)

        # Check body
        text = BeautifulSoup(body, "html.parser").get_text() if "<" in body else body

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)

        return None

    def _extract_tracking_info(self, body: str) -> Dict[str, Any]:
        """Extract tracking number, carrier, and URL from email body."""
        result = {
            "tracking_number": None,
            "carrier": None,
            "tracking_url": None,
        }

        # Parse HTML
        text = BeautifulSoup(body, "html.parser").get_text() if "<" in body else body

        # Try to find tracking URL first (most reliable)
        for carrier, pattern in self.TRACKING_URL_PATTERNS.items():
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                result["tracking_url"] = match.group(0)
                result["carrier"] = carrier
                break

        # Extract tracking number
        for carrier, pattern in self.TRACKING_PATTERNS.items():
            match = re.search(pattern, text)
            if match:
                result["tracking_number"] = match.group(0)
                if not result["carrier"]:
                    result["carrier"] = carrier
                break

        return result

    def _extract_delivery_date(self, body: str) -> Optional[datetime]:
        """Extract delivery date from email body."""
        text = BeautifulSoup(body, "html.parser").get_text() if "<" in body else body

        # Date patterns
        patterns = [
            r"delivered\s+on\s+(\w+\s+\d{1,2},?\s+\d{4})",
            r"delivered\s+(\d{1,2}/\d{1,2}/\d{2,4})",
            r"delivery\s+date\s*:?\s*(\w+\s+\d{1,2},?\s+\d{4})",
            r"delivery\s+date\s*:?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                try:
                    # Try different date formats
                    for fmt in ["%B %d, %Y", "%B %d %Y", "%m/%d/%Y", "%m/%d/%y"]:
                        try:
                            return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
                        except ValueError:
                            continue
                except Exception:
                    pass

        return None

    def mark_as_read(self, message_id: str) -> bool:
        """Mark an email as read."""
        if not self._connection:
            return False

        try:
            # Search for the message by Message-ID
            search_criteria = f'(HEADER Message-ID "{message_id}")'
            _, message_ids = self._connection.search(None, search_criteria)

            if message_ids[0]:
                msg_id = message_ids[0].split()[0]
                self._connection.store(msg_id, "+FLAGS", "\\Seen")
                return True
        except Exception as e:
            logger.error(f"Failed to mark email as read: {e}")

        return False

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


async def process_bcw_emails(db) -> int:
    """
    Process BCW emails and update order records.

    Args:
        db: Database session

    Returns:
        Number of orders updated
    """
    from app.models.bcw import BCWOrder, BCWOrderState, BCWOrderEvent

    updates = 0

    try:
        parser = BCWEmailParser()
        if not parser.connect():
            logger.error("Failed to connect to email server")
            return 0

        emails = parser.fetch_unread_bcw_emails()

        for parsed in emails:
            if not parsed.bcw_order_id:
                continue

            # Find matching order
            from sqlalchemy import select
            result = await db.execute(
                select(BCWOrder).where(BCWOrder.bcw_order_id == parsed.bcw_order_id)
            )
            order = result.scalar_one_or_none()

            if not order:
                logger.warning(f"No order found for BCW order ID: {parsed.bcw_order_id}")
                continue

            # Update based on email type
            if parsed.email_type == "shipping" and parsed.tracking_number:
                old_state = order.state
                order.tracking_number = parsed.tracking_number
                order.carrier = parsed.carrier
                order.tracking_url = parsed.tracking_url
                if order.state in [BCWOrderState.VENDOR_SUBMITTED, BCWOrderState.BACKORDERED]:
                    order.state = BCWOrderState.SHIPPED
                    order.shipped_at = parsed.date

                    # Log state change
                    event = BCWOrderEvent(
                        bcw_order_id=order.id,
                        from_state=old_state.value,
                        to_state=BCWOrderState.SHIPPED.value,
                        event_type="email_shipping_notification",
                        event_data={
                            "tracking_number": parsed.tracking_number,
                            "carrier": parsed.carrier,
                            "email_date": parsed.date.isoformat(),
                        },
                    )
                    db.add(event)
                    updates += 1

            elif parsed.email_type == "delivery" and parsed.delivered_date:
                old_state = order.state
                order.delivered_at = parsed.delivered_date
                if order.state == BCWOrderState.SHIPPED:
                    order.state = BCWOrderState.DELIVERED

                    event = BCWOrderEvent(
                        bcw_order_id=order.id,
                        from_state=old_state.value,
                        to_state=BCWOrderState.DELIVERED.value,
                        event_type="email_delivery_confirmation",
                        event_data={
                            "delivered_date": parsed.delivered_date.isoformat(),
                            "email_date": parsed.date.isoformat(),
                        },
                    )
                    db.add(event)
                    updates += 1

            # Mark email as processed
            parser.mark_as_read(parsed.message_id)

        await db.flush()
        parser.disconnect()

    except Exception as e:
        logger.error(f"Error processing BCW emails: {e}")

    return updates
