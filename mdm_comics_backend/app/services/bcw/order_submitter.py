"""
BCW Order Submitter

Submits orders to BCW with idempotency protection.
Per bcw_submission_flow in proposal doc.

Order submission flow:
1. Build cart with order items
2. Fill shipping address
3. Select shipping method
4. Submit order
5. Extract confirmation number
6. Verify totals match
"""
import logging
import hashlib
from datetime import datetime, timezone
from typing import Dict, Optional, List
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.services.bcw.browser_client import BCWBrowserClient, OrderConfirmation
from app.services.bcw.cart_builder import BCWCartBuilder, CartItem
from app.services.bcw.selectors import get_selector
from app.core.exceptions import BCWOrderError, DropshipIdempotencyError

logger = logging.getLogger(__name__)


@dataclass
class OrderSubmissionRequest:
    """Request to submit an order to BCW."""
    order_id: int
    correlation_id: str
    items: List[CartItem]
    shipping_address: Dict[str, str]
    shipping_method_id: str
    idempotency_key: str


@dataclass
class OrderSubmissionResult:
    """Result of order submission."""
    success: bool
    bcw_order_id: Optional[str] = None
    confirmation_number: Optional[str] = None
    bcw_total: Optional[float] = None
    bcw_shipping: Optional[float] = None
    error_message: Optional[str] = None


class BCWOrderSubmitter:
    """
    Submits orders to BCW with idempotency protection.

    Usage:
        async with BCWBrowserClient() as client:
            await client.login(username, password)

            submitter = BCWOrderSubmitter(client, db)
            result = await submitter.submit_order(request)
    """

    def __init__(self, browser_client: BCWBrowserClient, db: AsyncSession):
        self.client = browser_client
        self.db = db
        self.cart_builder = BCWCartBuilder(browser_client)

    async def check_idempotency(self, idempotency_key: str) -> Optional[str]:
        """
        Check if this order was already submitted.

        Returns:
            BCW order ID if duplicate, None otherwise
        """
        from app.models.bcw import BCWOrder

        result = await self.db.execute(
            select(BCWOrder)
            .where(BCWOrder.idempotency_key == idempotency_key)
            .where(BCWOrder.bcw_order_id.isnot(None))
        )
        existing = result.scalar_one_or_none()

        if existing:
            logger.warning(
                f"Duplicate order detected: idempotency_key={idempotency_key}, "
                f"existing bcw_order_id={existing.bcw_order_id}"
            )
            return existing.bcw_order_id

        return None

    @staticmethod
    def generate_idempotency_key(
        order_id: int,
        cart_items: List[CartItem],
        address: Dict[str, str],
    ) -> str:
        """
        Generate idempotency key from order components.

        Key = order_id + cart_hash + address_hash
        """
        # Hash cart items
        cart_str = "|".join(
            f"{item.sku}:{item.quantity}" for item in sorted(cart_items, key=lambda x: x.sku)
        )
        cart_hash = hashlib.sha256(cart_str.encode()).hexdigest()[:16]

        # Hash address
        addr_str = "|".join(
            f"{k}:{v}" for k, v in sorted(address.items())
        )
        addr_hash = hashlib.sha256(addr_str.encode()).hexdigest()[:16]

        return f"{order_id}:{cart_hash}:{addr_hash}"

    async def submit_order(
        self,
        request: OrderSubmissionRequest,
    ) -> OrderSubmissionResult:
        """
        Submit an order to BCW.

        Args:
            request: OrderSubmissionRequest with all order details

        Returns:
            OrderSubmissionResult with BCW order info
        """
        logger.info(
            f"Submitting order to BCW: order_id={request.order_id}, "
            f"correlation_id={request.correlation_id}"
        )

        # Check idempotency first
        existing_bcw_id = await self.check_idempotency(request.idempotency_key)
        if existing_bcw_id:
            raise DropshipIdempotencyError(
                message="Order already submitted to BCW",
                idempotency_key=request.idempotency_key,
                existing_order_id=existing_bcw_id,
            )

        try:
            # Build cart
            cart_success = await self.cart_builder.build_cart(request.items)
            if not cart_success:
                return OrderSubmissionResult(
                    success=False,
                    error_message="Failed to build cart with order items",
                )

            # Navigate to checkout
            await self.client._click_element("cart", "proceed_to_checkout")
            await self.client._page.wait_for_load_state("domcontentloaded")
            await self.client._human_delay()

            # Fill shipping address
            await self._fill_shipping_address(request.shipping_address)

            # Select shipping method
            await self._select_shipping_method(request.shipping_method_id)

            # Get order totals before submitting
            totals = await self._extract_order_totals()

            # Submit order
            await self.client._click_element("checkout", "place_order")
            await self.client._page.wait_for_load_state("domcontentloaded")
            await self.client._human_delay(2000, 4000)

            # Extract confirmation
            confirmation = await self._extract_confirmation()

            if not confirmation.bcw_order_id:
                # Take screenshot for debugging
                screenshot = await self.client._capture_error_screenshot("order_submit")
                return OrderSubmissionResult(
                    success=False,
                    error_message="Could not extract order confirmation",
                )

            logger.info(
                f"Order submitted successfully: bcw_order_id={confirmation.bcw_order_id}"
            )

            return OrderSubmissionResult(
                success=True,
                bcw_order_id=confirmation.bcw_order_id,
                confirmation_number=confirmation.confirmation_number,
                bcw_total=confirmation.total or totals.get("total"),
                bcw_shipping=totals.get("shipping"),
            )

        except DropshipIdempotencyError:
            raise
        except BCWOrderError:
            raise
        except Exception as e:
            logger.error(f"Order submission failed: {e}")
            await self.client._capture_error_screenshot("order_submit_error")
            return OrderSubmissionResult(
                success=False,
                error_message=str(e),
            )

    async def _fill_shipping_address(self, address: Dict[str, str]):
        """Fill shipping address form."""
        await self.client._fill_input("checkout", "shipping_name", address.get("name", ""))
        await self.client._fill_input("checkout", "shipping_address1", address.get("address1", ""))
        if address.get("address2"):
            await self.client._fill_input("checkout", "shipping_address2", address["address2"])
        await self.client._fill_input("checkout", "shipping_city", address.get("city", ""))

        # Select state
        state_select = await self.client._find_element("checkout", "shipping_state")
        await state_select.select_option(value=address.get("state", ""))
        await self.client._human_delay()

        await self.client._fill_input("checkout", "shipping_zip", address.get("zip", ""))
        await self.client._human_delay(1000, 2000)

    async def _select_shipping_method(self, method_id: str):
        """Select a shipping method."""
        # Wait for shipping methods to load
        await self.client._page.wait_for_selector(
            get_selector("checkout", "shipping_methods_container").primary,
            timeout=10000,
        )
        await self.client._human_delay()

        # Find and click the shipping method
        method_elements = await self.client._page.query_selector_all(
            get_selector("checkout", "shipping_method_option").primary
        )

        for element in method_elements:
            input_el = await element.query_selector("input")
            if input_el:
                value = await input_el.get_attribute("value")
                if value == method_id:
                    await element.click()
                    await self.client._human_delay()
                    return

        logger.warning(f"Shipping method {method_id} not found, using first option")
        if method_elements:
            await method_elements[0].click()

    async def _extract_order_totals(self) -> Dict[str, float]:
        """Extract order totals from checkout page."""
        totals = {}

        try:
            # Subtotal
            subtotal_el = await self.client._find_element("checkout", "order_subtotal", timeout=5000)
            if subtotal_el:
                text = await subtotal_el.text_content()
                totals["subtotal"] = self.client._parse_price(text)
        except Exception:
            pass

        try:
            # Shipping
            shipping_el = await self.client._find_element("checkout", "order_shipping", timeout=5000)
            if shipping_el:
                text = await shipping_el.text_content()
                totals["shipping"] = self.client._parse_price(text)
        except Exception:
            pass

        try:
            # Total
            total_el = await self.client._find_element("checkout", "order_total", timeout=5000)
            if total_el:
                text = await total_el.text_content()
                totals["total"] = self.client._parse_price(text)
        except Exception:
            pass

        return totals

    async def _extract_confirmation(self) -> OrderConfirmation:
        """Extract order confirmation details."""
        bcw_order_id = None
        confirmation_number = None
        total = None

        try:
            # Wait for confirmation page
            await self.client._page.wait_for_selector(
                get_selector("order_confirmation", "confirmation_container").primary,
                timeout=10000,
            )

            # Order number
            order_el = await self.client._find_element(
                "order_confirmation", "order_number", timeout=5000
            )
            if order_el:
                bcw_order_id = await order_el.text_content()
                bcw_order_id = bcw_order_id.strip() if bcw_order_id else None

            # Total
            total_el = await self.client._find_element(
                "order_confirmation", "order_total_confirmed", timeout=5000
            )
            if total_el:
                text = await total_el.text_content()
                total = self.client._parse_price(text)

        except Exception as e:
            logger.error(f"Failed to extract confirmation: {e}")

        return OrderConfirmation(
            bcw_order_id=bcw_order_id or "",
            confirmation_number=confirmation_number,
            total=total,
        )
