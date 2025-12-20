"""
BCW Return Submitter

Submits RMA (Return Merchandise Authorization) requests to BCW.
Per 20251219_INTEGRATED_BCW_REMEDIATION_PROPOSAL.json WS-01.

RMA submission flow:
1. Login to BCW account
2. Navigate to Returns/RMA section
3. Select order for return
4. Specify items and quantities
5. Select return reason
6. Submit RMA request
7. Extract RMA number for tracking
"""
import logging
import hashlib
from datetime import datetime, timezone
from typing import Dict, Optional, List
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.services.bcw.browser_client import BCWBrowserClient
from app.services.bcw.selectors import get_selector, SelectorConfig

logger = logging.getLogger(__name__)


# RMA-specific selectors (to be added to selectors.py)
RMA_SELECTORS = {
    "rma_link": SelectorConfig(
        primary="a[href*='returns'], a[href*='rma']",
        fallbacks=[
            ".returns-link",
            "#rma-link",
        ],
        description="Link to RMA/returns section"
    ),
    "order_search": SelectorConfig(
        primary="input[name='order_number'], #rma-order-search",
        fallbacks=[
            ".order-search-input",
        ],
        description="Order number search input for RMA"
    ),
    "order_result": SelectorConfig(
        primary=".order-result, .rma-order-item",
        fallbacks=[
            "[data-order-id]",
        ],
        description="Order search result for RMA"
    ),
    "item_checkbox": SelectorConfig(
        primary="input[type='checkbox'][name*='item'], .rma-item-select",
        fallbacks=[
            "[data-item-select]",
        ],
        description="Checkbox to select item for return"
    ),
    "quantity_input": SelectorConfig(
        primary="input[name*='qty'], input.rma-qty",
        fallbacks=[
            "[data-rma-qty]",
        ],
        description="Quantity input for return"
    ),
    "reason_select": SelectorConfig(
        primary="select[name='reason'], #return-reason",
        fallbacks=[
            ".rma-reason-select",
        ],
        description="Return reason dropdown"
    ),
    "notes_input": SelectorConfig(
        primary="textarea[name='notes'], #rma-notes",
        fallbacks=[
            ".rma-notes-input",
        ],
        description="Return notes/comments"
    ),
    "submit_rma": SelectorConfig(
        primary="button[type='submit'].rma-submit, #submit-rma",
        fallbacks=[
            ".btn-submit-return",
        ],
        description="Submit RMA button"
    ),
    "rma_confirmation": SelectorConfig(
        primary=".rma-confirmation, .return-confirmation",
        fallbacks=[
            ".rma-success",
        ],
        description="RMA confirmation container"
    ),
    "rma_number": SelectorConfig(
        primary=".rma-number, #return-number",
        fallbacks=[
            "[data-rma-number]",
        ],
        description="RMA reference number"
    ),
}


@dataclass
class ReturnItem:
    """Item to return."""
    bcw_sku: str
    quantity: int
    order_item_id: int


@dataclass
class ReturnSubmissionRequest:
    """Request to submit a return to BCW."""
    refund_request_id: int
    bcw_order_id: str
    correlation_id: str
    items: List[ReturnItem]
    reason_code: str
    reason_description: Optional[str] = None
    idempotency_key: Optional[str] = None


@dataclass
class ReturnSubmissionResult:
    """Result of return submission."""
    success: bool
    rma_number: Optional[str] = None
    error_message: Optional[str] = None
    screenshot_path: Optional[str] = None


class BCWReturnSubmitter:
    """
    Submits RMA requests to BCW.

    Usage:
        async with BCWBrowserClient() as client:
            await client.login(username, password)

            submitter = BCWReturnSubmitter(client, db)
            result = await submitter.submit_return(request)
    """

    # Mapping of our reason codes to BCW's dropdown values
    REASON_CODE_MAP = {
        "damaged": "damaged_defective",
        "wrong_item": "wrong_item_shipped",
        "not_as_described": "not_as_described",
        "quality_issue": "quality_issue",
        "changed_mind": "buyer_remorse",
        "other": "other",
    }

    def __init__(self, browser_client: BCWBrowserClient, db: AsyncSession):
        self.client = browser_client
        self.db = db

    async def check_existing_rma(
        self,
        bcw_order_id: str,
        refund_request_id: int,
    ) -> Optional[str]:
        """
        Check if RMA was already submitted for this refund request.

        Returns:
            RMA number if already submitted, None otherwise
        """
        from app.models.bcw import BCWRefundRequest

        result = await self.db.execute(
            select(BCWRefundRequest)
            .where(BCWRefundRequest.id == refund_request_id)
        )
        refund_request = result.scalar_one_or_none()

        if refund_request and refund_request.bcw_rma_number:
            logger.warning(
                f"RMA already exists for refund {refund_request_id}: "
                f"{refund_request.bcw_rma_number}"
            )
            return refund_request.bcw_rma_number

        return None

    @staticmethod
    def generate_idempotency_key(
        refund_request_id: int,
        bcw_order_id: str,
        items: List[ReturnItem],
    ) -> str:
        """Generate idempotency key for return submission."""
        items_str = "|".join(
            f"{item.bcw_sku}:{item.quantity}"
            for item in sorted(items, key=lambda x: x.bcw_sku)
        )
        items_hash = hashlib.sha256(items_str.encode()).hexdigest()[:16]
        return f"RMA:{refund_request_id}:{bcw_order_id}:{items_hash}"

    async def submit_return(
        self,
        request: ReturnSubmissionRequest,
    ) -> ReturnSubmissionResult:
        """
        Submit a return/RMA request to BCW.

        Args:
            request: ReturnSubmissionRequest with return details

        Returns:
            ReturnSubmissionResult with RMA number or error
        """
        logger.info(
            f"Submitting RMA to BCW: refund_request_id={request.refund_request_id}, "
            f"bcw_order_id={request.bcw_order_id}, "
            f"correlation_id={request.correlation_id}"
        )

        # Check for existing RMA
        existing_rma = await self.check_existing_rma(
            request.bcw_order_id,
            request.refund_request_id,
        )
        if existing_rma:
            return ReturnSubmissionResult(
                success=True,
                rma_number=existing_rma,
            )

        try:
            # Navigate to RMA section
            await self._navigate_to_rma_section()

            # Search for the order
            order_found = await self._search_order(request.bcw_order_id)
            if not order_found:
                return ReturnSubmissionResult(
                    success=False,
                    error_message=f"Order {request.bcw_order_id} not found in RMA portal",
                )

            # Select items for return
            await self._select_return_items(request.items)

            # Select reason and add notes
            await self._fill_return_details(
                request.reason_code,
                request.reason_description,
            )

            # Submit RMA
            await self._submit_rma_form()

            # Extract RMA number
            rma_number = await self._extract_rma_number()

            if not rma_number:
                screenshot = await self.client._capture_error_screenshot("rma_submit")
                return ReturnSubmissionResult(
                    success=False,
                    error_message="Could not extract RMA number from confirmation",
                    screenshot_path=screenshot,
                )

            logger.info(f"RMA submitted successfully: {rma_number}")

            return ReturnSubmissionResult(
                success=True,
                rma_number=rma_number,
            )

        except Exception as e:
            logger.error(f"RMA submission failed: {e}")
            screenshot = await self.client._capture_error_screenshot("rma_error")
            return ReturnSubmissionResult(
                success=False,
                error_message=str(e),
                screenshot_path=screenshot,
            )

    async def _navigate_to_rma_section(self):
        """Navigate to the RMA/Returns section of BCW."""
        # First go to account page
        await self.client._page.goto("https://www.bcwsupplies.com/account")
        await self.client._page.wait_for_load_state("domcontentloaded")
        await self.client._human_delay()

        # Find and click RMA link
        rma_selector = RMA_SELECTORS["rma_link"]
        for selector in rma_selector.all_selectors():
            try:
                element = await self.client._page.query_selector(selector)
                if element:
                    await element.click()
                    await self.client._page.wait_for_load_state("domcontentloaded")
                    await self.client._human_delay()
                    return
            except Exception:
                continue

        # Fallback: try direct URL
        await self.client._page.goto("https://www.bcwsupplies.com/returns")
        await self.client._page.wait_for_load_state("domcontentloaded")
        await self.client._human_delay()

    async def _search_order(self, bcw_order_id: str) -> bool:
        """Search for order in RMA portal."""
        try:
            # Find order search input
            search_selector = RMA_SELECTORS["order_search"]
            search_input = None

            for selector in search_selector.all_selectors():
                try:
                    search_input = await self.client._page.query_selector(selector)
                    if search_input:
                        break
                except Exception:
                    continue

            if not search_input:
                logger.error("Could not find order search input")
                return False

            # Enter order ID
            await search_input.fill(bcw_order_id)
            await self.client._human_delay()

            # Press enter or click search button
            await search_input.press("Enter")
            await self.client._page.wait_for_load_state("domcontentloaded")
            await self.client._human_delay(1000, 2000)

            # Check if order was found
            result_selector = RMA_SELECTORS["order_result"]
            for selector in result_selector.all_selectors():
                try:
                    result = await self.client._page.query_selector(selector)
                    if result:
                        await result.click()
                        await self.client._human_delay()
                        return True
                except Exception:
                    continue

            return False

        except Exception as e:
            logger.error(f"Order search failed: {e}")
            return False

    async def _select_return_items(self, items: List[ReturnItem]):
        """Select items and quantities for return."""
        for item in items:
            try:
                # Find the row for this SKU
                sku_element = await self.client._page.query_selector(
                    f"[data-sku='{item.bcw_sku}'], :has-text('{item.bcw_sku}')"
                )

                if not sku_element:
                    logger.warning(f"Could not find item {item.bcw_sku} on RMA form")
                    continue

                # Find the checkbox in this row
                parent = await sku_element.evaluate_handle("el => el.closest('tr, .rma-item-row, .item-row')")
                checkbox = await parent.query_selector(
                    RMA_SELECTORS["item_checkbox"].primary
                )

                if checkbox:
                    await checkbox.check()
                    await self.client._human_delay()

                # Set quantity if applicable
                qty_input = await parent.query_selector(
                    RMA_SELECTORS["quantity_input"].primary
                )
                if qty_input:
                    await qty_input.fill(str(item.quantity))
                    await self.client._human_delay()

            except Exception as e:
                logger.error(f"Error selecting item {item.bcw_sku}: {e}")

    async def _fill_return_details(
        self,
        reason_code: str,
        notes: Optional[str],
    ):
        """Fill return reason and notes."""
        try:
            # Select reason
            bcw_reason = self.REASON_CODE_MAP.get(reason_code, "other")
            reason_selector = RMA_SELECTORS["reason_select"]

            for selector in reason_selector.all_selectors():
                try:
                    reason_select = await self.client._page.query_selector(selector)
                    if reason_select:
                        await reason_select.select_option(value=bcw_reason)
                        await self.client._human_delay()
                        break
                except Exception:
                    continue

            # Add notes if provided
            if notes:
                notes_selector = RMA_SELECTORS["notes_input"]
                for selector in notes_selector.all_selectors():
                    try:
                        notes_input = await self.client._page.query_selector(selector)
                        if notes_input:
                            await notes_input.fill(notes)
                            await self.client._human_delay()
                            break
                    except Exception:
                        continue

        except Exception as e:
            logger.error(f"Error filling return details: {e}")

    async def _submit_rma_form(self):
        """Submit the RMA form."""
        submit_selector = RMA_SELECTORS["submit_rma"]

        for selector in submit_selector.all_selectors():
            try:
                submit_btn = await self.client._page.query_selector(selector)
                if submit_btn:
                    await submit_btn.click()
                    await self.client._page.wait_for_load_state("domcontentloaded")
                    await self.client._human_delay(2000, 4000)
                    return
            except Exception:
                continue

        raise Exception("Could not find RMA submit button")

    async def _extract_rma_number(self) -> Optional[str]:
        """Extract RMA number from confirmation page."""
        try:
            # Wait for confirmation
            conf_selector = RMA_SELECTORS["rma_confirmation"]
            await self.client._page.wait_for_selector(
                conf_selector.primary,
                timeout=10000,
            )

            # Get RMA number
            rma_selector = RMA_SELECTORS["rma_number"]
            for selector in rma_selector.all_selectors():
                try:
                    rma_element = await self.client._page.query_selector(selector)
                    if rma_element:
                        rma_number = await rma_element.text_content()
                        return rma_number.strip() if rma_number else None
                except Exception:
                    continue

            return None

        except Exception as e:
            logger.error(f"Error extracting RMA number: {e}")
            return None
