"""
BCW Dropship Integration Services

Browser automation for BCW website interaction.
Per 20251216_mdm_comics_bcw_initial_integration.json v1.2.0.

Components:
- selectors: Versioned DOM selectors
- browser_client: Playwright-based automation
- session_manager: Login/cookie management
- cart_builder: Shadow cart for shipping quotes
- order_submitter: Order placement
- status_poller: Order status extraction
- email_parser: Parse BCW notification emails
"""
from app.services.bcw.selectors import (
    SELECTOR_VERSION,
    get_selector,
    get_all_selectors_for_category,
    LOGIN_SELECTORS,
    SEARCH_SELECTORS,
    PRODUCT_DETAIL_SELECTORS,
    CART_SELECTORS,
    CHECKOUT_SELECTORS,
    ORDER_CONFIRMATION_SELECTORS,
    ORDER_STATUS_SELECTORS,
)
from app.services.bcw.browser_client import (
    BCWBrowserClient,
    ShippingOption,
    ProductInfo,
    OrderConfirmation,
)
from app.services.bcw.session_manager import BCWSessionManager
from app.services.bcw.cart_builder import BCWCartBuilder, CartItem, CartQuoteResult
from app.services.bcw.order_submitter import (
    BCWOrderSubmitter,
    OrderSubmissionRequest,
    OrderSubmissionResult,
)
from app.services.bcw.status_poller import BCWStatusPoller, OrderStatusInfo
from app.services.bcw.email_parser import BCWEmailParser, ParsedEmail, process_bcw_emails

__all__ = [
    # Selectors
    "SELECTOR_VERSION",
    "get_selector",
    "get_all_selectors_for_category",
    "LOGIN_SELECTORS",
    "SEARCH_SELECTORS",
    "PRODUCT_DETAIL_SELECTORS",
    "CART_SELECTORS",
    "CHECKOUT_SELECTORS",
    "ORDER_CONFIRMATION_SELECTORS",
    "ORDER_STATUS_SELECTORS",
    # Browser client
    "BCWBrowserClient",
    "ShippingOption",
    "ProductInfo",
    "OrderConfirmation",
    # Session manager
    "BCWSessionManager",
    # Cart builder
    "BCWCartBuilder",
    "CartItem",
    "CartQuoteResult",
    # Order submitter
    "BCWOrderSubmitter",
    "OrderSubmissionRequest",
    "OrderSubmissionResult",
    # Status poller
    "BCWStatusPoller",
    "OrderStatusInfo",
    # Email parser
    "BCWEmailParser",
    "ParsedEmail",
    "process_bcw_emails",
]
