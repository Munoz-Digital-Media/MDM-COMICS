"""
BCW DOM Selectors Registry

Versioned CSS/XPath selectors for BCW website automation.
Per DOM Change Detection Strategy in proposal doc:
- Selector versioning for change detection
- Fallback selectors for resilience
- Health check support

IMPORTANT: These selectors are placeholders and must be updated
based on actual BCW website inspection. The website structure
may change without notice.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class SelectorConfig:
    """Configuration for a single DOM element selector."""
    primary: str
    fallbacks: List[str] = field(default_factory=list)
    description: str = ""
    xpath: bool = False  # If True, use XPath instead of CSS

    def all_selectors(self) -> List[str]:
        """Return all selectors in order of preference."""
        return [self.primary] + self.fallbacks


# =============================================================================
# SELECTOR VERSION
# =============================================================================

SELECTOR_VERSION = "1.0.0"
SELECTOR_LAST_VERIFIED = "2025-12-16"

# =============================================================================
# LOGIN PAGE SELECTORS
# =============================================================================

LOGIN_SELECTORS = {
    "username": SelectorConfig(
        primary="input[name='email'], input[type='email']",
        fallbacks=[
            "#email",
            "#login-email",
            "input.email-input",
            "//input[@type='email']",  # XPath fallback
        ],
        description="Email/username input field on login page"
    ),
    "password": SelectorConfig(
        primary="input[name='password'], input[type='password']",
        fallbacks=[
            "#password",
            "#login-password",
            "input.password-input",
            "#pass",
            "input[name='login[password]']",
        ],
        description="Password input field on login page"
    ),
    "submit": SelectorConfig(
        primary="button[type='submit'], input[type='submit']",
        fallbacks=[
            "#login-submit",
            ".login-btn",
            "button.btn-primary",
        ],
        description="Login form submit button"
    ),
    "csrf_token": SelectorConfig(
        primary="input[name='_token'], input[name='csrf_token']",
        fallbacks=[
            "input[name='authenticity_token']",
            "meta[name='csrf-token']",
        ],
        description="CSRF token hidden input or meta tag"
    ),
    "login_error": SelectorConfig(
        primary=".alert-danger, .error-message, .login-error",
        fallbacks=[
            ".flash-error",
            "#login-error",
        ],
        description="Login error message container"
    ),
    "logged_in_indicator": SelectorConfig(
        primary=".user-menu, .account-dropdown, .my-account",
        fallbacks=[
            "#user-dropdown",
            ".logged-in",
            "a[href*='logout']",
        ],
        description="Element that indicates user is logged in"
    ),
}

# =============================================================================
# PRODUCT SEARCH SELECTORS
# =============================================================================

SEARCH_SELECTORS = {
    "search_input": SelectorConfig(
        primary="input[name='q'], input[name='search'], #search",
        fallbacks=[
            ".search-input",
            "input.search-field",
            "#product-search",
            "input[type='text'][placeholder*='Search']",
        ],
        description="Product search input field"
    ),
    "search_submit": SelectorConfig(
        primary="button[type='submit'].search-btn, .search-button, button.action.search",
        fallbacks=[
            "#search-submit",
            ".btn-search",
            "button[title='Search']",
            "button:has(.fa-search)",
        ],
        description="Search form submit button"
    ),
    "search_results": SelectorConfig(
        primary=".product-list, .search-results, .products",
        fallbacks=[
            "#search-results",
            ".product-grid",
        ],
        description="Container for search results"
    ),
    "product_card": SelectorConfig(
        primary=".product-card, .product-item, .product",
        fallbacks=[
            ".item-card",
            "[data-product-id]",
        ],
        description="Individual product card in results"
    ),
    "product_sku": SelectorConfig(
        primary=".sku, .product-sku, [data-sku]",
        fallbacks=[
            ".item-number",
            ".product-code",
        ],
        description="SKU/item number on product card"
    ),
    "product_price": SelectorConfig(
        primary=".price, .product-price, .item-price",
        fallbacks=[
            ".current-price",
            "[data-price]",
        ],
        description="Price on product card"
    ),
    "product_availability": SelectorConfig(
        primary=".availability, .stock-status, .in-stock",
        fallbacks=[
            ".stock-info",
            ".inventory-status",
        ],
        description="Stock/availability indicator"
    ),
    "no_results": SelectorConfig(
        primary=".no-results, .empty-results, .no-products",
        fallbacks=[
            ".search-empty",
            "p:contains('no results')",
        ],
        description="No results message"
    ),
}

# =============================================================================
# PRODUCT DETAIL SELECTORS
# =============================================================================

PRODUCT_DETAIL_SELECTORS = {
    "product_title": SelectorConfig(
        primary="h1.product-title, .product-name h1",
        fallbacks=[
            "#product-title",
            ".item-title",
        ],
        description="Product title/name"
    ),
    "product_price": SelectorConfig(
        primary=".product-price, .price-current, #product-price",
        fallbacks=[
            ".item-price",
            "[data-product-price]",
        ],
        description="Product price on detail page"
    ),
    "quantity_input": SelectorConfig(
        primary="input[name='quantity'], input.qty, #quantity",
        fallbacks=[
            "input.qty-input",
            "[data-qty-input]",
        ],
        description="Quantity input field"
    ),
    "add_to_cart": SelectorConfig(
        primary="button.add-to-cart, #add-to-cart, .btn-add-cart",
        fallbacks=[
            "[data-action='add-to-cart']",
            ".add-cart-btn",
        ],
        description="Add to cart button"
    ),
    "in_stock_indicator": SelectorConfig(
        primary=".in-stock, .available, .stock-available",
        fallbacks=[
            ".availability-in-stock",
            ":has-text('In Stock')",
        ],
        description="In stock indicator"
    ),
    "out_of_stock_indicator": SelectorConfig(
        primary=".out-of-stock, .unavailable, .stock-unavailable",
        fallbacks=[
            ".availability-out",
            ":has-text('Out of Stock')",
        ],
        description="Out of stock indicator"
    ),
    "backorder_indicator": SelectorConfig(
        primary=".backorder, .pre-order, .back-order",
        fallbacks=[
            ".availability-backorder",
            ":has-text('Backorder')",
        ],
        description="Backorder indicator"
    ),
    "backorder_date": SelectorConfig(
        primary=".backorder-date, .expected-date, .eta",
        fallbacks=[
            ".availability-date",
            ".ship-date",
        ],
        description="Expected backorder date"
    ),
}

# =============================================================================
# CART SELECTORS
# =============================================================================

CART_SELECTORS = {
    "cart_icon": SelectorConfig(
        primary=".cart-icon, #cart-link, a[href*='cart']",
        fallbacks=[
            ".shopping-cart",
            ".mini-cart",
        ],
        description="Cart icon/link in header"
    ),
    "cart_count": SelectorConfig(
        primary=".cart-count, .cart-qty, #cart-count",
        fallbacks=[
            ".cart-items-count",
            ".badge-cart",
        ],
        description="Cart item count badge"
    ),
    "cart_items": SelectorConfig(
        primary=".cart-item, .cart-line-item, .cart-product",
        fallbacks=[
            ".line-item",
            "[data-cart-item]",
        ],
        description="Individual cart line items"
    ),
    "cart_item_qty": SelectorConfig(
        primary="input.qty, input[name='qty'], .item-qty input",
        fallbacks=[
            ".quantity-input",
            "[data-qty]",
        ],
        description="Quantity input in cart"
    ),
    "cart_item_remove": SelectorConfig(
        primary=".remove-item, .btn-remove, a.remove",
        fallbacks=[
            "[data-action='remove']",
            ".delete-item",
        ],
        description="Remove item button"
    ),
    "cart_subtotal": SelectorConfig(
        primary=".cart-subtotal, .subtotal, #cart-total",
        fallbacks=[
            ".order-subtotal",
            "[data-cart-subtotal]",
        ],
        description="Cart subtotal amount"
    ),
    "cart_empty": SelectorConfig(
        primary=".cart-empty, .empty-cart, .no-items",
        fallbacks=[
            ":has-text('cart is empty')",
        ],
        description="Empty cart message"
    ),
    "proceed_to_checkout": SelectorConfig(
        primary="a.checkout-btn, button.checkout, #checkout-btn",
        fallbacks=[
            "[href*='checkout']",
            ".btn-checkout",
        ],
        description="Proceed to checkout button"
    ),
}

# =============================================================================
# CHECKOUT SELECTORS
# =============================================================================

CHECKOUT_SELECTORS = {
    # Shipping Address
    "shipping_name": SelectorConfig(
        primary="input[name='shipping_name'], #shipping-name",
        fallbacks=[
            "input[name='ship_to_name']",
            "#ship-name",
        ],
        description="Shipping recipient name"
    ),
    "shipping_address1": SelectorConfig(
        primary="input[name='shipping_address1'], #shipping-address1",
        fallbacks=[
            "input[name='ship_address_1']",
            "#ship-address",
        ],
        description="Shipping address line 1"
    ),
    "shipping_address2": SelectorConfig(
        primary="input[name='shipping_address2'], #shipping-address2",
        fallbacks=[
            "input[name='ship_address_2']",
        ],
        description="Shipping address line 2"
    ),
    "shipping_city": SelectorConfig(
        primary="input[name='shipping_city'], #shipping-city",
        fallbacks=[
            "input[name='ship_city']",
        ],
        description="Shipping city"
    ),
    "shipping_state": SelectorConfig(
        primary="select[name='shipping_state'], #shipping-state",
        fallbacks=[
            "select[name='ship_state']",
            "#ship-state",
        ],
        description="Shipping state dropdown"
    ),
    "shipping_zip": SelectorConfig(
        primary="input[name='shipping_zip'], #shipping-zip",
        fallbacks=[
            "input[name='ship_zip']",
            "#ship-postal",
        ],
        description="Shipping ZIP/postal code"
    ),

    # Shipping Methods
    "shipping_methods_container": SelectorConfig(
        primary=".shipping-methods, #shipping-options, .delivery-options",
        fallbacks=[
            ".shipping-rates",
            "[data-shipping-methods]",
        ],
        description="Container for shipping method options"
    ),
    "shipping_method_option": SelectorConfig(
        primary=".shipping-method, .shipping-option, input[name='shipping_method']",
        fallbacks=[
            ".delivery-option",
            "[data-shipping-rate]",
        ],
        description="Individual shipping method radio/option"
    ),
    "shipping_method_price": SelectorConfig(
        primary=".shipping-price, .shipping-cost, .method-price",
        fallbacks=[
            ".rate-amount",
            "[data-shipping-price]",
        ],
        description="Shipping method price"
    ),
    "shipping_method_name": SelectorConfig(
        primary=".shipping-name, .method-name, .carrier-name",
        fallbacks=[
            ".rate-name",
            "[data-shipping-name]",
        ],
        description="Shipping method name/carrier"
    ),

    # Order Summary
    "order_subtotal": SelectorConfig(
        primary=".order-subtotal, #subtotal, .summary-subtotal",
        fallbacks=[
            "[data-subtotal]",
        ],
        description="Order subtotal"
    ),
    "order_shipping": SelectorConfig(
        primary=".order-shipping, #shipping-total, .summary-shipping",
        fallbacks=[
            "[data-shipping-total]",
        ],
        description="Order shipping total"
    ),
    "order_tax": SelectorConfig(
        primary=".order-tax, #tax-total, .summary-tax",
        fallbacks=[
            "[data-tax-total]",
        ],
        description="Order tax amount"
    ),
    "order_total": SelectorConfig(
        primary=".order-total, #grand-total, .summary-total",
        fallbacks=[
            "[data-grand-total]",
        ],
        description="Order grand total"
    ),

    # Place Order
    "place_order": SelectorConfig(
        primary="button#place-order, button.place-order, #submit-order",
        fallbacks=[
            "[data-action='place-order']",
            ".btn-place-order",
        ],
        description="Place order button"
    ),
}

# =============================================================================
# ORDER CONFIRMATION SELECTORS
# =============================================================================

ORDER_CONFIRMATION_SELECTORS = {
    "confirmation_container": SelectorConfig(
        primary=".order-confirmation, #confirmation, .thank-you",
        fallbacks=[
            ".order-complete",
            ".checkout-success",
        ],
        description="Order confirmation page container"
    ),
    "order_number": SelectorConfig(
        primary=".order-number, #order-id, .confirmation-number",
        fallbacks=[
            "[data-order-number]",
            ".order-reference",
        ],
        description="Order/confirmation number"
    ),
    "order_date": SelectorConfig(
        primary=".order-date, .confirmation-date",
        fallbacks=[
            "[data-order-date]",
        ],
        description="Order date"
    ),
    "order_total_confirmed": SelectorConfig(
        primary=".order-total, .confirmation-total",
        fallbacks=[
            "[data-confirmation-total]",
        ],
        description="Confirmed order total"
    ),
}

# =============================================================================
# ORDER STATUS/HISTORY SELECTORS
# =============================================================================

ORDER_STATUS_SELECTORS = {
    "order_history_link": SelectorConfig(
        primary="a[href*='orders'], a[href*='order-history']",
        fallbacks=[
            ".order-history-link",
            "#my-orders",
        ],
        description="Link to order history page"
    ),
    "order_list": SelectorConfig(
        primary=".order-list, .orders-table, #order-history",
        fallbacks=[
            ".my-orders",
            "[data-orders]",
        ],
        description="Order history list/table"
    ),
    "order_row": SelectorConfig(
        primary=".order-row, .order-item, tr.order",
        fallbacks=[
            "[data-order-id]",
        ],
        description="Individual order row"
    ),
    "order_status": SelectorConfig(
        primary=".order-status, .status, .order-state",
        fallbacks=[
            "[data-status]",
            ".status-badge",
        ],
        description="Order status text"
    ),
    "tracking_number": SelectorConfig(
        primary=".tracking-number, .tracking, a[href*='track']",
        fallbacks=[
            "[data-tracking]",
            ".shipment-tracking",
        ],
        description="Tracking number"
    ),
    "tracking_link": SelectorConfig(
        primary="a.tracking-link, a[href*='tracking']",
        fallbacks=[
            ".track-shipment",
        ],
        description="Tracking link"
    ),
}

# =============================================================================
# SAVED ADDRESSES SELECTORS
# =============================================================================

SAVED_ADDRESSES_SELECTORS = {
    "address_list": SelectorConfig(
        primary=".address-list, .saved-addresses, #addresses",
        fallbacks=[
            ".my-addresses",
        ],
        description="Saved addresses list"
    ),
    "address_card": SelectorConfig(
        primary=".address-card, .address-item, .saved-address",
        fallbacks=[
            "[data-address-id]",
        ],
        description="Individual saved address card"
    ),
    "select_address": SelectorConfig(
        primary="input[name='address_id'], .select-address, .use-address",
        fallbacks=[
            "[data-action='select-address']",
        ],
        description="Select/use this address button"
    ),
    "add_new_address": SelectorConfig(
        primary=".add-address, #add-new-address, a[href*='address/new']",
        fallbacks=[
            ".btn-add-address",
        ],
        description="Add new address button"
    ),
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_selector(category: str, key: str) -> Optional[SelectorConfig]:
    """
    Get a selector configuration by category and key.

    Args:
        category: Selector category (e.g., 'login', 'cart', 'checkout')
        key: Selector key within the category

    Returns:
        SelectorConfig if found, None otherwise
    """
    category_map = {
        "login": LOGIN_SELECTORS,
        "search": SEARCH_SELECTORS,
        "product_detail": PRODUCT_DETAIL_SELECTORS,
        "cart": CART_SELECTORS,
        "checkout": CHECKOUT_SELECTORS,
        "order_confirmation": ORDER_CONFIRMATION_SELECTORS,
        "order_status": ORDER_STATUS_SELECTORS,
        "saved_addresses": SAVED_ADDRESSES_SELECTORS,
    }

    category_selectors = category_map.get(category)
    if category_selectors:
        return category_selectors.get(key)
    return None


def get_all_selectors_for_category(category: str) -> Dict[str, SelectorConfig]:
    """Get all selectors for a category."""
    category_map = {
        "login": LOGIN_SELECTORS,
        "search": SEARCH_SELECTORS,
        "product_detail": PRODUCT_DETAIL_SELECTORS,
        "cart": CART_SELECTORS,
        "checkout": CHECKOUT_SELECTORS,
        "order_confirmation": ORDER_CONFIRMATION_SELECTORS,
        "order_status": ORDER_STATUS_SELECTORS,
        "saved_addresses": SAVED_ADDRESSES_SELECTORS,
    }
    return category_map.get(category, {})


def validate_selectors() -> Dict[str, List[str]]:
    """
    Validate that all selectors have required fields.

    Returns:
        Dictionary of category -> list of issues
    """
    issues = {}

    all_categories = {
        "login": LOGIN_SELECTORS,
        "search": SEARCH_SELECTORS,
        "product_detail": PRODUCT_DETAIL_SELECTORS,
        "cart": CART_SELECTORS,
        "checkout": CHECKOUT_SELECTORS,
        "order_confirmation": ORDER_CONFIRMATION_SELECTORS,
        "order_status": ORDER_STATUS_SELECTORS,
        "saved_addresses": SAVED_ADDRESSES_SELECTORS,
    }

    for category, selectors in all_categories.items():
        category_issues = []
        for key, config in selectors.items():
            if not config.primary:
                category_issues.append(f"{key}: Missing primary selector")
            if not config.description:
                category_issues.append(f"{key}: Missing description")
        if category_issues:
            issues[category] = category_issues

    return issues
