"""
BCW Browser Client

Playwright-based browser automation for BCW website.
Per 20251216_mdm_comics_bcw_initial_integration.json v1.2.0.

Features:
- Headless Chrome automation
- Anti-bot mitigation (human-like delays, random mouse movements)
- Session persistence via cookies
- Circuit breaker pattern
- Screenshot capture on errors
- Selector fallback strategy

IMPORTANT: This requires the Playwright browser to be installed.
Run: playwright install chromium
"""
import asyncio
import logging
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

try:
    from playwright.async_api import (
        async_playwright,
        Browser,
        BrowserContext,
        Page,
        Playwright,
        TimeoutError as PlaywrightTimeoutError,
        Error as PlaywrightError,
    )
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    Browser = None
    BrowserContext = None
    Page = None
    Playwright = None
    PlaywrightTimeoutError = Exception
    PlaywrightError = Exception

from app.core.config import settings
from app.core.exceptions import (
    BCWError,
    BCWAuthError,
    BCWSelectorError,
    BCWRateLimitError,
    BCWCircuitOpenError,
)
from app.services.bcw.selectors import (
    SELECTOR_VERSION,
    get_selector,
    SelectorConfig,
)

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Base URL (can be overridden by settings)
BCW_BASE_URL = getattr(settings, 'BCW_BASE_URL', 'https://www.bcwsupplies.com')

# Rate limiting
MIN_ACTION_DELAY_MS = getattr(settings, 'BCW_MIN_ACTION_DELAY_MS', 2000)
MAX_ACTION_DELAY_MS = getattr(settings, 'BCW_MAX_ACTION_DELAY_MS', 5000)
MAX_ACTIONS_PER_HOUR = getattr(settings, 'BCW_MAX_ACTIONS_PER_HOUR', 60)

# Circuit breaker
CIRCUIT_BREAKER_THRESHOLD = getattr(settings, 'BCW_CIRCUIT_BREAKER_THRESHOLD', 5)
CIRCUIT_BREAKER_RESET_MS = getattr(settings, 'BCW_CIRCUIT_BREAKER_RESET_MS', 30000)

# Timeouts
DEFAULT_TIMEOUT_MS = 30000
NAVIGATION_TIMEOUT_MS = 60000

# Screenshots directory
SCREENSHOTS_DIR = Path("./bcw_screenshots")


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ProductInfo:
    """Product information extracted from BCW."""
    sku: str
    bcw_sku: Optional[str] = None
    name: Optional[str] = None
    price: Optional[float] = None
    in_stock: bool = False
    available_qty: Optional[int] = None
    backorder: bool = False
    backorder_date: Optional[str] = None


@dataclass
class ShippingOption:
    """Shipping option extracted from checkout."""
    method_id: str
    carrier: str
    service_name: str
    price: float
    estimated_days: Optional[int] = None


@dataclass
class OrderConfirmation:
    """Order confirmation data."""
    bcw_order_id: str
    confirmation_number: Optional[str] = None
    total: Optional[float] = None
    shipping_cost: Optional[float] = None


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================

class CircuitBreaker:
    """
    Simple circuit breaker implementation.

    States:
    - CLOSED: Normal operation
    - OPEN: Failing, reject requests
    - HALF_OPEN: Testing if service recovered
    """

    def __init__(self, threshold: int = 5, reset_timeout_ms: int = 30000):
        self.threshold = threshold
        self.reset_timeout_ms = reset_timeout_ms
        self.failures = 0
        self.state = "CLOSED"
        self.opened_at: Optional[datetime] = None

    def record_success(self):
        """Record a successful call."""
        self.failures = 0
        self.state = "CLOSED"
        self.opened_at = None

    def record_failure(self):
        """Record a failed call."""
        self.failures += 1
        if self.failures >= self.threshold:
            self.state = "OPEN"
            self.opened_at = datetime.now(timezone.utc)
            logger.warning(f"Circuit breaker OPEN after {self.failures} failures")

    def can_proceed(self) -> bool:
        """Check if a call can proceed."""
        if self.state == "CLOSED":
            return True

        if self.state == "OPEN":
            # Check if reset timeout has passed
            if self.opened_at:
                elapsed_ms = (datetime.now(timezone.utc) - self.opened_at).total_seconds() * 1000
                if elapsed_ms >= self.reset_timeout_ms:
                    self.state = "HALF_OPEN"
                    logger.info("Circuit breaker HALF_OPEN, testing...")
                    return True
            return False

        # HALF_OPEN - allow one request through
        return True

    def to_dict(self) -> Dict[str, Any]:
        """Export state for persistence."""
        return {
            "state": self.state,
            "failures": self.failures,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
        }


# =============================================================================
# BCW BROWSER CLIENT
# =============================================================================

class BCWBrowserClient:
    """
    Playwright-based browser automation for BCW website.

    Usage:
        async with BCWBrowserClient() as client:
            await client.login(username, password)
            product = await client.search_product("SKU123")
            await client.add_to_cart("SKU123", 1)
    """

    def __init__(self, base_url: str = BCW_BASE_URL, headless: bool = True):
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError(
                "Playwright is not installed. Run: pip install playwright && playwright install chromium"
            )

        self.base_url = base_url
        self.headless = headless

        # Playwright objects
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

        # State
        self._is_logged_in = False
        self._last_action_at: Optional[datetime] = None
        self._actions_this_hour = 0
        self._hour_reset_at: Optional[datetime] = None

        # Circuit breaker
        self._circuit_breaker = CircuitBreaker(
            threshold=CIRCUIT_BREAKER_THRESHOLD,
            reset_timeout_ms=CIRCUIT_BREAKER_RESET_MS,
        )

        # Ensure screenshots directory exists
        SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    async def __aenter__(self) -> "BCWBrowserClient":
        """Async context manager entry."""
        await self.init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def init(self):
        """Initialize the browser."""
        logger.info("Initializing BCW browser client...")

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

        # Create context with realistic settings
        self._context = await self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
        )

        # Create page
        self._page = await self._context.new_page()

        # Set default timeouts
        self._page.set_default_timeout(DEFAULT_TIMEOUT_MS)
        self._page.set_default_navigation_timeout(NAVIGATION_TIMEOUT_MS)

        logger.info("BCW browser client initialized")

    async def close(self):
        """Close the browser and cleanup."""
        logger.info("Closing BCW browser client...")

        if self._page:
            await self._page.close()
            self._page = None

        if self._context:
            await self._context.close()
            self._context = None

        if self._browser:
            await self._browser.close()
            self._browser = None

        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

        logger.info("BCW browser client closed")

    # =========================================================================
    # ANTI-BOT MITIGATION
    # =========================================================================

    async def _human_delay(self, min_ms: int = None, max_ms: int = None):
        """Add human-like delay between actions."""
        min_delay = min_ms or MIN_ACTION_DELAY_MS
        max_delay = max_ms or MAX_ACTION_DELAY_MS
        delay_ms = random.uniform(min_delay, max_delay)
        await asyncio.sleep(delay_ms / 1000)

    async def _random_mouse_movement(self):
        """Simulate random mouse movement."""
        if not self._page:
            return

        try:
            # Get viewport size
            viewport = self._page.viewport_size
            if not viewport:
                return

            # Move to random position
            x = random.randint(100, viewport["width"] - 100)
            y = random.randint(100, viewport["height"] - 100)
            await self._page.mouse.move(x, y)
        except Exception:
            pass  # Non-critical

    async def _check_rate_limit(self):
        """Check and enforce rate limiting."""
        now = datetime.now(timezone.utc)

        # Reset hourly counter if needed
        if self._hour_reset_at is None or now >= self._hour_reset_at:
            self._actions_this_hour = 0
            self._hour_reset_at = now.replace(minute=0, second=0, microsecond=0)
            # Add one hour
            from datetime import timedelta
            self._hour_reset_at += timedelta(hours=1)

        # Check limit
        if self._actions_this_hour >= MAX_ACTIONS_PER_HOUR:
            raise BCWRateLimitError(
                message=f"Rate limit exceeded: {MAX_ACTIONS_PER_HOUR} actions/hour",
                retry_after_seconds=int((self._hour_reset_at - now).total_seconds()),
            )

        self._actions_this_hour += 1
        self._last_action_at = now

    async def _check_circuit_breaker(self):
        """Check circuit breaker state."""
        if not self._circuit_breaker.can_proceed():
            raise BCWCircuitOpenError(
                message="Circuit breaker is OPEN - BCW automation suspended",
                details=self._circuit_breaker.to_dict(),
            )

    # =========================================================================
    # SELECTOR HELPERS
    # =========================================================================

    async def _find_element(
        self,
        category: str,
        key: str,
        timeout: int = DEFAULT_TIMEOUT_MS,
    ):
        """
        Find an element using selector with fallbacks.

        Args:
            category: Selector category (e.g., 'login', 'cart')
            key: Selector key within the category
            timeout: Timeout in milliseconds

        Returns:
            Element handle if found

        Raises:
            BCWSelectorError: If all selectors fail
        """
        if not self._page:
            raise BCWError("Browser not initialized", code="BCW_NOT_INITIALIZED")

        selector_config = get_selector(category, key)
        if not selector_config:
            raise BCWSelectorError(
                message=f"Unknown selector: {category}.{key}",
                selector_key=f"{category}.{key}",
                selector_version=SELECTOR_VERSION,
            )

        all_selectors = selector_config.all_selectors()
        last_error = None

        for selector in all_selectors:
            try:
                element = await self._page.wait_for_selector(
                    selector,
                    timeout=timeout // len(all_selectors),  # Divide timeout
                    state="visible",
                )
                if element:
                    return element
            except PlaywrightTimeoutError:
                last_error = f"Timeout waiting for selector: {selector}"
                continue
            except Exception as e:
                last_error = str(e)
                continue

        # All selectors failed - capture screenshot
        screenshot_path = await self._capture_error_screenshot(f"{category}_{key}")

        raise BCWSelectorError(
            message=f"All selectors failed for {category}.{key}",
            selector_key=f"{category}.{key}",
            selector_version=SELECTOR_VERSION,
            screenshot_path=screenshot_path,
            details={"last_error": last_error},
        )

    async def _fill_input(self, category: str, key: str, value: str):
        """Fill an input field with human-like typing."""
        element = await self._find_element(category, key)
        await element.click()
        await self._human_delay(100, 300)

        # Type with slight delays between characters
        await element.fill("")  # Clear first
        for char in value:
            await element.type(char, delay=random.uniform(50, 150))

    async def _click_element(self, category: str, key: str):
        """Click an element with random mouse movement first."""
        await self._random_mouse_movement()
        await self._human_delay(200, 500)

        element = await self._find_element(category, key)
        await element.click()

    async def _capture_error_screenshot(self, name: str) -> str:
        """Capture screenshot on error for debugging."""
        if not self._page:
            return ""

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"error_{name}_{timestamp}.png"
        filepath = SCREENSHOTS_DIR / filename

        try:
            await self._page.screenshot(path=str(filepath), full_page=True)
            logger.info(f"Error screenshot saved: {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Failed to capture screenshot: {e}")
            return ""

    # =========================================================================
    # NAVIGATION
    # =========================================================================

    async def navigate(self, path: str = "/"):
        """Navigate to a page on BCW website."""
        await self._check_circuit_breaker()
        await self._check_rate_limit()

        url = f"{self.base_url}{path}"
        logger.info(f"Navigating to: {url}")

        try:
            await self._page.goto(url, wait_until="domcontentloaded")
            await self._human_delay()
            self._circuit_breaker.record_success()
        except Exception as e:
            self._circuit_breaker.record_failure()
            raise BCWError(f"Navigation failed: {e}", code="BCW_NAVIGATION_FAILED")

    # =========================================================================
    # AUTHENTICATION
    # =========================================================================

    async def login(self, username: str, password: str) -> bool:
        """
        Log in to BCW website.

        Args:
            username: BCW account email
            password: BCW account password

        Returns:
            True if login successful

        Raises:
            BCWAuthError: If login fails
        """
        await self._check_circuit_breaker()
        await self._check_rate_limit()

        logger.info("Attempting BCW login...")

        try:
            # Navigate to login page
            await self.navigate("/account/login")
            await self._human_delay()

            # Fill login form
            await self._fill_input("login", "username", username)
            await self._human_delay(500, 1000)
            await self._fill_input("login", "password", password)
            await self._human_delay(500, 1000)

            # Submit form
            await self._click_element("login", "submit")

            # Wait for navigation
            await self._page.wait_for_load_state("domcontentloaded")
            await self._human_delay()

            # Check for login error
            try:
                error_element = await self._page.wait_for_selector(
                    get_selector("login", "login_error").primary,
                    timeout=3000,
                )
                if error_element:
                    error_text = await error_element.text_content()
                    raise BCWAuthError(
                        message=f"Login failed: {error_text}",
                        code="BCW_AUTH_FAILED",
                    )
            except PlaywrightTimeoutError:
                pass  # No error element = success

            # Verify logged in
            try:
                await self._page.wait_for_selector(
                    get_selector("login", "logged_in_indicator").primary,
                    timeout=5000,
                )
                self._is_logged_in = True
                self._circuit_breaker.record_success()
                logger.info("BCW login successful")
                return True
            except PlaywrightTimeoutError:
                raise BCWAuthError(
                    message="Login appeared to succeed but no logged-in indicator found",
                    code="BCW_AUTH_VERIFICATION_FAILED",
                )

        except BCWAuthError:
            self._circuit_breaker.record_failure()
            raise
        except Exception as e:
            self._circuit_breaker.record_failure()
            screenshot = await self._capture_error_screenshot("login")
            raise BCWAuthError(
                message=f"Login failed unexpectedly: {e}",
                code="BCW_AUTH_UNEXPECTED",
                details={"screenshot": screenshot},
            )

    async def is_logged_in(self) -> bool:
        """Check if currently logged in."""
        if not self._page:
            return False

        try:
            await self._page.wait_for_selector(
                get_selector("login", "logged_in_indicator").primary,
                timeout=3000,
            )
            return True
        except PlaywrightTimeoutError:
            return False

    # =========================================================================
    # PRODUCT SEARCH
    # =========================================================================

    async def search_product(self, sku: str) -> Optional[ProductInfo]:
        """
        Search for a product by SKU.

        Args:
            sku: Product SKU to search for

        Returns:
            ProductInfo if found, None otherwise
        """
        await self._check_circuit_breaker()
        await self._check_rate_limit()

        logger.info(f"Searching for product: {sku}")

        try:
            # Navigate to search or use search box
            await self._fill_input("search", "search_input", sku)
            await self._click_element("search", "search_submit")

            await self._page.wait_for_load_state("domcontentloaded")
            await self._human_delay()

            # Check for no results
            try:
                await self._page.wait_for_selector(
                    get_selector("search", "no_results").primary,
                    timeout=3000,
                )
                logger.info(f"No results for SKU: {sku}")
                return None
            except PlaywrightTimeoutError:
                pass

            # Find product in results
            product_cards = await self._page.query_selector_all(
                get_selector("search", "product_card").primary
            )

            if not product_cards:
                return None

            # Extract product info from first result
            card = product_cards[0]

            # Get SKU
            sku_element = await card.query_selector(
                get_selector("search", "product_sku").primary
            )
            bcw_sku = await sku_element.text_content() if sku_element else None

            # Get price
            price_element = await card.query_selector(
                get_selector("search", "product_price").primary
            )
            price_text = await price_element.text_content() if price_element else None
            price = self._parse_price(price_text)

            # Get availability
            avail_element = await card.query_selector(
                get_selector("search", "product_availability").primary
            )
            avail_text = await avail_element.text_content() if avail_element else ""
            in_stock = "in stock" in avail_text.lower()
            backorder = "backorder" in avail_text.lower()

            self._circuit_breaker.record_success()

            return ProductInfo(
                sku=sku,
                bcw_sku=bcw_sku.strip() if bcw_sku else None,
                price=price,
                in_stock=in_stock,
                backorder=backorder,
            )

        except BCWSelectorError:
            self._circuit_breaker.record_failure()
            raise
        except Exception as e:
            self._circuit_breaker.record_failure()
            logger.error(f"Product search failed: {e}")
            return None

    def _parse_price(self, price_text: Optional[str]) -> Optional[float]:
        """Parse price from text like '$12.99'."""
        if not price_text:
            return None

        import re
        match = re.search(r'\$?([\d,]+\.?\d*)', price_text)
        if match:
            try:
                return float(match.group(1).replace(',', ''))
            except ValueError:
                return None
        return None

    # =========================================================================
    # CART OPERATIONS
    # =========================================================================

    async def add_to_cart(self, sku: str, quantity: int = 1) -> bool:
        """
        Add a product to the cart.

        Args:
            sku: Product SKU
            quantity: Quantity to add

        Returns:
            True if successful
        """
        await self._check_circuit_breaker()
        await self._check_rate_limit()

        logger.info(f"Adding to cart: {sku} x {quantity}")

        try:
            # Search for product first
            product = await self.search_product(sku)
            if not product:
                raise BCWError(f"Product not found: {sku}", code="BCW_PRODUCT_NOT_FOUND")

            # Click on product to go to detail page
            product_cards = await self._page.query_selector_all(
                get_selector("search", "product_card").primary
            )
            if product_cards:
                await product_cards[0].click()
                await self._page.wait_for_load_state("domcontentloaded")
                await self._human_delay()

            # Set quantity
            qty_input = await self._find_element("product_detail", "quantity_input")
            await qty_input.fill(str(quantity))
            await self._human_delay()

            # Add to cart
            await self._click_element("product_detail", "add_to_cart")
            await self._human_delay(1000, 2000)

            self._circuit_breaker.record_success()
            logger.info(f"Added to cart: {sku} x {quantity}")
            return True

        except BCWSelectorError:
            self._circuit_breaker.record_failure()
            raise
        except Exception as e:
            self._circuit_breaker.record_failure()
            logger.error(f"Add to cart failed: {e}")
            return False

    async def get_cart_count(self) -> int:
        """Get the number of items in the cart."""
        try:
            cart_count_element = await self._find_element("cart", "cart_count", timeout=5000)
            count_text = await cart_count_element.text_content()
            return int(count_text.strip()) if count_text else 0
        except Exception:
            return 0

    async def clear_cart(self):
        """Remove all items from the cart."""
        await self._check_circuit_breaker()
        await self._check_rate_limit()

        logger.info("Clearing cart...")

        try:
            # Navigate to cart
            await self._click_element("cart", "cart_icon")
            await self._page.wait_for_load_state("domcontentloaded")
            await self._human_delay()

            # Remove items one by one
            while True:
                try:
                    remove_btn = await self._page.wait_for_selector(
                        get_selector("cart", "cart_item_remove").primary,
                        timeout=3000,
                    )
                    if remove_btn:
                        await remove_btn.click()
                        await self._human_delay(500, 1000)
                    else:
                        break
                except PlaywrightTimeoutError:
                    break

            self._circuit_breaker.record_success()
            logger.info("Cart cleared")

        except Exception as e:
            self._circuit_breaker.record_failure()
            logger.error(f"Clear cart failed: {e}")

    # =========================================================================
    # SHIPPING QUOTES
    # =========================================================================

    async def get_shipping_rates(
        self,
        address: Dict[str, str],
    ) -> List[ShippingOption]:
        """
        Get shipping rates for the current cart to an address.

        Args:
            address: Shipping address dictionary with keys:
                - name, address1, address2, city, state, zip

        Returns:
            List of ShippingOption
        """
        await self._check_circuit_breaker()
        await self._check_rate_limit()

        logger.info("Getting shipping rates...")

        try:
            # Navigate to checkout
            await self._click_element("cart", "proceed_to_checkout")
            await self._page.wait_for_load_state("domcontentloaded")
            await self._human_delay()

            # Fill shipping address
            await self._fill_input("checkout", "shipping_name", address.get("name", ""))
            await self._fill_input("checkout", "shipping_address1", address.get("address1", ""))
            if address.get("address2"):
                await self._fill_input("checkout", "shipping_address2", address["address2"])
            await self._fill_input("checkout", "shipping_city", address.get("city", ""))

            # Select state
            state_select = await self._find_element("checkout", "shipping_state")
            await state_select.select_option(value=address.get("state", ""))
            await self._human_delay()

            await self._fill_input("checkout", "shipping_zip", address.get("zip", ""))
            await self._human_delay(1000, 2000)

            # Wait for shipping methods to load
            await self._page.wait_for_selector(
                get_selector("checkout", "shipping_methods_container").primary,
                timeout=10000,
            )
            await self._human_delay()

            # Extract shipping options
            shipping_options = []
            method_elements = await self._page.query_selector_all(
                get_selector("checkout", "shipping_method_option").primary
            )

            for element in method_elements:
                try:
                    name_el = await element.query_selector(
                        get_selector("checkout", "shipping_method_name").primary
                    )
                    price_el = await element.query_selector(
                        get_selector("checkout", "shipping_method_price").primary
                    )

                    name = await name_el.text_content() if name_el else "Unknown"
                    price_text = await price_el.text_content() if price_el else "$0"
                    price = self._parse_price(price_text) or 0.0

                    # Try to get method ID from input
                    input_el = await element.query_selector("input")
                    method_id = await input_el.get_attribute("value") if input_el else name

                    shipping_options.append(ShippingOption(
                        method_id=method_id,
                        carrier=name.split()[0] if name else "Unknown",
                        service_name=name.strip() if name else "Unknown",
                        price=price,
                    ))
                except Exception as e:
                    logger.warning(f"Failed to parse shipping option: {e}")

            self._circuit_breaker.record_success()
            logger.info(f"Found {len(shipping_options)} shipping options")
            return shipping_options

        except BCWSelectorError:
            self._circuit_breaker.record_failure()
            raise
        except Exception as e:
            self._circuit_breaker.record_failure()
            logger.error(f"Get shipping rates failed: {e}")
            return []

    # =========================================================================
    # COOKIES / SESSION
    # =========================================================================

    async def get_cookies(self) -> List[Dict]:
        """Get current browser cookies for session persistence."""
        if not self._context:
            return []
        return await self._context.cookies()

    async def set_cookies(self, cookies: List[Dict]):
        """Restore cookies for session persistence."""
        if not self._context:
            return
        await self._context.add_cookies(cookies)

    # =========================================================================
    # STATE EXPORT
    # =========================================================================

    def get_state(self) -> Dict[str, Any]:
        """Export current state for persistence."""
        return {
            "is_logged_in": self._is_logged_in,
            "last_action_at": self._last_action_at.isoformat() if self._last_action_at else None,
            "actions_this_hour": self._actions_this_hour,
            "circuit_breaker": self._circuit_breaker.to_dict(),
        }
