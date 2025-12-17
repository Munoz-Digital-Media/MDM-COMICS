"""
BCW Cart Builder

Builds shadow carts on BCW for shipping quote retrieval.
Per challenge_1_shipping_unknown_until_submit in proposal doc.

Shadow cart flow:
1. Clear existing cart
2. Add items to cart
3. Navigate to checkout
4. Extract shipping rates
5. Abandon cart (don't complete checkout)
"""
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass

from app.services.bcw.browser_client import (
    BCWBrowserClient,
    ShippingOption,
    ProductInfo,
)
from app.core.exceptions import BCWCartError, BCWError

logger = logging.getLogger(__name__)


@dataclass
class CartItem:
    """Item to add to cart."""
    sku: str
    quantity: int
    bcw_sku: Optional[str] = None


@dataclass
class CartQuoteResult:
    """Result of getting shipping quotes from shadow cart."""
    success: bool
    shipping_options: List[ShippingOption]
    cart_subtotal: Optional[float] = None
    error_message: Optional[str] = None


class BCWCartBuilder:
    """
    Builds shadow carts for shipping quote extraction.

    Usage:
        async with BCWBrowserClient() as client:
            await client.login(username, password)

            cart_builder = BCWCartBuilder(client)
            result = await cart_builder.get_shipping_quote(
                items=[CartItem(sku="ABC123", quantity=2)],
                address={"name": "John", "address1": "123 Main", ...}
            )
    """

    def __init__(self, browser_client: BCWBrowserClient):
        self.client = browser_client

    async def build_cart(self, items: List[CartItem]) -> bool:
        """
        Build a cart with specified items.

        Args:
            items: List of CartItem to add

        Returns:
            True if all items added successfully
        """
        logger.info(f"Building shadow cart with {len(items)} items")

        # Clear existing cart first
        await self.client.clear_cart()

        # Add each item
        for item in items:
            try:
                success = await self.client.add_to_cart(item.sku, item.quantity)
                if not success:
                    logger.warning(f"Failed to add {item.sku} to cart")
                    return False
            except Exception as e:
                logger.error(f"Error adding {item.sku} to cart: {e}")
                return False

        # Verify cart count
        cart_count = await self.client.get_cart_count()
        expected_count = sum(item.quantity for item in items)

        if cart_count != expected_count:
            logger.warning(
                f"Cart count mismatch: expected {expected_count}, got {cart_count}"
            )

        logger.info(f"Shadow cart built with {cart_count} items")
        return True

    async def get_shipping_quote(
        self,
        items: List[CartItem],
        address: Dict[str, str],
    ) -> CartQuoteResult:
        """
        Build a shadow cart and get shipping quotes.

        Args:
            items: Items to add to cart
            address: Shipping address dictionary

        Returns:
            CartQuoteResult with shipping options
        """
        logger.info("Getting shipping quote via shadow cart")

        try:
            # Build the cart
            if not await self.build_cart(items):
                return CartQuoteResult(
                    success=False,
                    shipping_options=[],
                    error_message="Failed to build shadow cart",
                )

            # Get shipping rates
            shipping_options = await self.client.get_shipping_rates(address)

            if not shipping_options:
                return CartQuoteResult(
                    success=False,
                    shipping_options=[],
                    error_message="No shipping options returned",
                )

            logger.info(f"Got {len(shipping_options)} shipping options")

            return CartQuoteResult(
                success=True,
                shipping_options=shipping_options,
            )

        except BCWError as e:
            logger.error(f"Shadow cart quote failed: {e.message}")
            return CartQuoteResult(
                success=False,
                shipping_options=[],
                error_message=e.message,
            )
        except Exception as e:
            logger.error(f"Unexpected error in shadow cart: {e}")
            return CartQuoteResult(
                success=False,
                shipping_options=[],
                error_message=str(e),
            )
        finally:
            # Always clear cart after getting quote
            try:
                await self.client.clear_cart()
            except Exception:
                pass

    async def validate_items_available(
        self,
        items: List[CartItem],
    ) -> Dict[str, ProductInfo]:
        """
        Check availability of items before building cart.

        Args:
            items: Items to check

        Returns:
            Dict mapping SKU to ProductInfo
        """
        results = {}

        for item in items:
            try:
                product = await self.client.search_product(item.sku)
                if product:
                    results[item.sku] = product
                else:
                    results[item.sku] = ProductInfo(
                        sku=item.sku,
                        in_stock=False,
                    )
            except Exception as e:
                logger.error(f"Failed to check availability for {item.sku}: {e}")
                results[item.sku] = ProductInfo(
                    sku=item.sku,
                    in_stock=False,
                )

        return results
