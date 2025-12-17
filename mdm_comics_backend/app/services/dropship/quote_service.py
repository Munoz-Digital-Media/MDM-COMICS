"""
Dropship Quote Service

Manages shipping quote retrieval and caching for BCW orders.
Per challenge_1_shipping_unknown_until_submit in proposal doc.

Cache strategy:
- Key: quote:{address_hash}:{cart_hash}
- TTL: 15 minutes (900 seconds)
- Fallback: Get fresh quote if cache miss
"""
import hashlib
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete

from app.models.bcw import BCWShippingQuote
from app.services.bcw.browser_client import BCWBrowserClient, ShippingOption
from app.services.bcw.cart_builder import BCWCartBuilder, CartItem
from app.core.exceptions import ShippingError

logger = logging.getLogger(__name__)

# Quote cache TTL in seconds (15 minutes)
QUOTE_CACHE_TTL = 900


@dataclass
class ShippingQuoteResult:
    """Result of shipping quote request."""
    success: bool
    options: List[ShippingOption] = field(default_factory=list)
    from_cache: bool = False
    cache_key: Optional[str] = None
    expires_at: Optional[datetime] = None
    error_message: Optional[str] = None


class DropshipQuoteService:
    """
    Manages shipping quotes with caching.

    Usage:
        async with BCWBrowserClient() as client:
            await client.login(username, password)

            quote_service = DropshipQuoteService(client, db)
            result = await quote_service.get_shipping_quote(
                items=[CartItem(sku="ABC123", quantity=2)],
                address={"name": "John", "address1": "123 Main", ...}
            )
    """

    def __init__(
        self,
        browser_client: BCWBrowserClient,
        db: AsyncSession,
        cache_ttl: int = QUOTE_CACHE_TTL,
    ):
        self.client = browser_client
        self.db = db
        self.cache_ttl = cache_ttl
        self.cart_builder = BCWCartBuilder(browser_client)

    async def get_shipping_quote(
        self,
        items: List[CartItem],
        address: Dict[str, str],
        force_refresh: bool = False,
    ) -> ShippingQuoteResult:
        """
        Get shipping quotes for items to address.

        Checks cache first, then fetches fresh quote if needed.

        Args:
            items: Cart items
            address: Shipping address
            force_refresh: Skip cache and get fresh quote

        Returns:
            ShippingQuoteResult with shipping options
        """
        cache_key = self._generate_cache_key(items, address)

        # Check cache unless forced refresh
        if not force_refresh:
            cached = await self._get_cached_quote(cache_key)
            if cached:
                logger.info(f"Cache hit for shipping quote: {cache_key}")
                return cached

        # Fetch fresh quote via shadow cart
        logger.info(f"Fetching fresh shipping quote: {cache_key}")

        try:
            quote_result = await self.cart_builder.get_shipping_quote(items, address)

            if not quote_result.success:
                return ShippingQuoteResult(
                    success=False,
                    error_message=quote_result.error_message or "Failed to get shipping quote",
                )

            # Cache the result
            expires_at = await self._cache_quote(
                cache_key, items, address, quote_result.shipping_options
            )

            return ShippingQuoteResult(
                success=True,
                options=quote_result.shipping_options,
                from_cache=False,
                cache_key=cache_key,
                expires_at=expires_at,
            )

        except Exception as e:
            logger.error(f"Error getting shipping quote: {e}")
            return ShippingQuoteResult(
                success=False,
                error_message=str(e),
            )

    async def get_cheapest_option(
        self,
        items: List[CartItem],
        address: Dict[str, str],
    ) -> Optional[ShippingOption]:
        """Get the cheapest shipping option."""
        result = await self.get_shipping_quote(items, address)

        if not result.success or not result.options:
            return None

        return min(result.options, key=lambda x: x.price)

    async def get_fastest_option(
        self,
        items: List[CartItem],
        address: Dict[str, str],
    ) -> Optional[ShippingOption]:
        """Get the fastest shipping option (by estimated days)."""
        result = await self.get_shipping_quote(items, address)

        if not result.success or not result.options:
            return None

        # Filter options with estimated days
        with_estimates = [o for o in result.options if o.estimated_days]
        if with_estimates:
            return min(with_estimates, key=lambda x: x.estimated_days)

        # Fallback to most expensive (usually fastest)
        return max(result.options, key=lambda x: x.price)

    def _generate_cache_key(
        self,
        items: List[CartItem],
        address: Dict[str, str],
    ) -> str:
        """Generate cache key from items and address."""
        # Hash cart items (sorted for consistency)
        cart_str = "|".join(
            f"{item.sku}:{item.quantity}"
            for item in sorted(items, key=lambda x: x.sku)
        )
        cart_hash = hashlib.sha256(cart_str.encode()).hexdigest()[:16]

        # Hash address (sorted keys for consistency)
        addr_str = "|".join(
            f"{k}:{v}" for k, v in sorted(address.items())
        )
        addr_hash = hashlib.sha256(addr_str.encode()).hexdigest()[:16]

        return f"quote:{addr_hash}:{cart_hash}"

    async def _get_cached_quote(
        self,
        cache_key: str,
    ) -> Optional[ShippingQuoteResult]:
        """Get quote from cache if valid."""
        now = datetime.now(timezone.utc)

        result = await self.db.execute(
            select(BCWShippingQuote)
            .where(BCWShippingQuote.cache_key == cache_key)
            .where(BCWShippingQuote.expires_at > now)
        )
        cached = result.scalar_one_or_none()

        if not cached:
            return None

        # Deserialize options
        try:
            options_data = json.loads(cached.shipping_options_json)
            options = [
                ShippingOption(
                    method_id=opt.get("method_id", ""),
                    name=opt.get("name", ""),
                    price=opt.get("price", 0.0),
                    estimated_days=opt.get("estimated_days"),
                    carrier=opt.get("carrier"),
                )
                for opt in options_data
            ]

            return ShippingQuoteResult(
                success=True,
                options=options,
                from_cache=True,
                cache_key=cache_key,
                expires_at=cached.expires_at,
            )
        except Exception as e:
            logger.error(f"Failed to deserialize cached quote: {e}")
            return None

    async def _cache_quote(
        self,
        cache_key: str,
        items: List[CartItem],
        address: Dict[str, str],
        options: List[ShippingOption],
    ) -> datetime:
        """Cache quote in database."""
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=self.cache_ttl)

        # Serialize options
        options_data = [
            {
                "method_id": opt.method_id,
                "name": opt.name,
                "price": opt.price,
                "estimated_days": opt.estimated_days,
                "carrier": opt.carrier,
            }
            for opt in options
        ]

        # Serialize cart and address for debugging
        cart_data = [{"sku": item.sku, "quantity": item.quantity} for item in items]

        # Delete existing cache entry
        await self.db.execute(
            delete(BCWShippingQuote).where(BCWShippingQuote.cache_key == cache_key)
        )

        # Insert new entry
        quote = BCWShippingQuote(
            cache_key=cache_key,
            address_hash=cache_key.split(":")[1],
            cart_hash=cache_key.split(":")[2],
            shipping_options_json=json.dumps(options_data),
            address_json=json.dumps(address),
            cart_items_json=json.dumps(cart_data),
            expires_at=expires_at,
            created_at=now,
        )
        self.db.add(quote)
        await self.db.flush()

        return expires_at

    async def invalidate_cache(self, cache_key: str):
        """Invalidate a specific cache entry."""
        await self.db.execute(
            delete(BCWShippingQuote).where(BCWShippingQuote.cache_key == cache_key)
        )

    async def cleanup_expired_quotes(self) -> int:
        """Remove expired quotes from cache."""
        now = datetime.now(timezone.utc)

        result = await self.db.execute(
            delete(BCWShippingQuote).where(BCWShippingQuote.expires_at <= now)
        )

        deleted = result.rowcount
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} expired shipping quotes")

        return deleted
