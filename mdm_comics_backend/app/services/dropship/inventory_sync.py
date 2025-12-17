"""
BCW Inventory Sync Service

Synchronizes inventory data from BCW to local database.
Per challenge_2_inventory_not_realtime in proposal doc.

Sync strategy:
- Hourly sync for hot items (high velocity SKUs)
- Daily sync for long tail
- Real-time check at cart build (pre-purchase validation)

Stock Buffer Strategy:
- BCW qty >= STOCK_BUFFER_THRESHOLD: Show "In Stock"
- BCW qty > 0 but < threshold: Show "Low Stock"
- BCW qty == 0: Show "Out of Stock"
- This prevents overselling when inventory is nearly depleted
"""
import logging
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Set
from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from app.models.bcw import BCWInventorySnapshot, BCWProductMapping
from app.models.product import Product
from app.services.bcw.browser_client import BCWBrowserClient, ProductInfo
from app.services.bcw.session_manager import BCWSessionManager
from app.core.exceptions import BCWInventoryError

logger = logging.getLogger(__name__)

# Stock buffer threshold - don't show "in stock" unless BCW has at least this many
# Prevents overselling when inventory is nearly depleted
STOCK_BUFFER_THRESHOLD = 3

# Low stock warning threshold
LOW_STOCK_THRESHOLD = 5


class StockStatus(str, Enum):
    """Display stock status for customers."""
    IN_STOCK = "in_stock"
    LOW_STOCK = "low_stock"  # "Only X left!"
    OUT_OF_STOCK = "out_of_stock"
    BACKORDER = "backorder"  # Available but delayed
    UNAVAILABLE = "unavailable"  # Not sold/discontinued


@dataclass
class InventorySyncResult:
    """Result of inventory sync operation."""
    success: bool
    total_checked: int = 0
    updated_count: int = 0
    out_of_stock_count: int = 0
    backorder_count: int = 0
    error_count: int = 0
    errors: List[str] = field(default_factory=list)
    duration_ms: int = 0


@dataclass
class InventoryDelta:
    """Change in inventory state."""
    sku: str
    previous_stock: Optional[bool]
    current_stock: bool
    previous_qty: Optional[int]
    current_qty: Optional[int]
    became_backorder: bool = False
    backorder_date: Optional[datetime] = None


@dataclass
class DisplayStockInfo:
    """Stock information for customer display (with buffer applied)."""
    sku: str
    status: StockStatus
    display_qty: Optional[int] = None  # For "Only X left!" display
    backorder_date: Optional[datetime] = None
    message: Optional[str] = None  # Customer-facing message
    can_purchase: bool = False  # Whether add-to-cart should be enabled


class BCWInventorySyncService:
    """
    Syncs BCW inventory to local database.

    Usage:
        async with BCWBrowserClient() as client:
            await client.login(username, password)

            sync_service = BCWInventorySyncService(client, db)
            result = await sync_service.sync_hot_items()
    """

    def __init__(
        self,
        browser_client: BCWBrowserClient,
        db: AsyncSession,
    ):
        self.client = browser_client
        self.db = db

    async def sync_hot_items(
        self,
        limit: int = 100,
    ) -> InventorySyncResult:
        """
        Sync inventory for high-velocity SKUs.

        Hot items = SKUs with recent sales or inventory changes.

        Args:
            limit: Maximum SKUs to sync

        Returns:
            InventorySyncResult with sync metrics
        """
        start_time = datetime.now(timezone.utc)
        logger.info(f"Starting hot items inventory sync (limit={limit})")

        # Get hot SKUs (recent sales or inventory changes)
        skus = await self._get_hot_skus(limit)

        result = await self._sync_skus(skus)
        result.duration_ms = int(
            (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        )

        logger.info(
            f"Hot items sync complete: {result.updated_count}/{result.total_checked} updated, "
            f"{result.out_of_stock_count} OOS, {result.error_count} errors, "
            f"{result.duration_ms}ms"
        )

        return result

    async def sync_all_active_products(
        self,
        batch_size: int = 50,
        checkpoint_callback=None,
    ) -> InventorySyncResult:
        """
        Full inventory sync for all active products in the allowlist.

        Only syncs products in bcw_product_mappings with sync_inventory=True.

        Args:
            batch_size: SKUs per batch
            checkpoint_callback: Optional callback for progress checkpointing

        Returns:
            InventorySyncResult with aggregate metrics
        """
        start_time = datetime.now(timezone.utc)
        logger.info("Starting full inventory sync")

        # Get all active BCW product SKUs from the allowlist (mapping table)
        result = await self.db.execute(
            select(BCWProductMapping.bcw_sku)
            .where(BCWProductMapping.is_active == True)
            .where(BCWProductMapping.sync_inventory == True)
        )
        all_skus = [row[0] for row in result.fetchall()]

        logger.info(f"Found {len(all_skus)} active BCW products in allowlist to sync")

        aggregate = InventorySyncResult(success=True)

        # Process in batches
        for i in range(0, len(all_skus), batch_size):
            batch = all_skus[i:i + batch_size]
            batch_result = await self._sync_skus(batch)

            aggregate.total_checked += batch_result.total_checked
            aggregate.updated_count += batch_result.updated_count
            aggregate.out_of_stock_count += batch_result.out_of_stock_count
            aggregate.backorder_count += batch_result.backorder_count
            aggregate.error_count += batch_result.error_count
            aggregate.errors.extend(batch_result.errors)

            if not batch_result.success:
                aggregate.success = False

            # Checkpoint after each batch
            if checkpoint_callback:
                await checkpoint_callback(i + len(batch), len(all_skus))

            await self.db.commit()

        aggregate.duration_ms = int(
            (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        )

        logger.info(
            f"Full sync complete: {aggregate.updated_count}/{aggregate.total_checked} updated, "
            f"{aggregate.out_of_stock_count} OOS, {aggregate.error_count} errors, "
            f"{aggregate.duration_ms}ms"
        )

        return aggregate

    async def check_items_availability(
        self,
        skus: List[str],
    ) -> Dict[str, ProductInfo]:
        """
        Real-time availability check for specific SKUs.

        Used for pre-purchase validation.

        Args:
            skus: List of SKUs to check

        Returns:
            Dict mapping SKU to ProductInfo
        """
        logger.info(f"Real-time availability check for {len(skus)} SKUs")

        results = {}
        for sku in skus:
            try:
                product = await self.client.search_product(sku)
                if product:
                    results[sku] = product

                    # Update snapshot with fresh data
                    await self._update_snapshot(sku, product)
                else:
                    results[sku] = ProductInfo(sku=sku, in_stock=False)
            except Exception as e:
                logger.error(f"Failed to check availability for {sku}: {e}")
                results[sku] = ProductInfo(sku=sku, in_stock=False)

        return results

    async def get_display_stock_status(
        self,
        sku: str,
        use_cached: bool = True,
    ) -> DisplayStockInfo:
        """
        Get customer-facing stock status with buffer applied.

        Args:
            sku: Product SKU
            use_cached: Use cached snapshot if available (default True)

        Returns:
            DisplayStockInfo with buffered stock status
        """
        # Get inventory data
        if use_cached:
            snapshot = await self._get_cached_snapshot(sku)
        else:
            snapshot = None

        if not snapshot:
            # Fetch fresh data
            product = await self.client.search_product(sku)
            if product:
                await self._update_snapshot(sku, product)
                return self._calculate_display_status(
                    sku, product.available_qty, product.in_stock, product.backorder_date
                )
            else:
                return DisplayStockInfo(
                    sku=sku,
                    status=StockStatus.UNAVAILABLE,
                    message="This item is currently unavailable",
                    can_purchase=False,
                )

        return self._calculate_display_status(
            sku, snapshot.available_qty, snapshot.in_stock, snapshot.backorder_date
        )

    async def get_display_stock_batch(
        self,
        skus: List[str],
    ) -> Dict[str, DisplayStockInfo]:
        """
        Get display stock status for multiple SKUs.

        Uses cached snapshots for efficiency.

        Args:
            skus: List of SKUs

        Returns:
            Dict mapping SKU to DisplayStockInfo
        """
        results = {}

        for sku in skus:
            results[sku] = await self.get_display_stock_status(sku, use_cached=True)

        return results

    def _calculate_display_status(
        self,
        sku: str,
        qty: Optional[int],
        in_stock: bool,
        backorder_date: Optional[datetime],
    ) -> DisplayStockInfo:
        """
        Calculate customer-facing stock status with buffer logic.

        Buffer Logic:
        - qty >= STOCK_BUFFER_THRESHOLD (3): "In Stock"
        - qty > 0 but < threshold: "Low Stock - Only X left!"
        - qty == 0 or not in_stock: "Out of Stock"
        - Has backorder_date: "Backorder" (can still purchase)
        """
        # Handle backorder first
        if backorder_date:
            return DisplayStockInfo(
                sku=sku,
                status=StockStatus.BACKORDER,
                backorder_date=backorder_date,
                message=f"Available on backorder - ships by {backorder_date.strftime('%b %d')}",
                can_purchase=True,  # Allow purchase with backorder warning
            )

        # Not in stock at all
        if not in_stock or qty is None or qty == 0:
            return DisplayStockInfo(
                sku=sku,
                status=StockStatus.OUT_OF_STOCK,
                message="Out of stock",
                can_purchase=False,
            )

        # Low stock (below buffer threshold but > 0)
        if qty < STOCK_BUFFER_THRESHOLD:
            return DisplayStockInfo(
                sku=sku,
                status=StockStatus.LOW_STOCK,
                display_qty=qty,
                message=f"Low stock - only {qty} left!",
                can_purchase=True,
            )

        # Low stock warning (above buffer but below warning threshold)
        if qty < LOW_STOCK_THRESHOLD:
            return DisplayStockInfo(
                sku=sku,
                status=StockStatus.LOW_STOCK,
                display_qty=qty,
                message=f"Only {qty} left in stock",
                can_purchase=True,
            )

        # In stock with healthy quantity
        return DisplayStockInfo(
            sku=sku,
            status=StockStatus.IN_STOCK,
            message="In stock",
            can_purchase=True,
        )

    async def _get_cached_snapshot(self, sku: str) -> Optional[BCWInventorySnapshot]:
        """Get cached inventory snapshot if fresh enough."""
        # Consider snapshot stale after 1 hour
        stale_cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

        result = await self.db.execute(
            select(BCWInventorySnapshot)
            .where(BCWInventorySnapshot.sku == sku)
            .where(BCWInventorySnapshot.checked_at >= stale_cutoff)
        )
        return result.scalar_one_or_none()

    def can_fulfill_quantity(
        self,
        bcw_qty: Optional[int],
        requested_qty: int,
    ) -> bool:
        """
        Check if we can fulfill a requested quantity.

        Applies buffer - we won't sell the last few units to prevent overselling.

        Args:
            bcw_qty: BCW's reported quantity
            requested_qty: Customer's requested quantity

        Returns:
            True if we can safely fulfill the order
        """
        if bcw_qty is None:
            return False

        # We need BCW to have enough stock PLUS our buffer
        # e.g., if customer wants 2 and buffer is 3, BCW needs at least 5
        required_qty = requested_qty + STOCK_BUFFER_THRESHOLD - 1

        return bcw_qty >= required_qty

    async def _sync_skus(self, skus: List[str]) -> InventorySyncResult:
        """Sync inventory for a list of SKUs."""
        result = InventorySyncResult(success=True, total_checked=len(skus))

        for sku in skus:
            try:
                product = await self.client.search_product(sku)
                if not product:
                    result.error_count += 1
                    result.errors.append(f"Product not found: {sku}")
                    continue

                # Check for changes
                delta = await self._check_for_delta(sku, product)

                # Update snapshot
                await self._update_snapshot(sku, product)

                if delta:
                    result.updated_count += 1

                    # Track specific states
                    if not product.in_stock:
                        result.out_of_stock_count += 1
                    if product.backorder_date:
                        result.backorder_count += 1

                    # Alert on significant changes
                    await self._handle_delta_alerts(delta)

            except Exception as e:
                logger.error(f"Error syncing SKU {sku}: {e}")
                result.error_count += 1
                result.errors.append(f"{sku}: {str(e)}")

        if result.error_count > len(skus) * 0.5:
            result.success = False

        return result

    async def _get_hot_skus(self, limit: int) -> List[str]:
        """Get high-velocity SKUs for priority sync from the allowlist."""
        # SKUs with recent inventory changes or sales
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        # Get SKUs from recent snapshots with changes (must be in allowlist)
        result = await self.db.execute(
            select(BCWInventorySnapshot.sku)
            .join(
                BCWProductMapping,
                BCWInventorySnapshot.sku == BCWProductMapping.bcw_sku
            )
            .where(BCWInventorySnapshot.checked_at >= cutoff)
            .where(BCWProductMapping.is_active == True)
            .where(BCWProductMapping.sync_inventory == True)
            .order_by(BCWInventorySnapshot.checked_at.desc())
            .limit(limit)
        )
        recent_skus = set(row[0] for row in result.fetchall())

        # If not enough, add from allowlist
        if len(recent_skus) < limit:
            remaining = limit - len(recent_skus)
            result = await self.db.execute(
                select(BCWProductMapping.bcw_sku)
                .where(BCWProductMapping.is_active == True)
                .where(BCWProductMapping.sync_inventory == True)
                .where(~BCWProductMapping.bcw_sku.in_(recent_skus))
                .limit(remaining)
            )
            for row in result.fetchall():
                recent_skus.add(row[0])

        return list(recent_skus)

    async def _check_for_delta(
        self,
        sku: str,
        current: ProductInfo,
    ) -> Optional[InventoryDelta]:
        """Check if inventory state changed."""
        result = await self.db.execute(
            select(BCWInventorySnapshot)
            .where(BCWInventorySnapshot.sku == sku)
            .order_by(BCWInventorySnapshot.checked_at.desc())
            .limit(1)
        )
        previous = result.scalar_one_or_none()

        if not previous:
            # First snapshot, always a "change"
            return InventoryDelta(
                sku=sku,
                previous_stock=None,
                current_stock=current.in_stock,
                previous_qty=None,
                current_qty=current.available_qty,
                became_backorder=current.backorder_date is not None,
                backorder_date=current.backorder_date,
            )

        # Check for actual changes
        stock_changed = previous.in_stock != current.in_stock
        qty_changed = previous.available_qty != current.available_qty
        backorder_changed = (
            previous.backorder_date != current.backorder_date
            or previous.is_backorder != (current.backorder_date is not None)
        )

        if stock_changed or qty_changed or backorder_changed:
            return InventoryDelta(
                sku=sku,
                previous_stock=previous.in_stock,
                current_stock=current.in_stock,
                previous_qty=previous.available_qty,
                current_qty=current.available_qty,
                became_backorder=(
                    not previous.is_backorder
                    and current.backorder_date is not None
                ),
                backorder_date=current.backorder_date,
            )

        return None

    async def _update_snapshot(self, sku: str, product: ProductInfo):
        """Update or insert inventory snapshot."""
        now = datetime.now(timezone.utc)

        # Upsert snapshot
        stmt = insert(BCWInventorySnapshot).values(
            sku=sku,
            bcw_sku=product.bcw_sku,
            in_stock=product.in_stock,
            available_qty=product.available_qty,
            is_backorder=product.backorder_date is not None,
            backorder_date=product.backorder_date,
            bcw_price=product.price,
            checked_at=now,
            created_at=now,
        )

        # On conflict, update
        stmt = stmt.on_conflict_do_update(
            index_elements=["sku"],
            set_={
                "bcw_sku": product.bcw_sku,
                "in_stock": product.in_stock,
                "available_qty": product.available_qty,
                "is_backorder": product.backorder_date is not None,
                "backorder_date": product.backorder_date,
                "bcw_price": product.price,
                "checked_at": now,
            },
        )

        await self.db.execute(stmt)

    async def _handle_delta_alerts(self, delta: InventoryDelta):
        """Handle alerts for significant inventory changes."""
        # P1: Rapid stock drop (was in stock, now out)
        if delta.previous_stock is True and not delta.current_stock:
            logger.warning(f"ALERT: SKU {delta.sku} went out of stock")
            # TODO: Send P1 alert notification

        # P2: Backorder date change
        if delta.became_backorder:
            logger.warning(
                f"ALERT: SKU {delta.sku} is now on backorder "
                f"(estimated: {delta.backorder_date})"
            )
            # TODO: Send P2 alert notification

        # P3: Quantity significantly reduced
        if (
            delta.previous_qty
            and delta.current_qty
            and delta.current_qty < delta.previous_qty * 0.5
        ):
            logger.info(
                f"NOTICE: SKU {delta.sku} quantity dropped "
                f"from {delta.previous_qty} to {delta.current_qty}"
            )
