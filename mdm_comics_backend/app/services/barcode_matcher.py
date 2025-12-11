"""
Barcode Matcher Service

Handles matching scanned barcodes to existing products, comic issues, or PriceCharting.
Implements BLOCK-003 fix: atomic stock increment to prevent race conditions.

Per constitution_db.json Section 5: Track change provenance (who, when, reason).
"""
import logging
from datetime import datetime, timezone
from typing import Optional, Tuple
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models import Product, BarcodeQueue, StockMovement, Funko
from app.models.comic_data import ComicIssue

logger = logging.getLogger(__name__)


class BarcodeMatchResult:
    """Result of barcode matching attempt"""
    def __init__(
        self,
        matched: bool,
        match_type: Optional[str] = None,
        product_id: Optional[int] = None,
        comic_id: Optional[int] = None,
        funko_id: Optional[int] = None,
        confidence: int = 0,
        message: str = "",
        stock_incremented: bool = False,
        new_stock: Optional[int] = None
    ):
        self.matched = matched
        self.match_type = match_type  # existing_product, comic_issue, funko, pricecharting
        self.product_id = product_id
        self.comic_id = comic_id
        self.funko_id = funko_id
        self.confidence = confidence  # 0-100
        self.message = message
        self.stock_incremented = stock_incremented
        self.new_stock = new_stock


async def match_barcode(
    db: AsyncSession,
    barcode: str,
    barcode_type: str,
    user_id: int,
    auto_increment_stock: bool = True
) -> BarcodeMatchResult:
    """
    Match a barcode to existing products or data sources.

    Priority:
    1. Existing products.upc/isbn (increment stock if found)
    2. comic_issues.upc
    3. PriceCharting API (future - Phase 2.5)

    Args:
        db: Database session
        barcode: The barcode string (UPC, ISBN, EAN)
        barcode_type: Type of barcode (UPC, ISBN)
        user_id: User performing the scan
        auto_increment_stock: Whether to auto-increment stock on match

    Returns:
        BarcodeMatchResult with match details
    """
    # Priority 1: Check existing products by UPC or ISBN
    if barcode_type in ("UPC", "EAN"):
        result = await _match_product_by_upc(db, barcode, user_id, auto_increment_stock)
        if result.matched:
            return result
    elif barcode_type == "ISBN":
        result = await _match_product_by_isbn(db, barcode, user_id, auto_increment_stock)
        if result.matched:
            return result

    # Priority 2: Check comic_issues
    if barcode_type in ("UPC", "ISBN"):
        result = await _match_comic_issue(db, barcode, barcode_type)
        if result.matched:
            return result

    # Priority 3: Check funkos by handle pattern (future)
    # TODO: Implement funko matching

    # No match found
    return BarcodeMatchResult(
        matched=False,
        message=f"No match found for {barcode_type} {barcode}"
    )


async def _match_product_by_upc(
    db: AsyncSession,
    upc: str,
    user_id: int,
    auto_increment: bool
) -> BarcodeMatchResult:
    """Match and optionally increment stock for existing product by UPC.

    BLOCK-003: Uses atomic UPDATE ... RETURNING to prevent race conditions.
    """
    if not auto_increment:
        # Just check if exists
        result = await db.execute(
            select(Product)
            .where(Product.upc == upc)
            .where(Product.deleted_at.is_(None))
        )
        product = result.scalar_one_or_none()
        if product:
            return BarcodeMatchResult(
                matched=True,
                match_type="existing_product",
                product_id=product.id,
                confidence=100,
                message=f"Found existing product: {product.name}"
            )
        return BarcodeMatchResult(matched=False)

    # BLOCK-003: Atomic stock increment
    result = await db.execute(text("""
        UPDATE products
        SET stock = stock + 1, updated_at = NOW()
        WHERE upc = :upc AND deleted_at IS NULL
        RETURNING id, name, stock
    """), {"upc": upc})

    row = result.fetchone()
    if row:
        product_id, product_name, new_stock = row

        # Log the stock movement for audit
        movement = StockMovement(
            product_id=product_id,
            movement_type="received",
            quantity=1,
            previous_stock=new_stock - 1,
            new_stock=new_stock,
            reason="Barcode scan - stock increment",
            reference_type="scan_queue",
            user_id=user_id
        )
        db.add(movement)
        await db.commit()

        logger.info(f"Auto-incremented stock for product {product_id} ({product_name}) to {new_stock}")

        return BarcodeMatchResult(
            matched=True,
            match_type="existing_product",
            product_id=product_id,
            confidence=100,
            message=f"Incremented stock for {product_name}",
            stock_incremented=True,
            new_stock=new_stock
        )

    return BarcodeMatchResult(matched=False)


async def _match_product_by_isbn(
    db: AsyncSession,
    isbn: str,
    user_id: int,
    auto_increment: bool
) -> BarcodeMatchResult:
    """Match and optionally increment stock for existing product by ISBN.

    BLOCK-003: Uses atomic UPDATE ... RETURNING to prevent race conditions.
    """
    # Normalize ISBN (remove dashes)
    isbn_normalized = isbn.replace("-", "").replace(" ", "")

    if not auto_increment:
        result = await db.execute(
            select(Product)
            .where(Product.isbn == isbn_normalized)
            .where(Product.deleted_at.is_(None))
        )
        product = result.scalar_one_or_none()
        if product:
            return BarcodeMatchResult(
                matched=True,
                match_type="existing_product",
                product_id=product.id,
                confidence=100,
                message=f"Found existing product: {product.name}"
            )
        return BarcodeMatchResult(matched=False)

    # BLOCK-003: Atomic stock increment
    result = await db.execute(text("""
        UPDATE products
        SET stock = stock + 1, updated_at = NOW()
        WHERE isbn = :isbn AND deleted_at IS NULL
        RETURNING id, name, stock
    """), {"isbn": isbn_normalized})

    row = result.fetchone()
    if row:
        product_id, product_name, new_stock = row

        movement = StockMovement(
            product_id=product_id,
            movement_type="received",
            quantity=1,
            previous_stock=new_stock - 1,
            new_stock=new_stock,
            reason="Barcode scan - stock increment",
            reference_type="scan_queue",
            user_id=user_id
        )
        db.add(movement)
        await db.commit()

        logger.info(f"Auto-incremented stock for product {product_id} ({product_name}) to {new_stock}")

        return BarcodeMatchResult(
            matched=True,
            match_type="existing_product",
            product_id=product_id,
            confidence=100,
            message=f"Incremented stock for {product_name}",
            stock_incremented=True,
            new_stock=new_stock
        )

    return BarcodeMatchResult(matched=False)


async def _match_comic_issue(
    db: AsyncSession,
    barcode: str,
    barcode_type: str
) -> BarcodeMatchResult:
    """Check if barcode matches a comic issue in our catalog."""
    # Check comic_issues table for matching UPC
    result = await db.execute(
        select(ComicIssue)
        .where(ComicIssue.upc == barcode)
    )
    comic = result.scalar_one_or_none()

    if comic:
        return BarcodeMatchResult(
            matched=True,
            match_type="comic_issue",
            comic_id=comic.id,
            confidence=95,
            message=f"Matched comic: {comic.issue_name}"
        )

    return BarcodeMatchResult(matched=False)


async def process_barcode_queue_item(
    db: AsyncSession,
    queue_item: BarcodeQueue,
    action: str,
    product_data: Optional[dict] = None,
    user_id: int = None
) -> dict:
    """
    Process a queued barcode item.

    Actions:
    - create_product: Create new product from matched data
    - add_to_existing: Increment stock on existing product
    - skip: Mark as skipped

    Args:
        db: Database session
        queue_item: The BarcodeQueue item to process
        action: Action to take (create_product, add_to_existing, skip)
        product_data: Optional product data for create_product action
        user_id: User processing the item

    Returns:
        Dict with result details
    """
    if action == "skip":
        queue_item.status = "skipped"
        queue_item.processed_at = datetime.now(timezone.utc)
        queue_item.processed_by = user_id
        await db.commit()
        return {"status": "skipped", "queue_id": queue_item.id}

    if action == "add_to_existing" and queue_item.matched_product_id:
        # Atomic increment
        result = await db.execute(text("""
            UPDATE products
            SET stock = stock + 1, updated_at = NOW()
            WHERE id = :product_id AND deleted_at IS NULL
            RETURNING stock
        """), {"product_id": queue_item.matched_product_id})

        row = result.fetchone()
        if row:
            new_stock = row[0]

            # Log movement
            movement = StockMovement(
                product_id=queue_item.matched_product_id,
                movement_type="received",
                quantity=1,
                previous_stock=new_stock - 1,
                new_stock=new_stock,
                reason="Processed from scan queue",
                reference_type="scan_queue",
                reference_id=queue_item.id,
                user_id=user_id
            )
            db.add(movement)

            queue_item.status = "processed"
            queue_item.processed_at = datetime.now(timezone.utc)
            queue_item.processed_by = user_id
            await db.commit()

            return {
                "status": "processed",
                "queue_id": queue_item.id,
                "product_id": queue_item.matched_product_id,
                "new_stock": new_stock
            }

    if action == "create_product" and product_data:
        # Create new product
        product = Product(
            sku=product_data.get("sku", f"SCAN-{queue_item.barcode}"),
            name=product_data["name"],
            description=product_data.get("description", ""),
            category=product_data.get("category", "comics"),
            price=product_data.get("price", 0),
            original_price=product_data.get("original_price"),
            stock=product_data.get("stock", 1),
            upc=queue_item.barcode if queue_item.barcode_type == "UPC" else None,
            isbn=queue_item.barcode if queue_item.barcode_type == "ISBN" else None,
            bin_id=product_data.get("bin_id"),
            image_url=product_data.get("image_url"),
        )
        db.add(product)
        await db.flush()

        # Log initial stock
        movement = StockMovement(
            product_id=product.id,
            movement_type="received",
            quantity=product.stock,
            previous_stock=0,
            new_stock=product.stock,
            reason="Initial inventory from scan queue",
            reference_type="scan_queue",
            reference_id=queue_item.id,
            user_id=user_id
        )
        db.add(movement)

        queue_item.status = "processed"
        queue_item.matched_product_id = product.id
        queue_item.processed_at = datetime.now(timezone.utc)
        queue_item.processed_by = user_id
        await db.commit()

        return {
            "status": "created",
            "queue_id": queue_item.id,
            "product_id": product.id,
            "product_name": product.name
        }

    return {"status": "error", "message": "Invalid action or missing data"}
