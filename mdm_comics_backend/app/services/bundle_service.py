"""
Bundle Service v1.0.0

Core business logic for bundle CRUD operations.

Per constitution_db.json:
- DB-005: Track change provenance (who, when)
- Section 5: Critical tables track change provenance

Per constitution_cyberSec.json:
- Section 3: Input validated at service layer
"""
import re
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, List, Tuple
from uuid import uuid4

from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.bundle import Bundle, BundleItem, BundleStatus
from app.models.product import Product
from app.schemas.bundle import (
    BundleCreate, BundleUpdate, BundleItemCreate, BundleItemUpdate,
    BundlePricingRequest, BundlePricingResponse
)

logger = logging.getLogger(__name__)


# ==================== Constants ====================

MIN_MARGIN_PERCENT = Decimal("20.0")
WARNING_MARGIN_PERCENT = Decimal("25.0")
TARGET_MARGIN_PERCENT = Decimal("40.0")


# ==================== Bundle Service ====================

class BundleService:
    """Service for bundle CRUD operations."""

    @staticmethod
    def generate_sku(name: str, category: Optional[str] = None) -> str:
        """Generate unique bundle SKU."""
        # Extract category prefix
        if category:
            prefix = category.upper()[:3]
        else:
            prefix = "GEN"

        # Clean name for SKU
        clean_name = re.sub(r"[^A-Za-z0-9]", "", name.upper())[:6]

        # Add random suffix for uniqueness
        suffix = uuid4().hex[:4].upper()

        return f"BDL-{prefix}-{clean_name}-{suffix}"

    @staticmethod
    def generate_slug(name: str) -> str:
        """Generate URL-friendly slug from name."""
        # Lowercase and replace spaces/special chars with hyphens
        slug = name.lower()
        slug = re.sub(r"[^a-z0-9]+", "-", slug)
        slug = slug.strip("-")

        # Add random suffix for uniqueness
        suffix = uuid4().hex[:6]

        return f"{slug}-{suffix}"

    @staticmethod
    async def get_by_id(
        db: AsyncSession,
        bundle_id: int,
        include_items: bool = True
    ) -> Optional[Bundle]:
        """Get bundle by ID with optional items."""
        query = select(Bundle).where(Bundle.id == bundle_id)

        if include_items:
            query = query.options(
                selectinload(Bundle.items).selectinload(BundleItem.product)
            )

        result = await db.execute(query)
        return result.scalar_one_or_none()

    @staticmethod
    async def get_by_slug(
        db: AsyncSession,
        slug: str,
        include_items: bool = True
    ) -> Optional[Bundle]:
        """Get bundle by slug."""
        query = select(Bundle).where(Bundle.slug == slug)

        if include_items:
            query = query.options(
                selectinload(Bundle.items).selectinload(BundleItem.product)
            )

        result = await db.execute(query)
        return result.scalar_one_or_none()

    @staticmethod
    async def get_by_sku(db: AsyncSession, sku: str) -> Optional[Bundle]:
        """Get bundle by SKU."""
        result = await db.execute(
            select(Bundle).where(Bundle.sku == sku)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def list_bundles(
        db: AsyncSession,
        status: Optional[BundleStatus] = None,
        category: Optional[str] = None,
        search: Optional[str] = None,
        page: int = 1,
        per_page: int = 20,
        active_only: bool = False
    ) -> Tuple[List[Bundle], int]:
        """
        List bundles with filtering and pagination.

        Returns tuple of (bundles, total_count).
        """
        # Base query
        query = select(Bundle)
        count_query = select(func.count(Bundle.id))

        # Apply filters
        filters = []

        if status:
            filters.append(Bundle.status == status.value)
        elif active_only:
            filters.append(Bundle.status == BundleStatus.ACTIVE.value)

        if category:
            filters.append(Bundle.category == category)

        if search:
            search_filter = or_(
                Bundle.name.ilike(f"%{search}%"),
                Bundle.sku.ilike(f"%{search}%"),
                Bundle.short_description.ilike(f"%{search}%")
            )
            filters.append(search_filter)

        if filters:
            query = query.where(and_(*filters))
            count_query = count_query.where(and_(*filters))

        # Get total count
        total_result = await db.execute(count_query)
        total = total_result.scalar()

        # Apply pagination and ordering
        query = query.order_by(Bundle.display_order, Bundle.created_at.desc())
        query = query.offset((page - 1) * per_page).limit(per_page)

        # Load items relationship
        query = query.options(selectinload(Bundle.items))

        result = await db.execute(query)
        bundles = result.scalars().all()

        return list(bundles), total

    @staticmethod
    async def create_bundle(
        db: AsyncSession,
        data: BundleCreate,
        user_id: int
    ) -> Bundle:
        """
        Create a new bundle.

        Args:
            db: Database session
            data: Bundle creation data
            user_id: ID of user creating the bundle

        Returns:
            Created bundle
        """
        # Generate SKU and slug if not provided
        sku = data.sku or BundleService.generate_sku(data.name, data.category)
        slug = data.slug or BundleService.generate_slug(data.name)

        # Check for duplicate SKU
        existing_sku = await BundleService.get_by_sku(db, sku)
        if existing_sku:
            raise ValueError(f"Bundle with SKU '{sku}' already exists")

        # Check for duplicate slug
        existing_slug = await BundleService.get_by_slug(db, slug, include_items=False)
        if existing_slug:
            # Regenerate slug
            slug = BundleService.generate_slug(data.name)

        # Create bundle
        images_payload = [img.model_dump() for img in data.images] if data.images else []

        bundle = Bundle(
            sku=sku,
            slug=slug,
            name=data.name,
            short_description=data.short_description,
            description=data.description,
            bundle_price=data.bundle_price,
            category=data.category,
            tags=data.tags,
            image_url=data.image_url,
            images=images_payload,
            badge_text=data.badge_text,
            display_order=data.display_order,
            start_date=data.start_date,
            end_date=data.end_date,
            status=BundleStatus.DRAFT.value,
            created_by=user_id,
            updated_by=user_id,
        )

        db.add(bundle)
        await db.flush()  # Get the ID

        # Add initial items if provided
        for item_data in data.items:
            await BundleService.add_item(db, bundle.id, item_data)

        # Recalculate totals
        await BundleService._recalculate_bundle(db, bundle)

        await db.commit()
        await db.refresh(bundle)

        logger.info(f"Created bundle {bundle.sku} (ID: {bundle.id}) by user {user_id}")
        return bundle

    @staticmethod
    async def update_bundle(
        db: AsyncSession,
        bundle_id: int,
        data: BundleUpdate,
        user_id: int
    ) -> Optional[Bundle]:
        """Update an existing bundle."""
        bundle = await BundleService.get_by_id(db, bundle_id)
        if not bundle:
            return None

        # Update fields
        update_data = data.model_dump(exclude_unset=True)
        if "images" in update_data and update_data["images"] is not None:
            # Normalize BundleImage objects/dicts to plain dicts
            update_data["images"] = [
                img if isinstance(img, dict) else img.model_dump()
                for img in update_data["images"]
            ]

        for field, value in update_data.items():
            setattr(bundle, field, value)

        bundle.updated_by = user_id
        bundle.updated_at = datetime.now(timezone.utc)

        # Recalculate if price changed
        if "bundle_price" in update_data:
            await BundleService._recalculate_bundle(db, bundle)

        await db.commit()
        await db.refresh(bundle)

        logger.info(f"Updated bundle {bundle.sku} by user {user_id}")
        return bundle

    @staticmethod
    async def delete_bundle(
        db: AsyncSession,
        bundle_id: int,
        user_id: int
    ) -> bool:
        """
        Soft delete bundle by archiving it.

        Returns True if deleted, False if not found.
        """
        bundle = await BundleService.get_by_id(db, bundle_id, include_items=False)
        if not bundle:
            return False

        bundle.status = BundleStatus.ARCHIVED.value
        bundle.updated_by = user_id
        bundle.updated_at = datetime.now(timezone.utc)

        await db.commit()
        logger.info(f"Archived bundle {bundle.sku} by user {user_id}")
        return True

    @staticmethod
    async def add_item(
        db: AsyncSession,
        bundle_id: int,
        data: BundleItemCreate
    ) -> BundleItem:
        """Add an item to a bundle."""
        # Verify product exists
        product_result = await db.execute(
            select(Product).where(Product.id == data.product_id)
        )
        product = product_result.scalar_one_or_none()
        if not product:
            raise ValueError(f"Product {data.product_id} not found")

        # Check for duplicate
        existing = await db.execute(
            select(BundleItem).where(
                and_(
                    BundleItem.bundle_id == bundle_id,
                    BundleItem.product_id == data.product_id
                )
            )
        )
        if existing.scalar_one_or_none():
            raise ValueError(f"Product {data.product_id} already in bundle")

        # Create item
        item = BundleItem(
            bundle_id=bundle_id,
            product_id=data.product_id,
            quantity=data.quantity,
            unit_price=data.unit_price or product.price,
            unit_cost=data.unit_cost or product.original_price,
            display_order=data.display_order,
            is_featured=data.is_featured,
            custom_label=data.custom_label,
            options=data.options,
        )

        # Calculate line totals
        item.calculate_line_totals()

        db.add(item)
        await db.flush()

        return item

    @staticmethod
    async def update_item(
        db: AsyncSession,
        item_id: int,
        data: BundleItemUpdate
    ) -> Optional[BundleItem]:
        """Update a bundle item."""
        result = await db.execute(
            select(BundleItem).where(BundleItem.id == item_id)
        )
        item = result.scalar_one_or_none()
        if not item:
            return None

        # Update fields
        update_data = data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(item, field, value)

        item.calculate_line_totals()
        item.updated_at = datetime.now(timezone.utc)

        await db.flush()
        return item

    @staticmethod
    async def remove_item(db: AsyncSession, item_id: int) -> bool:
        """Remove an item from a bundle."""
        result = await db.execute(
            select(BundleItem).where(BundleItem.id == item_id)
        )
        item = result.scalar_one_or_none()
        if not item:
            return False

        bundle_id = item.bundle_id
        await db.delete(item)
        await db.flush()

        return True

    @staticmethod
    async def publish_bundle(
        db: AsyncSession,
        bundle_id: int,
        user_id: int
    ) -> Tuple[bool, Optional[str]]:
        """
        Publish a bundle (DRAFT -> ACTIVE).

        Returns (success, error_message).
        """
        bundle = await BundleService.get_by_id(db, bundle_id)
        if not bundle:
            return False, "Bundle not found"

        if bundle.status != BundleStatus.DRAFT.value:
            return False, f"Cannot publish bundle in {bundle.status} status"

        # Validate bundle has items
        if not bundle.items:
            return False, "Bundle must have at least one item"

        # Validate positive margin
        if bundle.margin_percent is not None and bundle.margin_percent < 0:
            return False, "Bundle has negative margin"

        # Publish
        bundle.status = BundleStatus.ACTIVE.value
        bundle.published_at = datetime.now(timezone.utc)
        bundle.updated_by = user_id
        bundle.updated_at = datetime.now(timezone.utc)

        await db.commit()
        logger.info(f"Published bundle {bundle.sku} by user {user_id}")
        return True, None

    @staticmethod
    async def unpublish_bundle(
        db: AsyncSession,
        bundle_id: int,
        user_id: int
    ) -> bool:
        """Unpublish a bundle (ACTIVE -> INACTIVE)."""
        bundle = await BundleService.get_by_id(db, bundle_id, include_items=False)
        if not bundle or bundle.status != BundleStatus.ACTIVE.value:
            return False

        bundle.status = BundleStatus.INACTIVE.value
        bundle.updated_by = user_id
        bundle.updated_at = datetime.now(timezone.utc)

        await db.commit()
        logger.info(f"Unpublished bundle {bundle.sku} by user {user_id}")
        return True

    @staticmethod
    async def duplicate_bundle(
        db: AsyncSession,
        bundle_id: int,
        user_id: int
    ) -> Optional[Bundle]:
        """Duplicate a bundle as a new DRAFT."""
        source = await BundleService.get_by_id(db, bundle_id)
        if not source:
            return None

        # Create new bundle
        new_bundle = Bundle(
            sku=BundleService.generate_sku(source.name, source.category),
            slug=BundleService.generate_slug(source.name),
            name=f"{source.name} (Copy)",
            short_description=source.short_description,
            description=source.description,
            bundle_price=source.bundle_price,
            category=source.category,
            tags=source.tags.copy() if source.tags else [],
            image_url=source.image_url,
            images=source.images.copy() if source.images else [],
            badge_text=source.badge_text,
            display_order=source.display_order,
            status=BundleStatus.DRAFT.value,
            created_by=user_id,
            updated_by=user_id,
        )

        db.add(new_bundle)
        await db.flush()

        # Copy items
        for item in source.items:
            new_item = BundleItem(
                bundle_id=new_bundle.id,
                product_id=item.product_id,
                quantity=item.quantity,
                unit_price=item.unit_price,
                unit_cost=item.unit_cost,
                line_price=item.line_price,
                line_cost=item.line_cost,
                display_order=item.display_order,
                is_featured=item.is_featured,
                custom_label=item.custom_label,
                options=item.options.copy() if item.options else {},
            )
            db.add(new_item)

        await db.flush()
        await BundleService._recalculate_bundle(db, new_bundle)

        await db.commit()
        await db.refresh(new_bundle)

        logger.info(f"Duplicated bundle {source.sku} to {new_bundle.sku} by user {user_id}")
        return new_bundle

    @staticmethod
    async def _recalculate_bundle(db: AsyncSession, bundle: Bundle) -> None:
        """Recalculate bundle totals from items."""
        # Always query items explicitly to avoid lazy loading issues in async context
        result = await db.execute(
            select(BundleItem).where(BundleItem.bundle_id == bundle.id)
        )
        items = result.scalars().all()

        # Calculate totals
        total_cost = Decimal("0")
        total_compare = Decimal("0")

        for item in items:
            if item.line_cost:
                total_cost += item.line_cost
            if item.line_price:
                total_compare += item.line_price

        bundle.cost = total_cost
        bundle.compare_at_price = total_compare

        if total_compare > 0:
            bundle.savings_amount = total_compare - bundle.bundle_price
            bundle.savings_percent = (bundle.savings_amount / total_compare) * 100
        else:
            bundle.savings_amount = Decimal("0")
            bundle.savings_percent = Decimal("0")

        if bundle.bundle_price and bundle.bundle_price > 0:
            bundle.margin_percent = ((bundle.bundle_price - total_cost) / bundle.bundle_price) * 100
        else:
            bundle.margin_percent = Decimal("0")

    @staticmethod
    def calculate_pricing(data: BundlePricingRequest) -> BundlePricingResponse:
        """Calculate pricing preview for proposed bundle items."""
        total_cost = Decimal("0")
        total_compare = Decimal("0")

        for item in data.items:
            unit_cost = item.unit_cost or Decimal("0")
            unit_price = item.unit_price or Decimal("0")

            total_cost += unit_cost * item.quantity
            total_compare += unit_price * item.quantity

        savings_amount = total_compare - data.bundle_price
        savings_percent = (savings_amount / total_compare * 100) if total_compare > 0 else Decimal("0")

        margin_amount = data.bundle_price - total_cost
        margin_percent = (margin_amount / data.bundle_price * 100) if data.bundle_price > 0 else Decimal("0")

        # Determine margin health
        is_healthy = margin_percent >= MIN_MARGIN_PERCENT
        warning = None
        if margin_percent < 0:
            warning = "NEGATIVE MARGIN: Bundle will lose money"
        elif margin_percent < MIN_MARGIN_PERCENT:
            warning = f"LOW MARGIN: Below minimum {MIN_MARGIN_PERCENT}%"
        elif margin_percent < WARNING_MARGIN_PERCENT:
            warning = f"CAUTION: Margin below recommended {WARNING_MARGIN_PERCENT}%"

        return BundlePricingResponse(
            total_cost=total_cost,
            total_compare_at=total_compare,
            bundle_price=data.bundle_price,
            savings_amount=savings_amount,
            savings_percent=savings_percent,
            margin_amount=margin_amount,
            margin_percent=margin_percent,
            is_margin_healthy=is_healthy,
            margin_warning=warning,
        )
