"""
BCW Dropship Integration Models

Per 20251216_mdm_comics_bcw_initial_integration.json v1.2.0:
- BCWConfig: Vendor credentials and session management
- BCWInventorySnapshot: Inventory state tracking
- BCWOrder: Vendor order mapping and state machine
- BCWOrderEvent: State change audit trail

DB Compliance:
- DB-001: Numeric(12,2) for monetary fields
- DB-003: FK with appropriate ON DELETE behavior
- DB-004: Indexes on FKs and query columns
- DB-005: Audit columns (updated_by, update_reason)
- constitution_pii.json: Encrypted PII fields
"""
from datetime import datetime, timezone
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, JSON,
    Numeric, ForeignKey, Index, Enum, Date, CheckConstraint
)
from sqlalchemy.orm import relationship

from app.core.database import Base


# =============================================================================
# ENUMS
# =============================================================================

class BCWOrderState(str, PyEnum):
    """
    Order state machine states per order_state_machine in proposal doc.
    """
    DRAFT = "DRAFT"
    PENDING_SHIPPING_QUOTE = "PENDING_SHIPPING_QUOTE"
    PENDING_PAYMENT = "PENDING_PAYMENT"
    PENDING_VENDOR_SUBMISSION = "PENDING_VENDOR_SUBMISSION"
    VENDOR_SUBMITTED = "VENDOR_SUBMITTED"
    BACKORDERED = "BACKORDERED"
    PARTIALLY_SHIPPED = "PARTIALLY_SHIPPED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    RETURN_REQUESTED = "RETURN_REQUESTED"
    RETURN_IN_TRANSIT = "RETURN_IN_TRANSIT"
    RETURN_RECEIVED = "RETURN_RECEIVED"
    REFUNDED = "REFUNDED"
    EXCEPTION_REVIEW = "EXCEPTION_REVIEW"


# =============================================================================
# BCW CONFIGURATION
# =============================================================================

class BCWConfig(Base):
    """
    BCW vendor configuration and session state.

    Stores encrypted credentials and browser session cookies.
    Per constitution_pii.json: All credentials encrypted at rest.
    """
    __tablename__ = "bcw_config"

    id = Column(Integer, primary_key=True, index=True)

    # Vendor identification
    vendor_code = Column(String(20), unique=True, nullable=False, default="BCW")
    vendor_name = Column(String(100), nullable=False, default="BCW Supplies")
    base_url = Column(String(255), nullable=False, default="https://www.bcwsupplies.com")

    # Encrypted credentials (per constitution_pii.json)
    username_encrypted = Column(Text, nullable=False)
    password_encrypted = Column(Text, nullable=False)

    # Session state (browser cookies stored as encrypted JSON)
    session_data_encrypted = Column(Text, nullable=True)
    session_expires_at = Column(DateTime(timezone=True), nullable=True)

    # Rate limiting state
    last_action_at = Column(DateTime(timezone=True), nullable=True)
    actions_this_hour = Column(Integer, default=0)
    hour_reset_at = Column(DateTime(timezone=True), nullable=True)

    # Circuit breaker state
    consecutive_failures = Column(Integer, default=0)
    circuit_opened_at = Column(DateTime(timezone=True), nullable=True)
    circuit_state = Column(String(20), default="CLOSED")  # CLOSED, OPEN, HALF_OPEN

    # Selector version tracking
    selector_version = Column(String(20), default="1.0.0")
    last_selector_check_at = Column(DateTime(timezone=True), nullable=True)
    selector_health_status = Column(String(20), default="HEALTHY")  # HEALTHY, DEGRADED, FAILED

    # Dynamic selector overrides (hot-patchable without deployment)
    # Keys: "category.selector_name", Values: CSS/XPath selectors
    selectors = Column(JSON, default=dict)

    # Configuration flags
    is_active = Column(Boolean, default=True)
    blind_shipping_enabled = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # DB-005: Audit columns
    updated_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    update_reason = Column(String(255), nullable=True)


# =============================================================================
# BCW INVENTORY
# =============================================================================

class BCWInventorySnapshot(Base):
    """
    Point-in-time inventory snapshot from BCW.

    Per inventory_polling in proposal doc:
    - Hourly for hot items, daily for long tail
    - Track backorder dates and availability
    - 90-day retention per constitution_data_hygiene.json
    """
    __tablename__ = "bcw_inventory_snapshots"

    id = Column(Integer, primary_key=True, index=True)

    # SKU mapping
    sku = Column(String(50), nullable=False, index=True)  # Our internal SKU
    bcw_sku = Column(String(50), nullable=True, index=True)  # BCW's SKU
    product_id = Column(Integer, ForeignKey("products.id", ondelete="SET NULL"), nullable=True, index=True)

    # Availability state
    in_stock = Column(Boolean, default=False)
    available_qty = Column(Integer, nullable=True)

    # Backorder info
    backorder = Column(Boolean, default=False)
    backorder_date = Column(Date, nullable=True)

    # Pricing (if scraped)
    unit_price = Column(Numeric(12, 2), nullable=True)

    # Sync metadata
    sync_batch_id = Column(String(36), nullable=True, index=True)  # UUID for batch tracking
    checked_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)

    # Source tracking
    data_source = Column(String(50), default="bcw_browser")  # bcw_browser, bcw_email, manual

    # Relationships
    product = relationship("Product", back_populates="bcw_inventory_snapshots")

    # Indexes per DB-004
    __table_args__ = (
        Index('ix_bcw_inventory_sku_checked', 'sku', 'checked_at'),
        Index('ix_bcw_inventory_product_checked', 'product_id', 'checked_at'),
        Index('ix_bcw_inventory_backorder', 'backorder', postgresql_where=backorder.is_(True)),
    )


# =============================================================================
# BCW ORDER
# =============================================================================

class BCWOrder(Base):
    """
    BCW vendor order tracking.

    Maps internal orders to BCW orders with full state machine.
    Per order_state_machine in proposal doc.
    """
    __tablename__ = "bcw_orders"

    id = Column(Integer, primary_key=True, index=True)

    # Link to internal order
    order_id = Column(Integer, ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)

    # BCW order reference
    bcw_order_id = Column(String(50), nullable=True, unique=True, index=True)
    bcw_confirmation_number = Column(String(100), nullable=True)

    # State machine
    state = Column(
        Enum(BCWOrderState, name="bcw_order_state"),
        default=BCWOrderState.DRAFT,
        nullable=False,
        index=True
    )
    previous_state = Column(String(50), nullable=True)

    # Idempotency (per idempotency_strategy in proposal)
    idempotency_key = Column(String(255), unique=True, nullable=False, index=True)

    # Correlation for distributed tracing
    correlation_id = Column(String(36), nullable=False, index=True)  # UUID

    # Shipping info from BCW
    bcw_shipping_method = Column(String(100), nullable=True)
    bcw_shipping_cost = Column(Numeric(12, 2), nullable=True)

    # Tracking
    tracking_number = Column(String(100), nullable=True, index=True)
    carrier = Column(String(50), nullable=True)

    # Timestamps
    submitted_at = Column(DateTime(timezone=True), nullable=True)
    confirmed_at = Column(DateTime(timezone=True), nullable=True)
    shipped_at = Column(DateTime(timezone=True), nullable=True)
    delivered_at = Column(DateTime(timezone=True), nullable=True)

    # Exception handling
    exception_category = Column(String(50), nullable=True)
    exception_details = Column(JSON, nullable=True)
    exception_resolved_at = Column(DateTime(timezone=True), nullable=True)

    # Retry tracking
    submission_attempts = Column(Integer, default=0)
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)

    # Standard timestamps
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # DB-005: Audit columns
    updated_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    update_reason = Column(String(255), nullable=True)

    # Relationships
    order = relationship("Order", back_populates="bcw_order")
    events = relationship("BCWOrderEvent", back_populates="bcw_order", order_by="BCWOrderEvent.created_at")

    # BCW Refund Request Module v1.0.0
    refund_requests = relationship("BCWRefundRequest", back_populates="bcw_order")

    # Indexes and constraints
    __table_args__ = (
        Index('ix_bcw_orders_state_created', 'state', 'created_at'),
        Index('ix_bcw_orders_pending', 'state', postgresql_where=state.in_([
            BCWOrderState.PENDING_VENDOR_SUBMISSION,
            BCWOrderState.VENDOR_SUBMITTED,
            BCWOrderState.BACKORDERED,
            BCWOrderState.PARTIALLY_SHIPPED,
        ])),
        CheckConstraint('submission_attempts >= 0', name='check_submission_attempts_non_negative'),
    )


# =============================================================================
# BCW ORDER EVENTS (Audit Trail)
# =============================================================================

class BCWOrderEvent(Base):
    """
    Immutable audit trail for BCW order state changes.

    Per audit_requirements in proposal doc:
    - All state transitions logged
    - correlation_id for distributed tracing
    - 730-day retention
    """
    __tablename__ = "bcw_order_events"

    id = Column(Integer, primary_key=True, index=True)

    # Link to BCW order
    bcw_order_id = Column(Integer, ForeignKey("bcw_orders.id", ondelete="CASCADE"), nullable=False, index=True)

    # State transition
    from_state = Column(String(50), nullable=True)
    to_state = Column(String(50), nullable=False)
    trigger = Column(String(100), nullable=False)  # e.g., "checkout_initiated", "bcw_order_placed"

    # Context
    correlation_id = Column(String(36), nullable=False, index=True)
    actor_type = Column(String(20), nullable=False)  # user, admin, system, job
    actor_id_hash = Column(String(64), nullable=True)  # SHA-256 hash (no PII)

    # Event data
    event_data = Column(JSON, nullable=True)

    # Timestamp (immutable)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)

    # Relationships
    bcw_order = relationship("BCWOrder", back_populates="events")

    # Indexes
    __table_args__ = (
        Index('ix_bcw_order_events_correlation', 'correlation_id'),
        Index('ix_bcw_order_events_trigger', 'trigger', 'created_at'),
    )


# =============================================================================
# BCW SHIPPING QUOTE CACHE
# =============================================================================

class BCWShippingQuote(Base):
    """
    Cached shipping quotes from BCW shadow cart.

    Per quote_service in proposal doc:
    - TTL: 15 minutes
    - Key: address_hash + cart_hash
    """
    __tablename__ = "bcw_shipping_quotes"

    id = Column(Integer, primary_key=True, index=True)

    # Cache key components
    address_hash = Column(String(64), nullable=False, index=True)  # SHA-256
    cart_hash = Column(String(64), nullable=False, index=True)  # SHA-256

    # Quote data
    shipping_options = Column(JSON, nullable=False)  # List of {method, price, estimated_days}
    lowest_price = Column(Numeric(12, 2), nullable=False)

    # Validity
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)

    # Source tracking
    correlation_id = Column(String(36), nullable=True)

    # Unique constraint for cache key
    __table_args__ = (
        Index('ix_bcw_quotes_cache_key', 'address_hash', 'cart_hash'),
        Index('ix_bcw_quotes_valid', 'expires_at', postgresql_where=expires_at > datetime.now(timezone.utc)),
    )


# =============================================================================
# BCW AUTOMATION ERRORS (for debugging)
# =============================================================================

class BCWAutomationError(Base):
    """
    Log of automation errors with screenshots for debugging.

    Stores failures with page state for selector troubleshooting.
    """
    __tablename__ = "bcw_automation_errors"

    id = Column(Integer, primary_key=True, index=True)

    # Error context
    error_type = Column(String(100), nullable=False, index=True)  # e.g., "BCWSelectorError"
    error_code = Column(String(50), nullable=False, index=True)
    error_message = Column(Text, nullable=False)

    # Selector info (if selector error)
    selector_key = Column(String(100), nullable=True)
    selector_version = Column(String(20), nullable=True)

    # Page state
    page_url = Column(String(500), nullable=True)
    screenshot_path = Column(String(500), nullable=True)  # Path to screenshot file
    page_html_path = Column(String(500), nullable=True)  # Path to HTML dump

    # Context
    correlation_id = Column(String(36), nullable=True, index=True)
    bcw_order_id = Column(Integer, ForeignKey("bcw_orders.id", ondelete="SET NULL"), nullable=True)

    # Resolution
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolution_notes = Column(Text, nullable=True)

    # Timestamp
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)

    # Indexes
    __table_args__ = (
        Index('ix_bcw_errors_unresolved', 'error_type', postgresql_where=resolved_at.is_(None)),
    )


# =============================================================================
# BCW PRODUCT MAPPING (ALLOWLIST)
# =============================================================================

class BCWProductMapping(Base):
    """
    BCW product allowlist with branded SKU mapping.

    This table controls which BCW products we offer and maps
    our branded SKUs to BCW's internal SKUs.

    Key Features:
    - Allowlist: Only sync/sell products in this table
    - Branded SKUs: Customer sees mdm_sku, system uses bcw_sku
    - Cost tracking: Track our cost from BCW for margin calculations
    - Category grouping: For bulk operations and reporting
    """
    __tablename__ = "bcw_product_mappings"

    id = Column(Integer, primary_key=True, index=True)

    # SKU mapping (customer never sees bcw_sku)
    mdm_sku = Column(String(50), unique=True, nullable=False, index=True)  # Our branded SKU
    bcw_sku = Column(String(50), nullable=False, index=True)  # BCW's SKU (internal use only)

    # Product info (cached from BCW)
    product_name = Column(String(255), nullable=True)  # For reference/admin UI
    bcw_category = Column(String(100), nullable=True)  # BCW's category
    mdm_category = Column(String(100), nullable=True)  # Our category assignment

    # Pricing
    bcw_cost = Column(Numeric(10, 2), nullable=True)  # Our cost from BCW
    bcw_msrp = Column(Numeric(10, 2), nullable=True)  # BCW's suggested retail
    our_price = Column(Numeric(10, 2), nullable=True)  # What we charge (can be NULL if set elsewhere)

    # Minimum margin protection
    min_margin_percent = Column(Numeric(5, 2), nullable=True)  # e.g., 25.00 = 25%

    # Link to our product (optional - for products we already have in catalog)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="SET NULL"), nullable=True)

    # Status flags
    is_active = Column(Boolean, default=True, nullable=False, index=True)  # In our catalog
    is_dropship_only = Column(Boolean, default=True, nullable=False)  # Not stocked by us
    sync_inventory = Column(Boolean, default=True, nullable=False)  # Include in inventory sync

    # Import metadata
    imported_at = Column(DateTime(timezone=True), nullable=True)
    imported_from = Column(String(100), nullable=True)  # e.g., "spreadsheet_2024_01"

    # Audit
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    updated_by = Column(String(100), nullable=True)

    # Relationships
    product = relationship("Product", back_populates="bcw_mapping", uselist=False)

    # Indexes
    __table_args__ = (
        Index('ix_bcw_mapping_active', 'is_active', postgresql_where=is_active.is_(True)),
        Index('ix_bcw_mapping_sync', 'sync_inventory', postgresql_where=sync_inventory.is_(True)),
    )
