"""
BCW Refund Request Models

RefundRequest Module v1.0.0 - BCW Supplies Only

Per 20251217_refund_request_bcw_supplies.md:
- RefundEligibilityPolicy: Product type refund rules
- BCWRefundRequest: Refund tracking with vendor credit blocking
- BCWRefundEvent: Immutable audit trail with hash chain

DB Compliance:
- DB-001: Numeric(12,2) for monetary fields
- DB-003: FK with appropriate ON DELETE behavior
- DB-004: Indexes on FKs and query columns
- DB-005: Audit columns (updated_by, update_reason)
- NASTY-008: Timezone-aware UTC timestamps
- constitution_pii.json: SHA-512 hash for actor identification
"""
from datetime import datetime, timezone
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, JSON,
    Numeric, ForeignKey, Index, Enum, CheckConstraint
)
from sqlalchemy.orm import relationship

from app.core.database import Base


# =============================================================================
# ENUMS
# =============================================================================

class BCWRefundState(str, PyEnum):
    """
    Refund request state machine.

    States follow vendor-first recovery pattern:
    1. Customer requests -> REQUESTED
    2. Admin validates eligibility -> APPROVED or DENIED
    3. If approved, initiate BCW return -> VENDOR_RETURN_INITIATED
    4. BCW processes return -> VENDOR_CREDIT_PENDING
    5. BCW issues credit -> VENDOR_CREDIT_RECEIVED
    6. ONLY THEN -> CUSTOMER_REFUND_PROCESSING (BLOCKING GATE)
    7. Stripe refund issued -> COMPLETED
    """
    # Initial states
    REQUESTED = "REQUESTED"
    UNDER_REVIEW = "UNDER_REVIEW"

    # Decision states
    APPROVED = "APPROVED"
    DENIED = "DENIED"

    # Vendor recovery flow (BLOCKING)
    VENDOR_RETURN_INITIATED = "VENDOR_RETURN_INITIATED"
    VENDOR_RETURN_IN_TRANSIT = "VENDOR_RETURN_IN_TRANSIT"
    VENDOR_RETURN_RECEIVED = "VENDOR_RETURN_RECEIVED"
    VENDOR_CREDIT_PENDING = "VENDOR_CREDIT_PENDING"
    VENDOR_CREDIT_RECEIVED = "VENDOR_CREDIT_RECEIVED"  # GATE: Must reach here first

    # Customer refund flow (GATED)
    CUSTOMER_REFUND_PROCESSING = "CUSTOMER_REFUND_PROCESSING"
    CUSTOMER_REFUND_ISSUED = "CUSTOMER_REFUND_ISSUED"

    # Terminal states
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    EXCEPTION = "EXCEPTION"


class RefundReasonCode(str, PyEnum):
    """Standard refund reason codes."""
    DAMAGED = "damaged"
    WRONG_ITEM = "wrong_item"
    DEFECTIVE = "defective"
    NOT_AS_DESCRIBED = "not_as_described"
    CHANGED_MIND = "changed_mind"


# =============================================================================
# REFUND ELIGIBILITY POLICY
# =============================================================================

class RefundEligibilityPolicy(Base):
    """
    Product type refund eligibility policy.

    Per business decision: Only BCW Supplies are refundable.
    Collectibles (comics, funkos, graded) are FINAL SALE.
    """
    __tablename__ = "refund_eligibility_policies"

    id = Column(Integer, primary_key=True, index=True)

    # Policy identification
    product_type = Column(String(50), unique=True, nullable=False, index=True)
    # e.g., "bcw_supply", "comic", "funko", "graded"

    # Eligibility
    is_refundable = Column(Boolean, default=False, nullable=False)
    requires_vendor_approval = Column(Boolean, default=True, nullable=False)

    # Time limits
    return_window_days = Column(Integer, nullable=True)  # NULL = no returns
    restocking_fee_percent = Column(Numeric(5, 2), default=0)  # e.g., 15.00 = 15%

    # Legal text
    policy_summary = Column(String(500), nullable=False)
    full_policy_text = Column(Text, nullable=False)

    # Display
    display_on_product_page = Column(Boolean, default=True, nullable=False)
    display_on_checkout = Column(Boolean, default=True, nullable=False)

    # Timestamps (NASTY-008)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # DB-005: Audit columns
    updated_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    update_reason = Column(String(255), nullable=True)


# =============================================================================
# BCW REFUND REQUEST
# =============================================================================

class BCWRefundRequest(Base):
    """
    BCW Supplies refund request with vendor credit recovery blocking.

    CRITICAL: Customer refund cannot be processed until
    state reaches VENDOR_CREDIT_RECEIVED.

    DB Compliance:
    - DB-001: Numeric(12,2) for monetary fields
    - DB-003: FK with CASCADE for order, SET NULL for user
    - DB-004: Indexes on state, created_at, order_id
    - DB-005: Audit columns
    - NASTY-008: Timezone-aware timestamps
    """
    __tablename__ = "bcw_refund_requests"

    id = Column(Integer, primary_key=True, index=True)

    # Request identification
    refund_number = Column(String(50), unique=True, nullable=False, index=True)
    # Format: RFD-YYYYMMDD-XXXXXXXX

    idempotency_key = Column(String(255), unique=True, nullable=False, index=True)
    correlation_id = Column(String(36), nullable=False, index=True)

    # Relationships
    order_id = Column(Integer, ForeignKey("orders.id", ondelete="CASCADE"),
                      nullable=False, index=True)
    bcw_order_id = Column(Integer, ForeignKey("bcw_orders.id", ondelete="SET NULL"),
                          nullable=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"),
                     nullable=True, index=True)

    # State machine
    state = Column(
        Enum(BCWRefundState, name="bcw_refund_state"),
        default=BCWRefundState.REQUESTED,
        nullable=False,
        index=True
    )
    previous_state = Column(String(50), nullable=True)

    # Request details
    reason_code = Column(String(50), nullable=False)
    reason_description = Column(Text, nullable=True)

    # Items being refunded (JSON array of order_item_ids with quantities)
    refund_items = Column(JSON, nullable=False)
    # [{order_item_id: 123, quantity: 2, unit_price: 4.99}]

    # Monetary values (DB-001: Numeric(12,2))
    original_amount = Column(Numeric(12, 2), nullable=False)
    restocking_fee = Column(Numeric(12, 2), default=0)
    refund_amount = Column(Numeric(12, 2), nullable=False)

    # Vendor credit tracking (BLOCKING GATE)
    vendor_credit_amount = Column(Numeric(12, 2), nullable=True)
    vendor_credit_reference = Column(String(100), nullable=True)
    vendor_credit_received_at = Column(DateTime(timezone=True), nullable=True)

    # Customer refund tracking (GATED)
    stripe_refund_id = Column(String(100), nullable=True)
    customer_refund_issued_at = Column(DateTime(timezone=True), nullable=True)

    # Return shipping
    return_tracking_number = Column(String(100), nullable=True)
    return_carrier = Column(String(50), nullable=True)
    return_label_url = Column(String(500), nullable=True)

    # Review
    reviewed_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    denial_reason = Column(Text, nullable=True)

    # Exception handling
    exception_category = Column(String(50), nullable=True)
    exception_details = Column(JSON, nullable=True)

    # Timestamps (NASTY-008)
    created_at = Column(DateTime(timezone=True),
                        default=lambda: datetime.now(timezone.utc), index=True)
    updated_at = Column(DateTime(timezone=True),
                        default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # DB-005: Audit columns
    updated_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    update_reason = Column(String(255), nullable=True)

    # Relationships
    order = relationship("Order", back_populates="refund_requests")
    bcw_order = relationship("BCWOrder", back_populates="refund_requests")
    user = relationship("User", foreign_keys=[user_id], back_populates="refund_requests")
    reviewer = relationship("User", foreign_keys=[reviewed_by])
    updater = relationship("User", foreign_keys=[updated_by])
    events = relationship("BCWRefundEvent", back_populates="refund_request",
                          order_by="BCWRefundEvent.created_at")

    # Indexes and constraints (DB-004, DB-006)
    __table_args__ = (
        Index('ix_bcw_refunds_state_created', 'state', 'created_at'),
        Index('ix_bcw_refunds_user', 'user_id', 'created_at'),
        CheckConstraint('refund_amount >= 0', name='check_refund_amount_non_negative'),
        CheckConstraint('restocking_fee >= 0', name='check_restocking_fee_non_negative'),
        CheckConstraint('original_amount >= 0', name='check_original_amount_non_negative'),
    )


# =============================================================================
# BCW REFUND EVENT (Immutable Audit Trail)
# =============================================================================

class BCWRefundEvent(Base):
    """
    Immutable audit trail for refund state changes.

    Per constitution_pii.json and constitution_cyberSec.json:
    - SHA-512 hash for actor identification
    - Hash chain for tamper evidence
    - 730-day retention
    """
    __tablename__ = "bcw_refund_events"

    id = Column(Integer, primary_key=True, index=True)

    # Link to refund request
    refund_request_id = Column(Integer,
                               ForeignKey("bcw_refund_requests.id", ondelete="CASCADE"),
                               nullable=False, index=True)

    # State transition
    from_state = Column(String(50), nullable=True)
    to_state = Column(String(50), nullable=False)
    trigger = Column(String(100), nullable=False)
    # e.g., "customer_requested", "admin_approved", "vendor_credit_confirmed"

    # Correlation
    correlation_id = Column(String(36), nullable=False, index=True)

    # Actor identification (PII-compliant)
    actor_type = Column(String(20), nullable=False)  # user, admin, system, vendor
    actor_id_hash = Column(String(128), nullable=True)  # SHA-512 hash

    # Event data (no PII)
    event_data = Column(JSON, nullable=True)

    # Hash chain for tamper evidence
    prev_event_hash = Column(String(128), nullable=True)  # SHA-512
    event_hash = Column(String(128), nullable=False)  # SHA-512

    # Timestamp (immutable)
    created_at = Column(DateTime(timezone=True),
                        default=lambda: datetime.now(timezone.utc), index=True)

    # Relationships
    refund_request = relationship("BCWRefundRequest", back_populates="events")

    # Indexes
    __table_args__ = (
        Index('ix_bcw_refund_events_correlation', 'correlation_id'),
        Index('ix_bcw_refund_events_trigger', 'trigger', 'created_at'),
    )


# =============================================================================
# VALID STATE TRANSITIONS
# =============================================================================

VALID_REFUND_TRANSITIONS = {
    BCWRefundState.REQUESTED: [
        BCWRefundState.UNDER_REVIEW,
        BCWRefundState.CANCELLED,
    ],
    BCWRefundState.UNDER_REVIEW: [
        BCWRefundState.APPROVED,
        BCWRefundState.DENIED,
        BCWRefundState.EXCEPTION,
    ],
    BCWRefundState.APPROVED: [
        BCWRefundState.VENDOR_RETURN_INITIATED,
        BCWRefundState.CANCELLED,
    ],
    BCWRefundState.VENDOR_RETURN_INITIATED: [
        BCWRefundState.VENDOR_RETURN_IN_TRANSIT,
        BCWRefundState.EXCEPTION,
    ],
    BCWRefundState.VENDOR_RETURN_IN_TRANSIT: [
        BCWRefundState.VENDOR_RETURN_RECEIVED,
        BCWRefundState.EXCEPTION,
    ],
    BCWRefundState.VENDOR_RETURN_RECEIVED: [
        BCWRefundState.VENDOR_CREDIT_PENDING,
    ],
    BCWRefundState.VENDOR_CREDIT_PENDING: [
        BCWRefundState.VENDOR_CREDIT_RECEIVED,
        BCWRefundState.EXCEPTION,
    ],
    # BLOCKING GATE
    BCWRefundState.VENDOR_CREDIT_RECEIVED: [
        BCWRefundState.CUSTOMER_REFUND_PROCESSING,
    ],
    BCWRefundState.CUSTOMER_REFUND_PROCESSING: [
        BCWRefundState.CUSTOMER_REFUND_ISSUED,
        BCWRefundState.EXCEPTION,
    ],
    BCWRefundState.CUSTOMER_REFUND_ISSUED: [
        BCWRefundState.COMPLETED,
    ],
    # Terminal states
    BCWRefundState.DENIED: [],
    BCWRefundState.COMPLETED: [],
    BCWRefundState.CANCELLED: [],
    BCWRefundState.EXCEPTION: [
        BCWRefundState.UNDER_REVIEW,  # Can re-review after exception
    ],
}


def can_process_customer_refund(refund_request: BCWRefundRequest) -> bool:
    """
    CRITICAL BUSINESS RULE:
    Customer refund can ONLY be processed after vendor credit is received.

    This ensures MDM Comics does not release funds until
    they have been recovered from BCW.
    """
    return (
        refund_request.state == BCWRefundState.VENDOR_CREDIT_RECEIVED and
        refund_request.vendor_credit_amount is not None and
        refund_request.vendor_credit_received_at is not None
    )
