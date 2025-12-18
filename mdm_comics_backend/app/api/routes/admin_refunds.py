"""
Admin Refund API Routes

BCW Refund Request Module v1.0.0

Endpoints for admins to:
- List and manage all refund requests
- Approve/deny refunds
- Record vendor returns and credits
- Process customer refunds (GATED by vendor credit)

CRITICAL: Customer refund can ONLY be processed after vendor credit is received.
"""
import logging
from decimal import Decimal
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.database import get_db
from app.api.deps import get_current_admin
from app.models import User, BCWRefundRequest, BCWRefundState, BCWRefundEvent
from app.services.refund_service import (
    BCWRefundService,
    RefundBlockedError,
    InvalidStateTransitionError,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/refunds", tags=["admin-refunds"])


# =============================================================================
# SCHEMAS
# =============================================================================

class ReviewRefundRequest(BaseModel):
    """Request to approve or deny a refund."""
    action: str = Field(..., pattern="^(approve|deny)$")
    notes: Optional[str] = None
    denial_reason: Optional[str] = None  # Required if action is deny


class InitiateVendorReturnRequest(BaseModel):
    """Request to initiate vendor return."""
    return_carrier: str
    return_tracking_number: str
    return_label_url: Optional[str] = None


class RecordVendorCreditRequest(BaseModel):
    """Request to record vendor credit received."""
    credit_amount: float
    credit_reference: str


class RefundEventResponse(BaseModel):
    """Refund event for audit trail."""
    id: int
    from_state: Optional[str]
    to_state: str
    trigger: str
    actor_type: str
    event_data: Optional[dict]
    created_at: str


class AdminRefundResponse(BaseModel):
    """Detailed refund response for admins."""
    id: int
    refund_number: str
    order_id: int
    bcw_order_id: Optional[int]
    user_id: Optional[int]
    state: str
    previous_state: Optional[str]
    reason_code: str
    reason_description: Optional[str]
    refund_items: List[dict]
    original_amount: float
    restocking_fee: float
    refund_amount: float

    # Vendor credit tracking
    vendor_credit_amount: Optional[float]
    vendor_credit_reference: Optional[str]
    vendor_credit_received_at: Optional[str]

    # Customer refund tracking
    stripe_refund_id: Optional[str]
    customer_refund_issued_at: Optional[str]

    # Return shipping
    return_tracking_number: Optional[str]
    return_carrier: Optional[str]
    return_label_url: Optional[str]

    # Review info
    reviewed_by: Optional[int]
    reviewed_at: Optional[str]
    denial_reason: Optional[str]

    # Timestamps
    created_at: str
    updated_at: str

    # Audit trail
    events: List[RefundEventResponse]

    class Config:
        from_attributes = True


class RefundListResponse(BaseModel):
    """List of refunds for admin."""
    refunds: List[AdminRefundResponse]
    total: int


class RefundStatsResponse(BaseModel):
    """Refund statistics."""
    total_requests: int
    pending_review: int
    pending_vendor_credit: int
    ready_for_refund: int
    completed: int
    denied: int
    total_refunded_amount: float


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def format_admin_refund_response(refund: BCWRefundRequest) -> AdminRefundResponse:
    """Format refund for admin response with events."""
    events = [
        RefundEventResponse(
            id=e.id,
            from_state=e.from_state,
            to_state=e.to_state,
            trigger=e.trigger,
            actor_type=e.actor_type,
            event_data=e.event_data,
            created_at=e.created_at.isoformat() if e.created_at else None,
        )
        for e in (refund.events or [])
    ]

    return AdminRefundResponse(
        id=refund.id,
        refund_number=refund.refund_number,
        order_id=refund.order_id,
        bcw_order_id=refund.bcw_order_id,
        user_id=refund.user_id,
        state=refund.state.value,
        previous_state=refund.previous_state,
        reason_code=refund.reason_code,
        reason_description=refund.reason_description,
        refund_items=refund.refund_items,
        original_amount=float(refund.original_amount),
        restocking_fee=float(refund.restocking_fee),
        refund_amount=float(refund.refund_amount),
        vendor_credit_amount=float(refund.vendor_credit_amount) if refund.vendor_credit_amount else None,
        vendor_credit_reference=refund.vendor_credit_reference,
        vendor_credit_received_at=refund.vendor_credit_received_at.isoformat() if refund.vendor_credit_received_at else None,
        stripe_refund_id=refund.stripe_refund_id,
        customer_refund_issued_at=refund.customer_refund_issued_at.isoformat() if refund.customer_refund_issued_at else None,
        return_tracking_number=refund.return_tracking_number,
        return_carrier=refund.return_carrier,
        return_label_url=refund.return_label_url,
        reviewed_by=refund.reviewed_by,
        reviewed_at=refund.reviewed_at.isoformat() if refund.reviewed_at else None,
        denial_reason=refund.denial_reason,
        created_at=refund.created_at.isoformat() if refund.created_at else None,
        updated_at=refund.updated_at.isoformat() if refund.updated_at else None,
        events=events,
    )


# =============================================================================
# ENDPOINTS
# =============================================================================

@router.get("/", response_model=RefundListResponse)
async def list_refund_requests(
    state: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """
    List all refund requests with optional state filter.
    Returns empty list if table doesn't exist yet.
    """
    try:
        query = select(BCWRefundRequest).options(
            selectinload(BCWRefundRequest.events)
        ).order_by(BCWRefundRequest.created_at.desc())

        if state:
            try:
                state_enum = BCWRefundState(state)
                query = query.where(BCWRefundRequest.state == state_enum)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid state: {state}"
                )

        query = query.limit(limit).offset(offset)
        result = await db.execute(query)
        refunds = list(result.scalars().all())

        return RefundListResponse(
            refunds=[format_admin_refund_response(r) for r in refunds],
            total=len(refunds),
        )
    except Exception as e:
        # Table may not exist yet - return empty list
        logger.warning(f"Refund list query failed (table may not exist): {e}")
        return RefundListResponse(refunds=[], total=0)


@router.get("/stats", response_model=RefundStatsResponse)
async def get_refund_stats(
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """
    Get refund statistics.
    Returns zeros if table doesn't exist yet.
    """
    try:
        from sqlalchemy import func

        # Total requests
        total_result = await db.execute(
            select(func.count(BCWRefundRequest.id))
        )
        total = total_result.scalar() or 0

        # Pending review
        pending_review_result = await db.execute(
            select(func.count(BCWRefundRequest.id))
            .where(BCWRefundRequest.state.in_([
                BCWRefundState.REQUESTED,
                BCWRefundState.UNDER_REVIEW,
            ]))
        )
        pending_review = pending_review_result.scalar() or 0

        # Pending vendor credit
        pending_vendor_result = await db.execute(
            select(func.count(BCWRefundRequest.id))
            .where(BCWRefundRequest.state.in_([
                BCWRefundState.VENDOR_RETURN_INITIATED,
                BCWRefundState.VENDOR_RETURN_IN_TRANSIT,
                BCWRefundState.VENDOR_RETURN_RECEIVED,
                BCWRefundState.VENDOR_CREDIT_PENDING,
            ]))
        )
        pending_vendor = pending_vendor_result.scalar() or 0

        # Ready for refund
        ready_result = await db.execute(
            select(func.count(BCWRefundRequest.id))
            .where(BCWRefundRequest.state == BCWRefundState.VENDOR_CREDIT_RECEIVED)
        )
        ready_for_refund = ready_result.scalar() or 0

        # Completed
        completed_result = await db.execute(
            select(func.count(BCWRefundRequest.id))
            .where(BCWRefundRequest.state == BCWRefundState.COMPLETED)
        )
        completed = completed_result.scalar() or 0

        # Denied
        denied_result = await db.execute(
            select(func.count(BCWRefundRequest.id))
            .where(BCWRefundRequest.state == BCWRefundState.DENIED)
        )
        denied = denied_result.scalar() or 0

        # Total refunded amount
        refunded_result = await db.execute(
            select(func.sum(BCWRefundRequest.refund_amount))
            .where(BCWRefundRequest.state == BCWRefundState.COMPLETED)
        )
        total_refunded = refunded_result.scalar() or 0

        return RefundStatsResponse(
            total_requests=total,
            pending_review=pending_review,
            pending_vendor_credit=pending_vendor,
            ready_for_refund=ready_for_refund,
            completed=completed,
            denied=denied,
            total_refunded_amount=float(total_refunded),
        )
    except Exception as e:
        # Table may not exist yet - return zeros
        logger.warning(f"Refund stats query failed (table may not exist): {e}")
        return RefundStatsResponse(
            total_requests=0,
            pending_review=0,
            pending_vendor_credit=0,
            ready_for_refund=0,
            completed=0,
            denied=0,
            total_refunded_amount=0.0,
        )


@router.get("/{refund_id}", response_model=AdminRefundResponse)
async def get_refund_request(
    refund_id: int,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """
    Get detailed refund request with audit trail.
    """
    result = await db.execute(
        select(BCWRefundRequest)
        .options(selectinload(BCWRefundRequest.events))
        .where(BCWRefundRequest.id == refund_id)
    )
    refund = result.scalar_one_or_none()

    if not refund:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Refund request not found"
        )

    return format_admin_refund_response(refund)


@router.post("/{refund_id}/review", response_model=AdminRefundResponse)
async def review_refund_request(
    refund_id: int,
    request: ReviewRefundRequest,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """
    Approve or deny a refund request.
    """
    result = await db.execute(
        select(BCWRefundRequest)
        .options(selectinload(BCWRefundRequest.events))
        .where(BCWRefundRequest.id == refund_id)
    )
    refund = result.scalar_one_or_none()

    if not refund:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Refund request not found"
        )

    # Move to under review first if needed
    if refund.state == BCWRefundState.REQUESTED:
        await BCWRefundService.transition_state(
            db=db,
            refund_request=refund,
            new_state=BCWRefundState.UNDER_REVIEW,
            trigger="admin_review_started",
            actor_type="admin",
            actor_id=admin.id,
        )

    try:
        if request.action == "approve":
            refund = await BCWRefundService.approve_refund(
                db=db,
                refund_request=refund,
                admin_id=admin.id,
                notes=request.notes,
            )
        else:
            if not request.denial_reason:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="denial_reason is required when denying a refund"
                )
            refund = await BCWRefundService.deny_refund(
                db=db,
                refund_request=refund,
                admin_id=admin.id,
                denial_reason=request.denial_reason,
            )

        await db.commit()
        await db.refresh(refund)

        logger.info(f"Admin {admin.id} {request.action}d refund {refund.refund_number}")
        return format_admin_refund_response(refund)

    except InvalidStateTransitionError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/{refund_id}/vendor-return", response_model=AdminRefundResponse)
async def initiate_vendor_return(
    refund_id: int,
    request: InitiateVendorReturnRequest,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """
    Initiate vendor return - record return shipping info.
    """
    result = await db.execute(
        select(BCWRefundRequest)
        .options(selectinload(BCWRefundRequest.events))
        .where(BCWRefundRequest.id == refund_id)
    )
    refund = result.scalar_one_or_none()

    if not refund:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Refund request not found"
        )

    refund.return_carrier = request.return_carrier
    refund.return_tracking_number = request.return_tracking_number
    refund.return_label_url = request.return_label_url

    try:
        refund = await BCWRefundService.transition_state(
            db=db,
            refund_request=refund,
            new_state=BCWRefundState.VENDOR_RETURN_INITIATED,
            trigger="vendor_return_initiated",
            actor_type="admin",
            actor_id=admin.id,
            event_data={
                "carrier": request.return_carrier,
                "tracking_number": request.return_tracking_number,
            },
        )
        await db.commit()
        await db.refresh(refund)

        return format_admin_refund_response(refund)

    except InvalidStateTransitionError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.put("/{refund_id}/vendor-credit", response_model=AdminRefundResponse)
async def record_vendor_credit(
    refund_id: int,
    request: RecordVendorCreditRequest,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """
    Record vendor credit received from BCW.

    This is the BLOCKING GATE - customer refund can only proceed after this.
    """
    result = await db.execute(
        select(BCWRefundRequest)
        .options(selectinload(BCWRefundRequest.events))
        .where(BCWRefundRequest.id == refund_id)
    )
    refund = result.scalar_one_or_none()

    if not refund:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Refund request not found"
        )

    # Transition through intermediate states if needed
    intermediate_states = [
        (BCWRefundState.VENDOR_RETURN_INITIATED, BCWRefundState.VENDOR_RETURN_IN_TRANSIT, "return_shipped"),
        (BCWRefundState.VENDOR_RETURN_IN_TRANSIT, BCWRefundState.VENDOR_RETURN_RECEIVED, "return_received"),
        (BCWRefundState.VENDOR_RETURN_RECEIVED, BCWRefundState.VENDOR_CREDIT_PENDING, "credit_pending"),
    ]

    try:
        for from_state, to_state, trigger in intermediate_states:
            if refund.state == from_state:
                refund = await BCWRefundService.transition_state(
                    db=db,
                    refund_request=refund,
                    new_state=to_state,
                    trigger=trigger,
                    actor_type="admin",
                    actor_id=admin.id,
                )

        # Now record the vendor credit
        refund = await BCWRefundService.record_vendor_credit(
            db=db,
            refund_request=refund,
            credit_amount=Decimal(str(request.credit_amount)),
            credit_reference=request.credit_reference,
            admin_id=admin.id,
        )
        await db.commit()
        await db.refresh(refund)

        logger.info(
            f"Admin {admin.id} recorded vendor credit for refund {refund.refund_number}: "
            f"${request.credit_amount} (ref: {request.credit_reference})"
        )
        return format_admin_refund_response(refund)

    except InvalidStateTransitionError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/{refund_id}/process-refund", response_model=AdminRefundResponse)
async def process_customer_refund(
    refund_id: int,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin)
):
    """
    Process customer refund via Stripe.

    CRITICAL: This endpoint is GATED - will REFUSE to process unless
    the refund is in VENDOR_CREDIT_RECEIVED state.

    This ensures MDM Comics does not release funds until vendor credit is recovered.
    """
    result = await db.execute(
        select(BCWRefundRequest)
        .options(selectinload(BCWRefundRequest.events))
        .where(BCWRefundRequest.id == refund_id)
    )
    refund = result.scalar_one_or_none()

    if not refund:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Refund request not found"
        )

    try:
        refund = await BCWRefundService.process_customer_refund(
            db=db,
            refund_request=refund,
            admin_id=admin.id,
        )
        await db.commit()
        await db.refresh(refund)

        logger.info(
            f"Admin {admin.id} processed customer refund for {refund.refund_number}: "
            f"${refund.refund_amount} via Stripe"
        )
        return format_admin_refund_response(refund)

    except RefundBlockedError as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e)
        )
    except InvalidStateTransitionError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
