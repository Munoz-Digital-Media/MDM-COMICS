"""
Customer Refund API Routes

BCW Refund Request Module v1.0.0

Endpoints for customers to:
- Request refunds (BCW Supplies only)
- View their refund requests
- Cancel pending requests

Collectibles (comics, Funkos, graded) are FINAL SALE - not refundable.
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.api.deps import get_current_user
from app.models import User, BCWRefundRequest, BCWRefundState
from app.services.refund_service import (
    BCWRefundService,
    RefundNotEligibleError,
    InvalidStateTransitionError,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/refunds", tags=["refunds"])


# =============================================================================
# SCHEMAS
# =============================================================================

class RefundItemRequest(BaseModel):
    """Item to refund."""
    order_item_id: int
    quantity: Optional[int] = None  # None = full quantity


class CreateRefundRequest(BaseModel):
    """Request to create a refund."""
    order_id: int
    items: List[RefundItemRequest]
    reason_code: str = Field(..., pattern="^(damaged|wrong_item|defective|not_as_described|changed_mind)$")
    reason_description: Optional[str] = None


class RefundItemResponse(BaseModel):
    """Refund item in response."""
    order_item_id: int
    quantity: int
    unit_price: float
    item_total: float
    product_name: str


class RefundResponse(BaseModel):
    """Refund request response."""
    id: int
    refund_number: str
    order_id: int
    state: str
    reason_code: str
    reason_description: Optional[str]
    original_amount: float
    restocking_fee: float
    refund_amount: float
    items: List[RefundItemResponse]
    created_at: str
    updated_at: str

    # Tracking info (when available)
    return_tracking_number: Optional[str]
    return_carrier: Optional[str]

    # Status info
    reviewed_at: Optional[str]
    denial_reason: Optional[str]
    customer_refund_issued_at: Optional[str]

    class Config:
        from_attributes = True


class RefundListResponse(BaseModel):
    """List of refund requests."""
    refunds: List[RefundResponse]
    total: int


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def format_refund_response(refund: BCWRefundRequest) -> RefundResponse:
    """Format refund request for API response."""
    return RefundResponse(
        id=refund.id,
        refund_number=refund.refund_number,
        order_id=refund.order_id,
        state=refund.state.value,
        reason_code=refund.reason_code,
        reason_description=refund.reason_description,
        original_amount=float(refund.original_amount),
        restocking_fee=float(refund.restocking_fee),
        refund_amount=float(refund.refund_amount),
        items=[
            RefundItemResponse(**item) for item in refund.refund_items
        ],
        created_at=refund.created_at.isoformat() if refund.created_at else None,
        updated_at=refund.updated_at.isoformat() if refund.updated_at else None,
        return_tracking_number=refund.return_tracking_number,
        return_carrier=refund.return_carrier,
        reviewed_at=refund.reviewed_at.isoformat() if refund.reviewed_at else None,
        denial_reason=refund.denial_reason,
        customer_refund_issued_at=refund.customer_refund_issued_at.isoformat() if refund.customer_refund_issued_at else None,
    )


# =============================================================================
# ENDPOINTS
# =============================================================================

@router.post("", response_model=RefundResponse, status_code=status.HTTP_201_CREATED)
async def create_refund_request(
    request: CreateRefundRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Request a refund for BCW Supply items.

    IMPORTANT: Only BCW Supplies are eligible for refunds.
    Collectibles (comics, Funkos, graded items) are FINAL SALE.

    A 15% restocking fee applies to all refunds.
    """
    try:
        refund = await BCWRefundService.create_refund_request(
            db=db,
            order_id=request.order_id,
            user_id=current_user.id,
            items=[item.model_dump() for item in request.items],
            reason_code=request.reason_code,
            reason_description=request.reason_description,
        )
        await db.commit()

        logger.info(f"User {current_user.id} created refund request {refund.refund_number}")
        return format_refund_response(refund)

    except RefundNotEligibleError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("", response_model=RefundListResponse)
async def list_refund_requests(
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    List the current user's refund requests.
    """
    refunds = await BCWRefundService.get_user_refund_requests(
        db=db,
        user_id=current_user.id,
        limit=limit,
        offset=offset,
    )

    return RefundListResponse(
        refunds=[format_refund_response(r) for r in refunds],
        total=len(refunds),
    )


@router.get("/{refund_id}", response_model=RefundResponse)
async def get_refund_request(
    refund_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get details of a specific refund request.
    """
    from sqlalchemy import select

    result = await db.execute(
        select(BCWRefundRequest)
        .where(BCWRefundRequest.id == refund_id)
        .where(BCWRefundRequest.user_id == current_user.id)
    )
    refund = result.scalar_one_or_none()

    if not refund:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Refund request not found"
        )

    return format_refund_response(refund)


@router.post("/{refund_id}/cancel", response_model=RefundResponse)
async def cancel_refund_request(
    refund_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Cancel a pending refund request.

    Can only cancel requests in REQUESTED or APPROVED state.
    """
    from sqlalchemy import select

    result = await db.execute(
        select(BCWRefundRequest)
        .where(BCWRefundRequest.id == refund_id)
        .where(BCWRefundRequest.user_id == current_user.id)
    )
    refund = result.scalar_one_or_none()

    if not refund:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Refund request not found"
        )

    # Check if can be cancelled
    cancellable_states = [BCWRefundState.REQUESTED, BCWRefundState.APPROVED]
    if refund.state not in cancellable_states:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel refund in state {refund.state.value}. "
                   f"Only requests in {[s.value for s in cancellable_states]} can be cancelled."
        )

    try:
        refund = await BCWRefundService.transition_state(
            db=db,
            refund_request=refund,
            new_state=BCWRefundState.CANCELLED,
            trigger="customer_cancelled",
            actor_type="user",
            actor_id=current_user.id,
        )
        await db.commit()

        logger.info(f"User {current_user.id} cancelled refund request {refund.refund_number}")
        return format_refund_response(refund)

    except InvalidStateTransitionError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/policy/{product_type}")
async def get_refund_policy(
    product_type: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Get the refund policy for a product type.

    Product types: bcw_supply, comic, funko, graded
    """
    policy = await BCWRefundService.get_refund_policy(db, product_type)

    if not policy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No policy found for product type: {product_type}"
        )

    return {
        "product_type": policy.product_type,
        "is_refundable": policy.is_refundable,
        "return_window_days": policy.return_window_days,
        "restocking_fee_percent": float(policy.restocking_fee_percent) if policy.restocking_fee_percent else 0,
        "policy_summary": policy.policy_summary,
        "full_policy_text": policy.full_policy_text,
    }
