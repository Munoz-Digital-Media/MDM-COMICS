"""
Admin DSAR (Data Subject Access Request) Routes

User Management System v1.0.0
Per constitution_pii.json: GDPR/CCPA compliance management
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Request, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

from app.core.database import get_db
from app.core.permissions import require_permission, Permission
from app.models.user import User
from app.models.user_audit_log import AuditAction
from app.services.dsar_service import DSARService
from app.services.retention_service import RetentionService
from app.services.audit_service import AuditService

router = APIRouter()


def get_client_ip(request: Request) -> Optional[str]:
    """Extract client IP from request."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else None


def get_user_agent(request: Request) -> Optional[str]:
    """Extract user agent from request."""
    return request.headers.get("user-agent")


# ============================================================
# Request/Response Models
# ============================================================

class DSARRequestResponse(BaseModel):
    """DSAR request response."""
    id: int
    user_id: int
    request_type: str
    status: str
    requested_at: str
    completed_at: Optional[str]
    processed_by: Optional[int]
    notes: Optional[str]
    ledger_tx_id: Optional[str]


class DSARListResponse(BaseModel):
    """List of DSAR requests."""
    requests: List[DSARRequestResponse]
    total: int


class DSARStatsResponse(BaseModel):
    """DSAR statistics."""
    by_status: Dict[str, int]
    by_type: Dict[str, int]
    pending_count: int
    avg_processing_hours: float
    compliance_deadline_days: int


class ProcessExportResponse(BaseModel):
    """Export processing result."""
    request_id: int
    export_hash: str
    completed_at: str


class ProcessDeletionResponse(BaseModel):
    """Deletion processing result."""
    request_id: int
    user_id: int
    pre_hash: str
    post_hash: str
    ledger_tx_id: str
    completed_at: str


class RectificationRequest(BaseModel):
    """Rectification corrections."""
    corrections: Dict[str, Any]


class RetentionStatusResponse(BaseModel):
    """Retention status response."""
    status: Dict[str, Dict[str, Any]]


class CleanupPreviewResponse(BaseModel):
    """Cleanup preview response."""
    preview: Dict[str, int]


class CleanupResultResponse(BaseModel):
    """Cleanup execution result."""
    results: Dict[str, int]
    total_cleaned: int


# ============================================================
# DSAR Management Endpoints
# ============================================================

@router.get("", response_model=DSARListResponse)
async def list_dsar_requests(
    status: Optional[str] = Query(None, description="Filter by status"),
    request_type: Optional[str] = Query(None, description="Filter by type (export/delete/rectify)"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(require_permission(Permission.DSAR_ADMIN)),
    db: AsyncSession = Depends(get_db)
):
    """
    List all DSAR requests with optional filters.

    Requires: dsar:admin permission
    """
    dsar_service = DSARService(db)

    requests = await dsar_service.get_all_requests(
        status=status,
        request_type=request_type,
        limit=limit,
        offset=offset
    )

    return DSARListResponse(
        requests=[
            DSARRequestResponse(
                id=r.id,
                user_id=r.user_id,
                request_type=r.request_type,
                status=r.status,
                requested_at=r.requested_at.isoformat() if r.requested_at else "",
                completed_at=r.completed_at.isoformat() if r.completed_at else None,
                processed_by=r.processed_by,
                notes=r.notes,
                ledger_tx_id=r.ledger_tx_id,
            )
            for r in requests
        ],
        total=len(requests)
    )


@router.get("/stats", response_model=DSARStatsResponse)
async def get_dsar_stats(
    current_user: User = Depends(require_permission(Permission.DSAR_ADMIN)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get DSAR statistics for dashboard.

    Requires: dsar:admin permission
    """
    dsar_service = DSARService(db)
    stats = await dsar_service.get_stats()

    return DSARStatsResponse(**stats)


@router.get("/{request_id}", response_model=DSARRequestResponse)
async def get_dsar_request(
    request_id: int,
    current_user: User = Depends(require_permission(Permission.DSAR_ADMIN)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get details of a specific DSAR request.

    Requires: dsar:admin permission
    """
    dsar_service = DSARService(db)

    dsar_request = await dsar_service.get_request(request_id)
    if not dsar_request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="DSAR request not found"
        )

    return DSARRequestResponse(
        id=dsar_request.id,
        user_id=dsar_request.user_id,
        request_type=dsar_request.request_type,
        status=dsar_request.status,
        requested_at=dsar_request.requested_at.isoformat() if dsar_request.requested_at else "",
        completed_at=dsar_request.completed_at.isoformat() if dsar_request.completed_at else None,
        processed_by=dsar_request.processed_by,
        notes=dsar_request.notes,
        ledger_tx_id=dsar_request.ledger_tx_id,
    )


@router.post("/{request_id}/cancel")
async def cancel_dsar_request(
    request_id: int,
    request: Request,
    current_user: User = Depends(require_permission(Permission.DSAR_ADMIN)),
    db: AsyncSession = Depends(get_db)
):
    """
    Cancel a pending DSAR request.

    Requires: dsar:admin permission
    """
    dsar_service = DSARService(db)
    audit_service = AuditService(db)

    try:
        dsar_request = await dsar_service.cancel_request(request_id)

        await audit_service.log(
            action=AuditAction.DSAR_CANCELLED,
            user_id=current_user.id,
            resource_type="dsar_request",
            resource_id=str(request_id),
            details={"request_type": dsar_request.request_type},
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return {"message": "DSAR request cancelled", "request_id": request_id}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/{request_id}/process-export")
async def process_export_request(
    request_id: int,
    request: Request,
    current_user: User = Depends(require_permission(Permission.DSAR_ADMIN)),
    db: AsyncSession = Depends(get_db)
):
    """
    Process a DSAR export request and return the data.

    Requires: dsar:admin permission
    """
    dsar_service = DSARService(db)
    audit_service = AuditService(db)

    try:
        result = await dsar_service.process_export(
            request_id=request_id,
            processor_id=current_user.id
        )

        await audit_service.log(
            action=AuditAction.DSAR_COMPLETED,
            user_id=current_user.id,
            resource_type="dsar_request",
            resource_id=str(request_id),
            details={
                "request_type": "export",
                "export_hash": result["export_hash"],
            },
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        # Return the full export data
        return JSONResponse(
            content={
                "request_id": result["request_id"],
                "export_data": result["export_data"],
                "export_hash": result["export_hash"],
                "completed_at": result["completed_at"],
            }
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/{request_id}/process-deletion", response_model=ProcessDeletionResponse)
async def process_deletion_request(
    request_id: int,
    request: Request,
    current_user: User = Depends(require_permission(Permission.DSAR_ADMIN)),
    db: AsyncSession = Depends(get_db)
):
    """
    Process a DSAR deletion request (Right to Erasure).

    This will:
    - Revoke all user sessions
    - Delete user addresses
    - Anonymize order records
    - Anonymize user account

    Requires: dsar:admin permission
    """
    dsar_service = DSARService(db)
    audit_service = AuditService(db)

    try:
        result = await dsar_service.process_deletion(
            request_id=request_id,
            processor_id=current_user.id
        )

        await audit_service.log(
            action=AuditAction.DSAR_COMPLETED,
            user_id=current_user.id,
            resource_type="dsar_request",
            resource_id=str(request_id),
            details={
                "request_type": "delete",
                "target_user_id": result["user_id"],
                "pre_hash": result["pre_hash"][:16] + "...",
                "post_hash": result["post_hash"][:16] + "...",
                "ledger_tx_id": result["ledger_tx_id"],
            },
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return ProcessDeletionResponse(**result)

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/{request_id}/process-rectification")
async def process_rectification_request(
    request_id: int,
    request: Request,
    data: RectificationRequest,
    current_user: User = Depends(require_permission(Permission.DSAR_ADMIN)),
    db: AsyncSession = Depends(get_db)
):
    """
    Process a DSAR rectification request (data correction).

    Requires: dsar:admin permission
    """
    dsar_service = DSARService(db)
    audit_service = AuditService(db)

    try:
        result = await dsar_service.process_rectification(
            request_id=request_id,
            corrections=data.corrections,
            processor_id=current_user.id
        )

        await audit_service.log(
            action=AuditAction.DSAR_COMPLETED,
            user_id=current_user.id,
            resource_type="dsar_request",
            resource_id=str(request_id),
            details={
                "request_type": "rectify",
                "updated_fields": result["updated_fields"],
            },
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return result

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


# ============================================================
# User Self-Service DSAR Endpoints (for /api/me routes)
# ============================================================

@router.post("/user/export")
async def request_data_export(
    request: Request,
    current_user: User = Depends(require_permission(Permission.USERS_SELF)),
    db: AsyncSession = Depends(get_db)
):
    """
    Request a data export (GDPR Article 15).

    User can request export of all their personal data.
    """
    dsar_service = DSARService(db)
    audit_service = AuditService(db)

    try:
        dsar_request = await dsar_service.create_request(
            user_id=current_user.id,
            request_type="export",
            notes="User-initiated data export request"
        )

        await audit_service.log(
            action=AuditAction.DSAR_REQUESTED,
            user_id=current_user.id,
            resource_type="dsar_request",
            resource_id=str(dsar_request.id),
            details={"request_type": "export"},
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return {
            "message": "Data export request submitted",
            "request_id": dsar_request.id,
            "status": dsar_request.status,
            "expected_completion_days": 30,  # GDPR requirement
        }

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/user/delete")
async def request_account_deletion(
    request: Request,
    current_user: User = Depends(require_permission(Permission.USERS_SELF)),
    db: AsyncSession = Depends(get_db)
):
    """
    Request account deletion (GDPR Article 17 - Right to Erasure).

    User can request deletion of their account.
    14-day grace period for cancellation.
    """
    dsar_service = DSARService(db)
    audit_service = AuditService(db)

    try:
        dsar_request = await dsar_service.create_request(
            user_id=current_user.id,
            request_type="delete",
            notes="User-initiated account deletion request"
        )

        await audit_service.log(
            action=AuditAction.DSAR_REQUESTED,
            user_id=current_user.id,
            resource_type="dsar_request",
            resource_id=str(dsar_request.id),
            details={"request_type": "delete"},
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return {
            "message": "Account deletion request submitted",
            "request_id": dsar_request.id,
            "status": dsar_request.status,
            "grace_period_days": 14,
            "cancellation_available": True,
        }

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/user/requests")
async def get_my_dsar_requests(
    current_user: User = Depends(require_permission(Permission.USERS_SELF)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get current user's DSAR request history.
    """
    dsar_service = DSARService(db)

    requests = await dsar_service.get_user_requests(current_user.id)

    return {
        "requests": [
            {
                "id": r.id,
                "type": r.request_type,
                "status": r.status,
                "requested_at": r.requested_at.isoformat() if r.requested_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            }
            for r in requests
        ]
    }


@router.post("/user/requests/{request_id}/cancel")
async def cancel_my_dsar_request(
    request_id: int,
    request: Request,
    current_user: User = Depends(require_permission(Permission.USERS_SELF)),
    db: AsyncSession = Depends(get_db)
):
    """
    Cancel user's own pending DSAR request.
    """
    dsar_service = DSARService(db)
    audit_service = AuditService(db)

    # Verify request belongs to user
    dsar_request = await dsar_service.get_request(request_id)
    if not dsar_request or dsar_request.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Request not found"
        )

    try:
        await dsar_service.cancel_request(request_id)

        await audit_service.log(
            action=AuditAction.DSAR_CANCELLED,
            user_id=current_user.id,
            resource_type="dsar_request",
            resource_id=str(request_id),
            details={"request_type": dsar_request.request_type, "self_cancel": True},
            ip_address=get_client_ip(request),
            user_agent=get_user_agent(request),
        )

        await db.commit()

        return {"message": "Request cancelled", "request_id": request_id}

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


# ============================================================
# Retention Management Endpoints
# ============================================================

@router.get("/retention/status", response_model=RetentionStatusResponse)
async def get_retention_status(
    current_user: User = Depends(require_permission(Permission.DSAR_ADMIN)),
    db: AsyncSession = Depends(get_db)
):
    """
    Get current data retention status.

    Requires: dsar:admin permission
    """
    retention_service = RetentionService(db)
    status = await retention_service.get_retention_status()

    return RetentionStatusResponse(status=status)


@router.get("/retention/preview", response_model=CleanupPreviewResponse)
async def preview_retention_cleanup(
    current_user: User = Depends(require_permission(Permission.DSAR_ADMIN)),
    db: AsyncSession = Depends(get_db)
):
    """
    Preview what would be cleaned up by retention policy.

    Requires: dsar:admin permission
    """
    retention_service = RetentionService(db)
    preview = await retention_service.preview_cleanup()

    return CleanupPreviewResponse(preview=preview)


@router.post("/retention/cleanup", response_model=CleanupResultResponse)
async def run_retention_cleanup(
    request: Request,
    current_user: User = Depends(require_permission(Permission.DSAR_ADMIN)),
    db: AsyncSession = Depends(get_db)
):
    """
    Execute retention cleanup.

    This will delete expired data according to retention policies.

    Requires: dsar:admin permission
    """
    retention_service = RetentionService(db)
    audit_service = AuditService(db)

    results = await retention_service.run_cleanup()

    total_cleaned = sum(results.values())

    await audit_service.log(
        action=AuditAction.RETENTION_CLEANUP,
        user_id=current_user.id,
        resource_type="system",
        resource_id="retention",
        details={"results": results, "total": total_cleaned},
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request),
    )

    await db.commit()

    return CleanupResultResponse(results=results, total_cleaned=total_cleaned)
