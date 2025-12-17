"""
Admin Feature Flags API v1.0.0

Per 20251216_shipping_compartmentalization_proposal.json:
- GET /api/admin/feature-flags - List all feature flags
- GET /api/admin/feature-flags/shipping - List shipping carrier flags
- PATCH /api/admin/feature-flags/shipping/{carrier} - Toggle carrier

Per constitution_binder.json:
- Admin-only access
- Audit trail for all changes
- Structured error responses
"""
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import logging

from app.core.database import get_db
from app.core.feature_flags import FeatureFlags
from app.models.feature_flag import FeatureFlag
from app.models.user import User
from app.api.deps import get_current_admin

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/feature-flags", tags=["admin", "feature-flags"])


# =============================================================================
# Schemas
# =============================================================================

class FeatureFlagResponse(BaseModel):
    """Response schema for a feature flag."""
    id: str
    module: str
    feature: str
    is_enabled: bool
    config_json: dict
    disabled_reason: Optional[str] = None
    disabled_at: Optional[datetime] = None
    disabled_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class FeatureFlagListResponse(BaseModel):
    """Response schema for listing feature flags."""
    flags: List[FeatureFlagResponse]
    total: int


class ToggleFeatureFlagRequest(BaseModel):
    """Request schema for toggling a feature flag."""
    is_enabled: bool
    reason: Optional[str] = Field(
        None,
        description="Required when disabling a feature",
        max_length=500
    )


class ToggleFeatureFlagResponse(BaseModel):
    """Response schema after toggling a feature flag."""
    success: bool
    flag: FeatureFlagResponse
    message: str


class UpdateConfigRequest(BaseModel):
    """Request schema for updating feature flag config."""
    config_json: dict = Field(..., description="New configuration object")


# =============================================================================
# Endpoints
# =============================================================================

@router.get("", response_model=FeatureFlagListResponse)
async def list_all_feature_flags(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """
    List all feature flags.

    Admin only. Returns all flags across all modules.
    """
    result = await db.execute(
        select(FeatureFlag).order_by(FeatureFlag.module, FeatureFlag.feature)
    )
    flags = result.scalars().all()

    return FeatureFlagListResponse(
        flags=[FeatureFlagResponse(
            id=str(f.id),
            module=f.module,
            feature=f.feature,
            is_enabled=f.is_enabled,
            config_json=f.config_json or {},
            disabled_reason=f.disabled_reason,
            disabled_at=f.disabled_at,
            disabled_by=f.disabled_by,
            created_at=f.created_at,
            updated_at=f.updated_at
        ) for f in flags],
        total=len(flags)
    )


@router.get("/shipping", response_model=FeatureFlagListResponse)
async def list_shipping_carrier_flags(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """
    List all shipping carrier flags.

    Admin only. Returns flags where module='shipping'.
    """
    result = await db.execute(
        select(FeatureFlag)
        .where(FeatureFlag.module == "shipping")
        .order_by(FeatureFlag.feature)
    )
    flags = result.scalars().all()

    return FeatureFlagListResponse(
        flags=[FeatureFlagResponse(
            id=str(f.id),
            module=f.module,
            feature=f.feature,
            is_enabled=f.is_enabled,
            config_json=f.config_json or {},
            disabled_reason=f.disabled_reason,
            disabled_at=f.disabled_at,
            disabled_by=f.disabled_by,
            created_at=f.created_at,
            updated_at=f.updated_at
        ) for f in flags],
        total=len(flags)
    )


@router.get("/{module}/{feature}", response_model=FeatureFlagResponse)
async def get_feature_flag(
    module: str,
    feature: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """
    Get a specific feature flag.

    Admin only.
    """
    result = await db.execute(
        select(FeatureFlag)
        .where(FeatureFlag.module == module)
        .where(FeatureFlag.feature == feature)
    )
    flag = result.scalar_one_or_none()

    if not flag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "flag_not_found",
                "message": f"Feature flag '{module}:{feature}' not found"
            }
        )

    return FeatureFlagResponse(
        id=str(flag.id),
        module=flag.module,
        feature=flag.feature,
        is_enabled=flag.is_enabled,
        config_json=flag.config_json or {},
        disabled_reason=flag.disabled_reason,
        disabled_at=flag.disabled_at,
        disabled_by=flag.disabled_by,
        created_at=flag.created_at,
        updated_at=flag.updated_at
    )


@router.patch("/shipping/{carrier}", response_model=ToggleFeatureFlagResponse)
async def toggle_shipping_carrier(
    carrier: str,
    request: ToggleFeatureFlagRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """
    Toggle a shipping carrier on/off.

    Admin only. Requires reason when disabling.
    Invalidates feature flag cache after update.

    Audit trail:
    - Logs who changed what and when
    - Stores disabled_reason, disabled_by, disabled_at
    """
    carrier_lower = carrier.lower()

    result = await db.execute(
        select(FeatureFlag)
        .where(FeatureFlag.module == "shipping")
        .where(FeatureFlag.feature == carrier_lower)
    )
    flag = result.scalar_one_or_none()

    if not flag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "carrier_not_found",
                "message": f"Carrier '{carrier}' not found in feature flags"
            }
        )

    # Require reason when disabling
    if not request.is_enabled and not request.reason:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "reason_required",
                "message": "Reason is required when disabling a carrier"
            }
        )

    # Update flag
    old_state = flag.is_enabled
    if request.is_enabled:
        flag.enable()
        action = "enabled"
    else:
        flag.disable(
            reason=request.reason,
            disabled_by=current_user.email
        )
        action = "disabled"

    await db.commit()
    await db.refresh(flag)

    # Invalidate cache
    FeatureFlags.invalidate_cache()

    # Log the change
    logger.info(
        f"Feature flag toggled: shipping:{carrier_lower} {action} by {current_user.email}",
        extra={
            "module": "shipping",
            "feature": carrier_lower,
            "old_state": old_state,
            "new_state": request.is_enabled,
            "user_id": current_user.id,
            "user_email": current_user.email,
            "reason": request.reason
        }
    )

    return ToggleFeatureFlagResponse(
        success=True,
        flag=FeatureFlagResponse(
            id=str(flag.id),
            module=flag.module,
            feature=flag.feature,
            is_enabled=flag.is_enabled,
            config_json=flag.config_json or {},
            disabled_reason=flag.disabled_reason,
            disabled_at=flag.disabled_at,
            disabled_by=flag.disabled_by,
            created_at=flag.created_at,
            updated_at=flag.updated_at
        ),
        message=f"Carrier '{carrier}' has been {action}"
    )


@router.patch("/{module}/{feature}", response_model=ToggleFeatureFlagResponse)
async def toggle_feature_flag(
    module: str,
    feature: str,
    request: ToggleFeatureFlagRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """
    Toggle any feature flag on/off.

    Admin only. Requires reason when disabling.
    """
    result = await db.execute(
        select(FeatureFlag)
        .where(FeatureFlag.module == module)
        .where(FeatureFlag.feature == feature)
    )
    flag = result.scalar_one_or_none()

    if not flag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "flag_not_found",
                "message": f"Feature flag '{module}:{feature}' not found"
            }
        )

    # Require reason when disabling
    if not request.is_enabled and not request.reason:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "reason_required",
                "message": "Reason is required when disabling a feature"
            }
        )

    # Update flag
    old_state = flag.is_enabled
    if request.is_enabled:
        flag.enable()
        action = "enabled"
    else:
        flag.disable(
            reason=request.reason,
            disabled_by=current_user.email
        )
        action = "disabled"

    await db.commit()
    await db.refresh(flag)

    # Invalidate cache
    FeatureFlags.invalidate_cache()

    # Log the change
    logger.info(
        f"Feature flag toggled: {module}:{feature} {action} by {current_user.email}",
        extra={
            "module": module,
            "feature": feature,
            "old_state": old_state,
            "new_state": request.is_enabled,
            "user_id": current_user.id,
            "user_email": current_user.email,
            "reason": request.reason
        }
    )

    return ToggleFeatureFlagResponse(
        success=True,
        flag=FeatureFlagResponse(
            id=str(flag.id),
            module=flag.module,
            feature=flag.feature,
            is_enabled=flag.is_enabled,
            config_json=flag.config_json or {},
            disabled_reason=flag.disabled_reason,
            disabled_at=flag.disabled_at,
            disabled_by=flag.disabled_by,
            created_at=flag.created_at,
            updated_at=flag.updated_at
        ),
        message=f"Feature '{module}:{feature}' has been {action}"
    )


@router.put("/{module}/{feature}/config", response_model=FeatureFlagResponse)
async def update_feature_config(
    module: str,
    feature: str,
    request: UpdateConfigRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin)
):
    """
    Update feature flag configuration.

    Admin only. Updates the config_json field.
    """
    result = await db.execute(
        select(FeatureFlag)
        .where(FeatureFlag.module == module)
        .where(FeatureFlag.feature == feature)
    )
    flag = result.scalar_one_or_none()

    if not flag:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "flag_not_found",
                "message": f"Feature flag '{module}:{feature}' not found"
            }
        )

    old_config = flag.config_json
    flag.config_json = request.config_json

    await db.commit()
    await db.refresh(flag)

    # Invalidate cache
    FeatureFlags.invalidate_cache()

    # Log the change
    logger.info(
        f"Feature flag config updated: {module}:{feature} by {current_user.email}",
        extra={
            "module": module,
            "feature": feature,
            "old_config": old_config,
            "new_config": request.config_json,
            "user_id": current_user.id,
            "user_email": current_user.email
        }
    )

    return FeatureFlagResponse(
        id=str(flag.id),
        module=flag.module,
        feature=flag.feature,
        is_enabled=flag.is_enabled,
        config_json=flag.config_json or {},
        disabled_reason=flag.disabled_reason,
        disabled_at=flag.disabled_at,
        disabled_by=flag.disabled_by,
        created_at=flag.created_at,
        updated_at=flag.updated_at
    )
