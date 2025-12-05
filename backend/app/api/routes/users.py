"""
User routes

P0-2: Admin promotion now uses separate ADMIN_PROMOTION_SECRET
      to prevent JWT secret leakage from enabling admin escalation.
"""
import hashlib
import logging
from fastapi import APIRouter, Depends, HTTPException, status, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel

from app.core.database import get_db
from app.core.security import get_password_hash
from app.core.config import settings
from app.models.user import User
from app.schemas.user import UserResponse, UserUpdate
from app.api.deps import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


class PromoteRequest(BaseModel):
    email: str


def _hash_for_log(value: str) -> str:
    """Create a truncated hash for audit logging without exposing PII."""
    return hashlib.sha256(value.encode()).hexdigest()[:12]


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(user: User = Depends(get_current_user)):
    """Get current user profile"""
    return user


@router.patch("/me", response_model=UserResponse)
async def update_current_user(
    update_data: UserUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Update current user profile"""
    if update_data.email is not None:
        user.email = update_data.email
    if update_data.name is not None:
        user.name = update_data.name
    if update_data.password is not None:
        user.hashed_password = get_password_hash(update_data.password)

    await db.commit()
    await db.refresh(user)

    return user


@router.post("/promote-admin", response_model=UserResponse)
async def promote_to_admin(
    request: PromoteRequest,
    req: Request,
    x_admin_secret: str = Header(..., alias="X-Admin-Secret"),
    db: AsyncSession = Depends(get_db)
):
    """
    Promote a user to admin status.

    P0-2 SECURITY FIX: Now requires separate ADMIN_PROMOTION_SECRET instead
    of reusing JWT SECRET_KEY. This prevents admin escalation if JWT secret
    is compromised. Endpoint is disabled if ADMIN_PROMOTION_SECRET is not set.
    """
    client_ip = req.client.host if req.client else "unknown"

    # P0-2: Check if admin promotion is configured
    if not settings.ADMIN_PROMOTION_SECRET:
        logger.warning(
            f"Admin promotion attempt blocked: ADMIN_PROMOTION_SECRET not configured. "
            f"IP={client_ip}, target_email_hash={_hash_for_log(request.email)}"
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin promotion is not configured on this server"
        )

    # P0-2: Use dedicated admin secret, not JWT secret
    if x_admin_secret != settings.ADMIN_PROMOTION_SECRET:
        logger.warning(
            f"Admin promotion attempt with invalid secret. "
            f"IP={client_ip}, target_email_hash={_hash_for_log(request.email)}"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid admin secret"
        )

    result = await db.execute(
        select(User).where(User.email == request.email)
    )
    user = result.scalar_one_or_none()

    if not user:
        logger.info(
            f"Admin promotion failed: user not found. "
            f"IP={client_ip}, target_email_hash={_hash_for_log(request.email)}"
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    user.is_admin = True
    await db.commit()
    await db.refresh(user)

    # Audit log successful promotion (PII-safe)
    logger.info(
        f"Admin promotion SUCCESS. "
        f"IP={client_ip}, user_id={user.id}, email_hash={_hash_for_log(request.email)}"
    )

    return user
