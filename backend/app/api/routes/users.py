"""
User routes
"""
from fastapi import APIRouter, Depends, HTTPException, status, Header
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel

from app.core.database import get_db
from app.core.security import get_password_hash
from app.core.config import settings
from app.models.user import User
from app.schemas.user import UserResponse, UserUpdate
from app.api.deps import get_current_user

router = APIRouter()


class PromoteRequest(BaseModel):
    email: str


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
    x_admin_secret: str = Header(..., alias="X-Admin-Secret"),
    db: AsyncSession = Depends(get_db)
):
    """Promote a user to admin status. Requires admin secret key."""
    if x_admin_secret != settings.SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid admin secret"
        )

    result = await db.execute(
        select(User).where(User.email == request.email)
    )
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    user.is_admin = True
    await db.commit()
    await db.refresh(user)

    return user
