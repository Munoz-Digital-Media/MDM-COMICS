"""
User routes
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_password_hash
from app.models.user import User
from app.schemas.user import UserResponse, UserUpdate
from app.api.deps import get_current_user

router = APIRouter()


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
