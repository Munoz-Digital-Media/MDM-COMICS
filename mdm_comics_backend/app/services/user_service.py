"""
User Service

Per constitution.json A5: User lifecycle management with soft-delete.
Per constitution_cyberSec.json: Account security management.
"""
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Tuple

from sqlalchemy import select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from passlib.context import CryptContext

from app.models.user import User
from app.models.password_reset import PasswordResetToken
from app.models.email_verification import EmailVerificationToken
from app.core.password_policy import PasswordPolicy
from app.core.account_lockout import AccountLockoutPolicy
from app.core.pii import pii_handler
from app.core.config import settings


# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class UserService:
    """
    Service for user management operations.

    Features:
    - User CRUD with soft-delete
    - Password management with policy
    - Account lockout handling
    - Email verification
    - Password reset tokens
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    # ============================================================
    # User CRUD
    # ============================================================

    async def get_user_by_id(
        self,
        user_id: int,
        include_deleted: bool = False,
    ) -> Optional[User]:
        """Get user by ID."""
        conditions = [User.id == user_id]

        if not include_deleted:
            conditions.append(User.deleted_at.is_(None))

        result = await self.db.execute(
            select(User).where(and_(*conditions))
        )
        return result.scalar_one_or_none()

    async def get_user_by_email(
        self,
        email: str,
        include_deleted: bool = False,
    ) -> Optional[User]:
        """Get user by email."""
        conditions = [func.lower(User.email) == email.lower()]

        if not include_deleted:
            conditions.append(User.deleted_at.is_(None))

        result = await self.db.execute(
            select(User).where(and_(*conditions))
        )
        return result.scalar_one_or_none()

    async def list_users(
        self,
        search: Optional[str] = None,
        is_active: Optional[bool] = None,
        is_admin: Optional[bool] = None,
        include_deleted: bool = False,
        limit: int = 50,
        offset: int = 0,
        order_by: str = "created_at",
        order_desc: bool = True,
    ) -> Tuple[List[User], int]:
        """
        List users with filtering and pagination.

        Returns:
            Tuple of (users, total_count)
        """
        conditions = []

        if not include_deleted:
            conditions.append(User.deleted_at.is_(None))

        if search:
            search_pattern = f"%{search.lower()}%"
            conditions.append(
                or_(
                    func.lower(User.email).like(search_pattern),
                    func.lower(User.name).like(search_pattern),
                )
            )

        if is_active is not None:
            conditions.append(User.is_active == is_active)

        if is_admin is not None:
            conditions.append(User.is_admin == is_admin)

        # Count query
        count_result = await self.db.execute(
            select(func.count(User.id)).where(and_(*conditions))
        )
        total = count_result.scalar() or 0

        # Main query
        query = select(User).where(and_(*conditions))

        # Ordering
        order_col = getattr(User, order_by, User.created_at)
        if order_desc:
            query = query.order_by(order_col.desc())
        else:
            query = query.order_by(order_col.asc())

        query = query.limit(limit).offset(offset)

        result = await self.db.execute(query)
        users = list(result.scalars().all())

        return users, total

    async def create_user(
        self,
        email: str,
        password: str,
        name: str,
        is_admin: bool = False,
        skip_password_validation: bool = False,
    ) -> User:
        """
        Create a new user.

        Args:
            email: User email
            password: Plain text password
            name: User name
            is_admin: Admin flag
            skip_password_validation: Skip policy (for seeding)

        Returns:
            Created user

        Raises:
            ValueError: If email exists or password invalid
        """
        # Check email uniqueness
        existing = await self.get_user_by_email(email, include_deleted=True)
        if existing:
            raise ValueError("Email already registered")

        # Validate password
        if not skip_password_validation:
            is_valid, errors = PasswordPolicy.validate(password, email=email, name=name)
            if not is_valid:
                raise ValueError("; ".join(errors))

        # Hash password
        hashed_password = pwd_context.hash(password)

        user = User(
            email=email.lower(),
            hashed_password=hashed_password,
            name=name,
            is_active=True,
            is_admin=is_admin,
            password_changed_at=datetime.now(timezone.utc),
        )

        self.db.add(user)
        await self.db.flush()
        return user

    async def update_user(
        self,
        user_id: int,
        name: Optional[str] = None,
        email: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[User]:
        """
        Update user profile.

        Args:
            user_id: User ID
            name: New name
            email: New email (requires re-verification)
            is_active: Active status

        Returns:
            Updated user or None
        """
        user = await self.get_user_by_id(user_id)
        if not user:
            return None

        if name is not None:
            user.name = name

        if email is not None and email.lower() != user.email.lower():
            # Check email uniqueness
            existing = await self.get_user_by_email(email)
            if existing:
                raise ValueError("Email already registered")
            user.email = email.lower()
            user.email_verified_at = None  # Require re-verification

        if is_active is not None:
            user.is_active = is_active

        user.updated_at = datetime.now(timezone.utc)
        await self.db.flush()
        return user

    async def soft_delete_user(self, user_id: int) -> bool:
        """
        Soft-delete a user.

        Args:
            user_id: User ID

        Returns:
            True if deleted
        """
        user = await self.get_user_by_id(user_id)
        if not user:
            return False

        user.soft_delete()
        await self.db.flush()
        return True

    async def restore_user(self, user_id: int) -> bool:
        """
        Restore a soft-deleted user.

        Args:
            user_id: User ID

        Returns:
            True if restored
        """
        user = await self.get_user_by_id(user_id, include_deleted=True)
        if not user or not user.is_deleted:
            return False

        user.restore()
        await self.db.flush()
        return True

    async def hard_delete_user(self, user_id: int) -> bool:
        """
        Permanently delete a user (GDPR right to erasure).

        Args:
            user_id: User ID

        Returns:
            True if deleted
        """
        user = await self.get_user_by_id(user_id, include_deleted=True)
        if not user:
            return False

        await self.db.delete(user)
        await self.db.flush()
        return True

    # ============================================================
    # Password Management
    # ============================================================

    async def change_password(
        self,
        user_id: int,
        current_password: str,
        new_password: str,
    ) -> bool:
        """
        Change user password.

        Args:
            user_id: User ID
            current_password: Current password for verification
            new_password: New password

        Returns:
            True if changed

        Raises:
            ValueError: If validation fails
        """
        user = await self.get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")

        # Verify current password
        if not pwd_context.verify(current_password, user.hashed_password):
            raise ValueError("Current password is incorrect")

        # Validate new password
        is_valid, errors = PasswordPolicy.validate(
            new_password,
            email=user.email,
            name=user.name,
        )
        if not is_valid:
            raise ValueError("; ".join(errors))

        # Update password
        user.hashed_password = pwd_context.hash(new_password)
        user.record_password_change()
        await self.db.flush()
        return True

    async def set_password(
        self,
        user_id: int,
        new_password: str,
        skip_validation: bool = False,
    ) -> bool:
        """
        Set user password (admin operation).

        Args:
            user_id: User ID
            new_password: New password
            skip_validation: Skip policy check

        Returns:
            True if set
        """
        user = await self.get_user_by_id(user_id)
        if not user:
            return False

        if not skip_validation:
            is_valid, errors = PasswordPolicy.validate(
                new_password,
                email=user.email,
                name=user.name,
            )
            if not is_valid:
                raise ValueError("; ".join(errors))

        user.hashed_password = pwd_context.hash(new_password)
        user.record_password_change()
        await self.db.flush()
        return True

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against a hash."""
        return pwd_context.verify(plain_password, hashed_password)

    # ============================================================
    # Password Reset
    # ============================================================

    async def create_password_reset_token(
        self,
        email: str,
        ip_address: Optional[str] = None,
    ) -> Optional[str]:
        """
        Create a password reset token.

        Args:
            email: User email
            ip_address: Requester IP

        Returns:
            Raw token or None if user not found
        """
        user = await self.get_user_by_email(email)
        if not user:
            return None

        # Generate token
        raw_token = pii_handler.generate_token(32)
        token_hash = pii_handler.hash_token(raw_token)

        # Expire old tokens
        await self.db.execute(
            select(PasswordResetToken)
            .where(
                and_(
                    PasswordResetToken.user_id == user.id,
                    PasswordResetToken.used_at.is_(None),
                )
            )
        )

        # Create token
        expires_minutes = getattr(settings, 'PASSWORD_RESET_TOKEN_MINUTES', 30)
        token = PasswordResetToken(
            user_id=user.id,
            token_hash=token_hash,
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=expires_minutes),
            ip_requested=pii_handler.hash_ip(ip_address) if ip_address else None,
        )

        self.db.add(token)
        await self.db.flush()
        return raw_token

    async def reset_password_with_token(
        self,
        token: str,
        new_password: str,
        ip_address: Optional[str] = None,
    ) -> bool:
        """
        Reset password using a token.

        Args:
            token: Raw reset token
            new_password: New password
            ip_address: User IP

        Returns:
            True if reset successful

        Raises:
            ValueError: If token invalid or password policy fails
        """
        token_hash = pii_handler.hash_token(token)

        result = await self.db.execute(
            select(PasswordResetToken)
            .options(selectinload(PasswordResetToken.user))
            .where(
                and_(
                    PasswordResetToken.token_hash == token_hash,
                    PasswordResetToken.used_at.is_(None),
                    PasswordResetToken.expires_at > datetime.now(timezone.utc),
                )
            )
        )
        reset_token = result.scalar_one_or_none()

        if not reset_token:
            raise ValueError("Invalid or expired reset token")

        user = reset_token.user
        if not user:
            raise ValueError("User not found")

        # Validate new password
        is_valid, errors = PasswordPolicy.validate(
            new_password,
            email=user.email,
            name=user.name,
        )
        if not is_valid:
            raise ValueError("; ".join(errors))

        # Update password
        user.hashed_password = pwd_context.hash(new_password)
        user.record_password_change()

        # Mark token as used
        reset_token.used_at = datetime.now(timezone.utc)
        reset_token.ip_used = pii_handler.hash_ip(ip_address) if ip_address else None

        await self.db.flush()
        return True

    # ============================================================
    # Email Verification
    # ============================================================

    async def create_email_verification_token(
        self,
        user_id: int,
        email: Optional[str] = None,
    ) -> Optional[str]:
        """
        Create an email verification token.

        Args:
            user_id: User ID
            email: Email to verify (defaults to user's email)

        Returns:
            Raw token
        """
        user = await self.get_user_by_id(user_id)
        if not user:
            return None

        target_email = email or user.email

        # Generate token
        raw_token = pii_handler.generate_token(32)
        token_hash = pii_handler.hash_token(raw_token)

        # Create token
        expires_hours = getattr(settings, 'EMAIL_VERIFICATION_TOKEN_HOURS', 24)
        token = EmailVerificationToken(
            user_id=user.id,
            email=target_email,
            token_hash=token_hash,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=expires_hours),
        )

        self.db.add(token)
        await self.db.flush()
        return raw_token

    async def verify_email_with_token(self, token: str) -> bool:
        """
        Verify email using a token.

        Args:
            token: Raw verification token

        Returns:
            True if verified

        Raises:
            ValueError: If token invalid
        """
        token_hash = pii_handler.hash_token(token)

        result = await self.db.execute(
            select(EmailVerificationToken)
            .options(selectinload(EmailVerificationToken.user))
            .where(
                and_(
                    EmailVerificationToken.token_hash == token_hash,
                    EmailVerificationToken.verified_at.is_(None),
                    EmailVerificationToken.expires_at > datetime.now(timezone.utc),
                )
            )
        )
        verify_token = result.scalar_one_or_none()

        if not verify_token:
            raise ValueError("Invalid or expired verification token")

        user = verify_token.user
        if not user:
            raise ValueError("User not found")

        # Update email if different
        if verify_token.email.lower() != user.email.lower():
            # Check email uniqueness
            existing = await self.get_user_by_email(verify_token.email)
            if existing and existing.id != user.id:
                raise ValueError("Email already registered")
            user.email = verify_token.email.lower()

        # Mark as verified
        user.verify_email()
        verify_token.verified_at = datetime.now(timezone.utc)

        await self.db.flush()
        return True

    # ============================================================
    # Account Lockout
    # ============================================================

    async def record_failed_login(
        self,
        user_id: int,
        ip_address: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record a failed login attempt."""
        user = await self.get_user_by_id(user_id)
        if not user:
            return {"locked": False, "attempts": 0, "remaining_attempts": 5}

        return await AccountLockoutPolicy.record_failed_attempt(
            self.db, user, ip_address
        )

    async def record_successful_login(
        self,
        user_id: int,
        ip_address: Optional[str] = None,
    ) -> None:
        """Record a successful login."""
        user = await self.get_user_by_id(user_id)
        if not user:
            return

        await AccountLockoutPolicy.record_successful_login(self.db, user)

        # Record IP hash
        if ip_address:
            user.last_login_ip_hash = pii_handler.hash_ip(ip_address)

    async def is_account_locked(self, user_id: int) -> bool:
        """Check if account is locked."""
        user = await self.get_user_by_id(user_id)
        if not user:
            return False

        return AccountLockoutPolicy.is_locked(user)

    async def unlock_account(
        self,
        user_id: int,
        by_admin: bool = False,
    ) -> bool:
        """Unlock a user account."""
        user = await self.get_user_by_id(user_id)
        if not user:
            return False

        await AccountLockoutPolicy.unlock_account(self.db, user, by_admin)
        return True

    # ============================================================
    # User Stats
    # ============================================================

    async def get_user_stats(self) -> Dict[str, int]:
        """Get user statistics."""
        total = await self.db.execute(
            select(func.count(User.id))
            .where(User.deleted_at.is_(None))
        )

        active = await self.db.execute(
            select(func.count(User.id))
            .where(
                and_(
                    User.deleted_at.is_(None),
                    User.is_active == True,
                )
            )
        )

        admins = await self.db.execute(
            select(func.count(User.id))
            .where(
                and_(
                    User.deleted_at.is_(None),
                    User.is_admin == True,
                )
            )
        )

        verified = await self.db.execute(
            select(func.count(User.id))
            .where(
                and_(
                    User.deleted_at.is_(None),
                    User.email_verified_at.isnot(None),
                )
            )
        )

        locked = await self.db.execute(
            select(func.count(User.id))
            .where(
                and_(
                    User.deleted_at.is_(None),
                    User.locked_until > datetime.now(timezone.utc),
                )
            )
        )

        return {
            "total": total.scalar() or 0,
            "active": active.scalar() or 0,
            "admins": admins.scalar() or 0,
            "verified": verified.scalar() or 0,
            "locked": locked.scalar() or 0,
        }

    def format_user_for_display(
        self,
        user: User,
        include_pii: bool = False,
    ) -> Dict[str, Any]:
        """
        Format user for API response.

        Args:
            user: User object
            include_pii: Include email (admin only)

        Returns:
            Dict safe for API response
        """
        data = {
            "id": user.id,
            "name": user.name,
            "is_active": user.is_active,
            "is_admin": user.is_admin,
            "is_email_verified": user.is_email_verified,
            "is_locked": user.is_locked,
            "created_at": user.created_at.isoformat() if user.created_at else None,
            "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
        }

        if include_pii:
            data["email"] = user.email
            data["email_masked"] = pii_handler.mask_email(user.email)

        return data
