"""
Account Lockout policy

Per constitution_cyberSec.json §8: Brute force protection
"""
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings


class AccountLockoutPolicy:
    """
    Manages account lockout for brute force protection.

    Features:
    - Progressive lockout (doubles on each subsequent lockout)
    - Automatic unlock after duration
    - Admin manual unlock capability
    """

    # Max failed attempts before lockout
    MAX_FAILED_ATTEMPTS = 5

    # Base lockout duration (minutes)
    LOCKOUT_DURATION_MINUTES = 15

    # Enable progressive lockout (doubles each time)
    PROGRESSIVE_LOCKOUT = True

    # Maximum lockout duration (minutes) for progressive lockout
    MAX_LOCKOUT_MINUTES = 24 * 60  # 24 hours

    @classmethod
    async def record_failed_attempt(
        cls,
        db: AsyncSession,
        user,
        ip_address: Optional[str] = None
    ) -> Dict:
        """
        Record a failed login attempt.

        Args:
            db: Database session
            user: User model instance
            ip_address: Optional IP for logging

        Returns:
            Dict with lockout status:
            {
                "locked": bool,
                "locked_until": datetime or None,
                "attempts": int,
                "remaining_attempts": int
            }
        """
        # Increment failed attempts
        user.failed_login_attempts = (user.failed_login_attempts or 0) + 1
        attempts = user.failed_login_attempts
        remaining = max(0, cls.MAX_FAILED_ATTEMPTS - attempts)

        result = {
            "locked": False,
            "locked_until": None,
            "attempts": attempts,
            "remaining_attempts": remaining
        }

        # Check if we should lock
        if attempts >= cls.MAX_FAILED_ATTEMPTS:
            lockout_minutes = cls.LOCKOUT_DURATION_MINUTES

            if cls.PROGRESSIVE_LOCKOUT:
                # Calculate progressive lockout
                lockout_count = getattr(user, 'lockout_count', 0) or 0
                lockout_minutes = min(
                    cls.LOCKOUT_DURATION_MINUTES * (2 ** lockout_count),
                    cls.MAX_LOCKOUT_MINUTES
                )

            user.locked_until = datetime.now(timezone.utc) + timedelta(minutes=lockout_minutes)
            user.lockout_count = (getattr(user, 'lockout_count', 0) or 0) + 1

            result["locked"] = True
            result["locked_until"] = user.locked_until

        await db.commit()
        return result

    @classmethod
    async def record_successful_login(cls, db: AsyncSession, user) -> None:
        """
        Reset failed attempts on successful login.

        Args:
            db: Database session
            user: User model instance
        """
        user.failed_login_attempts = 0
        # Don't reset lockout_count - keeps progressive lockout history
        # Reset locked_until in case it was set
        user.locked_until = None
        user.last_login_at = datetime.now(timezone.utc)
        await db.commit()

    @classmethod
    def is_locked(cls, user) -> bool:
        """
        Check if account is currently locked.

        Args:
            user: User model instance

        Returns:
            True if account is locked
        """
        if not user.locked_until:
            return False

        # Check if lock has expired
        if datetime.now(timezone.utc) > user.locked_until:
            return False

        return True

    @classmethod
    def get_lockout_remaining(cls, user) -> Optional[int]:
        """
        Get remaining lockout time in seconds.

        Args:
            user: User model instance

        Returns:
            Seconds remaining, or None if not locked
        """
        if not cls.is_locked(user):
            return None

        remaining = user.locked_until - datetime.now(timezone.utc)
        return max(0, int(remaining.total_seconds()))

    @classmethod
    async def unlock_account(cls, db: AsyncSession, user, by_admin: bool = False) -> None:
        """
        Manually unlock an account.

        Args:
            db: Database session
            user: User model instance
            by_admin: Whether this is an admin unlock (resets lockout_count)
        """
        user.failed_login_attempts = 0
        user.locked_until = None

        if by_admin:
            # Admin unlock resets progressive lockout
            user.lockout_count = 0

        await db.commit()

    @classmethod
    def get_lockout_message(cls, user) -> str:
        """
        Get user-friendly lockout message.

        Args:
            user: User model instance

        Returns:
            Message describing lockout status
        """
        if not cls.is_locked(user):
            return ""

        remaining = cls.get_lockout_remaining(user)
        if remaining is None:
            return ""

        if remaining >= 3600:
            hours = remaining // 3600
            return f"Account temporarily locked. Try again in {hours} hour{'s' if hours > 1 else ''}."
        elif remaining >= 60:
            minutes = remaining // 60
            return f"Account temporarily locked. Try again in {minutes} minute{'s' if minutes > 1 else ''}."
        else:
            return f"Account temporarily locked. Try again in {remaining} second{'s' if remaining > 1 else ''}."

    @classmethod
    def get_attempts_message(cls, attempts: int) -> str:
        """
        Get warning message about remaining attempts.

        Args:
            attempts: Number of failed attempts

        Returns:
            Warning message
        """
        remaining = max(0, cls.MAX_FAILED_ATTEMPTS - attempts)

        if remaining == 0:
            return "Account locked due to too many failed attempts."
        elif remaining <= 2:
            return f"Warning: {remaining} attempt{'s' if remaining > 1 else ''} remaining before account lockout."
        else:
            return ""
