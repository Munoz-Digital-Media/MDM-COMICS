"""
Session Service

Per constitution_cyberSec.json §4: Session management with concurrent limits.
"""
import secrets
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

from sqlalchemy import select, and_, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user_session import UserSession
from app.models.user_audit_log import AuditAction
from app.core.pii import pii_handler
from app.core.config import settings


class SessionService:
    """
    Service for managing user sessions.

    Features:
    - Concurrent session limits
    - Session tracking with hashed PII
    - Idle and absolute timeout
    - Session revocation
    """

    # Session limits from config
    MAX_CONCURRENT = getattr(settings, 'SESSION_MAX_CONCURRENT', 5)
    IDLE_TIMEOUT_MINUTES = getattr(settings, 'SESSION_IDLE_TIMEOUT_MINUTES', 60)
    ABSOLUTE_TIMEOUT_HOURS = getattr(settings, 'SESSION_ABSOLUTE_TIMEOUT_HOURS', 24)

    def __init__(self, db: AsyncSession):
        self.db = db

    @staticmethod
    def generate_session_token() -> str:
        """Generate a secure session token."""
        return secrets.token_urlsafe(32)

    @staticmethod
    def hash_token(token: str) -> str:
        """Hash session token for storage."""
        return hashlib.sha256(token.encode()).hexdigest()

    async def create_session(
        self,
        user_id: int,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        device_info: Optional[str] = None,
    ) -> tuple[UserSession, str]:
        """
        Create a new session for a user.

        Enforces concurrent session limit by revoking oldest sessions.

        Args:
            user_id: User ID
            ip_address: Client IP (will be hashed)
            user_agent: Client user agent (will be hashed)
            device_info: Device description

        Returns:
            Tuple of (session object, raw session token)
        """
        # Check concurrent session limit
        await self._enforce_session_limit(user_id)

        # Generate session token (JWT ID)
        import uuid
        token_jti = str(uuid.uuid4())

        # Hash PII
        ip_hash = pii_handler.hash_ip(ip_address) if ip_address else None
        ua_hash = pii_handler.hash_user_agent(user_agent) if user_agent else None

        # Calculate expiry
        expires_at = datetime.now(timezone.utc) + timedelta(hours=self.ABSOLUTE_TIMEOUT_HOURS)

        session = UserSession(
            user_id=user_id,
            token_jti=token_jti,
            ip_address_hash=ip_hash,
            user_agent_hash=ua_hash,
            device_type=device_info,
            expires_at=expires_at,
        )

        self.db.add(session)
        await self.db.flush()

        return session, token_jti

    async def _enforce_session_limit(self, user_id: int) -> int:
        """
        Enforce concurrent session limit.

        Revokes oldest sessions if limit exceeded.

        Args:
            user_id: User ID

        Returns:
            Number of sessions revoked
        """
        # Count active sessions (not revoked, not expired)
        result = await self.db.execute(
            select(func.count(UserSession.id))
            .where(
                and_(
                    UserSession.user_id == user_id,
                    UserSession.revoked_at.is_(None),
                    UserSession.expires_at > datetime.now(timezone.utc),
                )
            )
        )
        active_count = result.scalar() or 0

        if active_count < self.MAX_CONCURRENT:
            return 0

        # Revoke oldest sessions to make room
        sessions_to_revoke = active_count - self.MAX_CONCURRENT + 1

        result = await self.db.execute(
            select(UserSession.id)
            .where(
                and_(
                    UserSession.user_id == user_id,
                    UserSession.is_active == True,
                )
            )
            .order_by(UserSession.created_at.asc())
            .limit(sessions_to_revoke)
        )
        old_session_ids = [row[0] for row in result.fetchall()]

        if old_session_ids:
            await self.db.execute(
                update(UserSession)
                .where(UserSession.id.in_(old_session_ids))
                .values(
                    is_active=False,
                    revoked_at=datetime.now(timezone.utc),
                    revoke_reason="concurrent_limit",
                )
            )

        return len(old_session_ids)

    async def validate_session(self, token: str) -> Optional[UserSession]:
        """
        Validate a session token and update activity.

        Args:
            token: Raw session token

        Returns:
            Session if valid, None otherwise
        """
        token_hash = self.hash_token(token)

        result = await self.db.execute(
            select(UserSession)
            .where(
                and_(
                    UserSession.session_token_hash == token_hash,
                    UserSession.is_active == True,
                    UserSession.expires_at > datetime.now(timezone.utc),
                )
            )
        )
        session = result.scalar_one_or_none()

        if not session:
            return None

        # Check idle timeout
        idle_cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.IDLE_TIMEOUT_MINUTES)
        if session.last_activity_at < idle_cutoff:
            # Session expired due to idle
            session.is_active = False
            session.revoked_at = datetime.now(timezone.utc)
            session.revoke_reason = "idle_timeout"
            await self.db.flush()
            return None

        # Update last activity
        session.last_activity_at = datetime.now(timezone.utc)
        await self.db.flush()

        return session

    async def revoke_session(
        self,
        session_id: int,
        revoked_by_id: Optional[int] = None,
        reason: str = "user_logout",
    ) -> bool:
        """
        Revoke a specific session.

        Args:
            session_id: Session ID to revoke
            revoked_by_id: ID of user revoking (admin or self)
            reason: Reason for revocation

        Returns:
            True if session was revoked
        """
        result = await self.db.execute(
            select(UserSession)
            .where(UserSession.id == session_id)
        )
        session = result.scalar_one_or_none()

        if not session:
            return False

        session.is_active = False
        session.revoked_at = datetime.now(timezone.utc)
        session.revoked_by_id = revoked_by_id
        session.revoke_reason = reason

        await self.db.flush()
        return True

    async def revoke_all_sessions(
        self,
        user_id: int,
        except_session_id: Optional[int] = None,
        revoked_by_id: Optional[int] = None,
        reason: str = "revoke_all",
    ) -> int:
        """
        Revoke all sessions for a user.

        Args:
            user_id: User whose sessions to revoke
            except_session_id: Session to keep active (current session)
            revoked_by_id: ID of user revoking
            reason: Reason for revocation

        Returns:
            Number of sessions revoked
        """
        conditions = [
            UserSession.user_id == user_id,
            UserSession.is_active == True,
        ]

        if except_session_id:
            conditions.append(UserSession.id != except_session_id)

        result = await self.db.execute(
            select(UserSession)
            .where(and_(*conditions))
        )
        sessions = result.scalars().all()

        for session in sessions:
            session.is_active = False
            session.revoked_at = datetime.now(timezone.utc)
            session.revoked_by_id = revoked_by_id
            session.revoke_reason = reason

        await self.db.flush()
        return len(sessions)

    async def get_user_sessions(
        self,
        user_id: int,
        active_only: bool = True,
        limit: int = 20,
    ) -> List[UserSession]:
        """
        Get sessions for a user.

        Args:
            user_id: User ID
            active_only: Only return active sessions
            limit: Max sessions to return

        Returns:
            List of sessions
        """
        conditions = [UserSession.user_id == user_id]

        if active_only:
            conditions.extend([
                UserSession.is_active == True,
                UserSession.expires_at > datetime.now(timezone.utc),
            ])

        result = await self.db.execute(
            select(UserSession)
            .where(and_(*conditions))
            .order_by(UserSession.last_activity_at.desc())
            .limit(limit)
        )

        return list(result.scalars().all())

    async def get_active_session_count(self, user_id: int) -> int:
        """Get count of active sessions for a user."""
        result = await self.db.execute(
            select(func.count(UserSession.id))
            .where(
                and_(
                    UserSession.user_id == user_id,
                    UserSession.is_active == True,
                    UserSession.expires_at > datetime.now(timezone.utc),
                )
            )
        )
        return result.scalar() or 0

    async def cleanup_expired_sessions(self) -> int:
        """
        Mark expired sessions as inactive.

        Should be run periodically via scheduler.

        Returns:
            Number of sessions cleaned up
        """
        result = await self.db.execute(
            select(UserSession)
            .where(
                and_(
                    UserSession.is_active == True,
                    UserSession.expires_at <= datetime.now(timezone.utc),
                )
            )
        )
        expired = result.scalars().all()

        for session in expired:
            session.is_active = False
            session.revoked_at = datetime.now(timezone.utc)
            session.revoke_reason = "expired"

        await self.db.flush()
        return len(expired)

    async def get_session_by_id(self, session_id: int) -> Optional[UserSession]:
        """Get a session by ID."""
        result = await self.db.execute(
            select(UserSession)
            .where(UserSession.id == session_id)
        )
        return result.scalar_one_or_none()

    def format_session_for_display(self, session: UserSession, current_session_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Format session for API response.

        Args:
            session: Session object
            current_session_id: ID of current session (for marking)

        Returns:
            Dict safe for API response
        """
        return {
            "id": session.id,
            "device_info": session.device_info or "Unknown device",
            "is_active": session.is_active,
            "is_current": session.id == current_session_id if current_session_id else False,
            "created_at": session.created_at.isoformat(),
            "last_activity_at": session.last_activity_at.isoformat(),
            "expires_at": session.expires_at.isoformat(),
            # Note: IP and user agent hashes not exposed
        }
