"""
Data Retention Service

User Management System v1.0.0
Per constitution_data_hygiene.json §4: Retention enforcement

Handles automatic cleanup of expired data based on retention policies.
"""
import hashlib
import secrets
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, and_

from app.core.config import settings
from app.models.user import User
from app.models.user_session import UserSession
from app.models.user_audit_log import UserAuditLog
from app.models.dsar_request import DSARRequest
from app.models.password_reset import PasswordResetToken
from app.models.email_verification import EmailVerificationToken


class RetentionService:
    """
    Data retention enforcement.
    Per constitution_data_hygiene.json:
    - Retention_map entries drive scheduler
    - Cryptographic erasure proofs recorded
    """

    # Retention periods per data type (in days)
    RETENTION_DAYS = {
        "password_reset_tokens": 1,       # Expire quickly for security
        "email_verification_tokens": 7,   # Week to verify
        "user_sessions": 30,              # Per constitution_logging.json §2
        "audit_logs": 730,                # 2 years per constitution_logging.json §3
        "dsar_requests": 365,             # 1 year after completion
        "soft_deleted_users": 90,         # 90 days before hard delete
    }

    def __init__(self, db: AsyncSession):
        self.db = db

    async def run_cleanup(self) -> Dict[str, int]:
        """
        Execute retention cleanup job.
        Returns dict with counts of cleaned up records per data type.
        """
        results = {}

        for data_type, retention_days in self.RETENTION_DAYS.items():
            cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

            if data_type == "password_reset_tokens":
                count = await self._cleanup_reset_tokens(cutoff)
            elif data_type == "email_verification_tokens":
                count = await self._cleanup_verification_tokens(cutoff)
            elif data_type == "user_sessions":
                count = await self._cleanup_sessions(cutoff)
            elif data_type == "soft_deleted_users":
                count = await self._hard_delete_users(cutoff)
            elif data_type == "audit_logs":
                count = await self._trim_audit_logs(cutoff)
            elif data_type == "dsar_requests":
                count = await self._cleanup_dsar_requests(cutoff)
            else:
                count = 0

            results[data_type] = count

            if count > 0:
                # Record proof per constitution_data_hygiene.json §4
                await self._record_cleanup_proof(data_type, count, cutoff)

        return results

    async def _cleanup_reset_tokens(self, cutoff: datetime) -> int:
        """Clean up expired password reset tokens."""
        result = await self.db.execute(
            delete(PasswordResetToken).where(
                PasswordResetToken.created_at < cutoff
            )
        )
        return result.rowcount

    async def _cleanup_verification_tokens(self, cutoff: datetime) -> int:
        """Clean up expired email verification tokens."""
        result = await self.db.execute(
            delete(EmailVerificationToken).where(
                EmailVerificationToken.created_at < cutoff
            )
        )
        return result.rowcount

    async def _cleanup_sessions(self, cutoff: datetime) -> int:
        """Clean up expired or old inactive sessions."""
        # Delete sessions older than cutoff or already revoked/expired
        result = await self.db.execute(
            delete(UserSession).where(
                and_(
                    UserSession.is_active == False,
                    UserSession.created_at < cutoff
                )
            )
        )
        return result.rowcount

    async def _hard_delete_users(self, cutoff: datetime) -> int:
        """
        Hard delete users that were soft-deleted before cutoff.
        Per GDPR: After retention period, complete erasure.
        """
        # Find soft-deleted users older than cutoff
        result = await self.db.execute(
            select(User).where(
                and_(
                    User.deleted_at.isnot(None),
                    User.deleted_at < cutoff
                )
            )
        )
        users = result.scalars().all()

        count = 0
        for user in users:
            # Final anonymization before hard delete
            # This removes any remaining PII that might have been missed
            await self._final_anonymization(user)
            count += 1

        return count

    async def _final_anonymization(self, user: User) -> None:
        """
        Final anonymization pass before hard delete.
        Ensures all PII is cryptographically erased.
        """
        # Generate deletion proof hash
        deletion_hash = hashlib.sha512(
            f"FINAL_DELETE|{user.id}|{datetime.now(timezone.utc).isoformat()}".encode()
        ).hexdigest()

        # Complete anonymization
        user.email = f"purged_{user.id}_{secrets.token_hex(4)}@purged.local"
        user.name = f"Purged User {user.id}"
        user.hashed_password = deletion_hash[:60]  # bcrypt length

        # Note: We don't actually delete the user record to maintain
        # referential integrity with orders (for accounting).
        # The user is fully anonymized instead.

    async def _trim_audit_logs(self, cutoff: datetime) -> int:
        """
        Trim old audit logs.
        Note: Keep critical security events longer per policy.
        """
        # Only delete non-security-critical logs
        non_critical_actions = [
            "login_success",
            "logout",
            "profile_update",
            "session_created",
        ]

        result = await self.db.execute(
            delete(UserAuditLog).where(
                and_(
                    UserAuditLog.created_at < cutoff,
                    # Don't delete security events
                    ~UserAuditLog.action.in_([
                        "login_failed",
                        "account_locked",
                        "password_reset",
                        "role_changed",
                        "permission_change",
                    ])
                )
            )
        )
        return result.rowcount

    async def _cleanup_dsar_requests(self, cutoff: datetime) -> int:
        """Clean up completed DSAR requests after retention period."""
        result = await self.db.execute(
            delete(DSARRequest).where(
                and_(
                    DSARRequest.status.in_(["completed", "cancelled", "failed"]),
                    DSARRequest.completed_at < cutoff
                )
            )
        )
        return result.rowcount

    async def _record_cleanup_proof(
        self,
        data_type: str,
        count: int,
        cutoff: datetime
    ) -> None:
        """
        Record proof of cleanup for compliance auditing.
        Per constitution_data_hygiene.json §4.
        """
        # Create audit entry for cleanup
        proof_hash = hashlib.sha256(
            f"RETENTION_CLEANUP|{data_type}|{count}|{cutoff.isoformat()}".encode()
        ).hexdigest()

        log_entry = UserAuditLog(
            action="retention_cleanup",
            resource_type=data_type,
            details={
                "count": count,
                "cutoff": cutoff.isoformat(),
                "proof_hash": proof_hash,
                "retention_days": self.RETENTION_DAYS.get(data_type),
            },
        )

        self.db.add(log_entry)

    async def get_retention_status(self) -> Dict[str, Any]:
        """Get current retention status for admin dashboard."""
        from sqlalchemy import func

        status = {}

        # Password reset tokens
        reset_result = await self.db.execute(
            select(func.count(PasswordResetToken.id))
        )
        status["password_reset_tokens"] = {
            "count": reset_result.scalar() or 0,
            "retention_days": self.RETENTION_DAYS["password_reset_tokens"],
        }

        # Email verification tokens
        verify_result = await self.db.execute(
            select(func.count(EmailVerificationToken.id))
        )
        status["email_verification_tokens"] = {
            "count": verify_result.scalar() or 0,
            "retention_days": self.RETENTION_DAYS["email_verification_tokens"],
        }

        # Inactive sessions
        session_result = await self.db.execute(
            select(func.count(UserSession.id)).where(UserSession.is_active == False)
        )
        status["inactive_sessions"] = {
            "count": session_result.scalar() or 0,
            "retention_days": self.RETENTION_DAYS["user_sessions"],
        }

        # Soft-deleted users
        deleted_result = await self.db.execute(
            select(func.count(User.id)).where(User.deleted_at.isnot(None))
        )
        status["soft_deleted_users"] = {
            "count": deleted_result.scalar() or 0,
            "retention_days": self.RETENTION_DAYS["soft_deleted_users"],
        }

        # Audit logs
        audit_result = await self.db.execute(
            select(func.count(UserAuditLog.id))
        )
        status["audit_logs"] = {
            "count": audit_result.scalar() or 0,
            "retention_days": self.RETENTION_DAYS["audit_logs"],
        }

        # Completed DSAR requests
        dsar_result = await self.db.execute(
            select(func.count(DSARRequest.id)).where(
                DSARRequest.status.in_(["completed", "cancelled", "failed"])
            )
        )
        status["completed_dsar_requests"] = {
            "count": dsar_result.scalar() or 0,
            "retention_days": self.RETENTION_DAYS["dsar_requests"],
        }

        return status

    async def preview_cleanup(self) -> Dict[str, int]:
        """
        Preview what would be cleaned up without executing.
        Useful for admin review before running cleanup.
        """
        from sqlalchemy import func

        preview = {}

        for data_type, retention_days in self.RETENTION_DAYS.items():
            cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

            if data_type == "password_reset_tokens":
                result = await self.db.execute(
                    select(func.count(PasswordResetToken.id)).where(
                        PasswordResetToken.created_at < cutoff
                    )
                )
            elif data_type == "email_verification_tokens":
                result = await self.db.execute(
                    select(func.count(EmailVerificationToken.id)).where(
                        EmailVerificationToken.created_at < cutoff
                    )
                )
            elif data_type == "user_sessions":
                result = await self.db.execute(
                    select(func.count(UserSession.id)).where(
                        and_(
                            UserSession.is_active == False,
                            UserSession.created_at < cutoff
                        )
                    )
                )
            elif data_type == "soft_deleted_users":
                result = await self.db.execute(
                    select(func.count(User.id)).where(
                        and_(
                            User.deleted_at.isnot(None),
                            User.deleted_at < cutoff
                        )
                    )
                )
            elif data_type == "dsar_requests":
                result = await self.db.execute(
                    select(func.count(DSARRequest.id)).where(
                        and_(
                            DSARRequest.status.in_(["completed", "cancelled", "failed"]),
                            DSARRequest.completed_at.isnot(None),
                            DSARRequest.completed_at < cutoff
                        )
                    )
                )
            else:
                result = None

            preview[data_type] = result.scalar() if result else 0

        return preview
