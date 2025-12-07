"""
Audit Service

Per constitution_cyberSec.json §5: Immutable audit logging with hash chain.
Per constitution_db.json: All state changes logged.
"""
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from sqlalchemy import select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user_audit_log import UserAuditLog, AuditAction
from app.core.pii import pii_handler


class AuditService:
    """
    Service for immutable audit logging with hash chain verification.

    Features:
    - Hash chain for tamper detection
    - PII hashing (IP, user agent)
    - Structured event logging
    - Query and export capabilities
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def log(
        self,
        action: str,
        user_id: Optional[int] = None,
        target_user_id: Optional[int] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        session_id: Optional[int] = None,
    ) -> UserAuditLog:
        """
        Create an audit log entry with hash chain.

        Args:
            action: Action type (use AuditAction constants)
            user_id: ID of user performing action
            target_user_id: ID of user being acted upon (if different)
            resource_type: Type of resource (user, role, order, etc.)
            resource_id: ID of the resource
            details: Additional details as dict
            ip_address: Client IP (will be hashed)
            user_agent: Client user agent (will be hashed)
            session_id: Session ID if available

        Returns:
            Created audit log entry
        """
        # Get previous entry's hash for chain
        prev_hash = await self._get_last_hash()

        # Hash PII
        ip_hash = pii_handler.hash_ip(ip_address) if ip_address else None
        ua_hash = pii_handler.hash_user_agent(user_agent) if user_agent else None

        # Create entry
        entry = UserAuditLog(
            user_id=user_id,
            target_user_id=target_user_id,
            action=action,
            resource_type=resource_type,
            resource_id=str(resource_id) if resource_id else None,
            details=details,
            ip_hash=ip_hash,
            user_agent_hash=ua_hash,
            session_id=session_id,
            prev_hash=prev_hash,
        )

        # Calculate entry hash
        entry.entry_hash = self._calculate_hash(entry, prev_hash)

        self.db.add(entry)
        await self.db.flush()
        return entry

    def _calculate_hash(self, entry: UserAuditLog, prev_hash: Optional[str]) -> str:
        """Calculate hash for entry including previous hash."""
        data = {
            "user_id": entry.user_id,
            "target_user_id": entry.target_user_id,
            "action": entry.action,
            "resource_type": entry.resource_type,
            "resource_id": entry.resource_id,
            "details": entry.details,
            "ip_hash": entry.ip_hash,
            "user_agent_hash": entry.user_agent_hash,
            "session_id": entry.session_id,
            "prev_hash": prev_hash,
            "created_at": entry.created_at.isoformat() if entry.created_at else datetime.now(timezone.utc).isoformat(),
        }

        serialized = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()[:64]

    async def _get_last_hash(self) -> Optional[str]:
        """Get hash of the last audit entry."""
        result = await self.db.execute(
            select(UserAuditLog.entry_hash)
            .order_by(UserAuditLog.id.desc())
            .limit(1)
        )
        row = result.scalar_one_or_none()
        return row if row else None

    async def verify_chain(self, start_id: Optional[int] = None, end_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Verify hash chain integrity.

        Args:
            start_id: Starting entry ID (default: beginning)
            end_id: Ending entry ID (default: end)

        Returns:
            Dict with verification results:
            {
                "valid": bool,
                "entries_checked": int,
                "first_invalid_id": int or None,
                "error": str or None
            }
        """
        query = select(UserAuditLog).order_by(UserAuditLog.id)

        if start_id:
            query = query.where(UserAuditLog.id >= start_id)
        if end_id:
            query = query.where(UserAuditLog.id <= end_id)

        result = await self.db.execute(query)
        entries = result.scalars().all()

        if not entries:
            return {"valid": True, "entries_checked": 0, "first_invalid_id": None, "error": None}

        prev_hash = None
        entries_checked = 0

        for entry in entries:
            # First entry should have no prev_hash, or match the stored prev_hash
            if entries_checked == 0 and start_id:
                # If starting mid-chain, get the actual previous hash
                prev_result = await self.db.execute(
                    select(UserAuditLog.entry_hash)
                    .where(UserAuditLog.id < start_id)
                    .order_by(UserAuditLog.id.desc())
                    .limit(1)
                )
                prev_hash = prev_result.scalar_one_or_none()

            # Verify prev_hash matches
            if entry.prev_hash != prev_hash:
                return {
                    "valid": False,
                    "entries_checked": entries_checked,
                    "first_invalid_id": entry.id,
                    "error": f"prev_hash mismatch at entry {entry.id}"
                }

            # Verify entry_hash
            calculated = self._calculate_hash(entry, prev_hash)
            if entry.entry_hash != calculated:
                return {
                    "valid": False,
                    "entries_checked": entries_checked,
                    "first_invalid_id": entry.id,
                    "error": f"entry_hash mismatch at entry {entry.id}"
                }

            prev_hash = entry.entry_hash
            entries_checked += 1

        return {"valid": True, "entries_checked": entries_checked, "first_invalid_id": None, "error": None}

    async def get_user_audit_trail(
        self,
        user_id: int,
        include_as_target: bool = True,
        actions: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[UserAuditLog]:
        """
        Get audit trail for a specific user.

        Args:
            user_id: User ID to query
            include_as_target: Include entries where user is target
            actions: Filter by action types
            start_date: Filter by start date
            end_date: Filter by end date
            limit: Max entries to return
            offset: Pagination offset

        Returns:
            List of audit log entries
        """
        conditions = []

        if include_as_target:
            conditions.append(
                or_(
                    UserAuditLog.user_id == user_id,
                    UserAuditLog.target_user_id == user_id
                )
            )
        else:
            conditions.append(UserAuditLog.user_id == user_id)

        if actions:
            conditions.append(UserAuditLog.action.in_(actions))

        if start_date:
            conditions.append(UserAuditLog.created_at >= start_date)

        if end_date:
            conditions.append(UserAuditLog.created_at <= end_date)

        result = await self.db.execute(
            select(UserAuditLog)
            .where(and_(*conditions))
            .order_by(UserAuditLog.created_at.desc())
            .limit(limit)
            .offset(offset)
        )

        return list(result.scalars().all())

    async def get_security_events(
        self,
        hours: int = 24,
        limit: int = 100,
    ) -> List[UserAuditLog]:
        """
        Get security-related events for monitoring.

        Args:
            hours: Look back period in hours
            limit: Max entries to return

        Returns:
            List of security audit events
        """
        security_actions = [
            AuditAction.LOGIN_SUCCESS,
            AuditAction.LOGIN_FAILED,
            AuditAction.LOGOUT,
            AuditAction.PASSWORD_CHANGED,
            AuditAction.PASSWORD_RESET_REQUESTED,
            AuditAction.PASSWORD_RESET_COMPLETED,
            AuditAction.ACCOUNT_LOCKED,
            AuditAction.ACCOUNT_UNLOCKED,
            AuditAction.SESSION_REVOKED,
            AuditAction.ROLE_ASSIGNED,
            AuditAction.ROLE_REVOKED,
            AuditAction.PERMISSION_DENIED,
        ]

        cutoff = datetime.now(timezone.utc).replace(
            hour=datetime.now(timezone.utc).hour - hours
        )

        result = await self.db.execute(
            select(UserAuditLog)
            .where(
                and_(
                    UserAuditLog.action.in_(security_actions),
                    UserAuditLog.created_at >= cutoff
                )
            )
            .order_by(UserAuditLog.created_at.desc())
            .limit(limit)
        )

        return list(result.scalars().all())

    async def count_failed_logins(
        self,
        user_id: Optional[int] = None,
        ip_hash: Optional[str] = None,
        hours: int = 1,
    ) -> int:
        """
        Count failed login attempts.

        Args:
            user_id: Filter by user
            ip_hash: Filter by IP hash
            hours: Look back period

        Returns:
            Count of failed attempts
        """
        cutoff = datetime.now(timezone.utc).replace(
            hour=datetime.now(timezone.utc).hour - hours
        )

        conditions = [
            UserAuditLog.action == AuditAction.LOGIN_FAILED,
            UserAuditLog.created_at >= cutoff,
        ]

        if user_id:
            conditions.append(UserAuditLog.target_user_id == user_id)
        if ip_hash:
            conditions.append(UserAuditLog.ip_hash == ip_hash)

        result = await self.db.execute(
            select(func.count(UserAuditLog.id))
            .where(and_(*conditions))
        )

        return result.scalar() or 0

    async def export_user_audit_data(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Export all audit data for a user (DSAR compliance).

        Args:
            user_id: User ID to export

        Returns:
            List of audit entries as dicts (PII redacted)
        """
        entries = await self.get_user_audit_trail(
            user_id=user_id,
            include_as_target=True,
            limit=10000,  # Large limit for export
        )

        export_data = []
        for entry in entries:
            export_data.append({
                "id": entry.id,
                "action": entry.action,
                "resource_type": entry.resource_type,
                "resource_id": entry.resource_id,
                "details": entry.details,
                "created_at": entry.created_at.isoformat(),
                # IP and user agent hashes not included (PII)
            })

        return export_data


# Convenience function for quick logging
async def log_audit(
    db: AsyncSession,
    action: str,
    **kwargs
) -> UserAuditLog:
    """Convenience function for logging audit events."""
    service = AuditService(db)
    return await service.log(action, **kwargs)
