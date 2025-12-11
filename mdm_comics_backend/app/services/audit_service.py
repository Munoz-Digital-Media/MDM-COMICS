"""
Audit Service

Per constitution_cyberSec.json §5: Immutable audit logging with hash chain.
Per constitution_db.json: All state changes logged.

FIXED: Aligned with UserAuditLog model schema (2025-12-08)
"""
import hashlib
import json
from datetime import datetime, timezone, timedelta
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
    - PII hashing (IP, actor IDs)
    - Structured event logging
    - Query and export capabilities
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def log(
        self,
        action: str,
        actor_type: str = "user",
        actor_id: Optional[int] = None,
        resource_type: str = "user",
        resource_id: Optional[str] = None,
        outcome: str = "success",
        ip_address: Optional[str] = None,
        before_state: Optional[Dict[str, Any]] = None,
        after_state: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        # Compatibility aliases for auth.py
        user_id: Optional[int] = None,
        target_user_id: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
        user_agent: Optional[str] = None,
        session_id: Optional[int] = None,
    ) -> UserAuditLog:
        """
        Create an audit log entry with hash chain.

        Args:
            action: Action type (use AuditAction constants)
            actor_type: Type of actor ('user', 'admin', 'system', 'api')
            actor_id: ID of actor performing action (will be hashed)
            resource_type: Type of resource (user, role, order, etc.)
            resource_id: ID of the resource (will be hashed)
            outcome: Result of action ('success', 'failure', 'denied')
            ip_address: Client IP (will be hashed)
            before_state: State before change (will be hashed)
            after_state: State after change (will be hashed)
            metadata: Additional details as dict (no PII)
            user_id: Alias for actor_id (auth.py compatibility)
            target_user_id: Used for resource_id when resource_type is user
            details: Alias for metadata (auth.py compatibility)
            user_agent: Stored in metadata (auth.py compatibility)
            session_id: Stored in metadata (auth.py compatibility)

        Returns:
            Created audit log entry
        """
        # Handle compatibility aliases
        if user_id is not None and actor_id is None:
            actor_id = user_id
        if target_user_id is not None and resource_id is None:
            resource_id = str(target_user_id)
        if details is not None and metadata is None:
            metadata = details

        # Add user_agent and session_id to metadata if provided
        if user_agent or session_id:
            if metadata is None:
                metadata = {}
            if user_agent:
                metadata["user_agent"] = user_agent[:200] if user_agent else None
            if session_id:
                metadata["session_id"] = session_id

        # Get previous entry's hash for chain
        prev_hash = await self._get_last_hash()

        # Hash sensitive identifiers
        actor_id_hash = pii_handler.hash_for_lookup(str(actor_id)) if actor_id else pii_handler.hash_for_lookup("anonymous")
        resource_id_hash = pii_handler.hash_for_lookup(str(resource_id)) if resource_id else None
        ip_hash = pii_handler.hash_ip(ip_address) if ip_address else None

        # Hash state objects for change tracking
        before_hash = self._hash_state(before_state) if before_state else None
        after_hash = self._hash_state(after_state) if after_state else None

        # Create entry
        entry = UserAuditLog(
            actor_type=actor_type,
            actor_id_hash=actor_id_hash,
            action=action,
            resource_type=resource_type,
            resource_id_hash=resource_id_hash,
            outcome=outcome,
            ip_hash=ip_hash,
            before_hash=before_hash,
            after_hash=after_hash,
            event_metadata=metadata or {},
            prev_hash=prev_hash,
            entry_hash="",  # Will be calculated
        )

        # Calculate entry hash
        entry.entry_hash = self._calculate_hash(entry, prev_hash)

        self.db.add(entry)
        await self.db.flush()
        return entry

    def _hash_state(self, state: Dict[str, Any]) -> str:
        """Hash a state dictionary for change tracking."""
        serialized = json.dumps(state, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    def _calculate_hash(self, entry: UserAuditLog, prev_hash: Optional[str]) -> str:
        """Calculate hash for entry including previous hash."""
        data = {
            "actor_type": entry.actor_type,
            "actor_id_hash": entry.actor_id_hash,
            "action": entry.action,
            "resource_type": entry.resource_type,
            "resource_id_hash": entry.resource_id_hash,
            "outcome": entry.outcome,
            "ip_hash": entry.ip_hash,
            "before_hash": entry.before_hash,
            "after_hash": entry.after_hash,
            "event_metadata": entry.event_metadata,
            "prev_hash": prev_hash,
            "ts": entry.ts.isoformat() if entry.ts else datetime.now(timezone.utc).isoformat(),
        }

        serialized = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

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
            Dict with verification results
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
            if entries_checked == 0 and start_id:
                prev_result = await self.db.execute(
                    select(UserAuditLog.entry_hash)
                    .where(UserAuditLog.id < start_id)
                    .order_by(UserAuditLog.id.desc())
                    .limit(1)
                )
                prev_hash = prev_result.scalar_one_or_none()

            if entry.prev_hash != prev_hash:
                return {
                    "valid": False,
                    "entries_checked": entries_checked,
                    "first_invalid_id": entry.id,
                    "error": f"prev_hash mismatch at entry {entry.id}"
                }

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

    async def get_actor_audit_trail(
        self,
        actor_id: int,
        actions: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[UserAuditLog]:
        """
        Get audit trail for a specific actor.

        Args:
            actor_id: Actor ID to query (will be hashed for lookup)
            actions: Filter by action types
            start_date: Filter by start date
            end_date: Filter by end date
            limit: Max entries to return
            offset: Pagination offset

        Returns:
            List of audit log entries
        """
        actor_id_hash = pii_handler.hash_for_lookup(str(actor_id))
        conditions = [UserAuditLog.actor_id_hash == actor_id_hash]

        if actions:
            conditions.append(UserAuditLog.action.in_(actions))
        if start_date:
            conditions.append(UserAuditLog.ts >= start_date)
        if end_date:
            conditions.append(UserAuditLog.ts <= end_date)

        result = await self.db.execute(
            select(UserAuditLog)
            .where(and_(*conditions))
            .order_by(UserAuditLog.ts.desc())
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
            AuditAction.USER_LOGIN,
            AuditAction.USER_LOGIN_FAILED,
            AuditAction.USER_LOGOUT,
            AuditAction.USER_PASSWORD_CHANGE,
            AuditAction.USER_PASSWORD_RESET_REQUEST,
            AuditAction.USER_PASSWORD_RESET,
            AuditAction.USER_LOCKED,
            AuditAction.USER_UNLOCKED,
            AuditAction.SESSION_REVOKED,
            AuditAction.ROLE_ASSIGNED,
            AuditAction.ROLE_REVOKED,
        ]

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        result = await self.db.execute(
            select(UserAuditLog)
            .where(
                and_(
                    UserAuditLog.action.in_(security_actions),
                    UserAuditLog.ts >= cutoff
                )
            )
            .order_by(UserAuditLog.ts.desc())
            .limit(limit)
        )

        return list(result.scalars().all())

    async def count_failed_logins(
        self,
        actor_id: Optional[int] = None,
        ip_hash: Optional[str] = None,
        hours: int = 1,
    ) -> int:
        """
        Count failed login attempts.

        Args:
            actor_id: Filter by actor (will be hashed)
            ip_hash: Filter by IP hash
            hours: Look back period

        Returns:
            Count of failed attempts
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        conditions = [
            UserAuditLog.action == AuditAction.USER_LOGIN_FAILED,
            UserAuditLog.ts >= cutoff,
        ]

        if actor_id:
            actor_id_hash = pii_handler.hash_for_lookup(str(actor_id))
            conditions.append(UserAuditLog.actor_id_hash == actor_id_hash)
        if ip_hash:
            conditions.append(UserAuditLog.ip_hash == ip_hash)

        result = await self.db.execute(
            select(func.count(UserAuditLog.id))
            .where(and_(*conditions))
        )

        return result.scalar() or 0

    async def export_actor_audit_data(self, actor_id: int) -> List[Dict[str, Any]]:
        """
        Export all audit data for an actor (DSAR compliance).

        Args:
            actor_id: Actor ID to export

        Returns:
            List of audit entries as dicts (PII redacted)
        """
        entries = await self.get_actor_audit_trail(
            actor_id=actor_id,
            limit=10000,
        )

        export_data = []
        for entry in entries:
            export_data.append({
                "id": entry.id,
                "action": entry.action,
                "resource_type": entry.resource_type,
                "outcome": entry.outcome,
                "event_metadata": entry.event_metadata,
                "ts": entry.ts.isoformat(),
            })

        return export_data


async def log_audit(
    db: AsyncSession,
    action: str,
    **kwargs
) -> UserAuditLog:
    """Convenience function for logging audit events."""
    service = AuditService(db)
    return await service.log(action, **kwargs)
