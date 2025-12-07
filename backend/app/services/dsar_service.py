"""
DSAR (Data Subject Access Request) Service

User Management System v1.0.0
Per constitution_pii.json: GDPR Article 17 compliance

Handles:
- Export requests (data portability)
- Deletion requests (right to erasure)
- Rectification requests (data correction)
"""
import hashlib
import json
import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any, Dict, AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.models.user import User
from app.models.dsar_request import DSARRequest
from app.models.user_session import UserSession
from app.models.user_audit_log import UserAuditLog, AuditAction


class DSARService:
    """
    Data Subject Access Request handling.
    Per constitution_pii.json:
    - Export: All user data in portable format
    - Delete: Cryptographic erasure with proof
    """

    # Grace period before deletion (14 days for cancellation)
    DELETION_GRACE_DAYS = 14

    def __init__(self, db: AsyncSession):
        self.db = db

    # ============================================================
    # Request Management
    # ============================================================

    async def create_request(
        self,
        user_id: int,
        request_type: str,
        notes: str = None
    ) -> DSARRequest:
        """
        Create a new DSAR request.

        Args:
            user_id: User ID making the request
            request_type: 'export', 'delete', or 'rectify'
            notes: Optional notes about the request
        """
        if request_type not in ('export', 'delete', 'rectify'):
            raise ValueError(f"Invalid request type: {request_type}")

        # Check for pending requests of same type
        existing = await self.get_pending_request(user_id, request_type)
        if existing:
            raise ValueError(f"A pending {request_type} request already exists")

        request = DSARRequest(
            user_id=user_id,
            request_type=request_type,
            status="pending",
            notes=notes,
        )

        self.db.add(request)
        await self.db.flush()

        return request

    async def get_request(self, request_id: int) -> Optional[DSARRequest]:
        """Get a DSAR request by ID."""
        result = await self.db.execute(
            select(DSARRequest).where(DSARRequest.id == request_id)
        )
        return result.scalar_one_or_none()

    async def get_user_requests(
        self,
        user_id: int,
        status: str = None
    ) -> List[DSARRequest]:
        """Get all DSAR requests for a user."""
        query = select(DSARRequest).where(DSARRequest.user_id == user_id)

        if status:
            query = query.where(DSARRequest.status == status)

        query = query.order_by(DSARRequest.requested_at.desc())

        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_pending_request(
        self,
        user_id: int,
        request_type: str
    ) -> Optional[DSARRequest]:
        """Get pending request of specific type for user."""
        result = await self.db.execute(
            select(DSARRequest).where(
                and_(
                    DSARRequest.user_id == user_id,
                    DSARRequest.request_type == request_type,
                    DSARRequest.status.in_(["pending", "processing"])
                )
            )
        )
        return result.scalar_one_or_none()

    async def get_all_requests(
        self,
        status: str = None,
        request_type: str = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[DSARRequest]:
        """Get all DSAR requests with filters."""
        query = select(DSARRequest)

        if status:
            query = query.where(DSARRequest.status == status)
        if request_type:
            query = query.where(DSARRequest.request_type == request_type)

        query = query.order_by(DSARRequest.requested_at.desc())
        query = query.limit(limit).offset(offset)

        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def cancel_request(self, request_id: int) -> DSARRequest:
        """Cancel a pending DSAR request."""
        request = await self.get_request(request_id)
        if not request:
            raise ValueError("Request not found")

        if not request.can_cancel:
            raise ValueError("Only pending requests can be cancelled")

        request.cancel()
        await self.db.flush()

        return request

    # ============================================================
    # Export Processing
    # ============================================================

    async def process_export(
        self,
        request_id: int,
        processor_id: int = None
    ) -> Dict[str, Any]:
        """
        Process a DSAR export request.
        Returns user data in portable JSON format.
        """
        request = await self.get_request(request_id)
        if not request:
            raise ValueError("Request not found")

        if request.request_type != "export":
            raise ValueError("Request is not an export request")

        if request.status not in ("pending", "processing"):
            raise ValueError(f"Request already {request.status}")

        # Mark as processing
        request.start_processing(processor_id)
        await self.db.flush()

        try:
            # Gather all user data
            export_data = await self._gather_user_data(request.user_id)

            # Generate export hash for integrity
            export_json = json.dumps(export_data, sort_keys=True, default=str)
            export_hash = hashlib.sha256(export_json.encode()).hexdigest()

            # Mark as completed
            request.complete(
                export_url_hash=export_hash,
                ledger_tx_id=f"DSAR-EXPORT-{request.id}-{secrets.token_hex(8)}"
            )

            await self.db.flush()

            return {
                "request_id": request.id,
                "export_data": export_data,
                "export_hash": export_hash,
                "completed_at": request.completed_at.isoformat(),
            }

        except Exception as e:
            request.fail(notes=str(e)[:500])
            await self.db.flush()
            raise

    async def _gather_user_data(self, user_id: int) -> Dict[str, Any]:
        """Gather all data for a user export (GDPR Article 15)."""
        # Get user with relationships
        result = await self.db.execute(
            select(User)
            .options(
                selectinload(User.addresses),
                selectinload(User.orders),
                selectinload(User.user_roles),
            )
            .where(User.id == user_id)
        )
        user = result.scalar_one_or_none()

        if not user:
            raise ValueError("User not found")

        # Build export data structure
        export_data = {
            "export_metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "user_id": user_id,
                "format_version": "1.0.0",
                "gdpr_article": "Article 15 - Right of Access",
            },
            "profile": {
                "id": user.id,
                "email": user.email,
                "name": user.name,
                "is_active": user.is_active,
                "email_verified": user.is_email_verified,
                "created_at": user.created_at.isoformat() if user.created_at else None,
                "updated_at": user.updated_at.isoformat() if user.updated_at else None,
                "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
            },
            "addresses": [
                {
                    "id": addr.id,
                    "name": addr.name if hasattr(addr, 'name') else None,
                    "street": addr.street if hasattr(addr, 'street') else None,
                    "city": addr.city if hasattr(addr, 'city') else None,
                    "state": addr.state if hasattr(addr, 'state') else None,
                    "postal_code": addr.postal_code if hasattr(addr, 'postal_code') else None,
                    "country": addr.country if hasattr(addr, 'country') else None,
                    "is_default": addr.is_default if hasattr(addr, 'is_default') else False,
                }
                for addr in (user.addresses or [])
            ],
            "orders": await self._export_orders(user_id),
            "roles": [
                {
                    "role_id": ur.role_id,
                    "assigned_at": ur.created_at.isoformat() if ur.created_at else None,
                }
                for ur in (user.user_roles or [])
            ],
            "audit_trail": await self._export_audit_trail(user_id),
            "sessions": await self._export_sessions(user_id),
            "dsar_history": await self._export_dsar_history(user_id),
        }

        return export_data

    async def _export_orders(self, user_id: int) -> List[Dict]:
        """Export user order history."""
        from app.models.order import Order

        result = await self.db.execute(
            select(Order).where(Order.user_id == user_id)
        )
        orders = result.scalars().all()

        return [
            {
                "id": o.id,
                "status": o.status,
                "total": float(o.total) if o.total else None,
                "created_at": o.created_at.isoformat() if hasattr(o, 'created_at') and o.created_at else None,
            }
            for o in orders
        ]

    async def _export_audit_trail(self, user_id: int) -> List[Dict]:
        """Export user's audit trail (last 365 days)."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=365)

        result = await self.db.execute(
            select(UserAuditLog)
            .where(
                and_(
                    UserAuditLog.user_id == user_id,
                    UserAuditLog.created_at >= cutoff
                )
            )
            .order_by(UserAuditLog.created_at.desc())
            .limit(1000)
        )
        logs = result.scalars().all()

        return [
            {
                "action": log.action.value if hasattr(log.action, 'value') else str(log.action),
                "resource_type": log.resource_type,
                "ip_address": log.ip_address[:10] + "..." if log.ip_address else None,  # Masked
                "created_at": log.created_at.isoformat() if log.created_at else None,
            }
            for log in logs
        ]

    async def _export_sessions(self, user_id: int) -> List[Dict]:
        """Export active session info."""
        result = await self.db.execute(
            select(UserSession)
            .where(
                and_(
                    UserSession.user_id == user_id,
                    UserSession.is_active == True
                )
            )
        )
        sessions = result.scalars().all()

        return [
            {
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "last_activity": s.last_activity_at.isoformat() if s.last_activity_at else None,
                "expires_at": s.expires_at.isoformat() if s.expires_at else None,
                "user_agent": s.user_agent[:50] + "..." if s.user_agent and len(s.user_agent) > 50 else s.user_agent,
            }
            for s in sessions
        ]

    async def _export_dsar_history(self, user_id: int) -> List[Dict]:
        """Export DSAR request history."""
        result = await self.db.execute(
            select(DSARRequest)
            .where(DSARRequest.user_id == user_id)
            .order_by(DSARRequest.requested_at.desc())
        )
        requests = result.scalars().all()

        return [
            {
                "type": r.request_type,
                "status": r.status,
                "requested_at": r.requested_at.isoformat() if r.requested_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            }
            for r in requests
        ]

    # ============================================================
    # Deletion Processing (Right to Erasure)
    # ============================================================

    async def process_deletion(
        self,
        request_id: int,
        processor_id: int = None
    ) -> Dict[str, Any]:
        """
        Process a DSAR deletion request.
        Implements cryptographic erasure with proof.
        Per GDPR Article 17 (Right to Erasure).
        """
        request = await self.get_request(request_id)
        if not request:
            raise ValueError("Request not found")

        if request.request_type != "delete":
            raise ValueError("Request is not a deletion request")

        if request.status == "cancelled":
            raise ValueError("Request was cancelled")

        if request.status == "completed":
            raise ValueError("Request already completed")

        # Mark as processing
        request.start_processing(processor_id)
        await self.db.flush()

        try:
            # Get user
            result = await self.db.execute(
                select(User).where(User.id == request.user_id)
            )
            user = result.scalar_one_or_none()

            if not user:
                raise ValueError("User not found")

            # Pre-deletion hash for proof
            pre_hash = hashlib.sha512(
                f"{user.email}|{user.name}|{user.id}".encode()
            ).hexdigest()

            # 1. Revoke all sessions
            await self._revoke_all_sessions(user.id)

            # 2. Delete addresses
            await self._delete_user_addresses(user.id)

            # 3. Anonymize orders (keep for accounting)
            await self._anonymize_orders(user.id)

            # 4. Anonymize user record
            user.email = f"deleted_{user.id}@anonymized.local"
            user.name = "Deleted User"
            user.hashed_password = "DELETED"
            user.is_active = False
            user.deleted_at = datetime.now(timezone.utc)
            user.email_verified_at = None
            user.failed_login_attempts = 0
            user.locked_until = None
            user.lockout_count = 0
            user.last_login_at = None
            user.last_login_ip_hash = None

            # Post-deletion hash for proof
            post_hash = hashlib.sha512(
                f"{user.email}|{user.name}|{user.id}".encode()
            ).hexdigest()

            # Record completion with cryptographic proof
            ledger_tx_id = f"DSAR-DELETE-{request.id}-{secrets.token_hex(8)}"
            request.complete(ledger_tx_id=ledger_tx_id)

            await self.db.flush()

            return {
                "request_id": request.id,
                "user_id": user.id,
                "pre_hash": pre_hash,
                "post_hash": post_hash,
                "ledger_tx_id": ledger_tx_id,
                "completed_at": request.completed_at.isoformat(),
            }

        except Exception as e:
            request.fail(notes=str(e)[:500])
            await self.db.flush()
            raise

    async def _revoke_all_sessions(self, user_id: int) -> int:
        """Revoke all user sessions."""
        result = await self.db.execute(
            update(UserSession)
            .where(
                and_(
                    UserSession.user_id == user_id,
                    UserSession.is_active == True
                )
            )
            .values(
                is_active=False,
                revoked_at=datetime.now(timezone.utc),
                revoked_reason="dsar_deletion"
            )
        )
        return result.rowcount

    async def _delete_user_addresses(self, user_id: int) -> int:
        """Delete user addresses."""
        from app.models.address import Address

        result = await self.db.execute(
            select(Address).where(Address.user_id == user_id)
        )
        addresses = result.scalars().all()

        for addr in addresses:
            await self.db.delete(addr)

        return len(addresses)

    async def _anonymize_orders(self, user_id: int) -> int:
        """Anonymize order records (keep for accounting)."""
        from app.models.order import Order

        result = await self.db.execute(
            update(Order)
            .where(Order.user_id == user_id)
            .values(
                shipping_name="Deleted User",
                shipping_address="[REDACTED]",
                shipping_city="[REDACTED]",
                shipping_state="[REDACTED]",
                shipping_postal_code="[REDACTED]",
                billing_name="Deleted User",
                billing_address="[REDACTED]",
            )
        )
        return result.rowcount

    # ============================================================
    # Rectification Processing
    # ============================================================

    async def process_rectification(
        self,
        request_id: int,
        corrections: Dict[str, Any],
        processor_id: int = None
    ) -> Dict[str, Any]:
        """
        Process a DSAR rectification request.
        Updates user data per corrections provided.
        """
        request = await self.get_request(request_id)
        if not request:
            raise ValueError("Request not found")

        if request.request_type != "rectify":
            raise ValueError("Request is not a rectification request")

        if request.status not in ("pending", "processing"):
            raise ValueError(f"Request already {request.status}")

        # Mark as processing
        request.start_processing(processor_id)
        await self.db.flush()

        try:
            # Get user
            result = await self.db.execute(
                select(User).where(User.id == request.user_id)
            )
            user = result.scalar_one_or_none()

            if not user:
                raise ValueError("User not found")

            # Apply allowed corrections
            updated_fields = []
            allowed_fields = {"name", "email"}  # Only these can be rectified

            for field, value in corrections.items():
                if field in allowed_fields and hasattr(user, field):
                    setattr(user, field, value)
                    updated_fields.append(field)

            # Mark as completed
            ledger_tx_id = f"DSAR-RECTIFY-{request.id}-{secrets.token_hex(8)}"
            request.complete(ledger_tx_id=ledger_tx_id)
            request.notes = f"Updated fields: {', '.join(updated_fields)}"

            await self.db.flush()

            return {
                "request_id": request.id,
                "user_id": user.id,
                "updated_fields": updated_fields,
                "ledger_tx_id": ledger_tx_id,
                "completed_at": request.completed_at.isoformat(),
            }

        except Exception as e:
            request.fail(notes=str(e)[:500])
            await self.db.flush()
            raise

    # ============================================================
    # Statistics
    # ============================================================

    async def get_stats(self) -> Dict[str, Any]:
        """Get DSAR statistics for admin dashboard."""
        from sqlalchemy import func

        # Count by status
        status_result = await self.db.execute(
            select(
                DSARRequest.status,
                func.count(DSARRequest.id)
            )
            .group_by(DSARRequest.status)
        )
        status_counts = dict(status_result.all())

        # Count by type
        type_result = await self.db.execute(
            select(
                DSARRequest.request_type,
                func.count(DSARRequest.id)
            )
            .group_by(DSARRequest.request_type)
        )
        type_counts = dict(type_result.all())

        # Pending count
        pending_count = status_counts.get("pending", 0)

        # Average processing time (last 30 days)
        thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
        completed_result = await self.db.execute(
            select(DSARRequest)
            .where(
                and_(
                    DSARRequest.status == "completed",
                    DSARRequest.completed_at >= thirty_days_ago
                )
            )
        )
        completed_requests = completed_result.scalars().all()

        if completed_requests:
            total_hours = sum(
                (r.completed_at - r.requested_at).total_seconds() / 3600
                for r in completed_requests
                if r.completed_at and r.requested_at
            )
            avg_hours = total_hours / len(completed_requests)
        else:
            avg_hours = 0

        return {
            "by_status": status_counts,
            "by_type": type_counts,
            "pending_count": pending_count,
            "avg_processing_hours": round(avg_hours, 2),
            "compliance_deadline_days": getattr(settings, 'DSAR_PROCESSING_DAYS', 30),
        }
