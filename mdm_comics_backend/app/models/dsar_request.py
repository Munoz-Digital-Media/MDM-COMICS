"""
DSAR (Data Subject Access Request) model

Per constitution_pii.json: GDPR Article 17 compliance
"""
from datetime import datetime, timezone
from enum import Enum
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Text, Index, CheckConstraint

from app.core.database import Base


class DSARType(str, Enum):
    """DSAR request types."""
    EXPORT = "export"
    DELETE = "delete"
    RECTIFY = "rectify"


class DSARStatus(str, Enum):
    """DSAR request statuses."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DSARRequest(Base):
    """
    Tracks Data Subject Access Requests for GDPR/CCPA compliance.

    Request types:
    - export: User requests data export
    - delete: User requests account deletion (Right to Erasure)
    - rectify: User requests data correction

    Process:
    1. User submits request
    2. Request queued for processing
    3. Admin/system processes request
    4. Proof recorded for compliance
    """
    __tablename__ = "dsar_requests"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Request details
    request_type = Column(String(20), nullable=False)  # 'export', 'delete', 'rectify'
    status = Column(String(20), nullable=False, default="pending")  # 'pending', 'processing', 'completed', 'failed', 'cancelled'

    # Timestamps
    requested_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # For exports: hashed URL to encrypted export file
    export_url_hash = Column(String(128), nullable=True)

    # Processing metadata
    processed_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    notes = Column(Text, nullable=True)

    # Compliance proof (ledger transaction ID)
    ledger_tx_id = Column(String(128), nullable=True)

    __table_args__ = (
        Index('ix_dsar_user', 'user_id'),
        Index('ix_dsar_status', 'status'),
        Index('ix_dsar_type', 'request_type'),
        CheckConstraint(
            "request_type IN ('export', 'delete', 'rectify')",
            name='ck_dsar_request_type'
        ),
        CheckConstraint(
            "status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')",
            name='ck_dsar_status'
        ),
    )

    def __repr__(self):
        return f"<DSARRequest(id={self.id}, type='{self.request_type}', status='{self.status}')>"

    @property
    def is_pending(self) -> bool:
        return self.status == "pending"

    @property
    def is_completed(self) -> bool:
        return self.status == "completed"

    @property
    def can_cancel(self) -> bool:
        """Only pending requests can be cancelled."""
        return self.status == "pending"

    def cancel(self) -> None:
        """Cancel the request."""
        if not self.can_cancel:
            raise ValueError("Only pending requests can be cancelled")
        self.status = "cancelled"
        self.completed_at = datetime.now(timezone.utc)

    def start_processing(self, processor_id: int = None) -> None:
        """Mark request as being processed."""
        self.status = "processing"
        if processor_id:
            self.processed_by = processor_id

    def complete(self, export_url_hash: str = None, ledger_tx_id: str = None) -> None:
        """Mark request as completed."""
        self.status = "completed"
        self.completed_at = datetime.now(timezone.utc)
        if export_url_hash:
            self.export_url_hash = export_url_hash
        if ledger_tx_id:
            self.ledger_tx_id = ledger_tx_id

    def fail(self, notes: str = None) -> None:
        """Mark request as failed."""
        self.status = "failed"
        self.completed_at = datetime.now(timezone.utc)
        if notes:
            self.notes = notes
