"""
Match Review Queue Schemas

Per constitution_cyberSec.json:
- All inputs validated with Pydantic
- Sanitized inputs
- No raw PII in responses
"""

from datetime import datetime
from typing import Optional, Literal, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
import re


# =============================================================
# Request Schemas
# =============================================================

class MatchQueueFilter(BaseModel):
    """Filter for listing match queue items."""
    status: Literal['pending', 'approved', 'rejected', 'skipped', 'expired', 'all'] = 'pending'
    entity_type: Optional[Literal['comic', 'funko']] = None
    min_score: Optional[int] = Field(None, ge=0, le=10)
    max_score: Optional[int] = Field(None, ge=0, le=10)
    escalated_only: bool = False
    limit: int = Field(50, ge=1, le=200)
    offset: int = Field(0, ge=0)


class MatchApproval(BaseModel):
    """Approve a match candidate."""
    notes: Optional[str] = Field(None, max_length=500)


class MatchRejection(BaseModel):
    """Reject a match candidate."""
    reason: Literal['wrong_item', 'wrong_variant', 'wrong_year', 'duplicate', 'other']
    notes: Optional[str] = Field(None, max_length=500)


class MatchSkip(BaseModel):
    """Skip a match for later review."""
    notes: Optional[str] = Field(None, max_length=500)


class ManualLink(BaseModel):
    """Manually link an entity to a PriceCharting product."""
    entity_type: Literal['comic', 'funko']
    entity_id: int = Field(..., gt=0)
    pricecharting_id: str = Field(..., min_length=1, max_length=50)
    notes: Optional[str] = Field(None, max_length=500)

    @field_validator('pricecharting_id')
    @classmethod
    def validate_pc_id(cls, v: str) -> str:
        """Sanitize PriceCharting ID per constitution_cyberSec.json."""
        # Allow alphanumeric and hyphens only
        if not re.match(r'^[a-zA-Z0-9\-]+$', v):
            raise ValueError('Invalid PriceCharting ID format')
        return v


class BulkApproval(BaseModel):
    """Bulk approve matches with score >= 8."""
    match_ids: List[int] = Field(..., min_length=1, max_length=100)
    notes: Optional[str] = Field(None, max_length=500)

    @field_validator('match_ids')
    @classmethod
    def validate_ids(cls, v: List[int]) -> List[int]:
        """Ensure all IDs are positive."""
        if any(id <= 0 for id in v):
            raise ValueError('All match IDs must be positive integers')
        return v


class BulkRejection(BaseModel):
    """Bulk reject matches."""
    match_ids: List[int] = Field(..., min_length=1, max_length=100)
    reason: str = Field(..., pattern='^(wrong_item|wrong_variant|wrong_year|duplicate|other)$')
    notes: Optional[str] = Field(None, max_length=500)

    @field_validator('match_ids')
    @classmethod
    def validate_reject_ids(cls, v: List[int]) -> List[int]:
        """Ensure all IDs are positive."""
        if any(id <= 0 for id in v):
            raise ValueError('All match IDs must be positive integers')
        return v


class ManualSearch(BaseModel):
    """Search PriceCharting for manual linking."""
    query: str = Field(..., min_length=2, max_length=200)
    entity_type: Literal['comic', 'funko']


# =============================================================
# Response Schemas
# =============================================================

class EntitySummary(BaseModel):
    """Summary of source entity (comic or funko)."""
    id: int
    type: str
    name: str
    series_name: Optional[str] = None
    issue_number: Optional[str] = None
    publisher: Optional[str] = None
    year: Optional[int] = None
    isbn: Optional[str] = None
    upc: Optional[str] = None
    cover_image_url: Optional[str] = None


class CandidateSummary(BaseModel):
    """Summary of match candidate."""
    source: str
    id: str
    name: str
    price_loose: Optional[float] = None
    price_cib: Optional[float] = None
    price_graded: Optional[float] = None
    url: Optional[str] = None


class MatchQueueItem(BaseModel):
    """Single item in the match review queue."""
    id: int
    entity: EntitySummary
    candidate: CandidateSummary
    match_method: str
    match_score: Optional[int]
    match_details: Optional[Dict[str, Any]]
    status: str
    is_escalated: bool
    can_bulk_approve: bool
    is_locked: bool
    locked_by_current_user: bool = False
    created_at: datetime
    expires_at: Optional[datetime]

    class Config:
        from_attributes = True


class MatchQueueResponse(BaseModel):
    """Paginated match queue response."""
    items: List[MatchQueueItem]
    total: int
    pending_count: int
    escalated_count: int
    limit: int
    offset: int


class MatchQueueStats(BaseModel):
    """Queue statistics for dashboard."""
    pending_count: int
    escalated_count: int
    approved_today: int
    rejected_today: int
    avg_review_time_seconds: Optional[float]
    threshold_exceeded: bool  # True if pending > 20


class MatchActionResult(BaseModel):
    """Result of a match action (approve/reject/skip)."""
    success: bool
    match_id: int
    action: str
    message: str
    next_match_id: Optional[int] = None  # For continuous review flow


class BulkApprovalResult(BaseModel):
    """Result of bulk approval."""
    success: bool
    approved_count: int
    failed_count: int
    failed_ids: List[int]
    message: str


class BulkRejectionResult(BaseModel):
    """Result of bulk rejection."""
    success: bool
    rejected_count: int
    failed_count: int
    failed_ids: List[int]
    cleaned_s3: int
    cleaned_local: int
    message: str


class SearchResult(BaseModel):
    """PriceCharting search result for manual linking."""
    id: str
    name: str
    console: str
    price_loose: Optional[float]
    price_cib: Optional[float]
    price_graded: Optional[float]


class ManualSearchResponse(BaseModel):
    """Response from manual PriceCharting search."""
    query: str
    results: List[SearchResult]
    result_count: int
