"""
Grading schemas
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel


class GradeAnalysis(BaseModel):
    """Detailed breakdown of grade factors"""
    corners: Dict[str, Any]  # {"score": 8.5, "notes": "..."}
    spine: Dict[str, Any]
    pages: Dict[str, Any]
    centering: Dict[str, Any]
    defects: List[str]


class GradeRequest(BaseModel):
    """Request for AI grade estimation"""
    image_url: str
    additional_images: List[str] = []


class GradeResponse(BaseModel):
    """AI grade estimation result"""
    id: int
    estimated_grade: float
    confidence: float
    analysis: GradeAnalysis
    status: str
    processing_time_ms: int
    model_version: str
    created_at: datetime
    completed_at: Optional[datetime]

    class Config:
        from_attributes = True


class GradeEstimate(BaseModel):
    """Quick grade estimate for display"""
    grade: float
    confidence: float
    grade_label: str  # e.g., "Near Mint (9.4)"
    factors: Dict[str, float]  # Quick breakdown scores
