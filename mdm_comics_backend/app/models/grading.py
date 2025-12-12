"""
Grade estimation models
"""
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, JSON
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.core.utils import utcnow


class GradeRequest(Base):
    """Track AI grading requests and results"""
    __tablename__ = "grade_requests"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)  # Can be anonymous
    
    # Input
    image_url = Column(String, nullable=False)
    additional_images = Column(JSON, default=list)
    
    # Results
    estimated_grade = Column(Float)
    confidence = Column(Float)
    
    # Detailed analysis
    analysis = Column(JSON)  # Breakdown of defects, wear patterns, etc.
    """
    Example analysis structure:
    {
        "corners": {"score": 8.5, "notes": "Minor wear on top left"},
        "spine": {"score": 9.0, "notes": "Tight, minimal stress"},
        "pages": {"score": 9.2, "notes": "White, no yellowing"},
        "centering": {"score": 8.0, "notes": "Slightly off-center front"},
        "defects": ["small tear page 5", "minor color break spine"]
    }
    """
    
    # Status
    status = Column(String, default="pending")  # pending, processing, completed, failed
    error_message = Column(String)
    
    # Performance tracking
    processing_time_ms = Column(Integer)
    model_version = Column(String)
    
    # Timestamps
    created_at = Column(DateTime, default=utcnow)
    completed_at = Column(DateTime)

    # Relationships
    user = relationship("User", back_populates="grade_requests")
