"""
AI Grading routes
"""
import time
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.database import get_db
from app.models.grading import GradeRequest as GradeRequestModel
from app.models.user import User
from app.schemas.grading import GradeRequest, GradeResponse, GradeEstimate
from app.api.deps import get_optional_user
from app.ml.grade_estimator import GradeEstimatorService

router = APIRouter()

# Initialize ML service
grade_service = GradeEstimatorService()


@router.post("/estimate", response_model=GradeEstimate)
async def quick_estimate(
    request: GradeRequest,
    user: User = Depends(get_optional_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Quick grade estimate from image URL.
    Returns estimated grade with confidence score.
    """
    start_time = time.time()
    
    try:
        # Run ML estimation
        result = await grade_service.estimate_grade(request.image_url)
        
        processing_time = int((time.time() - start_time) * 1000)
        
        # Log request (optional user tracking)
        grade_request = GradeRequestModel(
            user_id=user.id if user else None,
            image_url=request.image_url,
            additional_images=request.additional_images,
            estimated_grade=result["grade"],
            confidence=result["confidence"],
            analysis=result["analysis"],
            status="completed",
            processing_time_ms=processing_time,
            model_version=grade_service.model_version
        )
        db.add(grade_request)
        await db.commit()
        
        return GradeEstimate(
            grade=result["grade"],
            confidence=result["confidence"],
            grade_label=result["grade_label"],
            factors=result["factors"]
        )
        
    except Exception as e:
        # Log failed request
        grade_request = GradeRequestModel(
            user_id=user.id if user else None,
            image_url=request.image_url,
            status="failed",
            error_message=str(e)
        )
        db.add(grade_request)
        await db.commit()
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Grade estimation failed: {str(e)}"
        )


@router.post("/estimate/upload", response_model=GradeEstimate)
async def estimate_from_upload(
    file: UploadFile = File(...),
    user: User = Depends(get_optional_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Grade estimate from uploaded image file.
    Accepts JPEG, PNG images.
    """
    # Validate file type
    if file.content_type not in ["image/jpeg", "image/png", "image/webp"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Accepts JPEG, PNG, or WebP"
        )
    
    start_time = time.time()
    
    try:
        # Read file contents
        contents = await file.read()
        
        # Run ML estimation
        result = await grade_service.estimate_grade_from_bytes(contents)
        
        processing_time = int((time.time() - start_time) * 1000)
        
        return GradeEstimate(
            grade=result["grade"],
            confidence=result["confidence"],
            grade_label=result["grade_label"],
            factors=result["factors"]
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Grade estimation failed: {str(e)}"
        )


@router.get("/history")
async def grade_history(
    user: User = Depends(get_optional_user),
    db: AsyncSession = Depends(get_db)
):
    """Get user's grade estimation history"""
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Login required to view history"
        )
    
    result = await db.execute(
        select(GradeRequestModel)
        .where(GradeRequestModel.user_id == user.id)
        .order_by(GradeRequestModel.created_at.desc())
        .limit(50)
    )
    requests = result.scalars().all()
    
    return {"history": requests}


@router.get("/grade-scale")
async def get_grade_scale():
    """
    Get CGC grade scale reference.
    Useful for displaying grade meanings to users.
    """
    return {
        "scale": [
            {"grade": 10.0, "label": "Gem Mint", "description": "Perfect in every way"},
            {"grade": 9.9, "label": "Mint", "description": "Nearly perfect, virtually flawless"},
            {"grade": 9.8, "label": "Near Mint/Mint", "description": "Nearly perfect with minor imperfections"},
            {"grade": 9.6, "label": "Near Mint+", "description": "Well preserved with minimal wear"},
            {"grade": 9.4, "label": "Near Mint", "description": "Nearly perfect with slight wear"},
            {"grade": 9.2, "label": "Near Mint-", "description": "Minor wear, still high grade"},
            {"grade": 9.0, "label": "Very Fine/Near Mint", "description": "Minor wear, excellent eye appeal"},
            {"grade": 8.5, "label": "Very Fine+", "description": "Light wear, still attractive"},
            {"grade": 8.0, "label": "Very Fine", "description": "Moderate wear, good eye appeal"},
            {"grade": 7.5, "label": "Very Fine-", "description": "Noticeable wear"},
            {"grade": 7.0, "label": "Fine/Very Fine", "description": "Above-average with wear"},
            {"grade": 6.5, "label": "Fine+", "description": "Above-average example"},
            {"grade": 6.0, "label": "Fine", "description": "Slightly above average"},
            {"grade": 5.5, "label": "Fine-", "description": "Average preservation"},
            {"grade": 5.0, "label": "Very Good/Fine", "description": "Shows significant wear"},
            {"grade": 4.5, "label": "Very Good+", "description": "Well-read copy"},
            {"grade": 4.0, "label": "Very Good", "description": "Average used copy"},
            {"grade": 3.5, "label": "Very Good-", "description": "Below-average preservation"},
            {"grade": 3.0, "label": "Good/Very Good", "description": "Significant wear"},
            {"grade": 2.5, "label": "Good+", "description": "Heavy wear visible"},
            {"grade": 2.0, "label": "Good", "description": "Heavily worn"},
            {"grade": 1.8, "label": "Good-", "description": "Very worn"},
            {"grade": 1.5, "label": "Fair/Good", "description": "Very heavy wear"},
            {"grade": 1.0, "label": "Fair", "description": "Heavily damaged but complete"},
            {"grade": 0.5, "label": "Poor", "description": "Barely holding together"},
        ]
    }
