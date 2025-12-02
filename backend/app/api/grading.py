from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.db import get_db, Product, GradeAnalysis
from app.schemas import GradeAnalysisRequest, GradeAnalysisResponse
from app.ml import get_grade_estimator, GradeEstimator

router = APIRouter(prefix="/grading", tags=["Grade Estimation"])


@router.post("/analyze/{product_id}", response_model=GradeAnalysisResponse)
async def analyze_product_grade(
    product_id: int,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    estimator: GradeEstimator = Depends(get_grade_estimator)
):
    """
    Analyze a comic book image and estimate its CGC grade.
    
    Uploads an image and runs it through the AI grade estimation model.
    Returns estimated grade, confidence score, and breakdown by category.
    """
    # Verify product exists
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()
    
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    
    # Validate file type
    if not file.content_type.startswith("image/"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be an image"
        )
    
    # Read and analyze image
    image_bytes = await file.read()
    grade_result = estimator.analyze_from_bytes(image_bytes)
    
    # TODO: Upload image to S3 and get URL
    image_url = f"https://placeholder.com/uploads/{product_id}/{file.filename}"
    
    # Save analysis to database
    analysis = GradeAnalysis(
        product_id=product_id,
        estimated_grade=Decimal(str(grade_result.estimated_grade)),
        confidence=Decimal(str(grade_result.confidence)),
        corner_wear=Decimal(str(grade_result.breakdown.corner_wear)),
        spine_stress=Decimal(str(grade_result.breakdown.spine_stress)),
        color_fading=Decimal(str(grade_result.breakdown.color_fading)),
        page_quality=Decimal(str(grade_result.breakdown.page_quality)),
        centering=Decimal(str(grade_result.breakdown.centering)),
        image_url=image_url,
        model_version=grade_result.model_version,
    )
    db.add(analysis)
    
    # Update product with latest grade estimate
    product.estimated_grade = analysis.estimated_grade
    product.grade_confidence = analysis.confidence
    product.grade_breakdown = {
        "corner_wear": float(analysis.corner_wear),
        "spine_stress": float(analysis.spine_stress),
        "color_fading": float(analysis.color_fading),
        "page_quality": float(analysis.page_quality),
        "centering": float(analysis.centering),
    }
    
    await db.commit()
    await db.refresh(analysis)
    
    return GradeAnalysisResponse(
        id=analysis.id,
        product_id=analysis.product_id,
        estimated_grade=analysis.estimated_grade,
        confidence=analysis.confidence,
        breakdown={
            "corner_wear": float(analysis.corner_wear),
            "spine_stress": float(analysis.spine_stress),
            "color_fading": float(analysis.color_fading),
            "page_quality": float(analysis.page_quality),
            "centering": float(analysis.centering),
        },
        image_url=analysis.image_url,
        model_version=analysis.model_version,
        created_at=analysis.created_at,
    )


@router.post("/analyze-url", response_model=GradeAnalysisResponse)
async def analyze_grade_from_url(
    request: GradeAnalysisRequest,
    product_id: int,
    db: AsyncSession = Depends(get_db),
    estimator: GradeEstimator = Depends(get_grade_estimator)
):
    """
    Analyze a comic book image from a URL.
    
    Alternative to file upload - provide a URL to an existing image.
    """
    if not request.image_url:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="image_url is required"
        )
    
    # Verify product exists
    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()
    
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Product not found"
        )
    
    # Analyze from URL
    grade_result = await estimator.analyze_from_url(request.image_url)
    
    # Save analysis to database
    analysis = GradeAnalysis(
        product_id=product_id,
        estimated_grade=Decimal(str(grade_result.estimated_grade)),
        confidence=Decimal(str(grade_result.confidence)),
        corner_wear=Decimal(str(grade_result.breakdown.corner_wear)),
        spine_stress=Decimal(str(grade_result.breakdown.spine_stress)),
        color_fading=Decimal(str(grade_result.breakdown.color_fading)),
        page_quality=Decimal(str(grade_result.breakdown.page_quality)),
        centering=Decimal(str(grade_result.breakdown.centering)),
        image_url=request.image_url,
        model_version=grade_result.model_version,
    )
    db.add(analysis)
    
    # Update product with latest grade estimate
    product.estimated_grade = analysis.estimated_grade
    product.grade_confidence = analysis.confidence
    product.grade_breakdown = {
        "corner_wear": float(analysis.corner_wear),
        "spine_stress": float(analysis.spine_stress),
        "color_fading": float(analysis.color_fading),
        "page_quality": float(analysis.page_quality),
        "centering": float(analysis.centering),
    }
    
    await db.commit()
    await db.refresh(analysis)
    
    return GradeAnalysisResponse(
        id=analysis.id,
        product_id=analysis.product_id,
        estimated_grade=analysis.estimated_grade,
        confidence=analysis.confidence,
        breakdown={
            "corner_wear": float(analysis.corner_wear),
            "spine_stress": float(analysis.spine_stress),
            "color_fading": float(analysis.color_fading),
            "page_quality": float(analysis.page_quality),
            "centering": float(analysis.centering),
        },
        image_url=analysis.image_url,
        model_version=analysis.model_version,
        created_at=analysis.created_at,
    )


@router.get("/history/{product_id}")
async def get_grade_history(
    product_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get grade analysis history for a product."""
    result = await db.execute(
        select(GradeAnalysis)
        .where(GradeAnalysis.product_id == product_id)
        .order_by(GradeAnalysis.created_at.desc())
    )
    analyses = result.scalars().all()
    
    return [
        GradeAnalysisResponse(
            id=a.id,
            product_id=a.product_id,
            estimated_grade=a.estimated_grade,
            confidence=a.confidence,
            breakdown={
                "corner_wear": float(a.corner_wear) if a.corner_wear else None,
                "spine_stress": float(a.spine_stress) if a.spine_stress else None,
                "color_fading": float(a.color_fading) if a.color_fading else None,
                "page_quality": float(a.page_quality) if a.page_quality else None,
                "centering": float(a.centering) if a.centering else None,
            },
            image_url=a.image_url,
            model_version=a.model_version,
            created_at=a.created_at,
        )
        for a in analyses
    ]
