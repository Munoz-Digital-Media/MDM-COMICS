"""
Admin API Routes for Brand Asset Management

v1.0.0: Upload, manage, and version brand assets (logos, banners, etc.)
Assets stored in S3, URLs tracked in database.

Per constitution_cyberSec.json Section 3:
- All admin endpoints require is_admin=True
- CSRF protection on mutations
- Input validation
"""
import logging
import hashlib
import re
from datetime import datetime, timezone
from typing import List, Optional
from io import BytesIO

from fastapi import APIRouter, HTTPException, Depends, Query, UploadFile, File, Form
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from PIL import Image

from app.core.database import get_db
from app.api.deps import get_current_admin
from app.models.user import User
from app.models.site_settings import BrandAsset, BrandAssetVersion, SiteSettings
from app.services.storage import StorageService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/assets", tags=["admin-assets"])

storage = StorageService()


# ----- Pydantic Schemas -----

class AssetResponse(BaseModel):
    id: int
    name: str
    slug: str
    asset_type: str
    current_version: int
    url: str
    content_type: str
    file_size: int
    width: Optional[int]
    height: Optional[int]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class AssetVersionResponse(BaseModel):
    id: int
    version: int
    url: str
    content_type: str
    file_size: int
    width: Optional[int]
    height: Optional[int]
    storage_class: str
    created_at: Optional[datetime]


class AssetUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)


# ----- Helper Functions -----

def slugify(text: str) -> str:
    """Convert text to URL-safe slug."""
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_-]+', '-', text)
    return text.strip('-')


def get_image_dimensions(content: bytes, content_type: str) -> tuple[Optional[int], Optional[int]]:
    """Extract width and height from image bytes."""
    if content_type == 'image/svg+xml':
        return None, None  # SVGs don't have fixed dimensions

    try:
        img = Image.open(BytesIO(content))
        return img.width, img.height
    except Exception as e:
        logger.warning(f"Could not get image dimensions: {e}")
        return None, None


# ----- Endpoints -----

@router.post("/upload", status_code=201)
async def upload_brand_asset(
    file: UploadFile = File(...),
    asset_type: str = Form(..., pattern="^(logo|banner|icon|favicon|social)$"),
    name: str = Form(..., min_length=1, max_length=255),
    setting_key: Optional[str] = Form(None),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Upload a new brand asset or update an existing one.

    Parameters:
    - file: Image file (PNG, JPG, GIF, WebP, SVG)
    - asset_type: Type of asset (logo, banner, icon, favicon, social)
    - name: Display name for the asset
    - setting_key: Optional - auto-save URL to site_settings (e.g., "rack_factor_logo_url")

    If an asset with the same slug exists, creates a new version.
    """
    # Validate storage is configured
    if not storage.is_configured():
        raise HTTPException(
            status_code=503,
            detail="Storage service not configured. Set S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY."
        )

    # Read file content
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    content_type = file.content_type or 'application/octet-stream'

    # Upload to S3
    result = await storage.upload_brand_asset(
        content=content,
        filename=file.filename or 'asset',
        content_type=content_type,
        asset_type=asset_type
    )

    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)

    # Get image dimensions
    width, height = get_image_dimensions(content, content_type)

    # Generate checksum
    checksum = hashlib.md5(content).hexdigest()

    # Generate slug
    slug = slugify(name)

    # Check if asset with this slug already exists
    existing = await db.execute(
        select(BrandAsset).where(
            BrandAsset.slug == slug,
            BrandAsset.deleted_at.is_(None)
        )
    )
    asset = existing.scalar_one_or_none()

    if asset:
        # Create new version
        old_version = asset.current_version
        new_version = old_version + 1

        # Archive current version
        version_record = BrandAssetVersion(
            asset_id=asset.id,
            version=old_version,
            url=asset.url,
            s3_key=asset.s3_key,
            content_type=asset.content_type,
            file_size=asset.file_size,
            width=asset.width,
            height=asset.height,
            checksum=asset.checksum,
            storage_class='STANDARD',
            created_at=asset.updated_at or asset.created_at,
            created_by=asset.updated_by or asset.created_by
        )
        db.add(version_record)

        # Update asset to new version
        asset.current_version = new_version
        asset.url = result.url
        asset.s3_key = result.key
        asset.content_type = content_type
        asset.file_size = len(content)
        asset.width = width
        asset.height = height
        asset.checksum = checksum
        asset.updated_at = datetime.now(timezone.utc)
        asset.updated_by = current_user.id

        logger.info(f"Updated brand asset {slug} to version {new_version} by user {current_user.id}")
    else:
        # Create new asset
        asset = BrandAsset(
            name=name,
            slug=slug,
            asset_type=asset_type,
            current_version=1,
            url=result.url,
            s3_key=result.key,
            content_type=content_type,
            file_size=len(content),
            width=width,
            height=height,
            checksum=checksum,
            created_by=current_user.id,
            updated_by=current_user.id
        )
        db.add(asset)
        await db.flush()  # Get the ID

        logger.info(f"Created new brand asset {slug} by user {current_user.id}")

    # Optionally update site setting
    if setting_key:
        setting = await db.execute(
            select(SiteSettings).where(SiteSettings.key == setting_key)
        )
        setting_obj = setting.scalar_one_or_none()

        if setting_obj:
            setting_obj.value = result.url
            setting_obj.brand_asset_id = asset.id
            setting_obj.updated_at = datetime.now(timezone.utc)
            setting_obj.updated_by = current_user.id
            logger.info(f"Updated site setting {setting_key} with asset URL")
        else:
            # Create new setting
            new_setting = SiteSettings(
                key=setting_key,
                value=result.url,
                value_type='url',
                category='branding',
                description=f'URL for {name}',
                brand_asset_id=asset.id,
                updated_by=current_user.id
            )
            db.add(new_setting)
            logger.info(f"Created site setting {setting_key} for asset")

    await db.commit()
    await db.refresh(asset)

    return {
        "id": asset.id,
        "name": asset.name,
        "slug": asset.slug,
        "asset_type": asset.asset_type,
        "version": asset.current_version,
        "url": asset.url,
        "content_type": asset.content_type,
        "file_size": asset.file_size,
        "width": asset.width,
        "height": asset.height,
        "setting_key": setting_key if setting_key else None
    }


@router.get("")
async def list_brand_assets(
    asset_type: Optional[str] = Query(None, pattern="^(logo|banner|icon|favicon|social)$"),
    include_deleted: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """List all brand assets with optional filtering."""
    query = select(BrandAsset).order_by(BrandAsset.updated_at.desc())

    if not include_deleted:
        query = query.where(BrandAsset.deleted_at.is_(None))

    if asset_type:
        query = query.where(BrandAsset.asset_type == asset_type)

    # Get total count
    count_query = select(func.count(BrandAsset.id))
    if not include_deleted:
        count_query = count_query.where(BrandAsset.deleted_at.is_(None))
    if asset_type:
        count_query = count_query.where(BrandAsset.asset_type == asset_type)

    total_result = await db.execute(count_query)
    total = total_result.scalar()

    # Get items
    query = query.offset(offset).limit(limit)
    result = await db.execute(query)
    assets = result.scalars().all()

    return {
        "items": [
            {
                "id": a.id,
                "name": a.name,
                "slug": a.slug,
                "asset_type": a.asset_type,
                "version": a.current_version,
                "url": a.url,
                "content_type": a.content_type,
                "file_size": a.file_size,
                "width": a.width,
                "height": a.height,
                "created_at": a.created_at.isoformat() if a.created_at else None,
                "updated_at": a.updated_at.isoformat() if a.updated_at else None,
                "is_deleted": a.deleted_at is not None
            }
            for a in assets
        ],
        "total": total
    }


@router.get("/{asset_id}")
async def get_brand_asset(
    asset_id: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get a single brand asset with its version history."""
    result = await db.execute(
        select(BrandAsset).where(BrandAsset.id == asset_id)
    )
    asset = result.scalar_one_or_none()

    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")

    # Get version history
    versions_result = await db.execute(
        select(BrandAssetVersion)
        .where(BrandAssetVersion.asset_id == asset_id)
        .order_by(BrandAssetVersion.version.desc())
    )
    versions = versions_result.scalars().all()

    # Get linked site settings
    settings_result = await db.execute(
        select(SiteSettings).where(SiteSettings.brand_asset_id == asset_id)
    )
    linked_settings = settings_result.scalars().all()

    return {
        "id": asset.id,
        "name": asset.name,
        "slug": asset.slug,
        "asset_type": asset.asset_type,
        "current_version": asset.current_version,
        "url": asset.url,
        "s3_key": asset.s3_key,
        "content_type": asset.content_type,
        "file_size": asset.file_size,
        "width": asset.width,
        "height": asset.height,
        "checksum": asset.checksum,
        "created_at": asset.created_at.isoformat() if asset.created_at else None,
        "updated_at": asset.updated_at.isoformat() if asset.updated_at else None,
        "deleted_at": asset.deleted_at.isoformat() if asset.deleted_at else None,
        "versions": [
            {
                "id": v.id,
                "version": v.version,
                "url": v.url,
                "content_type": v.content_type,
                "file_size": v.file_size,
                "width": v.width,
                "height": v.height,
                "storage_class": v.storage_class,
                "archived_at": v.archived_at.isoformat() if v.archived_at else None,
                "created_at": v.created_at.isoformat() if v.created_at else None
            }
            for v in versions
        ],
        "linked_settings": [
            {"key": s.key, "category": s.category}
            for s in linked_settings
        ]
    }


@router.patch("/{asset_id}")
async def update_brand_asset(
    asset_id: int,
    request: AssetUpdateRequest,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Update brand asset metadata (not the file - use upload for that)."""
    result = await db.execute(
        select(BrandAsset).where(BrandAsset.id == asset_id)
    )
    asset = result.scalar_one_or_none()

    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")

    if asset.deleted_at:
        raise HTTPException(status_code=400, detail="Cannot update deleted asset")

    # Update fields
    if request.name:
        asset.name = request.name
        asset.slug = slugify(request.name)

    asset.updated_at = datetime.now(timezone.utc)
    asset.updated_by = current_user.id

    await db.commit()

    return {"status": "updated", "asset_id": asset_id}


@router.delete("/{asset_id}")
async def delete_brand_asset(
    asset_id: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Soft delete a brand asset."""
    result = await db.execute(
        select(BrandAsset).where(BrandAsset.id == asset_id)
    )
    asset = result.scalar_one_or_none()

    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")

    if asset.deleted_at:
        raise HTTPException(status_code=400, detail="Asset already deleted")

    # Check if linked to any site settings
    settings_result = await db.execute(
        select(SiteSettings).where(SiteSettings.brand_asset_id == asset_id)
    )
    linked_settings = settings_result.scalars().all()

    if linked_settings:
        raise HTTPException(
            status_code=400,
            detail=f"Asset is linked to {len(linked_settings)} site setting(s). Unlink first."
        )

    # Soft delete
    asset.deleted_at = datetime.now(timezone.utc)
    asset.updated_by = current_user.id

    await db.commit()

    logger.info(f"Brand asset {asset_id} soft-deleted by user {current_user.id}")

    return {"status": "deleted", "asset_id": asset_id}


@router.post("/{asset_id}/restore")
async def restore_brand_asset(
    asset_id: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Restore a soft-deleted brand asset."""
    result = await db.execute(
        select(BrandAsset).where(BrandAsset.id == asset_id)
    )
    asset = result.scalar_one_or_none()

    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")

    if not asset.deleted_at:
        raise HTTPException(status_code=400, detail="Asset is not deleted")

    asset.deleted_at = None
    asset.updated_at = datetime.now(timezone.utc)
    asset.updated_by = current_user.id

    await db.commit()

    logger.info(f"Brand asset {asset_id} restored by user {current_user.id}")

    return {"status": "restored", "asset_id": asset_id}


@router.get("/{asset_id}/versions/{version}")
async def get_asset_version(
    asset_id: int,
    version: int,
    current_user: User = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db)
):
    """Get a specific version of a brand asset."""
    # First check if it's the current version
    result = await db.execute(
        select(BrandAsset).where(BrandAsset.id == asset_id)
    )
    asset = result.scalar_one_or_none()

    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")

    if asset.current_version == version:
        return {
            "version": version,
            "is_current": True,
            "url": asset.url,
            "content_type": asset.content_type,
            "file_size": asset.file_size,
            "width": asset.width,
            "height": asset.height,
            "storage_class": "STANDARD"
        }

    # Look in version history
    version_result = await db.execute(
        select(BrandAssetVersion).where(
            BrandAssetVersion.asset_id == asset_id,
            BrandAssetVersion.version == version
        )
    )
    version_record = version_result.scalar_one_or_none()

    if not version_record:
        raise HTTPException(status_code=404, detail=f"Version {version} not found")

    return {
        "version": version_record.version,
        "is_current": False,
        "url": version_record.url,
        "content_type": version_record.content_type,
        "file_size": version_record.file_size,
        "width": version_record.width,
        "height": version_record.height,
        "storage_class": version_record.storage_class,
        "archived_at": version_record.archived_at.isoformat() if version_record.archived_at else None
    }
