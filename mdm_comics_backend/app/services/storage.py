"""
Storage Service - S3-compatible object storage

v1.0.0: Brand assets, product images, and file uploads
Supports AWS S3, Cloudflare R2, MinIO, and other S3-compatible services.
"""
import os
import logging
import hashlib
import mimetypes
from datetime import datetime, timezone
from typing import Optional, BinaryIO, Tuple
from dataclasses import dataclass
from threading import Lock

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    """Result of a file upload."""
    success: bool
    url: Optional[str] = None
    key: Optional[str] = None
    error: Optional[str] = None
    content_type: Optional[str] = None
    size_bytes: Optional[int] = None


class StorageService:
    """
    S3-compatible storage service.

    Handles uploads to AWS S3, Cloudflare R2, or any S3-compatible service.
    """
    _validation_lock: Lock = Lock()
    _bucket_validated: bool = False

    # Allowed MIME types for brand assets
    ALLOWED_IMAGE_TYPES = {
        'image/png': '.png',
        'image/jpeg': '.jpg',
        'image/gif': '.gif',
        'image/webp': '.webp',
        'image/svg+xml': '.svg',
    }

    # Max file sizes (in bytes)
    MAX_BRAND_ASSET_SIZE = 5 * 1024 * 1024  # 5MB
    MAX_PRODUCT_IMAGE_SIZE = 10 * 1024 * 1024  # 10MB

    def __init__(self):
        self._client = None
        self._bucket = settings.S3_BUCKET
        self._region = settings.S3_REGION
        self._validate_configuration_once()

    @property
    def client(self):
        """Lazy-load S3 client."""
        if self._client is None:
            config = Config(
                signature_version='s3v4',
                retries={'max_attempts': 3, 'mode': 'standard'}
            )

            client_kwargs = {
                'service_name': 's3',
                'region_name': self._region,
                'aws_access_key_id': settings.S3_ACCESS_KEY,
                'aws_secret_access_key': settings.S3_SECRET_KEY,
                'config': config,
            }

            # Custom endpoint for R2/MinIO
            if settings.S3_ENDPOINT:
                client_kwargs['endpoint_url'] = settings.S3_ENDPOINT

            self._client = boto3.client(**client_kwargs)

        return self._client

    def _get_public_url(self, key: str) -> str:
        """Get public URL for an uploaded object."""
        if settings.S3_ENDPOINT:
            # R2 or custom endpoint - construct URL
            # For R2: https://<bucket>.<account>.r2.cloudflarestorage.com/<key>
            # Or use custom domain if configured
            endpoint = settings.S3_ENDPOINT.rstrip('/')
            return f"{endpoint}/{self._bucket}/{key}"
        else:
            # Standard AWS S3
            return f"https://{self._bucket}.s3.{self._region}.amazonaws.com/{key}"

    def get_public_url(self, key: str) -> str:
        """Get public URL for an S3 key (public wrapper for _get_public_url)."""
        return self._get_public_url(key)

    def _generate_key(self, folder: str, filename: str, content: bytes) -> str:
        """Generate unique key for uploaded file."""
        # Hash content for uniqueness
        content_hash = hashlib.md5(content).hexdigest()[:8]
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d')

        # Clean filename
        safe_filename = "".join(c for c in filename if c.isalnum() or c in '.-_').lower()

        return f"{folder}/{timestamp}_{content_hash}_{safe_filename}"

    def _validate_configuration_once(self):
        """Run storage validation a single time per process."""
        if StorageService._bucket_validated:
            return

        with StorageService._validation_lock:
            if StorageService._bucket_validated:
                return
            self._validate_configuration()
            StorageService._bucket_validated = True

    def _validate_configuration(self):
        """
        Validate S3 storage configuration and verify bucket access.

        Requires s3:ListBucket (head_bucket) permission on the bucket.
        """
        env = settings.ENVIRONMENT.lower()

        if not self.is_configured():
            if env == "development":
                logger.warning("S3 storage not configured; skipping in development")
                return
            raise RuntimeError("S3 storage not configured. Set S3_BUCKET or S3_BUCKET_NAME.")

        if env == "production":
            using_default_bucket = (
                self._bucket == "mdm-comics"
                and not (os.getenv("S3_BUCKET") or os.getenv("S3_BUCKET_NAME"))
            )
            if using_default_bucket:
                raise RuntimeError(
                    "S3 bucket is using the default value in production. "
                    "Set S3_BUCKET or S3_BUCKET_NAME to the correct bucket."
                )

        try:
            self.client.head_bucket(Bucket=self._bucket)
            logger.info(
                "S3 bucket validated: bucket=%s region=%s endpoint=%s",
                self._bucket,
                self._region,
                settings.S3_ENDPOINT or "aws",
            )
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            expected_region = e.response.get('ResponseMetadata', {}).get('HTTPHeaders', {}).get('x-amz-bucket-region')
            detail = f"S3 bucket validation failed for '{self._bucket}': {error_code} - {error_msg}"
            if expected_region and expected_region != self._region:
                detail += f" (hint: bucket region is {expected_region}; set S3_REGION/AWS_REGION accordingly)"
            if error_code in ("InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied"):
                detail += " (hint: credentials/role are missing access to this bucket)"
            raise RuntimeError(detail) from e

    def _validate_image(
        self,
        content: bytes,
        content_type: str,
        max_size: int,
    ) -> Tuple[bool, Optional[str]]:
        """Validate image file."""
        # Check content type
        if content_type not in self.ALLOWED_IMAGE_TYPES:
            return False, f"Invalid content type: {content_type}. Allowed: {list(self.ALLOWED_IMAGE_TYPES.keys())}"

        # Check size
        if len(content) > max_size:
            max_mb = max_size / (1024 * 1024)
            actual_mb = len(content) / (1024 * 1024)
            return False, f"File too large: {actual_mb:.1f}MB. Max: {max_mb:.0f}MB"

        # Basic magic byte validation
        if content_type == 'image/png' and not content.startswith(b'\x89PNG'):
            return False, "Invalid PNG file"
        if content_type == 'image/jpeg' and not content.startswith(b'\xff\xd8'):
            return False, "Invalid JPEG file"
        if content_type == 'image/gif' and not content.startswith(b'GIF'):
            return False, "Invalid GIF file"

        return True, None

    async def upload_brand_asset(
        self,
        content: bytes,
        filename: str,
        content_type: str,
        asset_type: str = "logo",  # logo, banner, icon, etc.
    ) -> UploadResult:
        """
        Upload a brand asset (logo, banner, etc.) to S3.

        Args:
            content: File content as bytes
            filename: Original filename
            content_type: MIME type
            asset_type: Type of asset (logo, banner, icon)

        Returns:
            UploadResult with URL on success
        """
        # Validate
        is_valid, error = self._validate_image(
            content, content_type, self.MAX_BRAND_ASSET_SIZE
        )
        if not is_valid:
            return UploadResult(success=False, error=error)

        # Generate key
        folder = f"assets/brand/{asset_type}"
        key = self._generate_key(folder, filename, content)

        try:
            # Upload to S3
            self.client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=content,
                ContentType=content_type,
                CacheControl='public, max-age=31536000',  # 1 year cache
                ACL='public-read',
            )

            url = self._get_public_url(key)

            logger.info(f"Uploaded brand asset: {key} -> {url}")

            return UploadResult(
                success=True,
                url=url,
                key=key,
                content_type=content_type,
                size_bytes=len(content),
            )

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', str(e))
            logger.error(f"S3 upload failed: {error_code} - {error_msg}")
            return UploadResult(success=False, error=f"Upload failed: {error_msg}")

        except Exception as e:
            logger.error(f"Storage upload error: {e}")
            return UploadResult(success=False, error=f"Upload failed: {str(e)}")

    async def upload_product_image(
        self,
        content: bytes,
        filename: str,
        content_type: str,
        product_type: str = "product",  # product, comic, funko
        product_id: Optional[int] = None,
    ) -> UploadResult:
        """
        Upload a product image to S3.

        Args:
            content: File content as bytes
            filename: Original filename
            content_type: MIME type
            product_type: Type of product
            product_id: Optional product ID for organization

        Returns:
            UploadResult with URL on success
        """
        # Validate
        is_valid, error = self._validate_image(
            content, content_type, self.MAX_PRODUCT_IMAGE_SIZE
        )
        if not is_valid:
            return UploadResult(success=False, error=error)

        # Generate key
        if product_id:
            folder = f"images/{product_type}/{product_id}"
        else:
            folder = f"images/{product_type}"
        key = self._generate_key(folder, filename, content)

        try:
            self.client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=content,
                ContentType=content_type,
                CacheControl='public, max-age=86400',  # 1 day cache
                ACL='public-read',
            )

            url = self._get_public_url(key)

            logger.info(f"Uploaded product image: {key}")

            return UploadResult(
                success=True,
                url=url,
                key=key,
                content_type=content_type,
                size_bytes=len(content),
            )

        except Exception as e:
            logger.error(f"Product image upload error: {e}")
            return UploadResult(success=False, error=f"Upload failed: {str(e)}")

    async def upload_bytes(
        self,
        content: bytes,
        key: str,
        content_type: str = "application/octet-stream",
        cache_control: str = "public, max-age=86400",
    ) -> Optional[str]:
        """
        Upload raw bytes to S3 with a custom key.

        Args:
            content: File content as bytes
            key: S3 key (path) for the object
            content_type: MIME type
            cache_control: Cache-Control header value

        Returns:
            Public URL on success, None on failure
        """
        if not self.is_configured():
            logger.error("S3 storage not configured")
            return None

        try:
            # Note: No ACL - bucket policy handles public access
            self.client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=content,
                ContentType=content_type,
                CacheControl=cache_control,
            )

            url = self._get_public_url(key)
            logger.info(f"Uploaded: {key}")
            return url

        except Exception as e:
            logger.error(f"Upload failed for {key}: {e}")
            return None

    async def delete_object(self, key: str) -> bool:
        """Delete an object from S3."""
        try:
            self.client.delete_object(Bucket=self._bucket, Key=key)
            logger.info(f"Deleted object: {key}")
            return True
        except Exception as e:
            logger.error(f"Delete failed for {key}: {e}")
            return False

    def is_configured(self) -> bool:
        """
        Check if S3 is configured.

        Note: We allow missing access/secret keys to support IAM/role-based
        auth; boto3 will fall back to the default credential chain.
        """
        return bool(settings.S3_BUCKET)
