"""
Image upload validation utilities

P2-1: Centralized validation for image uploads
- File type validation via magic bytes (not just content-type header)
- File size limits
- Image dimension validation
- Malicious content detection
"""
import io
from typing import Tuple, Optional
from fastapi import HTTPException, UploadFile


# Allowed image types with their magic bytes
IMAGE_SIGNATURES = {
    "image/jpeg": [
        bytes([0xFF, 0xD8, 0xFF, 0xE0]),  # JFIF
        bytes([0xFF, 0xD8, 0xFF, 0xE1]),  # EXIF
        bytes([0xFF, 0xD8, 0xFF, 0xE2]),  # ICC
        bytes([0xFF, 0xD8, 0xFF, 0xE8]),  # SPIFF
    ],
    "image/png": [
        bytes([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]),  # PNG
    ],
    "image/gif": [
        b"GIF87a",
        b"GIF89a",
    ],
    "image/webp": [
        # RIFF....WEBP (bytes 0-3 and 8-11)
        b"RIFF",
    ],
}

# Default limits
DEFAULT_MAX_SIZE_MB = 10
DEFAULT_MAX_DIMENSION = 8192  # 8K resolution max
DEFAULT_MIN_DIMENSION = 10  # Minimum 10x10 pixels


def validate_image_magic_bytes(content: bytes) -> Optional[str]:
    """
    Validate image by checking magic bytes (file signature).
    Returns detected MIME type or None if not a valid image.
    """
    for mime_type, signatures in IMAGE_SIGNATURES.items():
        for sig in signatures:
            if content.startswith(sig):
                # Special case for WebP - check WEBP marker at offset 8
                if mime_type == "image/webp":
                    if len(content) >= 12 and content[8:12] == b"WEBP":
                        return mime_type
                else:
                    return mime_type
    return None


def validate_image_dimensions(content: bytes, max_dim: int = DEFAULT_MAX_DIMENSION, min_dim: int = DEFAULT_MIN_DIMENSION) -> Tuple[int, int]:
    """
    Validate image dimensions using PIL.
    Returns (width, height) or raises HTTPException.
    """
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(content))
        width, height = img.size

        if width > max_dim or height > max_dim:
            raise HTTPException(
                status_code=400,
                detail=f"Image dimensions exceed maximum of {max_dim}x{max_dim} pixels"
            )

        if width < min_dim or height < min_dim:
            raise HTTPException(
                status_code=400,
                detail=f"Image dimensions must be at least {min_dim}x{min_dim} pixels"
            )

        return width, height
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid or corrupted image file: {str(e)}"
        )


async def validate_image_upload(
    file: UploadFile,
    allowed_types: list = None,
    max_size_mb: float = DEFAULT_MAX_SIZE_MB,
    validate_dimensions: bool = True,
    max_dimension: int = DEFAULT_MAX_DIMENSION,
) -> bytes:
    """
    Comprehensive image upload validation.

    P2-1: Validates:
    - Content-Type header (first check)
    - Magic bytes (actual file content)
    - File size
    - Image dimensions (optional)

    Args:
        file: FastAPI UploadFile
        allowed_types: List of allowed MIME types (default: jpeg, png)
        max_size_mb: Maximum file size in MB
        validate_dimensions: Whether to check image dimensions
        max_dimension: Maximum width/height in pixels

    Returns:
        bytes: The validated file content

    Raises:
        HTTPException: On validation failure
    """
    if allowed_types is None:
        allowed_types = ["image/jpeg", "image/png"]

    # 1. Check Content-Type header (untrusted but fast first check)
    if file.content_type not in allowed_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type. Allowed types: {', '.join(allowed_types)}"
        )

    # 2. Read file content
    content = await file.read()

    # 3. Check file size
    max_bytes = int(max_size_mb * 1024 * 1024)
    if len(content) > max_bytes:
        raise HTTPException(
            status_code=400,
            detail=f"File size exceeds maximum of {max_size_mb}MB"
        )

    if len(content) == 0:
        raise HTTPException(
            status_code=400,
            detail="Empty file uploaded"
        )

    # 4. Validate magic bytes (actual file type check)
    detected_type = validate_image_magic_bytes(content)
    if not detected_type:
        raise HTTPException(
            status_code=400,
            detail="File does not appear to be a valid image (invalid file signature)"
        )

    if detected_type not in allowed_types:
        raise HTTPException(
            status_code=400,
            detail=f"Detected file type '{detected_type}' is not allowed. Allowed: {', '.join(allowed_types)}"
        )

    # 5. Validate dimensions (also catches corrupted images)
    if validate_dimensions:
        validate_image_dimensions(content, max_dimension)

    return content
