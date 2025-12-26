"""
Cover Ingestion Service

v1.0.0: Local cover image ingestion pipeline
- Parses folder structure: publisher/series/volume/issue
- Extracts metadata from filenames (variant, CGC grade, FRONT/BACK)
- Fuzzy matches to comic_issues (2.5M records)
- Queues ALL items to Match Review for human approval
- Products are only created after approval in Match Review screen
- Supports incremental ingestion (tracks processed files)

This service handles both:
1. Bulk ingestion from local folders (15k books initially)
2. Mobile scanning workflow (future)
"""

import logging
import os
import re
import hashlib
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any
from pathlib import Path

from sqlalchemy import select, func, text, or_, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.product import Product
from app.models.comic_data import ComicIssue, ComicSeries, ComicPublisher
from app.models.match_review import MatchReviewQueue
from app.services.storage import StorageService, UploadResult
from app.services.match_review_service import MatchReviewService, MatchDisposition, route_match

logger = logging.getLogger(__name__)


# Publisher name normalization map
PUBLISHER_ALIASES = {
    'dc': ['dc', 'dc comics'],
    'marvel': ['marvel', 'marvel comics'],
    'image': ['image', 'image comics'],
    'dark horse': ['dark horse', 'dark horse comics'],
    'idw': ['idw', 'idw publishing'],
    'boom': ['boom', 'boom studios', 'boom! studios'],
    'dynamite': ['dynamite', 'dynamite entertainment'],
    'valiant': ['valiant', 'valiant comics'],
    'archie': ['archie', 'archie comics'],
    'aftershock': ['after shock', 'aftershock', 'after shock comics', 'aftershock comics'],
}


@dataclass
class CoverMetadata:
    """Extracted metadata from folder path and filename."""
    publisher: str
    series: str
    volume: Optional[int] = None
    issue_number: str = ""
    variant_code: Optional[str] = None  # 'a', 'b', 'BS', etc.
    cover_type: str = "FRONT"  # FRONT or BACK
    cgc_grade: Optional[float] = None
    filename: str = ""
    full_path: str = ""
    file_hash: Optional[str] = None

    # Matching results
    matched_issue_id: Optional[int] = None
    match_score: int = 0
    match_method: str = ""


@dataclass
class IngestionResult:
    """Result of a single cover ingestion."""
    success: bool
    file_path: str
    metadata: Optional[CoverMetadata] = None
    queue_id: Optional[int] = None  # Match Review queue ID
    comic_issue_id: Optional[int] = None
    match_score: int = 0
    disposition: str = ""  # auto_link, review, no_match
    error: Optional[str] = None
    skipped: bool = False
    skip_reason: Optional[str] = None


@dataclass
class BatchIngestionResult:
    """Result of batch ingestion."""
    total_files: int = 0
    processed: int = 0
    queued_for_review: int = 0
    high_confidence: int = 0  # Score >= 8
    medium_confidence: int = 0  # Score 5-7
    low_confidence: int = 0  # Score < 5
    skipped: int = 0
    errors: int = 0
    error_details: List[str] = field(default_factory=list)


class CoverIngestionService:
    """
    Service for ingesting local cover images into the Match Review queue.

    Workflow:
    1. Scan folder structure
    2. Parse metadata from path/filename
    3. Match to comic_issues (fuzzy)
    4. Queue to Match Review for human approval
    5. Products created only after approval in Match Review
    """

    # Supported image extensions
    SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}

    # Regex patterns for parsing
    VOLUME_PATTERN = re.compile(r'v(?:ol(?:ume)?)?\.?\s*(\d+)', re.IGNORECASE)
    ISSUE_PATTERN = re.compile(r'(\d+(?:\.\d+)?)\s*([a-zA-Z]{0,3})?\s*(FRONT|BACK)?', re.IGNORECASE)
    CGC_PATTERN = re.compile(r'CGC\s*(\d+(?:\.\d+)?)', re.IGNORECASE)

    def __init__(self, db: AsyncSession, storage: Optional[StorageService] = None):
        self.db = db
        self.storage = storage or StorageService()
        self.review_service = MatchReviewService(db)

    def parse_folder_path(self, file_path: str, base_path: str) -> CoverMetadata:
        """
        Parse folder structure to extract metadata.

        Expected structure:
        base_path/publisher/series/[volume]/issue/filename.jpg

        Examples:
        - marvel/deadpool/deadpool v5/deadpool v5 1/deadpool v5 1a BACK.jpg
        - dc/Batman/Batman v3/Batman v3 50/Batman_50BS_Lucio_Parillo_Trade_Dress CGC 9.6/file.jpg
        """
        rel_path = os.path.relpath(file_path, base_path)
        parts = Path(rel_path).parts
        filename = parts[-1]

        metadata = CoverMetadata(
            publisher="",
            series="",
            filename=filename,
            full_path=file_path
        )

        if len(parts) < 2:
            return metadata

        # First part is publisher
        metadata.publisher = self._normalize_publisher(parts[0])

        # Second part is series
        if len(parts) >= 2:
            metadata.series = parts[1] if len(parts) > 2 else parts[0]

        # Look for volume in folder names
        for part in parts[1:-1]:  # Exclude publisher and filename
            vol_match = self.VOLUME_PATTERN.search(part)
            if vol_match:
                metadata.volume = int(vol_match.group(1))
                break

        # Parse filename for issue details
        self._parse_filename(filename, metadata)

        # Fallback: if issue number not found in filename, check parent folder
        if not metadata.issue_number and len(parts) >= 2:
            parent_folder = parts[-2]
            # Try to extract issue from parent folder
            # Patterns: 
            # 1. "spawn_v1_1" -> 1
            # 2. "Series vVol 1" -> 1
            # 3. "1" -> 1
            
            # Look for number at end of string, possibly preceded by _ or space
            # e.g. "spawn_v1_1" matches "_1" -> 1
            # "Deadpool v5 1" matches " 1" -> 1
            
            # Pattern: (anything)(separator)(issue_num)(end)
            # where separator is _ or space or nothing if pure number
            
            folder_issue_pattern = re.compile(r'(?:^|[\s_])(\d+(?:\.\d+)?)$', re.IGNORECASE)
            folder_match = folder_issue_pattern.search(parent_folder)
            
            if folder_match:
                metadata.issue_number = folder_match.group(1)
            
            # Also check for variant in folder if not in filename? 
            # (Keeping it simple for now, user asked about data points/structure)

        # Check for CGC grade in folder path
        full_path_lower = file_path.lower()
        cgc_match = self.CGC_PATTERN.search(full_path_lower)
        if cgc_match:
            metadata.cgc_grade = float(cgc_match.group(1))

        # Generate file hash for deduplication
        try:
            with open(file_path, 'rb') as f:
                metadata.file_hash = hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            logger.warning(f"Could not hash file {file_path}: {e}")

        return metadata

    def _normalize_publisher(self, name: str) -> str:
        """Normalize publisher name to canonical form."""
        name_lower = name.lower().strip()

        for canonical, aliases in PUBLISHER_ALIASES.items():
            if name_lower in aliases:
                return canonical.title()

        return name.title()

    def _parse_filename(self, filename: str, metadata: CoverMetadata) -> None:
        """
        Extract issue number, variant code, and cover type from filename.

        Examples:
        - "deadpool v5 1a BACK.jpg" -> issue=1, variant=a, cover=BACK
        - "Batman v3 50BS cgc 9.6 FRONT.jpg" -> issue=50, variant=BS, cover=FRONT
        - "ASM-252 - Full Rear Cover.jpg" -> issue=252, cover=BACK
        """
        name = os.path.splitext(filename)[0]

        # Check for BACK/FRONT suffix
        if 'back' in name.lower() or 'rear' in name.lower():
            metadata.cover_type = "BACK"
        elif 'front' in name.lower():
            metadata.cover_type = "FRONT"

        # Extract CGC grade from filename
        cgc_match = self.CGC_PATTERN.search(name)
        if cgc_match and not metadata.cgc_grade:
            metadata.cgc_grade = float(cgc_match.group(1))

        # Extract issue number and variant
        # Try common patterns
        patterns = [
            # "series v# ##variant" - e.g., "deadpool v5 1a"
            r'v\d+\s+(\d+(?:\.\d+)?)\s*([a-zA-Z]{1,3})?\b',
            # "series ##variant" - e.g., "ASM-252"
            r'[\s\-_](\d+(?:\.\d+)?)\s*([a-zA-Z]{1,3})?\b',
            # Just numbers at end
            r'(\d+(?:\.\d+)?)\s*([a-zA-Z]{1,3})?\s*(?:BACK|FRONT|CGC)?',
        ]

        for pattern in patterns:
            match = re.search(pattern, name, re.IGNORECASE)
            if match:
                metadata.issue_number = match.group(1)
                if match.group(2):
                    variant = match.group(2).upper()
                    # Filter out BACK/FRONT/CGC from variant
                    if variant not in ('BACK', 'FRONT', 'CGC', 'JPG', 'PNG'):
                        metadata.variant_code = variant
                break

    async def match_to_comic_issue(self, metadata: CoverMetadata) -> Tuple[Optional[int], int, str]:
        """
        Match cover metadata to a comic_issue record.

        Returns: (issue_id, confidence_score, match_method)

        Match strategies (in priority order):
        1. Exact series + issue number + volume
        2. Series name fuzzy + issue number
        3. Publisher + series fuzzy + issue number
        """
        if not metadata.series or not metadata.issue_number:
            return None, 0, "insufficient_data"

        # Normalize series name for matching
        series_normalized = self._normalize_series_name(metadata.series)

        # Strategy 1: Exact match on series name + issue + volume
        if metadata.volume:
            result = await self.db.execute(
                select(ComicIssue)
                .join(ComicSeries, ComicIssue.series_id == ComicSeries.id)
                .where(
                    func.lower(ComicSeries.name).contains(series_normalized.lower()),
                    ComicSeries.volume == metadata.volume,
                    ComicIssue.number == metadata.issue_number
                )
                .limit(5)
            )
            issues = result.scalars().all()

            if len(issues) == 1:
                return issues[0].id, 9, "exact_series_volume_issue"
            elif len(issues) > 1:
                # Multiple matches - pick best by publisher
                for issue in issues:
                    if issue.publisher_name and metadata.publisher.lower() in issue.publisher_name.lower():
                        return issue.id, 8, "exact_series_volume_issue_publisher"
                return issues[0].id, 7, "exact_series_volume_issue_ambiguous"

        # Strategy 2: Series name + issue number (no volume)
        result = await self.db.execute(
            select(ComicIssue)
            .join(ComicSeries, ComicIssue.series_id == ComicSeries.id)
            .where(
                func.lower(ComicSeries.name).contains(series_normalized.lower()),
                ComicIssue.number == metadata.issue_number
            )
            .limit(10)
        )
        issues = result.scalars().all()

        if len(issues) == 1:
            return issues[0].id, 8, "series_issue"
        elif issues:
            # Filter by publisher if available
            for issue in issues:
                if issue.publisher_name and metadata.publisher.lower() in issue.publisher_name.lower():
                    return issue.id, 7, "series_issue_publisher"
            return issues[0].id, 5, "series_issue_ambiguous"

        # Strategy 3: Fuzzy search using ILIKE
        fuzzy_pattern = f"%{series_normalized}%"
        result = await self.db.execute(
            select(ComicIssue)
            .join(ComicSeries, ComicIssue.series_id == ComicSeries.id)
            .where(
                ComicSeries.name.ilike(fuzzy_pattern),
                ComicIssue.number == metadata.issue_number
            )
            .limit(10)
        )
        issues = result.scalars().all()

        if len(issues) == 1:
            return issues[0].id, 6, "fuzzy_series_issue"
        elif issues:
            # Filter by publisher
            for issue in issues:
                if issue.publisher_name and metadata.publisher.lower() in issue.publisher_name.lower():
                    return issue.id, 5, "fuzzy_series_issue_publisher"
            return issues[0].id, 4, "fuzzy_series_issue_ambiguous"

        # No match found
        return None, 0, "no_match"

    def _normalize_series_name(self, name: str) -> str:
        """Normalize series name for matching."""
        # Remove volume suffix
        name = re.sub(r'\s*v\d+\s*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*vol(?:ume)?\s*\d+\s*$', '', name, flags=re.IGNORECASE)

        # Remove common suffixes
        name = re.sub(r'\s*\(\d{4}\)$', '', name)  # Year in parens

        return name.strip()

    def _build_product_name(self, metadata: CoverMetadata) -> str:
        """Build descriptive product name for display."""
        parts = []

        # Series name
        parts.append(metadata.series)

        # Volume
        if metadata.volume:
            parts.append(f"Vol. {metadata.volume}")

        # Issue number
        if metadata.issue_number:
            parts.append(f"#{metadata.issue_number}")

        # Variant
        if metadata.variant_code:
            parts.append(f"({metadata.variant_code} Variant)")

        # CGC grade
        if metadata.cgc_grade:
            parts.append(f"CGC {metadata.cgc_grade}")

        return " ".join(parts) if parts else "Unknown Comic"

    async def ingest_single_cover(
        self,
        file_path: str,
        base_path: str,
        user_id: int
    ) -> IngestionResult:
        """
        Ingest a single cover image by queuing it for Match Review.

        Args:
            file_path: Full path to image file
            base_path: Base path for relative path calculation
            user_id: User performing ingestion

        Returns:
            IngestionResult with queue_id for Match Review
        """
        result = IngestionResult(success=False, file_path=file_path)

        # Check file exists and is supported
        if not os.path.exists(file_path):
            result.error = "File not found"
            return result

        ext = os.path.splitext(file_path)[1].lower()
        if ext not in self.SUPPORTED_EXTENSIONS:
            result.skipped = True
            result.skip_reason = f"Unsupported extension: {ext}"
            return result

        # Skip Thumbs.db and other system files
        filename = os.path.basename(file_path)
        if filename.lower() in ('thumbs.db', '.ds_store', 'desktop.ini'):
            result.skipped = True
            result.skip_reason = "System file"
            return result

        try:
            # Parse metadata
            metadata = self.parse_folder_path(file_path, base_path)
            result.metadata = metadata

            # Check if already in queue (dedup by file hash)
            if metadata.file_hash:
                existing = await self.db.execute(
                    select(MatchReviewQueue).where(
                        MatchReviewQueue.entity_type == "cover_ingestion",
                        MatchReviewQueue.candidate_id == metadata.file_hash
                    )
                )
                if existing.scalar_one_or_none():
                    result.skipped = True
                    result.skip_reason = "Already in review queue"
                    return result

            # Match to comic_issue
            issue_id, score, method = await self.match_to_comic_issue(metadata)
            metadata.matched_issue_id = issue_id
            metadata.match_score = score
            metadata.match_method = method

            result.comic_issue_id = issue_id
            result.match_score = score

            # Determine disposition for display
            disposition = route_match(method, score, 1 if issue_id else 0)
            result.disposition = disposition.value

            # Get comic issue details if matched
            comic_issue_data = None
            if issue_id:
                comic_result = await self.db.execute(
                    select(ComicIssue).where(ComicIssue.id == issue_id)
                )
                comic_issue = comic_result.scalar_one_or_none()
                if comic_issue:
                    comic_issue_data = {
                        "id": comic_issue.id,
                        "series_name": comic_issue.series_name,
                        "number": comic_issue.number,
                        "publisher_name": comic_issue.publisher_name,
                        "image": comic_issue.image,
                        "price_loose": float(comic_issue.price_loose) if comic_issue.price_loose else None,
                        "price_cgc_98": float(comic_issue.price_cgc_98) if comic_issue.price_cgc_98 else None,
                        "pricecharting_id": comic_issue.pricecharting_id,
                    }

            # Build product name for display
            product_name = self._build_product_name(metadata)

            # Upload to S3 if configured (enables cover display in Match Review)
            s3_url = None
            s3_key = None
            storage = StorageService()
            if storage.is_configured():
                try:
                    with open(file_path, 'rb') as f:
                        content = f.read()

                    # Determine content type from extension
                    ext = Path(file_path).suffix.lower()
                    content_types = {
                        '.jpg': 'image/jpeg',
                        '.jpeg': 'image/jpeg',
                        '.png': 'image/png',
                        '.webp': 'image/webp',
                    }
                    content_type = content_types.get(ext, 'image/jpeg')

                    # Upload to S3 under covers folder
                    upload_result = await storage.upload_product_image(
                        content=content,
                        filename=metadata.filename,
                        content_type=content_type,
                        product_type="covers",
                    )

                    if upload_result.success:
                        s3_url = upload_result.url
                        s3_key = upload_result.key
                        logger.info(f"Uploaded cover to S3: {s3_key}")
                    else:
                        logger.warning(f"S3 upload failed for {file_path}: {upload_result.error}")
                except Exception as e:
                    logger.warning(f"S3 upload error for {file_path}: {e}")

            # Queue to Match Review - ALL items go here for human approval
            queue_item = await self.review_service.add_to_queue(
                entity_type="cover_ingestion",
                entity_id=0,  # No product yet - created on approval
                candidate_source="local_folder",
                candidate_id=metadata.file_hash or file_path,
                candidate_name=product_name,
                candidate_data={
                    # File info
                    "file_path": metadata.full_path,
                    "filename": metadata.filename,
                    "cover_type": metadata.cover_type,
                    # S3 URL for cover display (if uploaded)
                    "s3_url": s3_url,
                    "s3_key": s3_key,
                    # Parsed metadata
                    "publisher": metadata.publisher,
                    "series": metadata.series,
                    "volume": metadata.volume,
                    "issue_number": metadata.issue_number,
                    "variant_code": metadata.variant_code,
                    "cgc_grade": metadata.cgc_grade,
                    # Matched comic issue data
                    "matched_comic_issue": comic_issue_data,
                    # For product creation on approval
                    "product_template": {
                        "name": product_name,
                        "category": "comics",
                        "subcategory": metadata.publisher,
                        "publisher": metadata.publisher,
                        "issue_number": metadata.issue_number,
                        "cgc_grade": metadata.cgc_grade,
                        "is_graded": metadata.cgc_grade is not None,
                        "stock": 1,
                    }
                },
                match_method=method,
                match_score=score,
                match_details={
                    "matched_issue_id": issue_id,
                    "disposition": disposition.value,
                }
            )

            result.queue_id = queue_item.id
            result.success = True

            await self.db.commit()

            logger.info(
                f"Queued cover for review: {file_path} -> Queue #{queue_item.id} "
                f"(match_score={score}, method={method})"
            )

            return result

        except Exception as e:
            logger.error(f"Error ingesting {file_path}: {e}")
            result.error = str(e)
            await self.db.rollback()
            return result

    async def ingest_folder(
        self,
        folder_path: str,
        user_id: int,
        limit: Optional[int] = None,
        progress_callback: Optional[callable] = None
    ) -> BatchIngestionResult:
        """
        Ingest all cover images from a folder recursively.

        All items are queued to Match Review for human approval.

        Args:
            folder_path: Root folder to scan
            user_id: User performing ingestion
            limit: Max files to process (for testing)
            progress_callback: Callback(current, total) for progress updates

        Returns:
            BatchIngestionResult
        """
        result = BatchIngestionResult()

        # Collect all image files
        image_files = []
        for root, dirs, files in os.walk(folder_path):
            # Skip web-optimized subfolders
            if 'web' in dirs:
                dirs.remove('web')

            for filename in files:
                ext = os.path.splitext(filename)[1].lower()
                if ext in self.SUPPORTED_EXTENSIONS:
                    image_files.append(os.path.join(root, filename))

        result.total_files = len(image_files)

        if limit:
            image_files = image_files[:limit]

        # Process each file
        for i, file_path in enumerate(image_files):
            if progress_callback:
                progress_callback(i + 1, result.total_files)

            ingestion = await self.ingest_single_cover(
                file_path=file_path,
                base_path=folder_path,
                user_id=user_id
            )

            result.processed += 1

            if ingestion.skipped:
                result.skipped += 1
            elif ingestion.success:
                result.queued_for_review += 1

                # Track confidence levels
                if ingestion.match_score >= 8:
                    result.high_confidence += 1
                elif ingestion.match_score >= 5:
                    result.medium_confidence += 1
                else:
                    result.low_confidence += 1
            else:
                result.errors += 1
                if ingestion.error:
                    result.error_details.append(f"{file_path}: {ingestion.error}")

        logger.info(
            f"Batch ingestion complete: {result.queued_for_review} queued for review "
            f"({result.high_confidence} high, {result.medium_confidence} medium, "
            f"{result.low_confidence} low confidence), {result.errors} errors"
        )

        return result

    async def scan_folder_preview(
        self,
        folder_path: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Preview what would be queued from a folder (without creating queue items).

        Returns list of parsed metadata for review before actual ingestion.
        """
        previews = []
        count = 0

        for root, dirs, files in os.walk(folder_path):
            if 'web' in dirs:
                dirs.remove('web')

            for filename in files:
                if count >= limit:
                    break

                ext = os.path.splitext(filename)[1].lower()
                if ext not in self.SUPPORTED_EXTENSIONS:
                    continue

                # Skip system files
                if filename.lower() in ('thumbs.db', '.ds_store', 'desktop.ini'):
                    continue

                file_path = os.path.join(root, filename)
                metadata = self.parse_folder_path(file_path, folder_path)

                # Try to match
                issue_id, score, method = await self.match_to_comic_issue(metadata)

                previews.append({
                    "file_path": file_path,
                    "publisher": metadata.publisher,
                    "series": metadata.series,
                    "volume": metadata.volume,
                    "issue_number": metadata.issue_number,
                    "variant_code": metadata.variant_code,
                    "cgc_grade": metadata.cgc_grade,
                    "cover_type": metadata.cover_type,
                    "matched_issue_id": issue_id,
                    "match_score": score,
                    "match_method": method,
                    "disposition": route_match(method, score, 1 if issue_id else 0).value,
                    "product_name": self._build_product_name(metadata),
                })

                count += 1

        return previews


# Singleton instance
_cover_ingestion_service: Optional[CoverIngestionService] = None


def get_cover_ingestion_service(db: AsyncSession) -> CoverIngestionService:
    """Get or create cover ingestion service instance."""
    return CoverIngestionService(db)
