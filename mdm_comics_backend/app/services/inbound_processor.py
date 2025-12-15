"""
Inbound Cover Processor

v1.21.0: Automated inbound cover processing
- Watches Inbound folder for new cover images
- Parses filename to extract metadata (publisher, series, vol, issue)
- Queues to Match Review
- Moves file to permanent location based on parsed metadata
- Runs every 5 minutes via cron

Filename format expected in Inbound:
  {publisher} {series} v{vol} {issue}{variant} {covertype}.jpg

Examples:
  marvel deadpool v5 1a BACK.jpg
  dc batman v3 50 cgc 9.8 FRONT.jpg
  image saga v1 1.jpg
"""

import logging
import os
import re
import shutil
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional, List, Tuple
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import async_session_maker
from app.services.cover_ingestion import CoverIngestionService, CoverMetadata, get_cover_ingestion_service

logger = logging.getLogger(__name__)

# Base path for cover images
COVERS_BASE_PATH = "F:/apps/mdm_comics/assets/comic_book_covers"
INBOUND_PATH = os.path.join(COVERS_BASE_PATH, "Inbound")

# Publisher name normalization
PUBLISHER_FOLDERS = {
    'marvel': 'marvel',
    'dc': 'dc',
    'image': 'image',
    'dark horse': 'dark horse comics',
    'darkhouse': 'dark horse comics',
    'idw': 'idw publishing',
    'boom': 'boom studios',
    'dynamite': 'dynamite entertainment',
    'valiant': 'valiant comics',
    'archie': 'archie comics',
    'aftershock': 'after shock comics',
    'after shock': 'after shock comics',
    'black mask': 'black mask studios',
    'bongo': 'bongo comics',
    'harvey': 'harvey comics',
}


@dataclass
class InboundResult:
    """Result of processing an inbound file."""
    success: bool
    file_path: str
    queue_id: Optional[int] = None
    destination_path: Optional[str] = None
    error: Optional[str] = None
    skipped: bool = False
    skip_reason: Optional[str] = None


class InboundProcessor:
    """
    Processes cover images from the Inbound folder.

    Workflow:
    1. Scan Inbound folder for new images
    2. Parse filename to extract metadata
    3. Queue to Match Review via CoverIngestionService
    4. Move file to permanent location
    """

    SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}

    # Regex to parse inbound filenames
    # Format: {publisher} {series} v{vol} {issue}{variant} {covertype}.jpg
    FILENAME_PATTERN = re.compile(
        r'^(?P<publisher>\w+(?:\s+\w+)?)\s+'  # Publisher (1-2 words)
        r'(?P<series>.+?)\s+'                  # Series name
        r'v(?P<vol>\d+)\s+'                    # Volume
        r'(?P<issue>\d+(?:\.\d+)?)'            # Issue number
        r'(?P<variant>[a-zA-Z]{0,3})?'         # Optional variant
        r'(?:\s+(?P<extra>.+?))?'              # Optional extra (CGC, cover type)
        r'\.(?P<ext>jpe?g|png|webp)$',         # Extension
        re.IGNORECASE
    )

    # Simpler pattern for files without full structure
    SIMPLE_PATTERN = re.compile(
        r'^(?P<series>.+?)\s+'
        r'(?:v(?P<vol>\d+)\s+)?'
        r'(?P<issue>\d+(?:\.\d+)?)'
        r'(?P<variant>[a-zA-Z]{0,3})?'
        r'(?:\s+(?P<extra>.+?))?'
        r'\.(?P<ext>jpe?g|png|webp)$',
        re.IGNORECASE
    )

    def __init__(self, db: AsyncSession):
        self.db = db
        self.cover_service = get_cover_ingestion_service(db)

    def parse_inbound_filename(self, filename: str) -> Optional[CoverMetadata]:
        """
        Parse an inbound filename to extract metadata.

        Expected format: {publisher} {series} v{vol} {issue}{variant} {extra}.jpg

        Examples:
        - "marvel deadpool v5 1a BACK.jpg"
        - "dc batman v3 50 cgc 9.8 FRONT.jpg"
        - "image saga v1 1.jpg"
        """
        # Try full pattern first
        match = self.FILENAME_PATTERN.match(filename)

        if match:
            groups = match.groupdict()
            publisher = self._normalize_publisher(groups['publisher'])
            series = groups['series'].strip()
            volume = int(groups['vol'])
            issue = groups['issue']
            variant = groups.get('variant', '').upper() if groups.get('variant') else None
            extra = groups.get('extra', '') or ''

            # Parse extra for CGC grade and cover type
            cgc_grade = None
            cover_type = "FRONT"

            if 'cgc' in extra.lower():
                cgc_match = re.search(r'cgc\s*(\d+(?:\.\d+)?)', extra, re.IGNORECASE)
                if cgc_match:
                    cgc_grade = float(cgc_match.group(1))

            if 'back' in extra.lower():
                cover_type = "BACK"

            # Filter variant from cover type keywords
            if variant in ('BAC', 'FRO', 'CGC'):
                variant = None

            return CoverMetadata(
                publisher=publisher,
                series=series,
                volume=volume,
                issue_number=issue,
                variant_code=variant,
                cover_type=cover_type,
                cgc_grade=cgc_grade,
                filename=filename,
                full_path=""  # Will be set later
            )

        # Try simple pattern (no publisher prefix)
        match = self.SIMPLE_PATTERN.match(filename)
        if match:
            groups = match.groupdict()
            series = groups['series'].strip()
            volume = int(groups['vol']) if groups.get('vol') else 1
            issue = groups['issue']
            variant = groups.get('variant', '').upper() if groups.get('variant') else None
            extra = groups.get('extra', '') or ''

            # Try to infer publisher from series name
            publisher = self._infer_publisher(series)

            cgc_grade = None
            cover_type = "FRONT"

            if 'cgc' in extra.lower():
                cgc_match = re.search(r'cgc\s*(\d+(?:\.\d+)?)', extra, re.IGNORECASE)
                if cgc_match:
                    cgc_grade = float(cgc_match.group(1))

            if 'back' in extra.lower():
                cover_type = "BACK"

            if variant in ('BAC', 'FRO', 'CGC'):
                variant = None

            return CoverMetadata(
                publisher=publisher,
                series=series,
                volume=volume,
                issue_number=issue,
                variant_code=variant,
                cover_type=cover_type,
                cgc_grade=cgc_grade,
                filename=filename,
                full_path=""
            )

        return None

    def _normalize_publisher(self, publisher: str) -> str:
        """Normalize publisher name to folder name."""
        pub_lower = publisher.lower().strip()
        return PUBLISHER_FOLDERS.get(pub_lower, pub_lower)

    def _infer_publisher(self, series: str) -> str:
        """Try to infer publisher from series name."""
        series_lower = series.lower()

        # Marvel characters/series
        marvel_keywords = ['spider', 'avenger', 'x-men', 'wolverine', 'iron man',
                          'captain america', 'thor', 'hulk', 'deadpool', 'venom',
                          'fantastic four', 'daredevil', 'punisher']
        if any(kw in series_lower for kw in marvel_keywords):
            return 'marvel'

        # DC characters/series
        dc_keywords = ['batman', 'superman', 'wonder woman', 'flash', 'aquaman',
                      'green lantern', 'justice league', 'teen titans', 'nightwing',
                      'harley quinn', 'joker', 'robin']
        if any(kw in series_lower for kw in dc_keywords):
            return 'dc'

        # Image series
        image_keywords = ['spawn', 'walking dead', 'saga', 'invincible', 'savage dragon']
        if any(kw in series_lower for kw in image_keywords):
            return 'image'

        return 'unknown'

    def build_destination_path(self, metadata: CoverMetadata) -> str:
        """
        Build the permanent destination path for a cover image.

        Structure: {publisher}/{series}/{series} v{vol}/{series} v{vol} {issue}/
        """
        publisher = metadata.publisher.lower()
        series = metadata.series
        vol = metadata.volume or 1
        issue = metadata.issue_number

        # Build folder names matching existing structure
        series_folder = series
        volume_folder = f"{series} v{vol}"
        issue_folder = f"{series} v{vol} {issue}"

        # Build full path
        dest_dir = os.path.join(
            COVERS_BASE_PATH,
            publisher,
            series_folder,
            volume_folder,
            issue_folder
        )

        return dest_dir

    def build_destination_filename(self, metadata: CoverMetadata) -> str:
        """Build the filename for permanent storage."""
        parts = [metadata.series, f"v{metadata.volume or 1}", metadata.issue_number]

        if metadata.variant_code:
            parts[-1] = f"{parts[-1]}{metadata.variant_code.lower()}"

        if metadata.cgc_grade:
            parts.append(f"cgc {metadata.cgc_grade}")

        parts.append(metadata.cover_type)

        # Get extension from original filename
        ext = os.path.splitext(metadata.filename)[1]

        return " ".join(parts) + ext

    async def process_inbound_file(self, file_path: str) -> InboundResult:
        """
        Process a single file from the Inbound folder.

        1. Parse filename
        2. Queue to Match Review
        3. Move to permanent location
        """
        result = InboundResult(success=False, file_path=file_path)
        filename = os.path.basename(file_path)

        # Skip system files
        if filename.lower() in ('thumbs.db', '.ds_store', 'desktop.ini'):
            result.skipped = True
            result.skip_reason = "System file"
            return result

        # Check extension
        ext = os.path.splitext(filename)[1].lower()
        if ext not in self.SUPPORTED_EXTENSIONS:
            result.skipped = True
            result.skip_reason = f"Unsupported extension: {ext}"
            return result

        try:
            # Parse filename
            metadata = self.parse_inbound_filename(filename)

            if not metadata:
                # Fallback: use cover_ingestion's parser with Inbound as base
                metadata = self.cover_service.parse_folder_path(file_path, INBOUND_PATH)

            metadata.full_path = file_path

            # Queue to Match Review
            ingestion_result = await self.cover_service.ingest_single_cover(
                file_path=file_path,
                base_path=INBOUND_PATH,
                user_id=1  # System user
            )

            if ingestion_result.skipped:
                result.skipped = True
                result.skip_reason = ingestion_result.skip_reason
                return result

            if not ingestion_result.success:
                result.error = ingestion_result.error
                return result

            result.queue_id = ingestion_result.queue_id

            # Build destination and move file
            dest_dir = self.build_destination_path(metadata)
            dest_filename = self.build_destination_filename(metadata)
            dest_path = os.path.join(dest_dir, dest_filename)

            # Upload image to S3 and update comic_issue record
            s3_key = None
            s3_url = None
            if ingestion_result.comic_issue_id:
                from app.services.storage import StorageService
                from app.models.comic_data import ComicIssue
                import hashlib

                storage = StorageService()
                if storage.is_configured():
                    # Read file content
                    with open(file_path, 'rb') as f:
                        content = f.read()

                    # Determine content type
                    ext = os.path.splitext(file_path)[1].lower()
                    content_type = 'image/jpeg'
                    if ext == '.png':
                        content_type = 'image/png'
                    elif ext == '.webp':
                        content_type = 'image/webp'

                    # Upload to S3 with comic_issue_id in key
                    s3_key = f"covers/comic_{ingestion_result.comic_issue_id}{ext}"
                    upload_result = await storage.upload_product_image(
                        content=content,
                        filename=dest_filename,
                        content_type=content_type,
                        product_type="cover",
                        product_id=ingestion_result.comic_issue_id
                    )

                    if upload_result.success:
                        s3_key = upload_result.key
                        s3_url = upload_result.url

                        # Update comic_issue record with S3 key
                        comic_issue = await self.db.execute(
                            select(ComicIssue).where(ComicIssue.id == ingestion_result.comic_issue_id)
                        )
                        issue_record = comic_issue.scalar_one_or_none()
                        if issue_record:
                            issue_record.cover_s3_key = s3_key
                            issue_record.image = s3_url  # Also set the image URL
                            issue_record.image_acquired_at = datetime.now(timezone.utc)
                            issue_record.image_checksum = hashlib.sha256(content).hexdigest()
                            await self.db.flush()
                            logger.info(f"Updated comic_issue {ingestion_result.comic_issue_id} with S3 key: {s3_key}")
                    else:
                        logger.warning(f"S3 upload failed: {upload_result.error}")

            # Update the queue item with S3 info
            from app.models.match_review import MatchReviewQueue
            queue_item = await self.db.execute(
                select(MatchReviewQueue).where(MatchReviewQueue.id == ingestion_result.queue_id)
            )
            queue_record = queue_item.scalar_one_or_none()
            if queue_record and queue_record.candidate_data:
                candidate_data = queue_record.candidate_data.copy()
                candidate_data['local_cover_path'] = dest_path
                candidate_data['s3_key'] = s3_key
                candidate_data['s3_url'] = s3_url
                candidate_data['matched_issue_id'] = ingestion_result.comic_issue_id
                queue_record.candidate_data = candidate_data
                await self.db.flush()

            # Create destination directory and move file
            os.makedirs(dest_dir, exist_ok=True)

            # Move file (copy + delete to handle cross-drive moves)
            shutil.copy2(file_path, dest_path)
            os.remove(file_path)

            result.destination_path = dest_path
            result.success = True

            logger.info(f"Processed inbound: {filename} -> {dest_path} (Queue #{result.queue_id})")

            return result

        except Exception as e:
            logger.error(f"Error processing inbound {file_path}: {e}")
            result.error = str(e)
            return result

    async def process_inbound_folder(self) -> dict:
        """
        Process all files in the Inbound folder.

        Returns summary of processing results.
        """
        results = {
            "processed": 0,
            "queued": 0,
            "moved": 0,
            "skipped": 0,
            "errors": 0,
            "error_details": []
        }

        if not os.path.exists(INBOUND_PATH):
            logger.warning(f"Inbound folder does not exist: {INBOUND_PATH}")
            return results

        # Get all files in Inbound (non-recursive)
        files = []
        for filename in os.listdir(INBOUND_PATH):
            file_path = os.path.join(INBOUND_PATH, filename)
            if os.path.isfile(file_path):
                files.append(file_path)

        if not files:
            logger.info("No files in Inbound folder")
            return results

        logger.info(f"Processing {len(files)} files from Inbound folder")

        for file_path in files:
            result = await self.process_inbound_file(file_path)
            results["processed"] += 1

            if result.skipped:
                results["skipped"] += 1
            elif result.success:
                results["queued"] += 1
                if result.destination_path:
                    results["moved"] += 1
            else:
                results["errors"] += 1
                if result.error:
                    results["error_details"].append(f"{result.file_path}: {result.error}")

        logger.info(
            f"Inbound processing complete: {results['queued']} queued, "
            f"{results['moved']} moved, {results['skipped']} skipped, "
            f"{results['errors']} errors"
        )

        return results


async def run_inbound_processor():
    """
    Run the inbound processor job.

    Called by cron scheduler every 5 minutes.

    First run: Processes ENTIRE comic_book_covers folder (initial sweep)
    Subsequent runs: Only watches Inbound folder for new additions
    """
    from sqlalchemy import text

    logger.info("Starting inbound processor job")

    async with async_session_maker() as db:
        # Check if first run has been completed
        result = await db.execute(text("""
            SELECT state_data FROM job_checkpoints
            WHERE job_name = 'inbound_processor'
        """))
        row = result.fetchone()

        first_run_completed = False
        if row and row[0]:
            state_data = row[0] if isinstance(row[0], dict) else {}
            first_run_completed = state_data.get('initial_sweep_completed', False)

        processor = InboundProcessor(db)

        if not first_run_completed:
            # FIRST RUN: Process entire comic_book_covers folder
            logger.info("FIRST RUN: Processing entire comic_book_covers folder")

            results = {
                "initial_sweep": True,
                "processed": 0,
                "queued": 0,
                "skipped": 0,
                "errors": 0,
                "error_details": []
            }

            # Use cover_ingestion service to process all existing covers
            cover_service = get_cover_ingestion_service(db)
            ingestion_result = await cover_service.ingest_folder(
                folder_path=COVERS_BASE_PATH,
                user_id=1  # System user
            )

            results["processed"] = ingestion_result.get("total_files", 0)
            results["queued"] = ingestion_result.get("queued", 0)
            results["skipped"] = ingestion_result.get("skipped", 0)
            results["errors"] = ingestion_result.get("errors", 0)

            logger.info(
                f"Initial sweep complete: {results['queued']} queued, "
                f"{results['skipped']} skipped, {results['errors']} errors"
            )

            # Mark initial sweep as completed
            await db.execute(text("""
                INSERT INTO job_checkpoints (job_name, state_data, created_at, updated_at)
                VALUES ('inbound_processor', :state_data, NOW(), NOW())
                ON CONFLICT (job_name) DO UPDATE SET
                    state_data = :state_data,
                    updated_at = NOW()
            """), {"state_data": '{"initial_sweep_completed": true}'})

            await db.commit()
            return results

        # SUBSEQUENT RUNS: Only watch Inbound folder
        # Ensure Inbound folder exists
        os.makedirs(INBOUND_PATH, exist_ok=True)

        results = await processor.process_inbound_folder()
        await db.commit()

    return results


# For testing
if __name__ == "__main__":
    import asyncio
    asyncio.run(run_inbound_processor())
