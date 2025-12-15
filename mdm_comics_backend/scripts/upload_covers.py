#!/usr/bin/env python3
"""
Local Cover Upload Script

Uploads cover images from local folders to S3 and queues them for Match Review.

Usage:
    python scripts/upload_covers.py --folder "F:/apps/mdm_comics/assets/comic_book_covers"
    python scripts/upload_covers.py --folder "F:/apps/mdm_comics/assets/comic_book_covers/Inbound" --limit 50

Requirements:
    pip install boto3 requests python-dotenv

Environment variables (or .env file):
    S3_ACCESS_KEY - AWS/R2 access key
    S3_SECRET_KEY - AWS/R2 secret key
    S3_BUCKET - Bucket name
    S3_REGION - Region (default: auto)
    S3_ENDPOINT - Custom endpoint for R2/MinIO (optional)
    API_URL - Backend API URL (default: https://mdm-comics-backend-development.up.railway.app)
    API_TOKEN - JWT token for authentication
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

import boto3
import requests
from botocore.config import Config
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_REGION = os.getenv('S3_REGION', 'auto')
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
API_URL = os.getenv('API_URL', 'https://mdm-comics-backend-development.up.railway.app')
API_TOKEN = os.getenv('API_TOKEN')

# Supported image extensions
SUPPORTED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}

# Publisher folder name normalization
PUBLISHER_FOLDERS = {
    'marvel': 'marvel',
    'dc': 'dc',
    'image': 'image',
    'dark horse': 'dark horse comics',
    'dark horse comics': 'dark horse comics',
    'idw': 'idw publishing',
    'idw publishing': 'idw publishing',
    'boom': 'boom studios',
    'boom studios': 'boom studios',
    'dynamite': 'dynamite entertainment',
    'dynamite entertainment': 'dynamite entertainment',
    'valiant': 'valiant comics',
    'archie': 'archie comics',
    'aftershock': 'after shock comics',
    'after shock': 'after shock comics',
}


@dataclass
class CoverMetadata:
    """Metadata extracted from cover image path."""
    publisher: str
    series: str
    volume: int
    issue_number: str
    variant_code: Optional[str] = None
    cover_type: str = "FRONT"
    cgc_grade: Optional[float] = None
    filename: str = ""
    full_path: str = ""
    file_hash: str = ""


@dataclass
class UploadResult:
    """Result of uploading a single cover."""
    success: bool
    file_path: str
    s3_key: Optional[str] = None
    s3_url: Optional[str] = None
    queue_id: Optional[int] = None
    error: Optional[str] = None
    skipped: bool = False
    skip_reason: Optional[str] = None


class S3Uploader:
    """Handles S3 uploads."""

    def __init__(self):
        if not all([S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET]):
            raise ValueError("Missing S3 credentials. Set S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET")

        config = Config(
            signature_version='s3v4',
            retries={'max_attempts': 3, 'mode': 'standard'}
        )

        client_kwargs = {
            'service_name': 's3',
            'region_name': S3_REGION,
            'aws_access_key_id': S3_ACCESS_KEY,
            'aws_secret_access_key': S3_SECRET_KEY,
            'config': config,
        }

        if S3_ENDPOINT:
            client_kwargs['endpoint_url'] = S3_ENDPOINT

        self.client = boto3.client(**client_kwargs)
        self.bucket = S3_BUCKET

    def upload(self, file_path: str, s3_key: str, content_type: str = 'image/jpeg') -> tuple[bool, Optional[str], Optional[str]]:
        """
        Upload a file to S3.

        Returns: (success, s3_url, error)
        """
        try:
            with open(file_path, 'rb') as f:
                content = f.read()

            self.client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=content,
                ContentType=content_type,
            )

            # Generate URL
            if S3_ENDPOINT:
                url = f"{S3_ENDPOINT.rstrip('/')}/{self.bucket}/{s3_key}"
            else:
                url = f"https://{self.bucket}.s3.{S3_REGION}.amazonaws.com/{s3_key}"

            return True, url, None

        except Exception as e:
            return False, None, str(e)


class CoverParser:
    """Parses cover image paths to extract metadata."""

    # Pattern for folder structure: publisher/series/series vol X/series vol X #/filename
    FOLDER_PATTERN = re.compile(
        r'^(?P<publisher>[^/\\]+)[/\\]'
        r'(?P<series>[^/\\]+)[/\\]'
        r'(?P<series_vol>[^/\\]+)[/\\]'
        r'(?P<issue_folder>[^/\\]+)[/\\]'
        r'(?P<filename>.+)$'
    )

    # Pattern for volume folder: "series v1" or "series vol 1"
    VOLUME_PATTERN = re.compile(r'v(?:ol)?\.?\s*(\d+)', re.IGNORECASE)

    # Pattern for issue folder: "series v1 1" or "series v1 #1"
    ISSUE_PATTERN = re.compile(r'(\d+(?:\.\d+)?)\s*$')

    # Pattern for filename: "series v1 1a FRONT.jpg"
    FILENAME_PATTERN = re.compile(
        r'(?P<issue>\d+(?:\.\d+)?)'
        r'(?P<variant>[a-zA-Z]{1,3})?'
        r'(?:\s+(?P<extra>.+?))?'
        r'\.(?P<ext>jpe?g|png|webp)$',
        re.IGNORECASE
    )

    def parse(self, file_path: str, base_path: str) -> Optional[CoverMetadata]:
        """Parse a file path to extract cover metadata."""
        # Get relative path from base
        try:
            rel_path = os.path.relpath(file_path, base_path)
        except ValueError:
            rel_path = file_path

        # Normalize path separators
        rel_path = rel_path.replace('\\', '/')

        # Try folder structure pattern
        match = self.FOLDER_PATTERN.match(rel_path)
        if match:
            return self._parse_folder_structure(match, file_path)

        # Fallback: try to parse just the filename
        return self._parse_filename_only(file_path)

    def _parse_folder_structure(self, match: re.Match, file_path: str) -> CoverMetadata:
        """Parse metadata from folder structure."""
        groups = match.groupdict()

        publisher = self._normalize_publisher(groups['publisher'])
        series = groups['series']
        series_vol = groups['series_vol']
        issue_folder = groups['issue_folder']
        filename = groups['filename']

        # Extract volume number
        vol_match = self.VOLUME_PATTERN.search(series_vol)
        volume = int(vol_match.group(1)) if vol_match else 1

        # Extract issue number from folder name
        issue_match = self.ISSUE_PATTERN.search(issue_folder)
        issue_number = issue_match.group(1) if issue_match else "1"

        # Parse filename for variant and cover type
        variant_code = None
        cover_type = "FRONT"
        cgc_grade = None

        fname_match = self.FILENAME_PATTERN.search(filename)
        if fname_match:
            variant = fname_match.group('variant')
            if variant and variant.upper() not in ('FRO', 'BAC', 'CGC'):
                variant_code = variant.upper()

            extra = fname_match.group('extra') or ''
            if 'back' in extra.lower():
                cover_type = "BACK"

            cgc_match = re.search(r'cgc\s*(\d+(?:\.\d+)?)', extra, re.IGNORECASE)
            if cgc_match:
                cgc_grade = float(cgc_match.group(1))

        # Calculate file hash
        file_hash = self._calculate_hash(file_path)

        return CoverMetadata(
            publisher=publisher,
            series=series,
            volume=volume,
            issue_number=issue_number,
            variant_code=variant_code,
            cover_type=cover_type,
            cgc_grade=cgc_grade,
            filename=filename,
            full_path=file_path,
            file_hash=file_hash
        )

    def _parse_filename_only(self, file_path: str) -> Optional[CoverMetadata]:
        """Fallback: parse just the filename."""
        filename = os.path.basename(file_path)

        # Try to extract basic info from filename
        # Pattern: "publisher series v1 1a FRONT.jpg"
        pattern = re.compile(
            r'^(?P<prefix>.+?)\s+'
            r'v(?P<vol>\d+)\s+'
            r'(?P<issue>\d+(?:\.\d+)?)'
            r'(?P<variant>[a-zA-Z]{1,3})?'
            r'(?:\s+(?P<extra>.+?))?'
            r'\.(?P<ext>jpe?g|png|webp)$',
            re.IGNORECASE
        )

        match = pattern.match(filename)
        if not match:
            return None

        groups = match.groupdict()
        prefix = groups['prefix']

        # Try to split prefix into publisher and series
        words = prefix.split()
        if len(words) >= 2:
            publisher = self._normalize_publisher(words[0])
            series = ' '.join(words[1:])
        else:
            publisher = 'unknown'
            series = prefix

        variant = groups.get('variant')
        if variant and variant.upper() in ('FRO', 'BAC', 'CGC'):
            variant = None

        extra = groups.get('extra') or ''
        cover_type = "BACK" if 'back' in extra.lower() else "FRONT"

        cgc_grade = None
        cgc_match = re.search(r'cgc\s*(\d+(?:\.\d+)?)', extra, re.IGNORECASE)
        if cgc_match:
            cgc_grade = float(cgc_match.group(1))

        return CoverMetadata(
            publisher=publisher,
            series=series,
            volume=int(groups['vol']),
            issue_number=groups['issue'],
            variant_code=variant.upper() if variant else None,
            cover_type=cover_type,
            cgc_grade=cgc_grade,
            filename=filename,
            full_path=file_path,
            file_hash=self._calculate_hash(file_path)
        )

    def _normalize_publisher(self, publisher: str) -> str:
        """Normalize publisher name."""
        pub_lower = publisher.lower().strip()
        return PUBLISHER_FOLDERS.get(pub_lower, pub_lower)

    def _calculate_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of file."""
        try:
            with open(file_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception:
            return ""


class MatchReviewAPI:
    """Calls the Railway backend API to create Match Review entries."""

    def __init__(self, api_url: str, token: str):
        self.api_url = api_url.rstrip('/')
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        })

    def queue_cover(self, metadata: CoverMetadata, s3_key: str, s3_url: str) -> tuple[bool, Optional[int], Optional[str]]:
        """
        Queue a cover for Match Review.

        Returns: (success, queue_id, error)
        """
        try:
            payload = {
                "source_type": "cover_upload",
                "source_id": metadata.file_hash,
                "candidate_data": {
                    "publisher": metadata.publisher,
                    "series": metadata.series,
                    "volume": metadata.volume,
                    "issue_number": metadata.issue_number,
                    "variant_code": metadata.variant_code,
                    "cover_type": metadata.cover_type,
                    "cgc_grade": metadata.cgc_grade,
                    "s3_key": s3_key,
                    "s3_url": s3_url,
                    "original_filename": metadata.filename,
                    "file_hash": metadata.file_hash,
                },
                "confidence_score": 5.0,  # Medium confidence - needs review
                "disposition": "review",
            }

            response = self.session.post(
                f"{self.api_url}/api/admin/match-queue/queue-cover",
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                return True, data.get('id'), data.get('message')
            elif response.status_code == 409:
                # Already exists
                return True, None, "Already queued"
            else:
                return False, None, f"HTTP {response.status_code}: {response.text}"

        except Exception as e:
            return False, None, str(e)

    def check_exists(self, file_hash: str) -> bool:
        """Check if a cover with this hash already exists in the queue."""
        try:
            response = self.session.get(
                f"{self.api_url}/api/admin/match-queue/check-hash/{file_hash}",
                timeout=10
            )
            if response.status_code == 200:
                return response.json().get('exists', False)
            return False
        except Exception:
            return False


def collect_files(folder_path: str, limit: int = 0) -> List[str]:
    """Collect all image files from a folder recursively."""
    files = []

    for root, dirs, filenames in os.walk(folder_path):
        # Skip system folders
        dirs[:] = [d for d in dirs if not d.startswith('.')]

        for filename in filenames:
            # Skip system files
            if filename.lower() in ('thumbs.db', '.ds_store', 'desktop.ini'):
                continue

            ext = os.path.splitext(filename)[1].lower()
            if ext in SUPPORTED_EXTENSIONS:
                files.append(os.path.join(root, filename))

                if limit > 0 and len(files) >= limit:
                    return files

    return files


def get_content_type(file_path: str) -> str:
    """Get content type for a file."""
    ext = os.path.splitext(file_path)[1].lower()
    return {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.webp': 'image/webp',
    }.get(ext, 'image/jpeg')


def main():
    parser = argparse.ArgumentParser(description='Upload cover images to S3 and queue for Match Review')
    parser.add_argument('--folder', required=True, help='Folder path containing cover images')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of files to process (0 = unlimited)')
    parser.add_argument('--dry-run', action='store_true', help='Parse files without uploading')
    parser.add_argument('--skip-existing', action='store_true', default=True, help='Skip files already in queue')
    args = parser.parse_args()

    folder_path = args.folder

    if not os.path.exists(folder_path):
        logger.error(f"Folder not found: {folder_path}")
        sys.exit(1)

    if not os.path.isdir(folder_path):
        logger.error(f"Not a directory: {folder_path}")
        sys.exit(1)

    # Validate configuration
    if not args.dry_run:
        if not all([S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET]):
            logger.error("Missing S3 credentials. Set S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET")
            sys.exit(1)

        if not API_TOKEN:
            logger.error("Missing API_TOKEN for authentication")
            sys.exit(1)

    # Collect files
    logger.info(f"Scanning folder: {folder_path}")
    files = collect_files(folder_path, args.limit)
    logger.info(f"Found {len(files)} image files")

    if not files:
        logger.info("No files to process")
        return

    # Initialize services
    parser_svc = CoverParser()
    uploader = None
    api = None

    if not args.dry_run:
        uploader = S3Uploader()
        api = MatchReviewAPI(API_URL, API_TOKEN)

    # Process files
    stats = {
        'processed': 0,
        'uploaded': 0,
        'queued': 0,
        'skipped': 0,
        'errors': 0,
    }

    for i, file_path in enumerate(files, 1):
        logger.info(f"[{i}/{len(files)}] Processing: {os.path.basename(file_path)}")

        # Parse metadata
        metadata = parser_svc.parse(file_path, folder_path)
        if not metadata:
            logger.warning(f"  Could not parse metadata, skipping")
            stats['skipped'] += 1
            continue

        stats['processed'] += 1

        if args.dry_run:
            logger.info(f"  Publisher: {metadata.publisher}")
            logger.info(f"  Series: {metadata.series}")
            logger.info(f"  Volume: {metadata.volume}")
            logger.info(f"  Issue: {metadata.issue_number}")
            logger.info(f"  Variant: {metadata.variant_code or 'N/A'}")
            logger.info(f"  Cover: {metadata.cover_type}")
            continue

        # Check if already exists
        if args.skip_existing and api.check_exists(metadata.file_hash):
            logger.info(f"  Already in queue, skipping")
            stats['skipped'] += 1
            continue

        # Generate S3 key
        s3_key = f"covers/{metadata.publisher}/{metadata.series}/v{metadata.volume}/{metadata.issue_number}"
        if metadata.variant_code:
            s3_key += f"_{metadata.variant_code}"
        s3_key += f"_{metadata.cover_type}"
        s3_key += f"_{metadata.file_hash[:8]}"
        ext = os.path.splitext(file_path)[1].lower()
        s3_key += ext

        # Upload to S3
        content_type = get_content_type(file_path)
        success, s3_url, error = uploader.upload(file_path, s3_key, content_type)

        if not success:
            logger.error(f"  S3 upload failed: {error}")
            stats['errors'] += 1
            continue

        stats['uploaded'] += 1
        logger.info(f"  Uploaded to S3: {s3_key}")

        # Queue for Match Review
        success, queue_id, error = api.queue_cover(metadata, s3_key, s3_url)

        if success:
            stats['queued'] += 1
            if queue_id:
                logger.info(f"  Queued for review: #{queue_id}")
            else:
                logger.info(f"  {error}")
        else:
            logger.error(f"  Queue failed: {error}")
            stats['errors'] += 1

    # Print summary
    logger.info("=" * 50)
    logger.info("SUMMARY")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Uploaded:  {stats['uploaded']}")
    logger.info(f"  Queued:    {stats['queued']}")
    logger.info(f"  Skipped:   {stats['skipped']}")
    logger.info(f"  Errors:    {stats['errors']}")


if __name__ == '__main__':
    main()
