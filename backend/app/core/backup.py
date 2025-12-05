"""
Database Backup Configuration and Utilities

P2-3: Database backup guidance and automation helpers

This module provides:
- Backup configuration for PostgreSQL
- Helper functions for automated backups
- Restore procedures documentation
- Integration with cloud storage (S3/R2)

Railway automatically provides point-in-time recovery for PostgreSQL,
but this module enables additional backup strategies for disaster recovery.
"""
import asyncio
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class BackupConfig:
    """
    Database backup configuration.

    For Railway PostgreSQL, automatic backups are enabled by default.
    This configuration is for additional manual/scheduled backups.
    """
    # Backup storage settings
    backup_dir: str = "/tmp/backups"
    retention_days: int = 30
    compress: bool = True

    # S3/R2 upload settings (optional)
    upload_to_s3: bool = False
    s3_bucket: str = ""
    s3_prefix: str = "db-backups/"

    # Notification settings
    notify_on_failure: bool = True
    notify_webhook: str = ""  # Slack/Discord webhook URL


# Default backup configuration
backup_config = BackupConfig(
    backup_dir=os.getenv("BACKUP_DIR", "/tmp/backups"),
    retention_days=int(os.getenv("BACKUP_RETENTION_DAYS", "30")),
    upload_to_s3=bool(os.getenv("BACKUP_UPLOAD_S3", "")),
    s3_bucket=settings.S3_BUCKET,
)


def get_database_url_for_backup() -> str:
    """
    Get database URL formatted for pg_dump.

    Converts asyncpg URL to standard PostgreSQL URL.
    """
    db_url = settings.DATABASE_URL

    # Convert asyncpg to psycopg2 format if needed
    if db_url.startswith("postgresql+asyncpg://"):
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")

    return db_url


def generate_backup_filename(prefix: str = "mdm_comics") -> str:
    """Generate timestamped backup filename."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}.sql.gz" if backup_config.compress else f"{prefix}_{timestamp}.sql"


async def create_backup(output_path: Optional[str] = None) -> dict:
    """
    Create a database backup using pg_dump.

    Args:
        output_path: Optional custom output path

    Returns:
        dict with backup status and metadata

    Note: Requires pg_dump to be installed on the system.
    """
    db_url = get_database_url_for_backup()
    filename = generate_backup_filename()
    backup_dir = Path(backup_config.backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)

    output_file = Path(output_path) if output_path else backup_dir / filename

    result = {
        "success": False,
        "filename": str(output_file),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "size_bytes": 0,
        "error": None,
    }

    try:
        # Build pg_dump command
        if backup_config.compress:
            # Pipe through gzip
            cmd = f'pg_dump "{db_url}" | gzip > "{output_file}"'
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        else:
            cmd = ["pg_dump", db_url, "-f", str(output_file)]
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            result["success"] = True
            result["size_bytes"] = output_file.stat().st_size if output_file.exists() else 0
            result["completed_at"] = datetime.now(timezone.utc).isoformat()
            logger.info(f"Backup created successfully: {output_file} ({result['size_bytes']} bytes)")
        else:
            result["error"] = stderr.decode()[:500]
            logger.error(f"Backup failed: {result['error']}")

    except FileNotFoundError:
        result["error"] = "pg_dump not found. Install PostgreSQL client tools."
        logger.error(result["error"])
    except Exception as e:
        result["error"] = str(e)[:500]
        logger.error(f"Backup error: {e}")

    return result


async def upload_to_s3(file_path: str, s3_key: Optional[str] = None) -> dict:
    """
    Upload backup file to S3/R2 storage.

    Args:
        file_path: Path to backup file
        s3_key: Optional custom S3 key

    Returns:
        dict with upload status
    """
    result = {
        "success": False,
        "s3_key": s3_key or f"{backup_config.s3_prefix}{Path(file_path).name}",
        "error": None,
    }

    if not backup_config.upload_to_s3:
        result["error"] = "S3 upload not configured"
        return result

    try:
        import boto3
        from botocore.config import Config

        # Configure S3 client
        s3_config = Config(
            signature_version='s3v4',
            retries={'max_attempts': 3}
        )

        client_kwargs = {
            "aws_access_key_id": settings.S3_ACCESS_KEY,
            "aws_secret_access_key": settings.S3_SECRET_KEY,
            "config": s3_config,
        }

        if settings.S3_ENDPOINT:
            client_kwargs["endpoint_url"] = settings.S3_ENDPOINT

        s3 = boto3.client("s3", **client_kwargs)

        # Upload file
        s3.upload_file(
            file_path,
            backup_config.s3_bucket,
            result["s3_key"],
        )

        result["success"] = True
        logger.info(f"Backup uploaded to S3: s3://{backup_config.s3_bucket}/{result['s3_key']}")

    except ImportError:
        result["error"] = "boto3 not installed. Run: pip install boto3"
    except Exception as e:
        result["error"] = str(e)[:500]
        logger.error(f"S3 upload error: {e}")

    return result


async def cleanup_old_backups(retention_days: Optional[int] = None) -> dict:
    """
    Remove backups older than retention period.

    Args:
        retention_days: Override default retention

    Returns:
        dict with cleanup statistics
    """
    retention = retention_days or backup_config.retention_days
    backup_dir = Path(backup_config.backup_dir)
    cutoff = datetime.now(timezone.utc).timestamp() - (retention * 24 * 60 * 60)

    result = {
        "files_removed": 0,
        "bytes_freed": 0,
        "errors": [],
    }

    if not backup_dir.exists():
        return result

    for file in backup_dir.glob("mdm_comics_*.sql*"):
        try:
            if file.stat().st_mtime < cutoff:
                size = file.stat().st_size
                file.unlink()
                result["files_removed"] += 1
                result["bytes_freed"] += size
                logger.info(f"Removed old backup: {file}")
        except Exception as e:
            result["errors"].append(f"{file}: {e}")
            logger.error(f"Failed to remove {file}: {e}")

    return result


def get_restore_instructions() -> str:
    """
    Get database restore instructions.

    Returns formatted instructions for restoring from backup.
    """
    return """
# Database Restore Instructions

## From Local Backup File

1. Stop the application to prevent writes during restore
2. Run restore command:

   # For compressed backup (.sql.gz):
   gunzip -c backup_file.sql.gz | psql DATABASE_URL

   # For uncompressed backup (.sql):
   psql DATABASE_URL < backup_file.sql

## From Railway Console

1. Go to Railway dashboard
2. Select PostgreSQL service
3. Click "Backups" tab
4. Choose point-in-time or snapshot restore

## From S3/R2 Storage

1. Download backup:
   aws s3 cp s3://bucket/db-backups/backup.sql.gz ./

2. Restore:
   gunzip -c backup.sql.gz | psql DATABASE_URL

## Important Notes

- Always test restores on a staging environment first
- Verify data integrity after restore
- Update any cached data (Redis, CDN, etc.)
- Railway provides automatic point-in-time recovery for 7 days

## Verify Restore

After restore, verify key tables:

   psql DATABASE_URL -c "SELECT COUNT(*) FROM users;"
   psql DATABASE_URL -c "SELECT COUNT(*) FROM products;"
   psql DATABASE_URL -c "SELECT COUNT(*) FROM orders;"
"""


# ============== BACKUP ENDPOINT DATA ==============

def get_backup_status() -> dict:
    """
    Get current backup configuration and status.

    Returns information suitable for admin dashboard or health check.
    """
    backup_dir = Path(backup_config.backup_dir)

    # Find most recent backup
    recent_backup = None
    if backup_dir.exists():
        backups = sorted(backup_dir.glob("mdm_comics_*.sql*"), key=lambda f: f.stat().st_mtime, reverse=True)
        if backups:
            recent = backups[0]
            recent_backup = {
                "filename": recent.name,
                "size_bytes": recent.stat().st_size,
                "created_at": datetime.fromtimestamp(recent.stat().st_mtime, tz=timezone.utc).isoformat(),
            }

    return {
        "config": {
            "backup_dir": backup_config.backup_dir,
            "retention_days": backup_config.retention_days,
            "compress": backup_config.compress,
            "upload_to_s3": backup_config.upload_to_s3,
        },
        "recent_backup": recent_backup,
        "railway_managed": True,  # Railway provides automatic backups
        "railway_backup_info": {
            "point_in_time_recovery": "7 days",
            "snapshot_backups": "automatic",
            "docs": "https://docs.railway.app/databases/postgresql#backups",
        },
    }
