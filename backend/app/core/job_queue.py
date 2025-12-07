"""
Job Queue Configuration

v1.5.0: Outreach System - ARQ worker settings with proper function imports
"""
from arq import cron
from arq.connections import RedisSettings

from app.core.config import settings

# Import actual functions - FIX-016: Use function references, not strings
from app.jobs.newsletter import (
    generate_weekly_newsletter,
    send_newsletter_batch,
    process_confirmation_email,
)
from app.jobs.analytics import (
    sync_price_changelog,
    generate_weekly_report,
)
from app.jobs.content import (
    generate_content_batch,
)
from app.jobs.social import (
    post_to_bluesky,
    process_content_queue,
)


def parse_redis_url(url: str) -> RedisSettings:
    """Parse Redis URL into ARQ RedisSettings."""
    if not url:
        # Default to localhost
        return RedisSettings()

    # Parse redis://host:port/db or redis://:password@host:port/db
    from urllib.parse import urlparse

    parsed = urlparse(url)

    return RedisSettings(
        host=parsed.hostname or "localhost",
        port=parsed.port or 6379,
        password=parsed.password,
        database=int(parsed.path.lstrip("/") or 0) if parsed.path else 0,
    )


class WorkerSettings:
    """
    ARQ Worker configuration.

    Schedules:
    - Price sync: Every 6 hours
    - Content generation: Every 2 hours
    - Social queue: Every 30 minutes
    - Newsletter: Sundays at 10 AM UTC
    - Weekly report: Mondays at 8 AM UTC
    """

    # Function imports (not strings) - FIX-016
    functions = [
        generate_weekly_newsletter,
        send_newsletter_batch,
        process_confirmation_email,
        sync_price_changelog,
        generate_weekly_report,
        generate_content_batch,
        post_to_bluesky,
        process_content_queue,
    ]

    # Cron schedules
    cron_jobs = [
        # Price changelog sync - every 6 hours
        cron(
            sync_price_changelog,
            hour={0, 6, 12, 18},
            minute=0,
        ),

        # Content generation - every 2 hours
        cron(
            generate_content_batch,
            hour={0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22},
            minute=15,
        ),

        # Social queue processing - every 30 minutes
        cron(
            process_content_queue,
            minute={0, 30},
        ),

        # Weekly newsletter - Sundays at 10 AM UTC
        cron(
            generate_weekly_newsletter,
            weekday="sun",
            hour=10,
            minute=0,
        ),

        # Weekly analytics report - Mondays at 8 AM UTC
        cron(
            generate_weekly_report,
            weekday="mon",
            hour=8,
            minute=0,
        ),
    ]

    # Redis connection
    redis_settings = parse_redis_url(settings.ARQ_REDIS_URL)

    # Worker settings
    max_jobs = 10
    job_timeout = 300  # 5 minutes
    keep_result = 3600  # 1 hour

    # Retry settings
    max_tries = 3
    retry_delay = 60  # 1 minute between retries
