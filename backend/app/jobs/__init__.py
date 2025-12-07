"""
Jobs Package

v1.5.0: Outreach System - Background job definitions for ARQ
"""
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

__all__ = [
    "generate_weekly_newsletter",
    "send_newsletter_batch",
    "process_confirmation_email",
    "sync_price_changelog",
    "generate_weekly_report",
    "generate_content_batch",
    "post_to_bluesky",
    "process_content_queue",
]
