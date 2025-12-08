"""
Meta Graph API Client

v1.5.1: Outreach System Phase 1 - Instagram/Facebook text-based posting

Supports:
- Facebook Page posts (text with optional link)
- Instagram Business/Creator posts (text-only captions require media)

Note: Instagram requires media. For Phase 1 (text-only), we post to Facebook.
Phase 2 will add AI image generation for Instagram.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import httpx

from app.core.config import settings
from app.core.redis_client import get_redis
from app.core.utils import utcnow

logger = logging.getLogger(__name__)


@dataclass
class PostResult:
    """Result of a social media post attempt."""
    success: bool
    post_id: Optional[str] = None
    post_url: Optional[str] = None
    platform: Optional[str] = None
    error: Optional[str] = None


class RateLimitError(Exception):
    """Raised when Meta API rate limit is hit."""
    pass


class AuthenticationError(Exception):
    """Raised when Meta API authentication fails."""
    pass


class MetaClient:
    """
    Meta Graph API client for Facebook and Instagram posting.

    Required environment variables:
    - META_ACCESS_TOKEN: Long-lived page access token
    - META_PAGE_ID: Facebook Page ID
    - META_INSTAGRAM_ACCOUNT_ID: Instagram Business Account ID (optional for Phase 2)

    Rate limits (per app):
    - Facebook: 200 posts per hour per page
    - Instagram: 25 posts per 24 hours
    """

    GRAPH_API_VERSION = "v18.0"
    GRAPH_API_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
    TOKEN_REDIS_KEY = "meta:token"
    RATE_LIMIT_REDIS_KEY = "meta:rate_limit"

    def __init__(self):
        self.access_token = getattr(settings, 'META_ACCESS_TOKEN', '')
        self.page_id = getattr(settings, 'META_PAGE_ID', '')
        self.instagram_account_id = getattr(settings, 'META_INSTAGRAM_ACCOUNT_ID', '')
        self._http_client: Optional[httpx.AsyncClient] = None
        self._lock = asyncio.Lock()

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create reusable HTTP client."""
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def close(self):
        """Close HTTP client on shutdown."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    def _validate_config(self, platform: str) -> Optional[str]:
        """Validate required configuration is present."""
        if not settings.MARKETING_SOCIAL_ENABLED:
            return "Social posting is disabled (MARKETING_SOCIAL_ENABLED=false)"

        if not self.access_token:
            return "META_ACCESS_TOKEN not configured"

        if platform == "facebook" and not self.page_id:
            return "META_PAGE_ID not configured"

        if platform == "instagram" and not self.instagram_account_id:
            return "META_INSTAGRAM_ACCOUNT_ID not configured"

        return None

    async def _check_rate_limit(self, platform: str) -> bool:
        """Check if we're within rate limits. Returns True if OK to post."""
        redis = await get_redis()
        if not redis:
            return True  # No Redis = no rate limiting (log warning)

        key = f"{self.RATE_LIMIT_REDIS_KEY}:{platform}"

        if platform == "facebook":
            # 200 posts per hour
            count = await redis.get(key)
            if count and int(count) >= 200:
                return False
            await redis.incr(key)
            await redis.expire(key, 3600)  # 1 hour TTL

        elif platform == "instagram":
            # 25 posts per 24 hours
            count = await redis.get(key)
            if count and int(count) >= 25:
                return False
            await redis.incr(key)
            await redis.expire(key, 86400)  # 24 hour TTL

        return True

    async def _handle_api_error(self, resp: httpx.Response) -> str:
        """Parse and return error message from Meta API response."""
        try:
            data = resp.json()
            error = data.get("error", {})
            message = error.get("message", resp.text)
            code = error.get("code", resp.status_code)

            # Check for specific error codes
            if code == 190:  # Invalid/expired token
                raise AuthenticationError(f"Meta API auth error: {message}")
            if code in (4, 17, 32, 613):  # Rate limit errors
                raise RateLimitError(f"Meta API rate limit: {message}")

            return f"Meta API error ({code}): {message}"
        except (json.JSONDecodeError, KeyError):
            return f"Meta API error: {resp.text}"

    async def create_facebook_post(
        self,
        text: str,
        link: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> PostResult:
        """
        Create a text post on the Facebook Page.

        Args:
            text: Post content (max 63,206 characters)
            link: Optional URL to attach to the post
            idempotency_key: Prevent duplicate posts

        Returns:
            PostResult with success status and post details
        """
        # Check idempotency
        if idempotency_key:
            redis = await get_redis()
            if redis:
                existing = await redis.get(f"meta:post:{idempotency_key}")
                if existing:
                    logger.info(f"Duplicate Facebook post prevented: {idempotency_key}")
                    data = json.loads(existing)
                    return PostResult(
                        success=True,
                        post_id=data["post_id"],
                        post_url=data["post_url"],
                        platform="facebook",
                    )

        # Validate configuration
        error = self._validate_config("facebook")
        if error:
            return PostResult(success=False, error=error, platform="facebook")

        # Check rate limit
        if not await self._check_rate_limit("facebook"):
            raise RateLimitError("Facebook rate limit exceeded (200/hour)")

        # Build post data
        post_data = {
            "message": text[:63206],  # Facebook character limit
            "access_token": self.access_token,
        }

        if link:
            post_data["link"] = link

        # Make API request
        http = await self._get_http_client()

        try:
            resp = await http.post(
                f"{self.GRAPH_API_BASE}/{self.page_id}/feed",
                data=post_data,
            )

            if resp.status_code == 429:
                raise RateLimitError("Facebook API rate limit exceeded")

            if resp.status_code != 200:
                error_msg = await self._handle_api_error(resp)
                return PostResult(success=False, error=error_msg, platform="facebook")

            data = resp.json()
            post_id = data.get("id", "")

            # Facebook post URL format
            post_url = f"https://www.facebook.com/{post_id.replace('_', '/posts/')}"

            # Store idempotency record
            if idempotency_key:
                redis = await get_redis()
                if redis:
                    await redis.setex(
                        f"meta:post:{idempotency_key}",
                        86400 * 7,  # 7 days
                        json.dumps({"post_id": post_id, "post_url": post_url}),
                    )

            logger.info(f"Posted to Facebook: {post_url}")

            return PostResult(
                success=True,
                post_id=post_id,
                post_url=post_url,
                platform="facebook",
            )

        except (RateLimitError, AuthenticationError):
            raise
        except Exception as e:
            logger.error(f"Facebook post error: {e}")
            return PostResult(success=False, error=str(e), platform="facebook")

    async def create_instagram_post(
        self,
        text: str,
        image_url: str,
        idempotency_key: Optional[str] = None,
    ) -> PostResult:
        """
        Create an Instagram post (requires image - Phase 2+).

        Instagram Content Publishing API requires:
        1. Create media container with image URL
        2. Publish the container

        Args:
            text: Caption (max 2,200 characters)
            image_url: Publicly accessible image URL (HTTPS, JPEG/PNG)
            idempotency_key: Prevent duplicate posts

        Returns:
            PostResult with success status and post details
        """
        # Check idempotency
        if idempotency_key:
            redis = await get_redis()
            if redis:
                existing = await redis.get(f"meta:instagram:{idempotency_key}")
                if existing:
                    logger.info(f"Duplicate Instagram post prevented: {idempotency_key}")
                    data = json.loads(existing)
                    return PostResult(
                        success=True,
                        post_id=data["post_id"],
                        post_url=data["post_url"],
                        platform="instagram",
                    )

        # Validate configuration
        error = self._validate_config("instagram")
        if error:
            return PostResult(success=False, error=error, platform="instagram")

        if not image_url:
            return PostResult(
                success=False,
                error="Instagram requires an image URL. Use Phase 2 AI image generation.",
                platform="instagram",
            )

        # Check rate limit
        if not await self._check_rate_limit("instagram"):
            raise RateLimitError("Instagram rate limit exceeded (25/24hr)")

        http = await self._get_http_client()

        try:
            # Step 1: Create media container
            container_resp = await http.post(
                f"{self.GRAPH_API_BASE}/{self.instagram_account_id}/media",
                data={
                    "image_url": image_url,
                    "caption": text[:2200],  # Instagram caption limit
                    "access_token": self.access_token,
                },
            )

            if container_resp.status_code != 200:
                error_msg = await self._handle_api_error(container_resp)
                return PostResult(success=False, error=error_msg, platform="instagram")

            container_id = container_resp.json().get("id")

            # Step 2: Wait for media processing (poll status)
            # Instagram processes media async, may take a few seconds
            for _ in range(10):
                status_resp = await http.get(
                    f"{self.GRAPH_API_BASE}/{container_id}",
                    params={
                        "fields": "status_code",
                        "access_token": self.access_token,
                    },
                )

                if status_resp.status_code == 200:
                    status = status_resp.json().get("status_code")
                    if status == "FINISHED":
                        break
                    if status == "ERROR":
                        return PostResult(
                            success=False,
                            error="Instagram media processing failed",
                            platform="instagram",
                        )

                await asyncio.sleep(2)

            # Step 3: Publish the container
            publish_resp = await http.post(
                f"{self.GRAPH_API_BASE}/{self.instagram_account_id}/media_publish",
                data={
                    "creation_id": container_id,
                    "access_token": self.access_token,
                },
            )

            if publish_resp.status_code != 200:
                error_msg = await self._handle_api_error(publish_resp)
                return PostResult(success=False, error=error_msg, platform="instagram")

            data = publish_resp.json()
            post_id = data.get("id", "")

            # Get permanent URL via API
            permalink_resp = await http.get(
                f"{self.GRAPH_API_BASE}/{post_id}",
                params={
                    "fields": "permalink",
                    "access_token": self.access_token,
                },
            )

            post_url = ""
            if permalink_resp.status_code == 200:
                post_url = permalink_resp.json().get("permalink", "")

            # Store idempotency record
            if idempotency_key:
                redis = await get_redis()
                if redis:
                    await redis.setex(
                        f"meta:instagram:{idempotency_key}",
                        86400 * 7,
                        json.dumps({"post_id": post_id, "post_url": post_url}),
                    )

            logger.info(f"Posted to Instagram: {post_url}")

            return PostResult(
                success=True,
                post_id=post_id,
                post_url=post_url,
                platform="instagram",
            )

        except (RateLimitError, AuthenticationError):
            raise
        except Exception as e:
            logger.error(f"Instagram post error: {e}")
            return PostResult(success=False, error=str(e), platform="instagram")

    async def get_page_insights(self) -> Dict[str, Any]:
        """Get basic page insights (engagement metrics)."""
        error = self._validate_config("facebook")
        if error:
            return {"error": error}

        http = await self._get_http_client()

        try:
            resp = await http.get(
                f"{self.GRAPH_API_BASE}/{self.page_id}",
                params={
                    "fields": "name,followers_count,fan_count",
                    "access_token": self.access_token,
                },
            )

            if resp.status_code == 200:
                return resp.json()
            return {"error": await self._handle_api_error(resp)}

        except Exception as e:
            return {"error": str(e)}
