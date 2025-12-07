"""
Bluesky AT Protocol Client

v1.5.0: Outreach System - Social media posting to Bluesky
"""
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional, List

import httpx

from app.core.config import settings
from app.core.redis_client import get_redis
from app.core.utils import utcnow

logger = logging.getLogger(__name__)


@dataclass
class PostResult:
    success: bool
    post_id: Optional[str] = None
    post_url: Optional[str] = None
    error: Optional[str] = None


class RateLimitError(Exception):
    pass


class AuthenticationError(Exception):
    pass


class BlueskyClient:
    """Bluesky AT Protocol client with httpx."""

    PDS_URL = "https://bsky.social"
    TOKEN_REFRESH_THRESHOLD_MINUTES = 5
    SESSION_REDIS_KEY = "bluesky:session"
    SESSION_REDIS_TTL = 6600  # 110 minutes (token expires at 120)

    def __init__(self):
        self.handle = settings.BLUESKY_HANDLE
        self.password = settings.BLUESKY_APP_PASSWORD
        self.session = None
        self.session_expires_at: Optional[datetime] = None
        self._lock = asyncio.Lock()
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create reusable HTTP client."""
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def close(self):
        """Close HTTP client on shutdown."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    async def _load_cached_session(self) -> bool:
        """Load session from Redis if available."""
        redis = await get_redis()
        if not redis:
            return False

        cached = await redis.get(self.SESSION_REDIS_KEY)
        if not cached:
            return False

        try:
            data = json.loads(cached)
            self.session = {
                "accessJwt": data["accessJwt"],
                "refreshJwt": data["refreshJwt"],
                "did": data["did"],
            }
            self.session_expires_at = datetime.fromisoformat(data["expires_at"])

            if self._session_valid():
                logger.info("Loaded Bluesky session from Redis cache")
                return True
        except Exception as e:
            logger.warning(f"Failed to load cached session: {e}")

        return False

    async def _cache_session(self) -> None:
        """Cache session to Redis."""
        redis = await get_redis()
        if not redis or not self.session:
            return

        data = {
            **self.session,
            "expires_at": self.session_expires_at.isoformat() if self.session_expires_at else None,
        }
        await redis.setex(
            self.SESSION_REDIS_KEY,
            self.SESSION_REDIS_TTL,
            json.dumps(data),
        )

    def _session_valid(self) -> bool:
        if not self.session or not self.session_expires_at:
            return False
        return utcnow() < self.session_expires_at

    def _should_refresh(self) -> bool:
        if not self.session_expires_at:
            return False
        threshold = utcnow() + timedelta(minutes=self.TOKEN_REFRESH_THRESHOLD_MINUTES)
        return threshold >= self.session_expires_at

    async def _ensure_authenticated(self) -> None:
        """Ensure we have a valid session."""
        if not settings.MARKETING_SOCIAL_ENABLED:
            raise ValueError("Social posting is disabled (MARKETING_SOCIAL_ENABLED=false)")

        async with self._lock:
            if not self.session:
                await self._load_cached_session()

            if self._session_valid():
                return

            if self.session and self._should_refresh():
                await self._refresh_session()
            else:
                await self._create_session()

            await self._cache_session()

    async def _create_session(self) -> None:
        """Create new authentication session."""
        logger.info("Creating new Bluesky session")

        http = await self._get_http_client()
        resp = await http.post(
            f"{self.PDS_URL}/xrpc/com.atproto.server.createSession",
            json={"identifier": self.handle, "password": self.password},
        )

        if resp.status_code != 200:
            raise AuthenticationError(f"Bluesky auth failed: {resp.text}")

        data = resp.json()
        self.session = {
            "accessJwt": data["accessJwt"],
            "refreshJwt": data["refreshJwt"],
            "did": data["did"],
        }
        self.session_expires_at = utcnow() + timedelta(hours=2)
        logger.info(f"Bluesky session created, expires at {self.session_expires_at}")

    async def _refresh_session(self) -> None:
        """Refresh existing session."""
        logger.info("Refreshing Bluesky session")

        http = await self._get_http_client()
        resp = await http.post(
            f"{self.PDS_URL}/xrpc/com.atproto.server.refreshSession",
            headers={"Authorization": f"Bearer {self.session['refreshJwt']}"},
        )

        if resp.status_code != 200:
            logger.warning("Session refresh failed, creating new session")
            await self._create_session()
            return

        data = resp.json()
        self.session["accessJwt"] = data["accessJwt"]
        self.session["refreshJwt"] = data["refreshJwt"]
        self.session_expires_at = utcnow() + timedelta(hours=2)
        logger.info(f"Bluesky session refreshed, expires at {self.session_expires_at}")

    async def create_post(
        self,
        text: str,
        images: Optional[List[bytes]] = None,
        idempotency_key: Optional[str] = None,
    ) -> PostResult:
        """Create a post on Bluesky with idempotency protection."""
        # Check idempotency via Redis
        if idempotency_key:
            redis = await get_redis()
            if redis:
                existing = await redis.get(f"bluesky:post:{idempotency_key}")
                if existing:
                    logger.info(f"Duplicate post prevented: {idempotency_key}")
                    return PostResult(
                        success=True,
                        post_id=existing,
                        post_url=f"https://bsky.app/profile/{self.handle}/post/{existing.split('/')[-1]}",
                    )

        try:
            await self._ensure_authenticated()
        except ValueError as e:
            return PostResult(success=False, error=str(e))
        except AuthenticationError as e:
            return PostResult(success=False, error=str(e))

        record = {
            "$type": "app.bsky.feed.post",
            "text": text,
            "createdAt": utcnow().isoformat().replace("+00:00", "Z"),
        }

        if images:
            embed = await self._upload_images(images)
            if embed:
                record["embed"] = embed

        http = await self._get_http_client()
        resp = await http.post(
            f"{self.PDS_URL}/xrpc/com.atproto.repo.createRecord",
            headers={"Authorization": f"Bearer {self.session['accessJwt']}"},
            json={
                "repo": self.session["did"],
                "collection": "app.bsky.feed.post",
                "record": record,
            },
        )

        if resp.status_code == 429:
            raise RateLimitError("Bluesky rate limit exceeded")

        if resp.status_code == 401:
            await self._create_session()
            raise AuthenticationError("Session expired, please retry")

        if resp.status_code != 200:
            return PostResult(success=False, error=resp.text)

        data = resp.json()
        post_uri = data["uri"]
        post_id = post_uri.split("/")[-1]

        # Store idempotency record
        if idempotency_key:
            redis = await get_redis()
            if redis:
                await redis.setex(
                    f"bluesky:post:{idempotency_key}",
                    86400 * 7,
                    post_uri,
                )

        return PostResult(
            success=True,
            post_id=post_uri,
            post_url=f"https://bsky.app/profile/{self.handle}/post/{post_id}",
        )

    async def _upload_images(self, images: List[bytes]) -> Optional[dict]:
        """Upload images and return embed object."""
        blobs = []
        http = await self._get_http_client()

        for img_data in images[:4]:
            try:
                resp = await http.post(
                    f"{self.PDS_URL}/xrpc/com.atproto.repo.uploadBlob",
                    headers={
                        "Authorization": f"Bearer {self.session['accessJwt']}",
                        "Content-Type": "image/jpeg",
                    },
                    content=img_data,
                )

                if resp.status_code == 200:
                    data = resp.json()
                    blobs.append({
                        "alt": "",
                        "image": data["blob"],
                    })
            except Exception as e:
                logger.error(f"Failed to upload image: {e}")

        if not blobs:
            return None

        return {
            "$type": "app.bsky.embed.images",
            "images": blobs,
        }
