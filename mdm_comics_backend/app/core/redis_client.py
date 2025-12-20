"""
Redis client for MDM Comics

BLOCK-004: Provides cross-instance webhook idempotency via Redis SET.
Also used for PriceCharting API response caching.

Per constitution_db.json Section 5: Critical operations need cross-instance coordination.
"""
import logging
from typing import Optional
import redis.asyncio as redis
from app.core.config import settings

logger = logging.getLogger(__name__)

# Global Redis client (initialized lazily)
_redis_client: Optional[redis.Redis] = None


async def get_redis() -> Optional[redis.Redis]:
    """Get Redis client, initializing if needed.

    Returns None if REDIS_URL not configured (graceful degradation).
    """
    global _redis_client

    if not settings.REDIS_URL:
        return None

    if _redis_client is None:
        try:
            _redis_client = redis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
            # Test connection
            await _redis_client.ping()
            logger.info("Redis connection established")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Falling back to in-memory.")
            _redis_client = None

    return _redis_client


async def close_redis():
    """Close Redis connection on shutdown."""
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        _redis_client = None


# ----- Webhook Idempotency -----

WEBHOOK_KEY_PREFIX = "webhook:event:"
WEBHOOK_TTL_HOURS = 24


async def is_webhook_processed(event_id: str) -> bool:
    """Check if webhook event was already processed.

    BLOCK-004: Uses Redis SET for cross-instance coordination.
    Falls back to False if Redis unavailable (allows processing).
    """
    client = await get_redis()
    if not client:
        return False

    try:
        return await client.exists(f"{WEBHOOK_KEY_PREFIX}{event_id}") > 0
    except Exception as e:
        logger.warning(f"Redis check failed for webhook {event_id}: {e}")
        return False


async def mark_webhook_processed(event_id: str, ttl_hours: int = WEBHOOK_TTL_HOURS) -> bool:
    """Mark webhook event as processed with TTL.

    Returns True if successfully marked, False if Redis unavailable.
    """
    client = await get_redis()
    if not client:
        return False

    try:
        await client.setex(
            f"{WEBHOOK_KEY_PREFIX}{event_id}",
            ttl_hours * 3600,
            "1"
        )
        return True
    except Exception as e:
        logger.warning(f"Redis mark failed for webhook {event_id}: {e}")
        return False


# ----- PriceCharting Cache -----

PC_CACHE_PREFIX = "pc:product:"
PC_CACHE_TTL_SECONDS = 3600  # 1 hour


async def get_pricecharting_cached(pc_id: int) -> Optional[dict]:
    """Get cached PriceCharting response.

    Returns None if not cached or Redis unavailable.
    """
    import json
    client = await get_redis()
    if not client:
        return None

    try:
        data = await client.get(f"{PC_CACHE_PREFIX}{pc_id}")
        if data:
            return json.loads(data)
    except Exception as e:
        logger.debug(f"Redis cache miss for PC {pc_id}: {e}")

    return None


async def set_pricecharting_cached(pc_id: int, data: dict, ttl_seconds: int = PC_CACHE_TTL_SECONDS) -> bool:
    """Cache PriceCharting response.

    Returns True if successfully cached, False if Redis unavailable.
    """
    import json
    client = await get_redis()
    if not client:
        return False

    try:
        await client.setex(
            f"{PC_CACHE_PREFIX}{pc_id}",
            ttl_seconds,
            json.dumps(data)
        )
        return True
    except Exception as e:
        logger.debug(f"Redis cache set failed for PC {pc_id}: {e}")
        return False


# ----- Homepage Cache -----

HOMEPAGE_SECTIONS_KEY = "homepage:sections"
FEATURED_BUNDLES_KEY = "homepage:featured_bundles"
HOMEPAGE_CACHE_TTL = 300  # 5 minutes


async def get_homepage_sections_cached() -> Optional[dict]:
    """Get cached homepage sections."""
    import json
    client = await get_redis()
    if not client:
        return None
    try:
        data = await client.get(HOMEPAGE_SECTIONS_KEY)
        if data:
            return json.loads(data)
    except Exception as e:
        logger.debug(f"Redis cache miss for homepage sections: {e}")
    return None


async def set_homepage_sections_cached(data: dict) -> bool:
    """Cache homepage sections."""
    import json
    client = await get_redis()
    if not client:
        return False
    try:
        await client.setex(HOMEPAGE_SECTIONS_KEY, HOMEPAGE_CACHE_TTL, json.dumps(data))
        return True
    except Exception as e:
        logger.debug(f"Redis cache set failed for homepage sections: {e}")
        return False


async def invalidate_homepage_cache() -> bool:
    """Invalidate homepage configuration cache."""
    client = await get_redis()
    if not client:
        return False
    try:
        await client.delete(HOMEPAGE_SECTIONS_KEY)
        # Also clear featured bundles as section order/visibility might affect them?
        # Maybe not strictly necessary but safer if logic intertwines
        await client.delete(FEATURED_BUNDLES_KEY)
        return True
    except Exception as e:
        logger.debug(f"Redis cache delete failed for homepage sections: {e}")
        return False
