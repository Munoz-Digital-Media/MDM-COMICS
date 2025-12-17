"""
Feature Flags Service v1.0.0

Per 20251216_shipping_compartmentalization_proposal.json:
- 30-second cache TTL for performance
- Carrier-level toggles via database
- No deployment required for toggling

Usage:
    # Check if a specific carrier is enabled
    if await FeatureFlags.is_carrier_enabled(CarrierCode.UPS):
        # UPS operations available

    # Get all enabled carriers
    carriers = await FeatureFlags.get_enabled_carriers()

    # Get carrier-specific config
    config = await FeatureFlags.get_carrier_config(CarrierCode.UPS)
"""
import time
import logging
from typing import Dict, List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.models.feature_flag import FeatureFlag
from app.models.carrier import CarrierCode

logger = logging.getLogger(__name__)


class FeatureFlagCache:
    """
    In-memory cache for feature flags with TTL.

    Cache is refreshed when stale (> cache_ttl seconds since last refresh).
    Thread-safe for async usage.
    """

    def __init__(self, cache_ttl: int = 30):
        self._cache: Dict[str, FeatureFlag] = {}
        self._cache_ttl = cache_ttl  # seconds
        self._last_refresh: float = 0

    @property
    def is_stale(self) -> bool:
        """Check if cache needs refresh."""
        return time.time() - self._last_refresh > self._cache_ttl

    async def refresh(self, db: Optional[AsyncSession] = None) -> None:
        """
        Refresh the cache from database.

        Uses provided session or creates new one.
        """
        try:
            if db:
                await self._load_flags(db)
            else:
                async with get_db_session() as session:
                    await self._load_flags(session)

            self._last_refresh = time.time()
            logger.debug(f"Feature flags cache refreshed: {len(self._cache)} flags loaded")

        except Exception as e:
            logger.error(f"Failed to refresh feature flags cache: {e}")
            # Keep stale cache on failure rather than clearing
            if not self._cache:
                raise

    async def _load_flags(self, db: AsyncSession) -> None:
        """Load all flags from database into cache."""
        result = await db.execute(select(FeatureFlag))
        flags = result.scalars().all()

        new_cache: Dict[str, FeatureFlag] = {}
        for flag in flags:
            new_cache[flag.flag_key] = flag

        self._cache = new_cache

    def get(self, key: str) -> Optional[FeatureFlag]:
        """Get a flag from cache by key (module:feature)."""
        return self._cache.get(key)

    def get_by_module(self, module: str) -> List[FeatureFlag]:
        """Get all flags for a module."""
        return [
            flag for flag in self._cache.values()
            if flag.module == module
        ]

    def clear(self) -> None:
        """Clear the cache (forces refresh on next access)."""
        self._cache.clear()
        self._last_refresh = 0


# Global cache instance
_flag_cache = FeatureFlagCache(cache_ttl=30)


class FeatureFlags:
    """
    Static methods for checking feature flags.

    Usage:
        # Check carrier
        if await FeatureFlags.is_carrier_enabled(CarrierCode.UPS):
            ...

        # Get all enabled carriers
        carriers = await FeatureFlags.get_enabled_carriers()
    """

    @classmethod
    async def _ensure_cache_fresh(cls, db: Optional[AsyncSession] = None) -> None:
        """Refresh cache if stale."""
        if _flag_cache.is_stale:
            await _flag_cache.refresh(db)

    @classmethod
    async def is_enabled(cls, module: str, feature: str, db: Optional[AsyncSession] = None) -> bool:
        """
        Check if a feature is enabled.

        Args:
            module: Module name (e.g., 'shipping')
            feature: Feature name (e.g., 'ups')
            db: Optional database session

        Returns:
            True if enabled, False if disabled or not found
        """
        await cls._ensure_cache_fresh(db)

        key = f"{module}:{feature}"
        flag = _flag_cache.get(key)

        return flag.is_enabled if flag else False

    @classmethod
    async def is_carrier_enabled(cls, carrier_code: CarrierCode, db: Optional[AsyncSession] = None) -> bool:
        """
        Check if a specific shipping carrier is enabled.

        Args:
            carrier_code: CarrierCode enum (UPS, USPS, etc.)
            db: Optional database session

        Returns:
            True if carrier is enabled
        """
        return await cls.is_enabled("shipping", carrier_code.value.lower(), db)

    @classmethod
    async def get_enabled_carriers(cls, db: Optional[AsyncSession] = None) -> List[CarrierCode]:
        """
        Get list of all enabled shipping carriers.

        Returns:
            List of enabled CarrierCode enums
        """
        await cls._ensure_cache_fresh(db)

        enabled = []
        for flag in _flag_cache.get_by_module("shipping"):
            if flag.is_enabled:
                try:
                    carrier = CarrierCode(flag.feature.upper())
                    enabled.append(carrier)
                except ValueError:
                    # Feature doesn't map to a CarrierCode
                    logger.warning(f"Unknown carrier feature: {flag.feature}")
                    continue

        return enabled

    @classmethod
    async def get_config(cls, module: str, feature: str, db: Optional[AsyncSession] = None) -> dict:
        """
        Get configuration for a feature.

        Args:
            module: Module name
            feature: Feature name
            db: Optional database session

        Returns:
            Config dict or empty dict if not found
        """
        await cls._ensure_cache_fresh(db)

        key = f"{module}:{feature}"
        flag = _flag_cache.get(key)

        return flag.config_json if flag else {}

    @classmethod
    async def get_carrier_config(cls, carrier_code: CarrierCode, db: Optional[AsyncSession] = None) -> dict:
        """
        Get configuration for a specific carrier.

        Args:
            carrier_code: CarrierCode enum

        Returns:
            Config dict (e.g., {"sandbox_mode": false})
        """
        return await cls.get_config("shipping", carrier_code.value.lower(), db)

    @classmethod
    async def get_flag(cls, module: str, feature: str, db: Optional[AsyncSession] = None) -> Optional[FeatureFlag]:
        """
        Get the full FeatureFlag object.

        Args:
            module: Module name
            feature: Feature name

        Returns:
            FeatureFlag or None
        """
        await cls._ensure_cache_fresh(db)
        return _flag_cache.get(f"{module}:{feature}")

    @classmethod
    async def get_all_flags(cls, db: Optional[AsyncSession] = None) -> List[FeatureFlag]:
        """
        Get all feature flags.

        Returns:
            List of all FeatureFlag objects
        """
        await cls._ensure_cache_fresh(db)
        return list(_flag_cache._cache.values())

    @classmethod
    async def get_module_flags(cls, module: str, db: Optional[AsyncSession] = None) -> List[FeatureFlag]:
        """
        Get all flags for a specific module.

        Args:
            module: Module name (e.g., 'shipping')

        Returns:
            List of FeatureFlag objects for that module
        """
        await cls._ensure_cache_fresh(db)
        return _flag_cache.get_by_module(module)

    @classmethod
    def invalidate_cache(cls) -> None:
        """
        Invalidate the cache (forces refresh on next access).

        Call this after updating flags in the database.
        """
        _flag_cache.clear()
        logger.info("Feature flags cache invalidated")
