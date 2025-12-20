"""
BCW Session Manager

Manages BCW authentication sessions including:
- Credential encryption/decryption
- Cookie persistence
- Session expiry tracking
- Auto re-login on session expiry

Per constitution_pii.json: All credentials encrypted at rest.
"""
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.exceptions import BCWAuthError
from app.services.encryption import encrypt_pii, decrypt_pii

logger = logging.getLogger(__name__)

# Session TTL (8 hours default)
SESSION_TTL_HOURS = 8


class BCWSessionManager:
    """
    Manages BCW session state with database persistence.

    Usage:
        async with get_db_session() as db:
            session_mgr = BCWSessionManager(db)
            cookies = await session_mgr.get_valid_session()
            if not cookies:
                # Need to login
                ...
            await session_mgr.save_session(new_cookies)
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_config(self):
        """Get BCW configuration from database."""
        from app.models.bcw import BCWConfig

        result = await self.db.execute(
            select(BCWConfig).where(BCWConfig.vendor_code == "BCW")
        )
        return result.scalar_one_or_none()

    async def get_credentials(self) -> Optional[Dict[str, str]]:
        """
        Get decrypted BCW credentials.

        Returns:
            Dict with 'username' and 'password' or None
        """
        config = await self.get_config()
        if not config:
            logger.warning("No BCW configuration found")
            return None

        try:
            return {
                "username": decrypt_pii(config.username_encrypted),
                "password": decrypt_pii(config.password_encrypted),
            }
        except Exception as e:
            logger.error(f"Failed to decrypt BCW credentials: {e}")
            raise BCWAuthError(
                message="Failed to decrypt BCW credentials",
                code="BCW_CREDENTIAL_DECRYPT_FAILED",
            )

    async def get_selectors(self) -> Dict[str, str]:
        """
        Get dynamic selector overrides from database.

        Returns:
            Dict of 'category.key' -> selector string
        """
        config = await self.get_config()
        if not config or not config.selectors:
            return {}

        return config.selectors or {}

    async def get_valid_session(self) -> Optional[List[Dict]]:
        """
        Get valid session cookies if available.

        Returns:
            List of cookie dictionaries or None if expired/missing
        """
        config = await self.get_config()
        if not config:
            return None

        # Check if session exists and not expired
        if not config.session_data_encrypted:
            return None

        if config.session_expires_at:
            if datetime.now(timezone.utc) >= config.session_expires_at:
                logger.info("BCW session expired")
                return None

        try:
            session_json = decrypt_pii(config.session_data_encrypted)
            return json.loads(session_json)
        except Exception as e:
            logger.error(f"Failed to decrypt session data: {e}")
            return None

    async def save_session(
        self,
        cookies: List[Dict],
        ttl_hours: int = SESSION_TTL_HOURS,
    ):
        """
        Save session cookies to database.

        Args:
            cookies: List of cookie dictionaries from browser
            ttl_hours: Session TTL in hours
        """
        config = await self.get_config()
        if not config:
            logger.error("Cannot save session - no BCW config found")
            return

        try:
            session_json = json.dumps(cookies)
            config.session_data_encrypted = encrypt_pii(session_json)
            config.session_expires_at = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)
            config.updated_at = datetime.now(timezone.utc)

            await self.db.flush()
            logger.info(f"BCW session saved, expires at {config.session_expires_at}")

        except Exception as e:
            logger.error(f"Failed to save session: {e}")

    async def clear_session(self):
        """Clear stored session data."""
        config = await self.get_config()
        if config:
            config.session_data_encrypted = None
            config.session_expires_at = None
            config.updated_at = datetime.now(timezone.utc)
            await self.db.flush()
            logger.info("BCW session cleared")

    async def update_circuit_breaker_state(
        self,
        state: str,
        failures: int,
        opened_at: Optional[datetime] = None,
    ):
        """Update circuit breaker state in database."""
        config = await self.get_config()
        if config:
            config.circuit_state = state
            config.consecutive_failures = failures
            config.circuit_opened_at = opened_at
            config.updated_at = datetime.now(timezone.utc)
            await self.db.flush()

    async def update_rate_limit_state(
        self,
        actions_this_hour: int,
        hour_reset_at: Optional[datetime] = None,
    ):
        """Update rate limit counters in database."""
        config = await self.get_config()
        if config:
            config.actions_this_hour = actions_this_hour
            config.hour_reset_at = hour_reset_at
            config.last_action_at = datetime.now(timezone.utc)
            config.updated_at = datetime.now(timezone.utc)
            await self.db.flush()

    async def update_selector_health(
        self,
        status: str,
        version: str = None,
    ):
        """Update selector health status."""
        config = await self.get_config()
        if config:
            config.selector_health_status = status
            if version:
                config.selector_version = version
            config.last_selector_check_at = datetime.now(timezone.utc)
            config.updated_at = datetime.now(timezone.utc)
            await self.db.flush()
