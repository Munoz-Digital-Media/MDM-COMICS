"""
BCW Session Manager

Manages BCW authentication sessions.
Refactored to use Environment Variables instead of DB config.

- Credentials from BCW_USERNAME / BCW_PASSWORD
- Session cookies from BCW_SESSION_COOKIES (immutable injection)
"""
import json
import logging
import os
from typing import Optional, Dict, List, Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import BCWAuthError

logger = logging.getLogger(__name__)


class BCWSessionManager:
    """
    Manages BCW session state via Environment Variables.
    Database persistence for config has been deprecated.
    """

    def __init__(self, db: AsyncSession = None):
        # DB session kept for interface compatibility but unused for config
        self.db = db

    async def get_credentials(self) -> Optional[Dict[str, str]]:
        """
        Get BCW credentials from environment variables.

        Returns:
            Dict with 'username' and 'password' or None
        """
        username = os.getenv("BCW_USERNAME")
        password = os.getenv("BCW_PASSWORD")

        if not username or not password:
            logger.warning("[BCW] Credentials not set in environment (BCW_USERNAME/BCW_PASSWORD)")
            return None

        return {
            "username": username,
            "password": password,
        }

    async def get_selectors(self) -> Dict[str, str]:
        """
        Get dynamic selector overrides.
        
        Currently returns empty dict as DB config is deprecated.
        Selector logic falls back to code-defined defaults.
        """
        return {}

    async def get_valid_session(self) -> Optional[List[Dict]]:
        """
        Get valid session cookies from environment.

        Returns:
            List of cookie dictionaries or None
        """
        session_json = os.getenv("BCW_SESSION_COOKIES")
        if not session_json:
            return None

        try:
            return json.loads(session_json)
        except json.JSONDecodeError as e:
            logger.error(f"[BCW] Failed to parse BCW_SESSION_COOKIES: {e}")
            return None

    async def save_session(
        self,
        cookies: List[Dict],
        ttl_hours: int = 8,
    ):
        """
        Save session cookies.
        
        NO-OP: Session is managed via static environment variables.
        We do not persist runtime session changes back to the environment.
        """
        logger.debug("[BCW] Session save requested (ignored - using immutable env config)")
        pass

    async def clear_session(self):
        """Clear stored session data (NO-OP)."""
        pass

    async def update_circuit_breaker_state(
        self,
        state: str,
        failures: int,
        opened_at: Any = None,
    ):
        """Update circuit breaker state (NO-OP)."""
        pass

    async def update_rate_limit_state(
        self,
        actions_this_hour: int,
        hour_reset_at: Any = None,
    ):
        """Update rate limit counters (NO-OP)."""
        pass

    async def update_selector_health(
        self,
        status: str,
        version: str = None,
    ):
        """Update selector health status (NO-OP)."""
        pass

