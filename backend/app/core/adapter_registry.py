"""
Data Source Adapter Registry v1.0.0

Per 20251207_MDM_COMICS_DATA_ACQUISITION_PIPELINE.json:
- Abstract DataSourceAdapter interface for all data sources
- Adapter registry for enable/disable and discovery
- Extensible: new adapters plug in via config

Each source has its own Adapter responsible for:
- Authentication
- Paging/pagination
- Error handling
- Normalization
- Delta-fetching

Uses ResilientHTTPClient for all network operations.
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Dict, List, Optional, Type

from app.core.http_client import ResilientHTTPClient
from app.core.utils import utcnow

logger = logging.getLogger(__name__)


class AdapterStatus(str, Enum):
    """Adapter operational status."""
    ENABLED = "enabled"
    DISABLED = "disabled"
    ERROR = "error"
    RATE_LIMITED = "rate_limited"


class DataSourceType(str, Enum):
    """Types of data sources."""
    API = "api"                    # REST/GraphQL API
    SCRAPER = "scraper"            # Web scraping
    DATABASE_DUMP = "database_dump"  # Periodic DB exports
    FILE_IMPORT = "file_import"    # CSV/JSON file imports


@dataclass
class AdapterConfig:
    """Configuration for a data source adapter."""
    name: str
    source_type: DataSourceType
    enabled: bool = True
    priority: int = 100  # Lower = higher priority for conflict resolution

    # Rate limiting
    requests_per_second: float = 2.0
    burst_limit: int = 5

    # Authentication
    auth_type: Optional[str] = None  # 'api_key', 'basic', 'oauth', None
    api_key_env_var: Optional[str] = None
    username_env_var: Optional[str] = None
    password_env_var: Optional[str] = None

    # Licensing/compliance
    license_type: str = "unknown"
    requires_attribution: bool = False
    attribution_text: Optional[str] = None
    images_allowed: bool = True

    # Extra config
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FetchResult:
    """Result from a data fetch operation."""
    success: bool
    records: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    next_cursor: Optional[str] = None
    has_more: bool = False
    total_count: Optional[int] = None
    fetched_at: datetime = field(default_factory=utcnow)


class DataSourceAdapter(ABC):
    """
    Abstract base class for data source adapters.

    All data source adapters must implement this interface.
    """

    def __init__(self, config: AdapterConfig, client: ResilientHTTPClient):
        self.config = config
        self.client = client
        self._status = AdapterStatus.ENABLED if config.enabled else AdapterStatus.DISABLED
        self._last_fetch_at: Optional[datetime] = None
        self._error_count = 0
        self._success_count = 0

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def status(self) -> AdapterStatus:
        return self._status

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if the data source is reachable and credentials are valid.

        Returns True if healthy, False otherwise.
        """
        pass

    @abstractmethod
    async def fetch_page(
        self,
        page: int = 1,
        page_size: int = 100,
        cursor: Optional[str] = None,
        **filters
    ) -> FetchResult:
        """
        Fetch a single page of records.

        Args:
            page: Page number (1-indexed)
            page_size: Records per page
            cursor: Continuation cursor (alternative to page)
            **filters: Source-specific filters

        Returns:
            FetchResult with records and pagination info
        """
        pass

    @abstractmethod
    async def fetch_by_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single record by its external ID.

        Args:
            external_id: The ID in the source system

        Returns:
            Record dict or None if not found
        """
        pass

    @abstractmethod
    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a raw record to our canonical schema.

        Args:
            record: Raw record from the source

        Returns:
            Normalized record matching our schema
        """
        pass

    async def fetch_all(
        self,
        max_pages: int = 100,
        **filters
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Fetch all records, yielding normalized records.

        Handles pagination automatically.
        """
        cursor = None
        page = 1

        while page <= max_pages:
            logger.info(f"[{self.name}] Fetching page {page}...")

            result = await self.fetch_page(
                page=page,
                cursor=cursor,
                **filters
            )

            if not result.success:
                logger.error(f"[{self.name}] Fetch failed: {result.errors}")
                self._error_count += 1
                break

            self._success_count += 1
            self._last_fetch_at = utcnow()

            for record in result.records:
                yield self.normalize(record)

            if not result.has_more:
                break

            cursor = result.next_cursor
            page += 1

    async def search(self, query: str, **kwargs) -> FetchResult:
        """
        Search for records matching a query.

        Default implementation - subclasses can override for source-specific search.
        """
        raise NotImplementedError(f"{self.name} does not support search")

    def get_stats(self) -> Dict[str, Any]:
        """Get adapter statistics."""
        return {
            "name": self.name,
            "status": self._status.value,
            "source_type": self.config.source_type.value,
            "enabled": self.config.enabled,
            "last_fetch_at": self._last_fetch_at.isoformat() if self._last_fetch_at else None,
            "success_count": self._success_count,
            "error_count": self._error_count,
        }


class AdapterRegistry:
    """
    Registry for data source adapters.

    Manages adapter lifecycle, discovery, and status.
    """

    def __init__(self):
        self._adapters: Dict[str, DataSourceAdapter] = {}
        self._configs: Dict[str, AdapterConfig] = {}

    def register(
        self,
        config: AdapterConfig,
        adapter_class: Type[DataSourceAdapter],
        client: ResilientHTTPClient
    ) -> DataSourceAdapter:
        """
        Register an adapter with the registry.

        Args:
            config: Adapter configuration
            adapter_class: The adapter class to instantiate
            client: HTTP client for the adapter

        Returns:
            The instantiated adapter
        """
        if config.name in self._adapters:
            logger.warning(f"Adapter '{config.name}' already registered, replacing")

        adapter = adapter_class(config, client)
        self._adapters[config.name] = adapter
        self._configs[config.name] = config

        logger.info(f"Registered adapter: {config.name} (type: {config.source_type.value})")
        return adapter

    def get(self, name: str) -> Optional[DataSourceAdapter]:
        """Get an adapter by name."""
        return self._adapters.get(name)

    def get_all(self) -> List[DataSourceAdapter]:
        """Get all registered adapters."""
        return list(self._adapters.values())

    def get_enabled(self) -> List[DataSourceAdapter]:
        """Get all enabled adapters."""
        return [a for a in self._adapters.values() if a.status == AdapterStatus.ENABLED]

    def get_by_type(self, source_type: DataSourceType) -> List[DataSourceAdapter]:
        """Get adapters by source type."""
        return [
            a for a in self._adapters.values()
            if a.config.source_type == source_type
        ]

    def enable(self, name: str) -> bool:
        """Enable an adapter."""
        if name in self._adapters:
            self._adapters[name]._status = AdapterStatus.ENABLED
            self._configs[name].enabled = True
            logger.info(f"Enabled adapter: {name}")
            return True
        return False

    def disable(self, name: str) -> bool:
        """Disable an adapter."""
        if name in self._adapters:
            self._adapters[name]._status = AdapterStatus.DISABLED
            self._configs[name].enabled = False
            logger.info(f"Disabled adapter: {name}")
            return True
        return False

    def unregister(self, name: str) -> bool:
        """Remove an adapter from the registry."""
        if name in self._adapters:
            del self._adapters[name]
            del self._configs[name]
            logger.info(f"Unregistered adapter: {name}")
            return True
        return False

    async def health_check_all(self) -> Dict[str, bool]:
        """Run health checks on all adapters."""
        results = {}
        for name, adapter in self._adapters.items():
            try:
                results[name] = await adapter.health_check()
                if not results[name]:
                    adapter._status = AdapterStatus.ERROR
            except Exception as e:
                logger.error(f"Health check failed for {name}: {e}")
                results[name] = False
                adapter._status = AdapterStatus.ERROR
        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all adapters."""
        return {
            "total_adapters": len(self._adapters),
            "enabled": len([a for a in self._adapters.values() if a.status == AdapterStatus.ENABLED]),
            "disabled": len([a for a in self._adapters.values() if a.status == AdapterStatus.DISABLED]),
            "error": len([a for a in self._adapters.values() if a.status == AdapterStatus.ERROR]),
            "adapters": {name: adapter.get_stats() for name, adapter in self._adapters.items()}
        }


# Global registry instance
adapter_registry = AdapterRegistry()


# Pre-defined configurations for known data sources
PRICECHARTING_CONFIG = AdapterConfig(
    name="pricecharting",
    source_type=DataSourceType.API,
    enabled=True,
    priority=10,  # High priority for pricing
    requests_per_second=1.0,
    burst_limit=3,
    auth_type="api_key",
    api_key_env_var="PRICECHARTING_API_TOKEN",
    license_type="proprietary",
    requires_attribution=False,
    images_allowed=True,
)

METRON_CONFIG = AdapterConfig(
    name="metron",
    source_type=DataSourceType.API,
    enabled=True,
    priority=20,
    requests_per_second=2.0,
    burst_limit=5,
    auth_type="basic",
    username_env_var="METRON_USERNAME",
    password_env_var="METRON_PASSWORD",
    license_type="proprietary",
    requires_attribution=False,
    images_allowed=True,
)

GCD_CONFIG = AdapterConfig(
    name="gcd",
    source_type=DataSourceType.DATABASE_DUMP,  # Primary method is DB dump
    enabled=False,  # Disabled by default - P2 priority
    priority=30,
    requests_per_second=1.0,
    burst_limit=3,
    license_type="CC-BY-SA-4.0",
    requires_attribution=True,
    attribution_text="Bibliographic data from Grand Comics Database (https://comics.org) under CC BY-SA 4.0 license.",
    images_allowed=False,  # Images are publisher copyright
)

MARVEL_FANDOM_CONFIG = AdapterConfig(
    name="marvel_fandom",
    source_type=DataSourceType.API,  # MediaWiki API
    enabled=False,  # Disabled by default - P3 priority
    priority=40,
    requests_per_second=1.0,
    burst_limit=3,
    license_type="CC-BY-SA-3.0",
    requires_attribution=True,
    attribution_text="Character data sourced from Marvel Database (https://marvel.fandom.com) under CC BY-SA 3.0 license.",
    images_allowed=False,  # Images are NOT covered by CC BY-SA
)
