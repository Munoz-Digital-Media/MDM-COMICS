"""
Dropship Integration Services

Orchestrates dropship fulfillment workflows.
Per 20251216_mdm_comics_bcw_initial_integration.json v1.2.0.

Components:
- inventory_sync: BCW inventory synchronization with buffer logic
- quote_service: Shipping quote caching and retrieval
- orchestrator: Main dropship workflow coordinator
- messaging: Customer-facing message templates
"""
from app.services.dropship.inventory_sync import (
    BCWInventorySyncService,
    InventorySyncResult,
    DisplayStockInfo,
    StockStatus,
    STOCK_BUFFER_THRESHOLD,
    LOW_STOCK_THRESHOLD,
)
from app.services.dropship.quote_service import (
    DropshipQuoteService,
    ShippingQuoteResult,
)
from app.services.dropship.orchestrator import (
    DropshipOrchestrator,
    DropshipOrderResult,
    AddressValidationResult,
)
from app.services.dropship.messaging import (
    DropshipMessaging,
    DropshipMessage,
    MessageType,
)

__all__ = [
    # Inventory sync
    "BCWInventorySyncService",
    "InventorySyncResult",
    "DisplayStockInfo",
    "StockStatus",
    "STOCK_BUFFER_THRESHOLD",
    "LOW_STOCK_THRESHOLD",
    # Quote service
    "DropshipQuoteService",
    "ShippingQuoteResult",
    # Orchestrator
    "DropshipOrchestrator",
    "DropshipOrderResult",
    "AddressValidationResult",
    # Messaging
    "DropshipMessaging",
    "DropshipMessage",
    "MessageType",
]
