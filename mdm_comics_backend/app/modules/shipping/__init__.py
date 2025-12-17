"""
Shipping Module v1.0.0

Per 20251216_shipping_compartmentalization_proposal.json:
- Compartmentalized shipping functionality
- Carrier-level feature flags (UPS, USPS individually toggleable)
- BaseCarrier interface for all carrier implementations
- CarrierFactory for dependency injection
"""
from app.modules.shipping.carriers import CarrierFactory, get_carrier
from app.modules.shipping.carriers.base import BaseCarrier

__all__ = [
    "CarrierFactory",
    "get_carrier",
    "BaseCarrier",
]
