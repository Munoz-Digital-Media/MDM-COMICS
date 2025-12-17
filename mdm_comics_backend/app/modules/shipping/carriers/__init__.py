"""
Carrier Registry and Factory v1.0.0

Per 20251216_shipping_compartmentalization_proposal.json:
- CarrierFactory creates carrier instances based on CarrierCode
- Only returns enabled carriers (checks feature flags)
- Carriers can share code internally (codependent, not isolated)
"""
from typing import Dict, List, Optional, Type
import logging

from app.models.carrier import CarrierCode, Carrier
from app.core.feature_flags import FeatureFlags
from app.modules.shipping.carriers.base import BaseCarrier

logger = logging.getLogger(__name__)

# Registry of carrier implementations
_CARRIER_REGISTRY: Dict[CarrierCode, Type[BaseCarrier]] = {}


def register_carrier(carrier_code: CarrierCode):
    """
    Decorator to register a carrier implementation.

    Usage:
        @register_carrier(CarrierCode.UPS)
        class UPSCarrier(BaseCarrier):
            ...
    """
    def decorator(cls: Type[BaseCarrier]):
        _CARRIER_REGISTRY[carrier_code] = cls
        logger.info(f"Registered carrier: {carrier_code.value} -> {cls.__name__}")
        return cls
    return decorator


class CarrierFactory:
    """
    Factory for creating carrier instances.

    Checks feature flags before returning carriers.
    Returns None for disabled carriers.
    """

    @classmethod
    async def get_carrier(
        cls,
        carrier_code: CarrierCode,
        carrier_config: Optional[Carrier] = None
    ) -> Optional[BaseCarrier]:
        """
        Get a carrier instance if enabled.

        Args:
            carrier_code: The carrier to get
            carrier_config: Optional Carrier model with credentials

        Returns:
            BaseCarrier instance or None if disabled/not found
        """
        # Check if carrier is enabled
        if not await FeatureFlags.is_carrier_enabled(carrier_code):
            logger.debug(f"Carrier {carrier_code.value} is disabled")
            return None

        # Get carrier class from registry
        carrier_cls = _CARRIER_REGISTRY.get(carrier_code)
        if not carrier_cls:
            logger.warning(f"No implementation registered for carrier: {carrier_code.value}")
            return None

        # Create instance
        return carrier_cls(carrier_config)

    @classmethod
    async def get_enabled_carriers(
        cls,
        carrier_configs: Optional[Dict[CarrierCode, Carrier]] = None
    ) -> List[BaseCarrier]:
        """
        Get all enabled carrier instances.

        Args:
            carrier_configs: Optional dict of CarrierCode -> Carrier config

        Returns:
            List of enabled BaseCarrier instances
        """
        enabled_codes = await FeatureFlags.get_enabled_carriers()
        carriers = []

        for code in enabled_codes:
            config = carrier_configs.get(code) if carrier_configs else None
            carrier = await cls.get_carrier(code, config)
            if carrier:
                carriers.append(carrier)

        return carriers

    @classmethod
    def get_registered_carriers(cls) -> List[CarrierCode]:
        """Get list of all registered carrier codes."""
        return list(_CARRIER_REGISTRY.keys())


async def get_carrier(
    carrier_code: CarrierCode,
    carrier_config: Optional[Carrier] = None
) -> Optional[BaseCarrier]:
    """
    Convenience function to get a carrier.

    Equivalent to CarrierFactory.get_carrier().
    """
    return await CarrierFactory.get_carrier(carrier_code, carrier_config)


# Import carriers to trigger registration
# These imports must be at the bottom to avoid circular imports
from app.modules.shipping.carriers.ups import UPSCarrier  # noqa: E402, F401
from app.modules.shipping.carriers.usps import USPSCarrier  # noqa: E402, F401
