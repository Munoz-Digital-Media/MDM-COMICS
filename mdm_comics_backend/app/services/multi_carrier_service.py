"""
Multi-Carrier Shipping Service v1.0.0

Per 20251216_shipping_compartmentalization_proposal.json:
- Aggregates rates from all enabled carriers
- Uses CarrierFactory for carrier instantiation
- Respects feature flags for carrier availability
- Returns combined rate list sorted by price

Usage:
    service = MultiCarrierService(db)
    rates = await service.get_all_carrier_rates(origin, destination, packages)
"""
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.carrier import Carrier, CarrierCode
from app.models.shipment import ShipmentRate
from app.core.feature_flags import FeatureFlags
from app.modules.shipping.carriers import CarrierFactory, get_carrier
from app.modules.shipping.carriers.base import (
    BaseCarrier,
    AddressInput,
    Package,
    Rate,
)
from app.services.encryption import decrypt_pii

logger = logging.getLogger(__name__)


@dataclass
class MultiCarrierRate:
    """Rate from any carrier with carrier identification."""
    carrier_code: CarrierCode
    carrier_name: str
    service_code: str
    service_name: str
    rate: float
    currency: str
    delivery_date: Optional[datetime]
    delivery_days: Optional[int]
    guaranteed: bool
    ttl_seconds: int
    expires_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "carrier_code": self.carrier_code.value,
            "carrier_name": self.carrier_name,
            "service_code": self.service_code,
            "service_name": self.service_name,
            "rate": self.rate,
            "currency": self.currency,
            "delivery_date": self.delivery_date.isoformat() if self.delivery_date else None,
            "delivery_days": self.delivery_days,
            "guaranteed": self.guaranteed,
            "ttl_seconds": self.ttl_seconds,
            "expires_at": self.expires_at.isoformat(),
        }


class MultiCarrierService:
    """
    Service for multi-carrier shipping operations.

    Aggregates rates from all enabled carriers and provides
    unified interface for multi-carrier shipping.
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self._carrier_configs: Dict[CarrierCode, Carrier] = {}

    async def _get_carrier_config(self, carrier_code: CarrierCode) -> Optional[Carrier]:
        """Get carrier configuration from database."""
        if carrier_code in self._carrier_configs:
            return self._carrier_configs[carrier_code]

        result = await self.db.execute(
            select(Carrier).where(
                and_(
                    Carrier.code == carrier_code,
                    Carrier.is_active == True,
                )
            )
        )
        carrier = result.scalar_one_or_none()

        if carrier:
            self._carrier_configs[carrier_code] = carrier

        return carrier

    async def get_enabled_carriers(self) -> List[CarrierCode]:
        """Get list of all enabled carriers."""
        return await FeatureFlags.get_enabled_carriers(self.db)

    async def get_carrier_instance(self, carrier_code: CarrierCode) -> Optional[BaseCarrier]:
        """Get a carrier instance if enabled."""
        config = await self._get_carrier_config(carrier_code)
        return await get_carrier(carrier_code, config)

    async def get_all_carrier_rates(
        self,
        origin: AddressInput,
        destination: AddressInput,
        packages: List[Package],
        carrier_filter: Optional[CarrierCode] = None,
    ) -> List[MultiCarrierRate]:
        """
        Get shipping rates from all enabled carriers.

        Args:
            origin: Origin address
            destination: Destination address
            packages: List of packages
            carrier_filter: Optional - only get rates from this carrier

        Returns:
            List of MultiCarrierRate sorted by rate (lowest first)
        """
        enabled_carriers = await self.get_enabled_carriers()

        if not enabled_carriers:
            logger.warning("No carriers enabled for rate lookup")
            return []

        # Filter to specific carrier if requested
        if carrier_filter:
            if carrier_filter not in enabled_carriers:
                logger.warning(f"Requested carrier {carrier_filter.value} is not enabled")
                return []
            enabled_carriers = [carrier_filter]

        all_rates: List[MultiCarrierRate] = []
        now = datetime.now(timezone.utc)

        for carrier_code in enabled_carriers:
            try:
                carrier = await self.get_carrier_instance(carrier_code)
                if not carrier:
                    logger.warning(f"Could not instantiate carrier: {carrier_code.value}")
                    continue

                logger.info(f"Fetching rates from {carrier_code.value}")
                rates = await carrier.get_rates(origin, destination, packages)

                for rate in rates:
                    # Calculate expiration time
                    from datetime import timedelta
                    expires_at = now + timedelta(seconds=rate.ttl_seconds)

                    all_rates.append(MultiCarrierRate(
                        carrier_code=rate.carrier_code,
                        carrier_name=carrier.carrier_name,
                        service_code=rate.service_code,
                        service_name=rate.service_name,
                        rate=rate.rate,
                        currency=rate.currency,
                        delivery_date=rate.delivery_date,
                        delivery_days=rate.delivery_days,
                        guaranteed=rate.guaranteed,
                        ttl_seconds=rate.ttl_seconds,
                        expires_at=expires_at,
                    ))

                logger.info(f"Got {len(rates)} rates from {carrier_code.value}")

            except Exception as e:
                logger.error(f"Error getting rates from {carrier_code.value}: {e}")
                continue

        # Sort by rate (lowest first)
        all_rates.sort(key=lambda r: r.rate)

        return all_rates

    async def get_carrier_rates(
        self,
        carrier_code: CarrierCode,
        origin: AddressInput,
        destination: AddressInput,
        packages: List[Package],
    ) -> List[Rate]:
        """
        Get rates from a specific carrier.

        Args:
            carrier_code: The carrier to query
            origin: Origin address
            destination: Destination address
            packages: List of packages

        Returns:
            List of Rate objects from that carrier

        Raises:
            ValueError if carrier is not enabled
        """
        if not await FeatureFlags.is_carrier_enabled(carrier_code, self.db):
            raise ValueError(f"Carrier {carrier_code.value} is not enabled")

        carrier = await self.get_carrier_instance(carrier_code)
        if not carrier:
            raise ValueError(f"Could not instantiate carrier {carrier_code.value}")

        return await carrier.get_rates(origin, destination, packages)

    def address_from_model(self, address) -> AddressInput:
        """
        Convert Address model to AddressInput.

        Decrypts PII fields for API calls.
        """
        return AddressInput(
            address_line1=decrypt_pii(address.address_line1_encrypted) if address.address_line1_encrypted else "",
            address_line2=decrypt_pii(address.address_line2_encrypted) if address.address_line2_encrypted else None,
            address_line3=decrypt_pii(address.address_line3_encrypted) if address.address_line3_encrypted else None,
            city=address.city,
            state_province=address.state_province,
            postal_code=address.postal_code,
            country_code=address.country_code,
            recipient_name=decrypt_pii(address.recipient_name_encrypted) if address.recipient_name_encrypted else None,
            company_name=decrypt_pii(address.company_name_encrypted) if address.company_name_encrypted else None,
            phone=decrypt_pii(address.phone_encrypted) if address.phone_encrypted else None,
            email=decrypt_pii(address.email_encrypted) if address.email_encrypted else None,
            residential=address.residential if hasattr(address, 'residential') else True,
        )

    def package_from_dict(self, pkg_dict: Dict) -> Package:
        """Convert package dict to Package dataclass."""
        return Package(
            weight=pkg_dict.get("weight", 0.5),
            length=pkg_dict.get("length", 0.0),
            width=pkg_dict.get("width", 0.0),
            height=pkg_dict.get("height", 0.0),
            package_type=pkg_dict.get("package_type", "02"),
            declared_value=pkg_dict.get("declared_value", 0.0),
            description=pkg_dict.get("description"),
        )
