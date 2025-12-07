"""
Pytest configuration and fixtures for MDM Comics tests.
UPS Shipping Integration v1.28.0
"""
import asyncio
import os
import pytest
from typing import AsyncGenerator, Generator
from unittest.mock import MagicMock, AsyncMock

# Set test environment before importing app modules
os.environ["ENVIRONMENT"] = "development"
os.environ["DATABASE_URL"] = "postgresql+asyncpg://test:test@localhost:5432/test_db"
os.environ["SECRET_KEY"] = "test-secret-key-for-unit-tests-only"


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_db() -> AsyncMock:
    """Create mock async database session."""
    db = AsyncMock()
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    db.rollback = AsyncMock()
    db.execute = AsyncMock()
    return db


@pytest.fixture
def mock_ups_client() -> AsyncMock:
    """Create mock UPS client."""
    from app.services.ups_client import UPSRateResult, UPSShipmentResult, UPSTrackingResult

    client = AsyncMock()

    # Mock validate_address
    client.validate_address = AsyncMock(return_value=(
        True,  # is_valid
        None,  # corrected_address
        ["Address validated"]  # messages
    ))

    # Mock get_rates
    mock_rate = UPSRateResult(
        service_code="03",
        service_name="UPS Ground",
        base_charge=10.0,
        fuel_surcharge=1.50,
        residential_surcharge=0.0,
        delivery_area_surcharge=0.0,
        other_surcharges=0.0,
        total_charges=11.50,
        currency="USD",
        guaranteed_delivery=False,
        estimated_delivery=None,
        estimated_days=5,
        raw_response={"test": True},
    )
    client.get_rates = AsyncMock(return_value=[mock_rate])

    # Mock create_shipment
    mock_shipment = UPSShipmentResult(
        shipment_id="1Z999AA10123456784",
        tracking_number="1Z999AA10123456784",
        label_data="base64encodedlabeldata",
        label_format="ZPL",
        total_charges=11.50,
        raw_response={"test": True},
    )
    client.create_shipment = AsyncMock(return_value=mock_shipment)

    # Mock track_shipment
    mock_tracking = UPSTrackingResult(
        tracking_number="1Z999AA10123456784",
        status="I",
        status_description="In Transit",
        delivered=False,
        delivery_date=None,
        signature=None,
        events=[],
    )
    client.track_shipment = AsyncMock(return_value=mock_tracking)

    # Mock void_shipment
    client.void_shipment = AsyncMock(return_value=True)

    # Mock close
    client.close = AsyncMock()

    return client


@pytest.fixture
def sample_address_data() -> dict:
    """Sample address data for tests."""
    return {
        "recipient_name": "John Doe",
        "address_line1": "123 Main Street",
        "address_line2": "Apt 4B",
        "city": "New York",
        "state_province": "NY",
        "postal_code": "10001",
        "country_code": "US",
        "phone": "212-555-1234",
        "email": "john.doe@example.com",
        "residential": True,
    }


@pytest.fixture
def sample_package_data() -> list:
    """Sample package data for tests."""
    return [
        {
            "weight": 1.5,
            "weight_unit": "LBS",
            "length": 12,
            "width": 10,
            "height": 2,
            "dimension_unit": "IN",
            "package_type": "02",
            "declared_value": 50.0,
        }
    ]
