"""
Tests for shipping service.
UPS Shipping Integration v1.28.0
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import os

# Set test environment
os.environ["ENVIRONMENT"] = "development"
os.environ["DATABASE_URL"] = "postgresql+asyncpg://test:test@localhost:5432/test_db"
os.environ["SECRET_KEY"] = "test-secret-key-for-unit-tests-only"

from app.services.shipping_service import ShippingService, ShippingError


class TestShippingService:
    """Test ShippingService class."""

    @pytest.fixture
    def shipping_service(self, mock_db):
        """Create shipping service with mock database."""
        return ShippingService(mock_db)

    @pytest.mark.asyncio
    async def test_get_carrier_not_found(self, shipping_service, mock_db):
        """Test getting carrier when not configured."""
        # Mock empty result
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute.return_value = mock_result

        with pytest.raises(ShippingError) as exc_info:
            await shipping_service._get_carrier()

        assert exc_info.value.code == "CARRIER_NOT_FOUND"

    @pytest.mark.asyncio
    async def test_get_carrier_cached(self, shipping_service, mock_db):
        """Test carrier caching."""
        from app.models.carrier import Carrier, CarrierCode

        # Create mock carrier
        mock_carrier = MagicMock(spec=Carrier)
        mock_carrier.code = CarrierCode.UPS
        mock_carrier.is_active = True

        # First call returns carrier
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_carrier
        mock_db.execute.return_value = mock_result

        # First call should query DB
        carrier1 = await shipping_service._get_carrier()
        assert carrier1 == mock_carrier
        assert mock_db.execute.call_count == 1

        # Second call should use cache
        carrier2 = await shipping_service._get_carrier()
        assert carrier2 == mock_carrier
        assert mock_db.execute.call_count == 1  # No additional DB call

    @pytest.mark.asyncio
    async def test_close_cleanup(self, shipping_service, mock_ups_client):
        """Test resource cleanup on close."""
        shipping_service._ups_client = mock_ups_client

        await shipping_service.close()

        mock_ups_client.close.assert_called_once()
        assert shipping_service._ups_client is None


class TestAddressCreation:
    """Test address creation functionality."""

    @pytest.fixture
    def shipping_service(self, mock_db):
        """Create shipping service with mock database."""
        return ShippingService(mock_db)

    @pytest.mark.asyncio
    async def test_create_address_encrypts_pii(self, shipping_service, mock_db, sample_address_data):
        """Test that PII fields are encrypted when creating address."""
        from app.services.encryption import decrypt_pii

        # Mock the UPS client for validation
        with patch.object(shipping_service, '_get_ups_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_client.validate_address = AsyncMock(return_value=(True, None, ["OK"]))
            mock_get_client.return_value = mock_client

            address = await shipping_service.create_address(
                user_id=1,
                validate=False,  # Skip validation for this test
                **sample_address_data
            )

            # Verify PII fields are encrypted
            assert address.recipient_name_encrypted != sample_address_data["recipient_name"]
            assert address.address_line1_encrypted != sample_address_data["address_line1"]
            assert address.phone_encrypted != sample_address_data["phone"]
            assert address.email_encrypted != sample_address_data["email"]

            # Verify encrypted values can be decrypted
            assert decrypt_pii(address.recipient_name_encrypted) == sample_address_data["recipient_name"]
            assert decrypt_pii(address.address_line1_encrypted) == sample_address_data["address_line1"]

    @pytest.mark.asyncio
    async def test_create_address_hashes_phone(self, shipping_service, mock_db, sample_address_data):
        """Test that phone number is hashed for lookup."""
        address = await shipping_service.create_address(
            user_id=1,
            validate=False,
            **sample_address_data
        )

        # Verify phone hash is created
        assert address.phone_hash is not None
        assert address.phone_hash.startswith("sha256:")

        # Verify last 4 digits are stored
        assert address.phone_last4 == "1234"

    @pytest.mark.asyncio
    async def test_create_address_non_us_skips_validation(self, shipping_service, mock_db, sample_address_data):
        """Test that non-US addresses skip UPS validation."""
        sample_address_data["country_code"] = "CA"

        address = await shipping_service.create_address(
            user_id=1,
            validate=True,
            **sample_address_data
        )

        # Should be pending since validation was skipped
        from app.models.address import AddressValidationStatus
        assert address.validation_status == AddressValidationStatus.PENDING


class TestRateQuoting:
    """Test rate quoting functionality."""

    @pytest.fixture
    def shipping_service(self, mock_db):
        """Create shipping service with mock database."""
        return ShippingService(mock_db)

    @pytest.mark.asyncio
    async def test_get_shipping_rates_address_not_found(self, shipping_service, mock_db):
        """Test getting rates when address doesn't exist."""
        # Mock no address found
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute.return_value = mock_result

        with pytest.raises(ShippingError) as exc_info:
            await shipping_service.get_shipping_rates(destination_address_id=999)

        assert exc_info.value.code == "ADDRESS_NOT_FOUND"


class TestShipmentCreation:
    """Test shipment creation functionality."""

    @pytest.fixture
    def shipping_service(self, mock_db):
        """Create shipping service with mock database."""
        return ShippingService(mock_db)

    @pytest.mark.asyncio
    async def test_create_shipment_order_not_found(self, shipping_service, mock_db):
        """Test creating shipment when order doesn't exist."""
        # Mock no order found
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute.return_value = mock_result

        with pytest.raises(ShippingError) as exc_info:
            await shipping_service.create_shipment(
                order_id=999,
                destination_address_id=1,
                service_code="03",
            )

        assert exc_info.value.code == "ORDER_NOT_FOUND"


class TestTracking:
    """Test tracking functionality."""

    @pytest.fixture
    def shipping_service(self, mock_db):
        """Create shipping service with mock database."""
        return ShippingService(mock_db)

    @pytest.mark.asyncio
    async def test_update_tracking_shipment_not_found(self, shipping_service, mock_db):
        """Test updating tracking when shipment doesn't exist."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute.return_value = mock_result

        with pytest.raises(ShippingError) as exc_info:
            await shipping_service.update_tracking(shipment_id=999)

        assert exc_info.value.code == "SHIPMENT_NOT_FOUND"

    @pytest.mark.asyncio
    async def test_update_tracking_no_tracking_number(self, shipping_service, mock_db):
        """Test updating tracking when shipment has no tracking number."""
        from app.models.shipment import Shipment

        mock_shipment = MagicMock(spec=Shipment)
        mock_shipment.id = 1
        mock_shipment.tracking_number = None

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_shipment
        mock_db.execute.return_value = mock_result

        with pytest.raises(ShippingError) as exc_info:
            await shipping_service.update_tracking(shipment_id=1)

        assert exc_info.value.code == "NO_TRACKING"


class TestVoidShipment:
    """Test void shipment functionality."""

    @pytest.fixture
    def shipping_service(self, mock_db):
        """Create shipping service with mock database."""
        return ShippingService(mock_db)

    @pytest.mark.asyncio
    async def test_void_shipment_not_found(self, shipping_service, mock_db):
        """Test voiding shipment that doesn't exist."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute.return_value = mock_result

        with pytest.raises(ShippingError) as exc_info:
            await shipping_service.void_shipment(shipment_id=999)

        assert exc_info.value.code == "SHIPMENT_NOT_FOUND"

    @pytest.mark.asyncio
    async def test_void_shipment_already_picked_up(self, shipping_service, mock_db):
        """Test voiding shipment that's already been picked up."""
        from app.models.shipment import Shipment, ShipmentStatus

        mock_shipment = MagicMock(spec=Shipment)
        mock_shipment.id = 1
        mock_shipment.status = ShipmentStatus.IN_TRANSIT

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_shipment
        mock_db.execute.return_value = mock_result

        with pytest.raises(ShippingError) as exc_info:
            await shipping_service.void_shipment(shipment_id=1)

        assert exc_info.value.code == "CANNOT_VOID"


class TestShippingError:
    """Test ShippingError exception."""

    def test_error_with_message(self):
        """Test creating error with message."""
        error = ShippingError(message="Test error")
        assert str(error) == "Test error"
        assert error.code == "SHIPPING_ERROR"
        assert error.details == {}

    def test_error_with_code(self):
        """Test creating error with custom code."""
        error = ShippingError(message="Not found", code="NOT_FOUND")
        assert error.code == "NOT_FOUND"

    def test_error_with_details(self):
        """Test creating error with details."""
        details = {"field": "address_line1", "reason": "too long"}
        error = ShippingError(message="Validation failed", details=details)
        assert error.details == details
