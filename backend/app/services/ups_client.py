"""
UPS API Client for MDM Comics Shipping Integration v1.28.0

Implements UPS OAuth 2.0 authentication and core shipping APIs:
- Address Validation
- Rating (get shipping quotes)
- Shipping (create labels)
- Tracking

Per constitution_binder.json: All external API calls must be logged and have proper error handling.
"""
import base64
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field

import httpx

from app.core.config import settings
from app.services.encryption import decrypt_pii, sanitize_for_logging

logger = logging.getLogger(__name__)

# UPS API URLs
UPS_PRODUCTION_URL = "https://onlinetools.ups.com"
UPS_SANDBOX_URL = "https://wwwcie.ups.com"

# OAuth endpoints
OAUTH_TOKEN_PATH = "/security/v1/oauth/token"

# API endpoints
ADDRESS_VALIDATION_PATH = "/api/addressvalidation/v1/1"  # 1 = street level validation
RATING_PATH = "/api/rating/v2403/Rate"  # v2403 is current version
SHIPPING_PATH = "/api/shipments/v2403/ship"
TRACKING_PATH = "/api/track/v1/details"
VOID_PATH = "/api/shipments/v2403/void"

# Rate quote TTL in minutes
RATE_QUOTE_TTL_MINUTES = 30


@dataclass
class UPSCredentials:
    """UPS API credentials."""
    client_id: str
    client_secret: str
    account_number: str
    use_sandbox: bool = False

    @property
    def base_url(self) -> str:
        return UPS_SANDBOX_URL if self.use_sandbox else UPS_PRODUCTION_URL


@dataclass
class UPSAddress:
    """Address structure for UPS APIs."""
    name: str
    address_line1: str
    city: str
    state_province: str
    postal_code: str
    country_code: str
    address_line2: Optional[str] = None
    address_line3: Optional[str] = None
    company_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    residential: bool = True

    def to_ups_format(self) -> Dict:
        """Convert to UPS API format."""
        address = {
            "Name": self.name[:35],  # UPS limit
            "Address": {
                "AddressLine": [self.address_line1],
                "City": self.city,
                "StateProvinceCode": self.state_province[:5] if self.state_province else "",
                "PostalCode": self.postal_code,
                "CountryCode": self.country_code,
            },
        }

        if self.address_line2:
            address["Address"]["AddressLine"].append(self.address_line2)
        if self.address_line3:
            address["Address"]["AddressLine"].append(self.address_line3)
        if self.company_name:
            address["AttentionName"] = self.name[:35]
            address["Name"] = self.company_name[:35]
        if self.phone:
            address["Phone"] = {"Number": self.phone[:15]}
        if self.email:
            address["EMailAddress"] = self.email[:50]
        if self.residential:
            address["Address"]["ResidentialAddressIndicator"] = ""

        return address


@dataclass
class UPSPackage:
    """Package details for UPS APIs."""
    weight: float
    weight_unit: str = "LBS"  # LBS or KGS
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    dimension_unit: str = "IN"  # IN or CM
    package_type: str = "02"  # Customer Supplied Package
    declared_value: Optional[float] = None
    declared_value_currency: str = "USD"

    def to_ups_format(self) -> Dict:
        """Convert to UPS API format."""
        package = {
            "PackagingType": {"Code": self.package_type},
            "PackageWeight": {
                "UnitOfMeasurement": {"Code": self.weight_unit},
                "Weight": str(round(self.weight, 1)),
            },
        }

        if self.length and self.width and self.height:
            package["Dimensions"] = {
                "UnitOfMeasurement": {"Code": self.dimension_unit},
                "Length": str(round(self.length, 1)),
                "Width": str(round(self.width, 1)),
                "Height": str(round(self.height, 1)),
            }

        if self.declared_value:
            package["PackageServiceOptions"] = {
                "DeclaredValue": {
                    "CurrencyCode": self.declared_value_currency,
                    "MonetaryValue": str(round(self.declared_value, 2)),
                }
            }

        return package


@dataclass
class UPSRate:
    """Shipping rate from UPS."""
    service_code: str
    service_name: str
    total_charges: float
    currency: str
    base_charge: float = 0.0
    fuel_surcharge: float = 0.0
    residential_surcharge: float = 0.0
    delivery_area_surcharge: float = 0.0
    other_surcharges: float = 0.0
    guaranteed_delivery: bool = False
    estimated_delivery: Optional[datetime] = None
    estimated_days: Optional[int] = None
    billing_weight: Optional[float] = None
    raw_response: Dict = field(default_factory=dict)


@dataclass
class UPSShipmentResult:
    """Result of creating a shipment."""
    shipment_id: str
    tracking_number: str
    label_data: str  # Base64 encoded
    label_format: str
    total_charges: float
    currency: str
    raw_response: Dict = field(default_factory=dict)


@dataclass
class UPSTrackingEvent:
    """Tracking event from UPS."""
    event_type: str
    description: str
    event_time: datetime
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    event_code: Optional[str] = None


@dataclass
class UPSTrackingResult:
    """Complete tracking result."""
    tracking_number: str
    status: str
    status_description: str
    events: List[UPSTrackingEvent] = field(default_factory=list)
    delivered: bool = False
    delivery_date: Optional[datetime] = None
    estimated_delivery: Optional[datetime] = None
    signature: Optional[str] = None
    raw_response: Dict = field(default_factory=dict)


class UPSAPIError(Exception):
    """UPS API error with details."""

    def __init__(self, message: str, code: Optional[str] = None, details: Optional[Dict] = None):
        self.message = message
        self.code = code
        self.details = details or {}
        super().__init__(message)


class UPSClient:
    """
    UPS API Client with OAuth 2.0 authentication.

    Handles token refresh and provides methods for all shipping operations.
    """

    def __init__(self, credentials: UPSCredentials):
        self.credentials = credentials
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=30.0,
                headers={"Content-Type": "application/json"},
            )
        return self._http_client

    async def close(self):
        """Close HTTP client."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def _ensure_token(self) -> str:
        """Ensure we have a valid OAuth token."""
        if self._access_token and self._token_expires_at:
            # Refresh 5 minutes before expiry
            if datetime.now(timezone.utc) < self._token_expires_at - timedelta(minutes=5):
                return self._access_token

        # Get new token
        client = await self._get_http_client()
        url = f"{self.credentials.base_url}{OAUTH_TOKEN_PATH}"

        # Basic auth header
        auth_string = f"{self.credentials.client_id}:{self.credentials.client_secret}"
        auth_header = base64.b64encode(auth_string.encode()).decode()

        try:
            response = await client.post(
                url,
                headers={
                    "Authorization": f"Basic {auth_header}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"grant_type": "client_credentials"},
            )

            if response.status_code != 200:
                logger.error(f"UPS OAuth failed: {response.status_code} - {response.text[:500]}")
                raise UPSAPIError(
                    message="Failed to authenticate with UPS",
                    code="AUTH_FAILED",
                    details={"status": response.status_code},
                )

            data = response.json()
            self._access_token = data["access_token"]
            expires_in = int(data.get("expires_in", 3600))
            self._token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

            logger.info(f"UPS OAuth token obtained, expires in {expires_in}s")
            return self._access_token

        except httpx.RequestError as e:
            logger.error(f"UPS OAuth request failed: {e}")
            raise UPSAPIError(message=f"Network error during authentication: {e}", code="NETWORK_ERROR")

    async def _make_request(
        self,
        method: str,
        path: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict:
        """Make authenticated API request."""
        token = await self._ensure_token()
        client = await self._get_http_client()
        url = f"{self.credentials.base_url}{path}"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "transId": f"mdm_{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
            "transactionSrc": "MDM Comics",
        }

        try:
            if method.upper() == "GET":
                response = await client.get(url, headers=headers, params=params)
            elif method.upper() == "POST":
                response = await client.post(url, headers=headers, json=data)
            elif method.upper() == "DELETE":
                response = await client.delete(url, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            # Log request (sanitized)
            logger.debug(f"UPS API {method} {path} -> {response.status_code}")

            if response.status_code >= 400:
                error_data = {}
                try:
                    error_data = response.json()
                except Exception:
                    error_data = {"raw": response.text[:500]}

                # Extract UPS error details
                error_msg = "UPS API error"
                error_code = str(response.status_code)

                if "response" in error_data:
                    errors = error_data.get("response", {}).get("errors", [])
                    if errors:
                        error_msg = errors[0].get("message", error_msg)
                        error_code = errors[0].get("code", error_code)

                logger.error(f"UPS API error: {error_code} - {sanitize_for_logging(error_msg)}")
                raise UPSAPIError(message=error_msg, code=error_code, details=error_data)

            return response.json()

        except httpx.RequestError as e:
            logger.error(f"UPS API request failed: {e}")
            raise UPSAPIError(message=f"Network error: {e}", code="NETWORK_ERROR")

    # ==================== Address Validation ====================

    async def validate_address(self, address: UPSAddress) -> Tuple[bool, Optional[UPSAddress], List[str]]:
        """
        Validate an address using UPS Address Validation API.

        Returns:
            Tuple of (is_valid, corrected_address, messages)
        """
        request_data = {
            "XAVRequest": {
                "AddressKeyFormat": {
                    "ConsigneeName": address.name,
                    "AddressLine": [address.address_line1],
                    "PoliticalDivision2": address.city,
                    "PoliticalDivision1": address.state_province,
                    "PostcodePrimaryLow": address.postal_code,
                    "CountryCode": address.country_code,
                }
            }
        }

        if address.address_line2:
            request_data["XAVRequest"]["AddressKeyFormat"]["AddressLine"].append(address.address_line2)

        try:
            response = await self._make_request("POST", ADDRESS_VALIDATION_PATH, data=request_data)

            xav_response = response.get("XAVResponse", {})
            valid_indicator = xav_response.get("ValidAddressIndicator") is not None
            ambiguous_indicator = xav_response.get("AmbiguousAddressIndicator") is not None
            no_candidates = xav_response.get("NoCandidatesIndicator") is not None

            messages = []
            corrected_address = None

            if no_candidates:
                messages.append("No valid address found for the provided information")
                return False, None, messages

            if ambiguous_indicator:
                messages.append("Multiple addresses match - please verify")
                candidates = xav_response.get("Candidate", [])
                if isinstance(candidates, dict):
                    candidates = [candidates]
                if candidates:
                    # Return first candidate as suggestion
                    addr_key = candidates[0].get("AddressKeyFormat", {})
                    corrected_address = UPSAddress(
                        name=address.name,
                        address_line1=addr_key.get("AddressLine", [""])[0] if addr_key.get("AddressLine") else "",
                        city=addr_key.get("PoliticalDivision2", ""),
                        state_province=addr_key.get("PoliticalDivision1", ""),
                        postal_code=f"{addr_key.get('PostcodePrimaryLow', '')}-{addr_key.get('PostcodeExtendedLow', '')}".rstrip("-"),
                        country_code=addr_key.get("CountryCode", ""),
                        residential=address.residential,
                    )

            if valid_indicator:
                # Get the validated address
                addr_key = xav_response.get("AddressKeyFormat", {})
                if addr_key:
                    corrected_address = UPSAddress(
                        name=address.name,
                        address_line1=addr_key.get("AddressLine", [""])[0] if addr_key.get("AddressLine") else "",
                        city=addr_key.get("PoliticalDivision2", ""),
                        state_province=addr_key.get("PoliticalDivision1", ""),
                        postal_code=f"{addr_key.get('PostcodePrimaryLow', '')}-{addr_key.get('PostcodeExtendedLow', '')}".rstrip("-"),
                        country_code=addr_key.get("CountryCode", ""),
                        residential=address.residential,
                    )

                # Check classification
                classification = xav_response.get("AddressClassification", {})
                if classification.get("Code") == "1":
                    messages.append("Address classified as commercial")
                    if corrected_address:
                        corrected_address.residential = False
                elif classification.get("Code") == "2":
                    messages.append("Address classified as residential")

            return valid_indicator, corrected_address, messages

        except UPSAPIError:
            raise
        except Exception as e:
            logger.error(f"Address validation failed: {e}")
            raise UPSAPIError(message=f"Address validation failed: {e}", code="VALIDATION_ERROR")

    # ==================== Rating ====================

    async def get_rates(
        self,
        origin: UPSAddress,
        destination: UPSAddress,
        packages: List[UPSPackage],
        service_code: Optional[str] = None,
    ) -> List[UPSRate]:
        """
        Get shipping rates for a shipment.

        Args:
            origin: Origin address
            destination: Destination address
            packages: List of packages
            service_code: Optional specific service (None for all available)

        Returns:
            List of available rates
        """
        # Build shipper (origin)
        shipper = origin.to_ups_format()
        shipper["ShipperNumber"] = self.credentials.account_number

        # Build ship to (destination)
        ship_to = destination.to_ups_format()

        # Build ship from (same as shipper for now)
        ship_from = origin.to_ups_format()

        # Build packages
        package_list = [pkg.to_ups_format() for pkg in packages]

        request_data = {
            "RateRequest": {
                "Request": {
                    "SubVersion": "2403",
                    "TransactionReference": {
                        "CustomerContext": f"MDM Rate {datetime.now().isoformat()}",
                    },
                },
                "Shipment": {
                    "Shipper": shipper,
                    "ShipTo": ship_to,
                    "ShipFrom": ship_from,
                    "Package": package_list if len(package_list) > 1 else package_list[0],
                },
            }
        }

        # Add service if specified
        if service_code:
            request_data["RateRequest"]["Shipment"]["Service"] = {"Code": service_code}
        else:
            # Request Shop rates (all available services)
            request_data["RateRequest"]["Request"]["RequestOption"] = "Shop"

        try:
            response = await self._make_request("POST", RATING_PATH, data=request_data)

            rate_response = response.get("RateResponse", {})
            rated_shipments = rate_response.get("RatedShipment", [])

            if isinstance(rated_shipments, dict):
                rated_shipments = [rated_shipments]

            rates = []
            for rs in rated_shipments:
                service = rs.get("Service", {})
                total = rs.get("TotalCharges", {})

                # Parse surcharges
                base_charge = float(rs.get("TransportationCharges", {}).get("MonetaryValue", 0))
                surcharges = rs.get("ItemizedCharges", [])
                if isinstance(surcharges, dict):
                    surcharges = [surcharges]

                fuel = 0.0
                residential = 0.0
                delivery_area = 0.0
                other = 0.0

                for sc in surcharges:
                    code = sc.get("Code", "")
                    value = float(sc.get("MonetaryValue", 0))
                    if code in ["375", "376"]:  # Fuel surcharge codes
                        fuel += value
                    elif code == "270":  # Residential
                        residential += value
                    elif code in ["110", "111"]:  # Delivery area
                        delivery_area += value
                    else:
                        other += value

                # Estimated delivery
                est_delivery = None
                est_days = None
                guaranteed = False

                time_in_transit = rs.get("TimeInTransit", {})
                if time_in_transit:
                    service_summary = time_in_transit.get("ServiceSummary", {})
                    if service_summary:
                        est_arrival = service_summary.get("EstimatedArrival", {})
                        if est_arrival:
                            date_str = est_arrival.get("Date", "")
                            time_str = est_arrival.get("Time", "")
                            if date_str:
                                try:
                                    est_delivery = datetime.strptime(
                                        f"{date_str} {time_str or '1800'}",
                                        "%Y%m%d %H%M"
                                    ).replace(tzinfo=timezone.utc)
                                except ValueError:
                                    pass
                            guaranteed = est_arrival.get("Guarantee") is not None

                    business_days = time_in_transit.get("ServiceSummary", {}).get("EstimatedArrival", {}).get("BusinessDaysInTransit")
                    if business_days:
                        est_days = int(business_days)

                # Get service name from our map or UPS response
                from app.models.carrier import UPS_SERVICE_CODES
                service_name = UPS_SERVICE_CODES.get(
                    service.get("Code", ""),
                    service.get("Description", f"UPS Service {service.get('Code', 'Unknown')}")
                )

                rates.append(UPSRate(
                    service_code=service.get("Code", ""),
                    service_name=service_name,
                    total_charges=float(total.get("MonetaryValue", 0)),
                    currency=total.get("CurrencyCode", "USD"),
                    base_charge=base_charge,
                    fuel_surcharge=fuel,
                    residential_surcharge=residential,
                    delivery_area_surcharge=delivery_area,
                    other_surcharges=other,
                    guaranteed_delivery=guaranteed,
                    estimated_delivery=est_delivery,
                    estimated_days=est_days,
                    billing_weight=float(rs.get("BillingWeight", {}).get("Weight", 0)),
                    raw_response=rs,
                ))

            return sorted(rates, key=lambda r: r.total_charges)

        except UPSAPIError:
            raise
        except Exception as e:
            logger.error(f"Rate request failed: {e}")
            raise UPSAPIError(message=f"Rate request failed: {e}", code="RATE_ERROR")

    # ==================== Shipping (Label Creation) ====================

    async def create_shipment(
        self,
        origin: UPSAddress,
        destination: UPSAddress,
        packages: List[UPSPackage],
        service_code: str,
        label_format: str = "ZPL",
        reference: Optional[str] = None,
        signature_required: bool = False,
    ) -> UPSShipmentResult:
        """
        Create a shipment and get shipping label.

        Args:
            origin: Origin address
            destination: Destination address
            packages: List of packages
            service_code: UPS service code
            label_format: Label format (ZPL, GIF, PNG, EPL)
            reference: Optional reference number
            signature_required: Require signature on delivery

        Returns:
            Shipment result with tracking number and label
        """
        # Build shipper
        shipper = origin.to_ups_format()
        shipper["ShipperNumber"] = self.credentials.account_number

        # Build ship to
        ship_to = destination.to_ups_format()

        # Build ship from
        ship_from = origin.to_ups_format()

        # Build packages
        package_list = [pkg.to_ups_format() for pkg in packages]

        # Add signature if required
        if signature_required:
            for pkg in package_list:
                if "PackageServiceOptions" not in pkg:
                    pkg["PackageServiceOptions"] = {}
                pkg["PackageServiceOptions"]["DeliveryConfirmation"] = {"DCISType": "2"}  # Signature Required

        # Label image format
        image_format = {
            "ZPL": {"Code": "ZPL", "Description": "ZPL"},
            "GIF": {"Code": "GIF", "Description": "GIF"},
            "PNG": {"Code": "PNG", "Description": "PNG"},
            "EPL": {"Code": "EPL", "Description": "EPL2"},
        }.get(label_format.upper(), {"Code": "ZPL", "Description": "ZPL"})

        request_data = {
            "ShipmentRequest": {
                "Request": {
                    "SubVersion": "2403",
                    "TransactionReference": {
                        "CustomerContext": reference or f"MDM Ship {datetime.now().isoformat()}",
                    },
                },
                "Shipment": {
                    "Description": "Comic Books and Collectibles",
                    "Shipper": shipper,
                    "ShipTo": ship_to,
                    "ShipFrom": ship_from,
                    "PaymentInformation": {
                        "ShipmentCharge": {
                            "Type": "01",  # Transportation
                            "BillShipper": {
                                "AccountNumber": self.credentials.account_number,
                            },
                        },
                    },
                    "Service": {"Code": service_code},
                    "Package": package_list if len(package_list) > 1 else package_list[0],
                },
                "LabelSpecification": {
                    "LabelImageFormat": image_format,
                    "LabelStockSize": {"Height": "6", "Width": "4"},
                },
            }
        }

        # Add reference if provided
        if reference:
            request_data["ShipmentRequest"]["Shipment"]["ReferenceNumber"] = {
                "Code": "01",  # Customer Reference
                "Value": reference[:35],
            }

        try:
            response = await self._make_request("POST", SHIPPING_PATH, data=request_data)

            ship_response = response.get("ShipmentResponse", {})
            shipment_results = ship_response.get("ShipmentResults", {})

            # Get tracking number
            package_results = shipment_results.get("PackageResults", {})
            if isinstance(package_results, list):
                package_results = package_results[0]

            tracking_number = package_results.get("TrackingNumber", "")

            # Get label
            label_data = ""
            shipping_label = package_results.get("ShippingLabel", {})
            if shipping_label:
                graphic = shipping_label.get("GraphicImage", "")
                if graphic:
                    label_data = graphic

            # Get charges
            shipment_charges = shipment_results.get("ShipmentCharges", {})
            total_charges = float(shipment_charges.get("TotalCharges", {}).get("MonetaryValue", 0))
            currency = shipment_charges.get("TotalCharges", {}).get("CurrencyCode", "USD")

            # Get shipment ID
            shipment_id = shipment_results.get("ShipmentIdentificationNumber", "")

            return UPSShipmentResult(
                shipment_id=shipment_id,
                tracking_number=tracking_number,
                label_data=label_data,
                label_format=label_format,
                total_charges=total_charges,
                currency=currency,
                raw_response=response,
            )

        except UPSAPIError:
            raise
        except Exception as e:
            logger.error(f"Create shipment failed: {e}")
            raise UPSAPIError(message=f"Create shipment failed: {e}", code="SHIP_ERROR")

    # ==================== Tracking ====================

    async def track_shipment(self, tracking_number: str) -> UPSTrackingResult:
        """
        Get tracking information for a shipment.

        Args:
            tracking_number: UPS tracking number

        Returns:
            Tracking result with events
        """
        params = {"locale": "en_US", "returnSignature": "true"}

        try:
            response = await self._make_request(
                "GET",
                f"{TRACKING_PATH}/{tracking_number}",
                params=params,
            )

            track_response = response.get("trackResponse", {})
            shipments = track_response.get("shipment", [])

            if not shipments:
                raise UPSAPIError(message="No tracking information found", code="NOT_FOUND")

            shipment = shipments[0] if isinstance(shipments, list) else shipments
            packages = shipment.get("package", [])

            if not packages:
                raise UPSAPIError(message="No package information found", code="NOT_FOUND")

            package = packages[0] if isinstance(packages, list) else packages

            # Get current status
            current_status = package.get("currentStatus", {})
            status = current_status.get("code", "")
            status_description = current_status.get("description", "")

            # Parse events
            activities = package.get("activity", [])
            if isinstance(activities, dict):
                activities = [activities]

            events = []
            for activity in activities:
                status_info = activity.get("status", {})
                location = activity.get("location", {}).get("address", {})

                # Parse datetime
                event_time = None
                date_str = activity.get("date", "")
                time_str = activity.get("time", "")
                if date_str:
                    try:
                        event_time = datetime.strptime(
                            f"{date_str} {time_str or '000000'}",
                            "%Y%m%d %H%M%S"
                        ).replace(tzinfo=timezone.utc)
                    except ValueError:
                        pass

                events.append(UPSTrackingEvent(
                    event_type=status_info.get("type", ""),
                    description=status_info.get("description", ""),
                    event_time=event_time or datetime.now(timezone.utc),
                    city=location.get("city", ""),
                    state=location.get("stateProvince", ""),
                    postal_code=location.get("postalCode", ""),
                    country=location.get("country", ""),
                    event_code=status_info.get("code", ""),
                ))

            # Check if delivered
            delivered = status.upper() in ["D", "DELIVERED"]
            delivery_date = None
            if delivered and events:
                delivery_date = events[0].event_time

            # Get signature
            signature = None
            delivery_info = package.get("deliveryInformation", {})
            if delivery_info:
                signature = delivery_info.get("signature", {}).get("name", "")

            return UPSTrackingResult(
                tracking_number=tracking_number,
                status=status,
                status_description=status_description,
                events=events,
                delivered=delivered,
                delivery_date=delivery_date,
                signature=signature,
                raw_response=response,
            )

        except UPSAPIError:
            raise
        except Exception as e:
            logger.error(f"Track shipment failed: {e}")
            raise UPSAPIError(message=f"Track shipment failed: {e}", code="TRACK_ERROR")

    # ==================== Void Shipment ====================

    async def void_shipment(self, shipment_id: str, tracking_numbers: Optional[List[str]] = None) -> bool:
        """
        Void a shipment (before pickup).

        Args:
            shipment_id: UPS shipment identification number
            tracking_numbers: Optional list of specific tracking numbers to void

        Returns:
            True if voided successfully
        """
        try:
            path = f"{VOID_PATH}/cancel/{shipment_id}"
            response = await self._make_request("DELETE", path)

            void_response = response.get("VoidShipmentResponse", {})
            summary = void_response.get("SummaryResult", {})

            status = summary.get("Status", {})
            if status.get("Code") == "1":
                logger.info(f"Shipment {shipment_id} voided successfully")
                return True

            logger.warning(f"Void shipment returned non-success: {summary}")
            return False

        except UPSAPIError:
            raise
        except Exception as e:
            logger.error(f"Void shipment failed: {e}")
            raise UPSAPIError(message=f"Void shipment failed: {e}", code="VOID_ERROR")


# Factory function for creating client from carrier config
async def create_ups_client_from_carrier(carrier) -> UPSClient:
    """Create UPS client from Carrier model instance."""
    from app.services.encryption import decrypt_pii

    credentials = UPSCredentials(
        client_id=decrypt_pii(carrier.client_id_encrypted) if carrier.client_id_encrypted else "",
        client_secret=decrypt_pii(carrier.client_secret_encrypted) if carrier.client_secret_encrypted else "",
        account_number=decrypt_pii(carrier.account_number_encrypted) if carrier.account_number_encrypted else "",
        use_sandbox=carrier.use_sandbox,
    )

    return UPSClient(credentials)
