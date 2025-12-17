"""
MDM Comics Exception Hierarchy

Structured exception classes for BCW dropship automation and other subsystems.
Per constitution_logging.json: All exceptions include code, message, and details
for audit trail and debugging.

Exception Hierarchy:
    MDMBaseError
    ├── BCWError
    │   ├── BCWAuthError
    │   ├── BCWSelectorError
    │   ├── BCWInventoryError
    │   ├── BCWCartError
    │   └── BCWOrderError
    ├── ShippingError
    │   ├── ShippingQuoteError
    │   ├── ShippingValidationError
    │   └── ShippingLabelError
    ├── PaymentError
    │   ├── PaymentAuthError
    │   ├── PaymentCaptureError
    │   └── PaymentRefundError
    └── InventoryError
        ├── StockError
        └── BackorderError
"""
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class MDMBaseError(Exception):
    """
    Base exception for all MDM Comics custom errors.

    Attributes:
        message: Human-readable error description
        code: Machine-readable error code for programmatic handling
        details: Additional context for debugging/audit
        severity: P0-P3 severity level per severity_policy
    """

    default_code: str = "MDM_ERROR"
    default_severity: str = "P2"

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        severity: Optional[str] = None,
    ):
        self.message = message
        self.code = code or self.default_code
        self.details = details or {}
        self.severity = severity or self.default_severity
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/serialization."""
        return {
            "error_type": self.__class__.__name__,
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
            "details": self.details,
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(code={self.code!r}, message={self.message!r})"


# =============================================================================
# BCW DROPSHIP ERRORS
# =============================================================================

class BCWError(MDMBaseError):
    """Base exception for BCW integration errors."""
    default_code = "BCW_ERROR"
    default_severity = "P1"


class BCWAuthError(BCWError):
    """Authentication/session failures with BCW."""
    default_code = "BCW_AUTH_FAILED"
    default_severity = "P0"  # Auth failures are critical


class BCWSelectorError(BCWError):
    """DOM selector failures - website may have changed."""
    default_code = "BCW_SELECTOR_FAILED"
    default_severity = "P1"

    def __init__(
        self,
        message: str,
        selector_key: Optional[str] = None,
        selector_version: Optional[str] = None,
        screenshot_path: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        details.update({
            "selector_key": selector_key,
            "selector_version": selector_version,
            "screenshot_path": screenshot_path,
        })
        super().__init__(message, details=details, **kwargs)


class BCWInventoryError(BCWError):
    """Inventory sync failures."""
    default_code = "BCW_INVENTORY_SYNC_FAILED"
    default_severity = "P1"


class BCWCartError(BCWError):
    """Cart building/manipulation failures."""
    default_code = "BCW_CART_ERROR"
    default_severity = "P1"


class BCWOrderError(BCWError):
    """Order submission failures."""
    default_code = "BCW_ORDER_FAILED"
    default_severity = "P0"  # Order failures are critical

    def __init__(
        self,
        message: str,
        order_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        details.update({
            "order_id": order_id,
            "idempotency_key": idempotency_key,
        })
        super().__init__(message, details=details, **kwargs)


class BCWRateLimitError(BCWError):
    """Rate limit exceeded for BCW automation."""
    default_code = "BCW_RATE_LIMITED"
    default_severity = "P2"

    def __init__(
        self,
        message: str,
        retry_after_seconds: Optional[int] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        details["retry_after_seconds"] = retry_after_seconds
        super().__init__(message, details=details, **kwargs)


class BCWCircuitOpenError(BCWError):
    """Circuit breaker is open - BCW automation suspended."""
    default_code = "BCW_CIRCUIT_OPEN"
    default_severity = "P1"


# =============================================================================
# SHIPPING ERRORS
# =============================================================================

class ShippingError(MDMBaseError):
    """Base exception for shipping-related errors."""
    default_code = "SHIPPING_ERROR"
    default_severity = "P1"


class ShippingQuoteError(ShippingError):
    """Failed to get shipping quote."""
    default_code = "SHIPPING_QUOTE_FAILED"


class ShippingValidationError(ShippingError):
    """Address validation failed."""
    default_code = "SHIPPING_VALIDATION_FAILED"
    default_severity = "P2"

    def __init__(
        self,
        message: str,
        address_type: Optional[str] = None,  # e.g., "PO_BOX", "APO_MPO"
        **kwargs
    ):
        details = kwargs.pop("details", {})
        details["address_type"] = address_type
        super().__init__(message, details=details, **kwargs)


class ShippingLabelError(ShippingError):
    """Failed to create shipping label."""
    default_code = "SHIPPING_LABEL_FAILED"


# =============================================================================
# PAYMENT ERRORS
# =============================================================================

class PaymentError(MDMBaseError):
    """Base exception for payment processing errors."""
    default_code = "PAYMENT_ERROR"
    default_severity = "P0"  # Payment errors are always critical


class PaymentAuthError(PaymentError):
    """Payment authorization failed."""
    default_code = "PAYMENT_AUTH_FAILED"


class PaymentCaptureError(PaymentError):
    """Payment capture failed."""
    default_code = "PAYMENT_CAPTURE_FAILED"

    def __init__(
        self,
        message: str,
        auth_id: Optional[str] = None,
        authorized_amount: Optional[float] = None,
        capture_amount: Optional[float] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        details.update({
            "auth_id": auth_id,
            "authorized_amount": authorized_amount,
            "capture_amount": capture_amount,
        })
        super().__init__(message, details=details, **kwargs)


class PaymentRefundError(PaymentError):
    """Refund failed."""
    default_code = "PAYMENT_REFUND_FAILED"


# =============================================================================
# INVENTORY ERRORS
# =============================================================================

class InventoryError(MDMBaseError):
    """Base exception for inventory-related errors."""
    default_code = "INVENTORY_ERROR"
    default_severity = "P1"


class StockError(InventoryError):
    """Stock-related errors (oversell, unavailable)."""
    default_code = "STOCK_ERROR"

    def __init__(
        self,
        message: str,
        sku: Optional[str] = None,
        requested_qty: Optional[int] = None,
        available_qty: Optional[int] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        details.update({
            "sku": sku,
            "requested_qty": requested_qty,
            "available_qty": available_qty,
        })
        super().__init__(message, details=details, **kwargs)


class BackorderError(InventoryError):
    """Backorder-related errors."""
    default_code = "BACKORDER_ERROR"
    default_severity = "P2"

    def __init__(
        self,
        message: str,
        sku: Optional[str] = None,
        expected_date: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        details.update({
            "sku": sku,
            "expected_date": expected_date,
        })
        super().__init__(message, details=details, **kwargs)


# =============================================================================
# DROPSHIP ORCHESTRATION ERRORS
# =============================================================================

class DropshipError(MDMBaseError):
    """Base exception for dropship orchestration errors."""
    default_code = "DROPSHIP_ERROR"
    default_severity = "P1"


class DropshipQuoteError(DropshipError):
    """Failed to get dropship quote."""
    default_code = "DROPSHIP_QUOTE_FAILED"


class DropshipSubmissionError(DropshipError):
    """Failed to submit dropship order."""
    default_code = "DROPSHIP_SUBMISSION_FAILED"
    default_severity = "P0"


class DropshipIdempotencyError(DropshipError):
    """Duplicate order detected via idempotency key."""
    default_code = "DROPSHIP_DUPLICATE_ORDER"
    default_severity = "P0"

    def __init__(
        self,
        message: str,
        idempotency_key: Optional[str] = None,
        existing_order_id: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        details.update({
            "idempotency_key": idempotency_key,
            "existing_order_id": existing_order_id,
        })
        super().__init__(message, details=details, **kwargs)


# =============================================================================
# EMAIL PARSING ERRORS
# =============================================================================

class EmailParseError(MDMBaseError):
    """Failed to parse BCW notification email."""
    default_code = "EMAIL_PARSE_FAILED"
    default_severity = "P2"


# =============================================================================
# EXCEPTION CATALOG (for exception_catalog in proposal doc)
# =============================================================================

EXCEPTION_CATALOG = {
    "SHIPPING_QUOTE_FAILED": {"class": ShippingQuoteError, "severity": "P1"},
    "VENDOR_LOGIN_FAILED": {"class": BCWAuthError, "severity": "P0"},
    "VENDOR_SUBMIT_FAILED": {"class": BCWOrderError, "severity": "P0"},
    "INVENTORY_MISMATCH_OVERSELL_RISK": {"class": StockError, "severity": "P1"},
    "ADDRESS_BLOCKED": {"class": ShippingValidationError, "severity": "P2"},
    "BACKORDER_DATE_SLIPPED": {"class": BackorderError, "severity": "P2"},
    "PARTIAL_SHIPMENT_DETECTED": {"class": ShippingError, "severity": "P2"},
    "CARRIER_EXCEPTION": {"class": ShippingError, "severity": "P2"},
    "PAYMENT_CAPTURE_FAILED": {"class": PaymentCaptureError, "severity": "P0"},
    "REFUND_FAILED": {"class": PaymentRefundError, "severity": "P1"},
    "POLICY_OVERRIDE_REQUIRED": {"class": DropshipError, "severity": "P1"},
    "DUPLICATE_ORDER_DETECTED": {"class": DropshipIdempotencyError, "severity": "P0"},
}
