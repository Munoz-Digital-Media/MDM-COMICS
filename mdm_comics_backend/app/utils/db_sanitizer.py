"""
Database Input Sanitizer v1.0.0

Constitution Compliance: Phase 3 Input Validation (constitution_cyberSec.json)
Constitution Compliance: Phase 4 Schema Validation (constitution_db.json)

This module provides a centralized validation layer between adapter outputs
and database writes. ALL pipeline jobs MUST use these functions before INSERT/UPDATE.

Created after incident MDM-INC-2024-002 (GCD import v1.10.3 type errors).
"""
import logging
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class SanitizationError(Exception):
    """Raised when sanitization fails and cannot recover."""
    pass


def sanitize_date(
    value: Any,
    field_name: str = "date",
    strict: bool = False
) -> Optional[date]:
    """
    Sanitize value for PostgreSQL DATE column.

    Constitution Compliance:
    - Phase 3: Input validation before DB write
    - Phase 4: Type safety for schema columns

    Args:
        value: Raw value from adapter (can be str, date, datetime, None, '')
        field_name: Name of field (for logging)
        strict: If True, raise error on invalid format; if False, return None

    Returns:
        date object or None

    Raises:
        SanitizationError: If strict=True and value cannot be parsed
    """
    # Handle None and empty values
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()
        if value == '':
            return None

    # Already a date
    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    # datetime -> date
    if isinstance(value, datetime):
        return value.date()

    # String parsing
    if isinstance(value, str):
        # Common date formats from various sources
        formats = [
            '%Y-%m-%d',      # ISO: 2023-05-15
            '%Y-%m',         # Year-month: 2023-05
            '%Y',            # Year only: 2023
            '%m/%d/%Y',      # US: 05/15/2023
            '%d/%m/%Y',      # EU: 15/05/2023
            '%B %Y',         # Month Year: May 2023
            '%b %Y',         # Mon Year: May 2023
            '%Y%m%d',        # Compact: 20230515
        ]

        for fmt in formats:
            try:
                parsed = datetime.strptime(value, fmt)
                return parsed.date()
            except ValueError:
                continue

        # Try to extract year at minimum
        year_match = re.search(r'\b(19|20)\d{2}\b', value)
        if year_match:
            try:
                return date(int(year_match.group(0)), 1, 1)
            except ValueError:
                pass

        if strict:
            raise SanitizationError(f"Cannot parse {field_name}: '{value}'")

        logger.debug(f"Could not parse date for {field_name}: '{value}', returning None")
        return None

    # Unknown type
    if strict:
        raise SanitizationError(f"Unexpected type for {field_name}: {type(value)}")

    logger.warning(f"Unexpected type for {field_name}: {type(value)}, returning None")
    return None


def sanitize_decimal(
    value: Any,
    field_name: str = "decimal",
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    strict: bool = False
) -> Optional[Decimal]:
    """
    Sanitize value for PostgreSQL DECIMAL/NUMERIC column.

    Constitution Compliance:
    - Phase 3: Input validation before DB write
    - Phase 4: Type safety for schema columns

    Args:
        value: Raw value from adapter
        field_name: Name of field (for logging)
        min_value: Minimum allowed value (optional)
        max_value: Maximum allowed value (optional)
        strict: If True, raise error on invalid; if False, return None

    Returns:
        Decimal object or None
    """
    # Handle None and empty values
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()
        if value == '':
            return None

        # Remove currency symbols and commas
        value = re.sub(r'[$,\s]', '', value)

        # Handle parentheses as negative (accounting format)
        if value.startswith('(') and value.endswith(')'):
            value = '-' + value[1:-1]

    # Convert to Decimal
    try:
        if isinstance(value, Decimal):
            result = value
        elif isinstance(value, (int, float)):
            result = Decimal(str(value))
        elif isinstance(value, str):
            result = Decimal(value)
        else:
            if strict:
                raise SanitizationError(f"Unexpected type for {field_name}: {type(value)}")
            logger.warning(f"Unexpected type for {field_name}: {type(value)}, returning None")
            return None

        # Range validation
        if min_value is not None and result < Decimal(str(min_value)):
            if strict:
                raise SanitizationError(f"{field_name} below minimum: {result} < {min_value}")
            logger.warning(f"{field_name} below minimum, returning None: {result} < {min_value}")
            return None

        if max_value is not None and result > Decimal(str(max_value)):
            if strict:
                raise SanitizationError(f"{field_name} above maximum: {result} > {max_value}")
            logger.warning(f"{field_name} above maximum, returning None: {result} > {max_value}")
            return None

        return result

    except (InvalidOperation, ValueError) as e:
        if strict:
            raise SanitizationError(f"Cannot parse {field_name}: '{value}' - {e}")
        logger.debug(f"Could not parse decimal for {field_name}: '{value}', returning None")
        return None


def sanitize_string(
    value: Any,
    field_name: str = "string",
    max_length: Optional[int] = None,
    allow_empty: bool = True,
    strip: bool = True
) -> Optional[str]:
    """
    Sanitize value for PostgreSQL VARCHAR/TEXT column.

    Args:
        value: Raw value from adapter
        field_name: Name of field (for logging)
        max_length: Maximum string length (truncates if exceeded)
        allow_empty: If False, converts '' to None
        strip: If True, strip whitespace

    Returns:
        String or None
    """
    if value is None:
        return None

    # Convert to string
    result = str(value)

    if strip:
        result = result.strip()

    if not allow_empty and result == '':
        return None

    if max_length and len(result) > max_length:
        logger.debug(f"Truncating {field_name} from {len(result)} to {max_length} chars")
        result = result[:max_length]

    return result


def sanitize_integer(
    value: Any,
    field_name: str = "integer",
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
    strict: bool = False
) -> Optional[int]:
    """
    Sanitize value for PostgreSQL INTEGER/BIGINT column.

    Args:
        value: Raw value from adapter
        field_name: Name of field (for logging)
        min_value: Minimum allowed value
        max_value: Maximum allowed value
        strict: If True, raise error on invalid

    Returns:
        Integer or None
    """
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()
        if value == '':
            return None

    try:
        if isinstance(value, bool):
            result = 1 if value else 0
        elif isinstance(value, int):
            result = value
        elif isinstance(value, float):
            result = int(value)
        elif isinstance(value, str):
            # Handle decimal strings
            result = int(float(value))
        else:
            if strict:
                raise SanitizationError(f"Unexpected type for {field_name}: {type(value)}")
            return None

        # Range validation
        if min_value is not None and result < min_value:
            if strict:
                raise SanitizationError(f"{field_name} below minimum: {result} < {min_value}")
            return None

        if max_value is not None and result > max_value:
            if strict:
                raise SanitizationError(f"{field_name} above maximum: {result} > {max_value}")
            return None

        return result

    except (ValueError, TypeError) as e:
        if strict:
            raise SanitizationError(f"Cannot parse {field_name}: '{value}' - {e}")
        return None


def sanitize_boolean(
    value: Any,
    field_name: str = "boolean"
) -> Optional[bool]:
    """
    Sanitize value for PostgreSQL BOOLEAN column.

    Args:
        value: Raw value from adapter
        field_name: Name of field (for logging)

    Returns:
        Boolean or None
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        value = value.strip().lower()
        if value == '':
            return None
        if value in ('true', 'yes', '1', 't', 'y'):
            return True
        if value in ('false', 'no', '0', 'f', 'n'):
            return False
        return None

    if isinstance(value, (int, float)):
        return bool(value)

    return None


def sanitize_url(
    value: Any,
    field_name: str = "url",
    max_length: int = 2048
) -> Optional[str]:
    """
    Sanitize value for URL storage.

    Args:
        value: Raw value from adapter
        field_name: Name of field (for logging)
        max_length: Maximum URL length

    Returns:
        URL string or None
    """
    if value is None:
        return None

    result = sanitize_string(value, field_name, max_length=max_length, allow_empty=False)

    if result and not result.startswith(('http://', 'https://')):
        # Could be a relative URL or invalid
        if result.startswith('//'):
            result = 'https:' + result
        elif result.startswith('/'):
            # Relative URL - can't validate without base
            pass
        else:
            logger.debug(f"URL missing protocol for {field_name}: {result}")
            # Assume https
            result = 'https://' + result

    return result


class RecordSanitizer:
    """
    Batch sanitizer for database records.

    Usage:
        sanitizer = RecordSanitizer()
        sanitizer.add_date('cover_date', record.get('release_date'))
        sanitizer.add_decimal('price', record.get('cover_price'), min_value=0)
        sanitizer.add_string('title', record.get('title'), max_length=500)

        params = sanitizer.get_params()
        errors = sanitizer.get_errors()
    """

    def __init__(self, strict: bool = False):
        self._params: Dict[str, Any] = {}
        self._errors: List[str] = []
        self._strict = strict

    def add_date(self, name: str, value: Any) -> 'RecordSanitizer':
        try:
            self._params[name] = sanitize_date(value, name, strict=self._strict)
        except SanitizationError as e:
            self._errors.append(str(e))
            self._params[name] = None
        return self

    def add_decimal(
        self, name: str, value: Any,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None
    ) -> 'RecordSanitizer':
        try:
            self._params[name] = sanitize_decimal(
                value, name, min_value, max_value, strict=self._strict
            )
        except SanitizationError as e:
            self._errors.append(str(e))
            self._params[name] = None
        return self

    def add_string(
        self, name: str, value: Any,
        max_length: Optional[int] = None,
        allow_empty: bool = True
    ) -> 'RecordSanitizer':
        self._params[name] = sanitize_string(value, name, max_length, allow_empty)
        return self

    def add_integer(
        self, name: str, value: Any,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None
    ) -> 'RecordSanitizer':
        try:
            self._params[name] = sanitize_integer(
                value, name, min_value, max_value, strict=self._strict
            )
        except SanitizationError as e:
            self._errors.append(str(e))
            self._params[name] = None
        return self

    def add_boolean(self, name: str, value: Any) -> 'RecordSanitizer':
        self._params[name] = sanitize_boolean(value, name)
        return self

    def add_url(self, name: str, value: Any, max_length: int = 2048) -> 'RecordSanitizer':
        self._params[name] = sanitize_url(value, name, max_length)
        return self

    def add_raw(self, name: str, value: Any) -> 'RecordSanitizer':
        """Add a value without sanitization (use with caution)."""
        self._params[name] = value
        return self

    def get_params(self) -> Dict[str, Any]:
        """Get all sanitized parameters."""
        return self._params.copy()

    def get_errors(self) -> List[str]:
        """Get any sanitization errors encountered."""
        return self._errors.copy()

    def has_errors(self) -> bool:
        """Check if any sanitization errors occurred."""
        return len(self._errors) > 0


# Convenience functions for common patterns

def sanitize_gcd_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize a GCD record for database insertion.

    Standard fields from GCD adapter:
    - release_date -> cover_date (DATE)
    - cover_price -> price (DECIMAL)
    - issue_number -> number (VARCHAR)
    - story_title -> issue_name (TEXT)
    - isbn, upc -> VARCHAR
    - page_count -> INTEGER
    """
    sanitizer = RecordSanitizer()

    sanitizer.add_date('cover_date', record.get('release_date'))
    sanitizer.add_decimal('price', record.get('cover_price'), min_value=0)
    sanitizer.add_string('number', record.get('issue_number'), max_length=50)
    sanitizer.add_string('issue_name', record.get('story_title'), max_length=500)
    sanitizer.add_string('isbn', record.get('isbn'), max_length=20, allow_empty=False)
    sanitizer.add_string('upc', record.get('upc'), max_length=20, allow_empty=False)
    sanitizer.add_integer('page_count', record.get('page_count'), min_value=0)
    sanitizer.add_integer('gcd_id', record.get('gcd_id'))
    sanitizer.add_integer('gcd_series_id', record.get('gcd_series_id'))
    sanitizer.add_integer('gcd_publisher_id', record.get('gcd_publisher_id'))

    return sanitizer.get_params()


def sanitize_enrichment_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize enrichment data from MSE adapters.

    Standard enrichment fields:
    - cover_image_url -> URL
    - description -> TEXT
    - pricing data -> DECIMAL
    """
    sanitizer = RecordSanitizer()

    sanitizer.add_url('cover_image_url', data.get('cover_image_url'))
    sanitizer.add_string('description', data.get('description'), max_length=10000)
    sanitizer.add_decimal('price_raw', data.get('price_raw'), min_value=0)
    sanitizer.add_decimal('price_graded_9_8', data.get('price_graded_9_8'), min_value=0)
    sanitizer.add_decimal('price_graded_9_6', data.get('price_graded_9_6'), min_value=0)
    sanitizer.add_string('data_source', data.get('source'), max_length=50)

    return sanitizer.get_params()
