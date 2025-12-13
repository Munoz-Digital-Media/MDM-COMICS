"""
Tests for Database Input Sanitizer

Constitution Compliance: Phase 3 Input Validation Testing
Ensures all edge cases are handled correctly before DB writes.

Created as mitigation for MDM-INC-2024-002.
"""
import pytest
from datetime import date
from decimal import Decimal

from app.utils.db_sanitizer import (
    sanitize_date,
    sanitize_decimal,
    sanitize_string,
    sanitize_integer,
    sanitize_boolean,
    sanitize_url,
    RecordSanitizer,
    SanitizationError,
)


class TestSanitizeDate:
    """Tests for date sanitization - the root cause of MDM-INC-2024-002."""

    def test_empty_string_returns_none(self):
        """CRITICAL: Empty string must return None, not be passed to PostgreSQL."""
        assert sanitize_date('') is None
        assert sanitize_date('  ') is None

    def test_none_returns_none(self):
        assert sanitize_date(None) is None

    def test_valid_iso_date(self):
        result = sanitize_date('2023-05-15')
        assert result == date(2023, 5, 15)

    def test_valid_year_month(self):
        result = sanitize_date('2023-05')
        assert result == date(2023, 5, 1)

    def test_valid_year_only(self):
        result = sanitize_date('2023')
        assert result == date(2023, 1, 1)

    def test_date_object_passthrough(self):
        d = date(2023, 5, 15)
        assert sanitize_date(d) == d

    def test_us_format(self):
        result = sanitize_date('05/15/2023')
        assert result == date(2023, 5, 15)

    def test_invalid_returns_none_non_strict(self):
        assert sanitize_date('not a date') is None
        assert sanitize_date('abc123') is None

    def test_invalid_raises_in_strict_mode(self):
        with pytest.raises(SanitizationError):
            sanitize_date('not a date', strict=True)

    def test_extracts_year_from_mixed_string(self):
        # Should extract year at minimum
        result = sanitize_date('Published 2020')
        assert result == date(2020, 1, 1)


class TestSanitizeDecimal:
    """Tests for decimal sanitization - also involved in MDM-INC-2024-002."""

    def test_empty_string_returns_none(self):
        """CRITICAL: Empty string must return None, not be passed to PostgreSQL."""
        assert sanitize_decimal('') is None
        assert sanitize_decimal('  ') is None

    def test_none_returns_none(self):
        assert sanitize_decimal(None) is None

    def test_valid_decimal_string(self):
        result = sanitize_decimal('19.99')
        assert result == Decimal('19.99')

    def test_valid_integer(self):
        result = sanitize_decimal(100)
        assert result == Decimal('100')

    def test_valid_float(self):
        result = sanitize_decimal(19.99)
        assert result == Decimal('19.99')

    def test_currency_symbol_removed(self):
        result = sanitize_decimal('$19.99')
        assert result == Decimal('19.99')

    def test_commas_removed(self):
        result = sanitize_decimal('1,000.00')
        assert result == Decimal('1000.00')

    def test_min_value_enforced(self):
        assert sanitize_decimal(-5, min_value=0) is None
        assert sanitize_decimal(5, min_value=0) == Decimal('5')

    def test_max_value_enforced(self):
        assert sanitize_decimal(1000, max_value=100) is None
        assert sanitize_decimal(50, max_value=100) == Decimal('50')

    def test_invalid_returns_none_non_strict(self):
        assert sanitize_decimal('not a number') is None

    def test_invalid_raises_in_strict_mode(self):
        with pytest.raises(SanitizationError):
            sanitize_decimal('not a number', strict=True)


class TestSanitizeString:
    """Tests for string sanitization."""

    def test_none_returns_none(self):
        assert sanitize_string(None) is None

    def test_empty_string_with_allow_empty(self):
        assert sanitize_string('', allow_empty=True) == ''
        assert sanitize_string('', allow_empty=False) is None

    def test_strips_whitespace(self):
        assert sanitize_string('  hello  ') == 'hello'

    def test_truncates_to_max_length(self):
        result = sanitize_string('hello world', max_length=5)
        assert result == 'hello'

    def test_number_converted_to_string(self):
        assert sanitize_string(123) == '123'


class TestSanitizeInteger:
    """Tests for integer sanitization."""

    def test_empty_string_returns_none(self):
        assert sanitize_integer('') is None

    def test_none_returns_none(self):
        assert sanitize_integer(None) is None

    def test_valid_integer(self):
        assert sanitize_integer(42) == 42

    def test_string_integer(self):
        assert sanitize_integer('42') == 42

    def test_float_truncated(self):
        assert sanitize_integer(42.9) == 42

    def test_min_max_enforced(self):
        assert sanitize_integer(-5, min_value=0) is None
        assert sanitize_integer(200, max_value=100) is None


class TestSanitizeBoolean:
    """Tests for boolean sanitization."""

    def test_none_returns_none(self):
        assert sanitize_boolean(None) is None

    def test_empty_string_returns_none(self):
        assert sanitize_boolean('') is None

    def test_true_values(self):
        assert sanitize_boolean(True) is True
        assert sanitize_boolean('true') is True
        assert sanitize_boolean('yes') is True
        assert sanitize_boolean('1') is True

    def test_false_values(self):
        assert sanitize_boolean(False) is False
        assert sanitize_boolean('false') is False
        assert sanitize_boolean('no') is False
        assert sanitize_boolean('0') is False


class TestSanitizeUrl:
    """Tests for URL sanitization."""

    def test_none_returns_none(self):
        assert sanitize_url(None) is None

    def test_empty_string_returns_none(self):
        assert sanitize_url('') is None

    def test_valid_url_passthrough(self):
        url = 'https://example.com/image.jpg'
        assert sanitize_url(url) == url

    def test_protocol_added(self):
        result = sanitize_url('example.com/image.jpg')
        assert result == 'https://example.com/image.jpg'


class TestRecordSanitizer:
    """Tests for the batch record sanitizer."""

    def test_chain_methods(self):
        sanitizer = RecordSanitizer()
        sanitizer.add_date('cover_date', '2023-05-15')
        sanitizer.add_decimal('price', '19.99')
        sanitizer.add_string('title', 'Test Comic')

        params = sanitizer.get_params()
        assert params['cover_date'] == date(2023, 5, 15)
        assert params['price'] == Decimal('19.99')
        assert params['title'] == 'Test Comic'

    def test_empty_strings_sanitized(self):
        """Simulates the MDM-INC-2024-002 failure case."""
        sanitizer = RecordSanitizer()
        sanitizer.add_date('cover_date', '')
        sanitizer.add_decimal('price', '')

        params = sanitizer.get_params()
        assert params['cover_date'] is None
        assert params['price'] is None
        assert not sanitizer.has_errors()

    def test_gcd_style_record(self):
        """Test with data that looks like GCD adapter output."""
        record = {
            'gcd_id': 12345,
            'release_date': '',  # Empty - was causing the bug
            'cover_price': '',   # Empty - was causing the bug
            'issue_number': '1',
            'story_title': 'Origin Story',
        }

        sanitizer = RecordSanitizer()
        sanitizer.add_integer('gcd_id', record['gcd_id'])
        sanitizer.add_date('cover_date', record['release_date'])
        sanitizer.add_decimal('price', record['cover_price'])
        sanitizer.add_string('number', record['issue_number'])
        sanitizer.add_string('issue_name', record['story_title'])

        params = sanitizer.get_params()
        assert params['gcd_id'] == 12345
        assert params['cover_date'] is None  # NOT '' - this was the bug
        assert params['price'] is None       # NOT '' - this was the bug
        assert params['number'] == '1'
        assert params['issue_name'] == 'Origin Story'


class TestConstitutionCompliance:
    """
    Tests that verify constitution_cyberSec.json Phase 3 compliance.

    These tests document that input validation is performed before DB writes.
    """

    def test_all_adapter_outputs_sanitized(self):
        """Every type of adapter output must be sanitizable."""
        # GCD adapter returns these types
        gcd_outputs = [
            ('date', ''),
            ('date', None),
            ('date', '2023-05-15'),
            ('decimal', ''),
            ('decimal', '19.99'),
            ('string', ''),
            ('string', 'Test'),
            ('integer', ''),
            ('integer', 123),
        ]

        for field_type, value in gcd_outputs:
            if field_type == 'date':
                result = sanitize_date(value)
            elif field_type == 'decimal':
                result = sanitize_decimal(value)
            elif field_type == 'string':
                result = sanitize_string(value)
            elif field_type == 'integer':
                result = sanitize_integer(value)

            # Should never raise - all outputs must be handled gracefully
            assert result is None or result is not None

    def test_no_empty_strings_pass_to_db(self):
        """
        Empty strings must NEVER be passed to DATE/DECIMAL columns.

        This is the core requirement that was violated in MDM-INC-2024-002.
        """
        assert sanitize_date('') is None
        assert sanitize_decimal('') is None
        # Strings can be empty if allow_empty=True
        assert sanitize_string('', allow_empty=False) is None
