"""
Tests for encryption service.
UPS Shipping Integration v1.28.0
"""
import pytest
import os

# Set test environment
os.environ["ENVIRONMENT"] = "development"
os.environ["DATABASE_URL"] = "postgresql+asyncpg://test:test@localhost:5432/test_db"
os.environ["SECRET_KEY"] = "test-secret-key-for-unit-tests-only"

from app.services.encryption import (
    encrypt_pii,
    decrypt_pii,
    hash_phone,
    get_phone_last4,
    mask_email,
    mask_address_line,
    sanitize_for_logging,
)


class TestEncryption:
    """Test PII encryption/decryption."""

    def test_encrypt_decrypt_roundtrip(self):
        """Test that encryption and decryption produce original value."""
        original = "John Doe"
        encrypted = encrypt_pii(original)
        decrypted = decrypt_pii(encrypted)

        assert decrypted == original
        assert encrypted != original  # Should be different from plaintext

    def test_encrypt_empty_string(self):
        """Test encrypting empty string returns empty string."""
        result = encrypt_pii("")
        assert result == ""

    def test_decrypt_empty_string(self):
        """Test decrypting empty string returns empty string."""
        result = decrypt_pii("")
        assert result == ""

    def test_encrypt_unicode(self):
        """Test encrypting unicode characters."""
        original = "José García 中文 🎉"
        encrypted = encrypt_pii(original)
        decrypted = decrypt_pii(encrypted)

        assert decrypted == original

    def test_encrypt_long_string(self):
        """Test encrypting long strings."""
        original = "A" * 10000
        encrypted = encrypt_pii(original)
        decrypted = decrypt_pii(encrypted)

        assert decrypted == original

    def test_decrypt_invalid_token(self):
        """Test decrypting invalid token raises error."""
        with pytest.raises(ValueError, match="invalid token"):
            decrypt_pii("invalid-encrypted-data")

    def test_encryption_determinism(self):
        """Test that encryption produces different ciphertexts for same input."""
        original = "Test Data"
        encrypted1 = encrypt_pii(original)
        encrypted2 = encrypt_pii(original)

        # Fernet uses random IV, so each encryption should be different
        assert encrypted1 != encrypted2
        # But both should decrypt to the same value
        assert decrypt_pii(encrypted1) == decrypt_pii(encrypted2) == original


class TestPhoneHashing:
    """Test phone number hashing and masking."""

    def test_hash_phone_basic(self):
        """Test basic phone hashing."""
        phone = "212-555-1234"
        hashed = hash_phone(phone)

        assert hashed.startswith("sha256:")
        assert len(hashed) == 71  # "sha256:" + 64 hex chars

    def test_hash_phone_normalization(self):
        """Test that phone numbers are normalized before hashing."""
        # Different formats should produce same hash
        hash1 = hash_phone("212-555-1234")
        hash2 = hash_phone("(212) 555-1234")
        hash3 = hash_phone("2125551234")
        hash4 = hash_phone("+1 212 555 1234")

        # All should produce same hash (after stripping non-digits)
        # Note: +1 adds a digit, so it will be different
        assert hash1 == hash2 == hash3

    def test_hash_phone_empty(self):
        """Test hashing empty phone returns empty string."""
        assert hash_phone("") == ""
        assert hash_phone(None) == ""

    def test_hash_phone_only_non_digits(self):
        """Test hashing string with only non-digits returns empty."""
        assert hash_phone("abc-def-ghij") == ""

    def test_get_phone_last4(self):
        """Test getting last 4 digits of phone."""
        assert get_phone_last4("212-555-1234") == "1234"
        assert get_phone_last4("(212) 555-1234") == "1234"
        assert get_phone_last4("123") == "123"
        assert get_phone_last4("") == ""

    def test_get_phone_last4_short_number(self):
        """Test getting last 4 of short number."""
        assert get_phone_last4("12") == "12"
        assert get_phone_last4("1") == "1"


class TestMasking:
    """Test PII masking functions."""

    def test_mask_email_basic(self):
        """Test basic email masking."""
        masked = mask_email("john.doe@example.com")
        assert masked == "j***@example.com"

    def test_mask_email_short_local(self):
        """Test masking email with short local part."""
        assert mask_email("a@example.com") == "*@example.com"
        assert mask_email("ab@example.com") == "a*@example.com"
        assert mask_email("abc@example.com") == "a**@example.com"

    def test_mask_email_invalid(self):
        """Test masking invalid email."""
        assert mask_email("not-an-email") == "***"
        assert mask_email("") == "***"

    def test_mask_address_line(self):
        """Test address line masking."""
        masked = mask_address_line("123 Main Street")
        assert masked.startswith("123 ")  # Street number preserved
        assert "***" in masked

    def test_mask_address_empty(self):
        """Test masking empty address."""
        assert mask_address_line("") == "***"
        assert mask_address_line(None) == "***"

    def test_mask_address_with_unit(self):
        """Test masking address with unit number."""
        masked = mask_address_line("456 Oak Ave Apt 12")
        assert masked.startswith("456 ")


class TestSanitizeForLogging:
    """Test log sanitization."""

    def test_sanitize_phone_numbers(self):
        """Test phone numbers are redacted."""
        text = "Call me at 212-555-1234 or (800) 123-4567"
        sanitized = sanitize_for_logging(text)

        assert "212-555-1234" not in sanitized
        assert "(800) 123-4567" not in sanitized
        assert "[PHONE]" in sanitized

    def test_sanitize_emails(self):
        """Test emails are redacted."""
        text = "Email john@example.com for more info"
        sanitized = sanitize_for_logging(text)

        assert "john@example.com" not in sanitized
        assert "[EMAIL]" in sanitized

    def test_sanitize_zip_codes(self):
        """Test ZIP codes are redacted."""
        text = "Send to 10001 or 90210-1234"
        sanitized = sanitize_for_logging(text)

        assert "10001" not in sanitized
        assert "90210-1234" not in sanitized
        assert "[ZIP]" in sanitized

    def test_sanitize_max_length(self):
        """Test max length truncation."""
        long_text = "A" * 1000
        sanitized = sanitize_for_logging(long_text, max_length=100)

        assert len(sanitized) == 100

    def test_sanitize_empty(self):
        """Test sanitizing empty string."""
        assert sanitize_for_logging("") == ""
        assert sanitize_for_logging(None) == ""

    def test_sanitize_preserves_safe_text(self):
        """Test that non-PII text is preserved."""
        text = "Order #12345 shipped via UPS Ground"
        sanitized = sanitize_for_logging(text)

        assert "Order #12345" in sanitized
        assert "UPS Ground" in sanitized
