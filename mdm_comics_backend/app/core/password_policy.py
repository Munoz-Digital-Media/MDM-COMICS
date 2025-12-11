"""
Password Policy enforcement

Per constitution_cyberSec.json §3 and OWASP ASVS L2
"""
import re
import hashlib
from typing import Optional, Tuple, List


class PasswordPolicy:
    """
    Enforces password complexity requirements.

    Configuration follows NIST 800-63B guidelines:
    - Minimum length (12 chars)
    - Complexity requirements
    - No forced rotation (MAX_AGE_DAYS = 0)
    - Breach database check (optional)
    """

    # Length requirements
    MIN_LENGTH = 12
    MAX_LENGTH = 128

    # Complexity requirements
    REQUIRE_UPPERCASE = True
    REQUIRE_LOWERCASE = True
    REQUIRE_DIGIT = True
    REQUIRE_SPECIAL = True
    SPECIAL_CHARS = "!@#$%^&*()_+-=[]{}|;:',.<>?/~`"

    # Password history
    HISTORY_COUNT = 5  # Prevent reuse of last N passwords

    # Expiration (0 = never, per NIST 800-63B)
    MAX_AGE_DAYS = 0

    # Common/weak passwords to reject
    COMMON_PASSWORDS = {
        "password", "123456", "12345678", "qwerty", "abc123",
        "monkey", "1234567", "letmein", "trustno1", "dragon",
        "baseball", "iloveyou", "master", "sunshine", "ashley",
        "bailey", "shadow", "123123", "654321", "superman",
        "qazwsx", "michael", "football", "password1", "password123"
    }

    @classmethod
    def validate(
        cls,
        password: str,
        email: Optional[str] = None,
        name: Optional[str] = None
    ) -> Tuple[bool, List[str]]:
        """
        Validate password against policy.

        Args:
            password: The password to validate
            email: User's email (to check password doesn't contain it)
            name: User's name (to check password doesn't contain it)

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        # Length checks
        if len(password) < cls.MIN_LENGTH:
            errors.append(f"Password must be at least {cls.MIN_LENGTH} characters")

        if len(password) > cls.MAX_LENGTH:
            errors.append(f"Password must be at most {cls.MAX_LENGTH} characters")

        # Complexity checks
        if cls.REQUIRE_UPPERCASE and not any(c.isupper() for c in password):
            errors.append("Password must contain at least one uppercase letter")

        if cls.REQUIRE_LOWERCASE and not any(c.islower() for c in password):
            errors.append("Password must contain at least one lowercase letter")

        if cls.REQUIRE_DIGIT and not any(c.isdigit() for c in password):
            errors.append("Password must contain at least one number")

        if cls.REQUIRE_SPECIAL and not any(c in cls.SPECIAL_CHARS for c in password):
            errors.append(f"Password must contain at least one special character ({cls.SPECIAL_CHARS[:10]}...)")

        # Common password check
        if password.lower() in cls.COMMON_PASSWORDS:
            errors.append("Password is too common, please choose a stronger password")

        # Context-based checks
        if email:
            email_local = email.split("@")[0].lower()
            if len(email_local) > 3 and email_local in password.lower():
                errors.append("Password should not contain your email address")

        if name:
            name_parts = [p.lower() for p in name.split() if len(p) > 2]
            for part in name_parts:
                if part in password.lower():
                    errors.append("Password should not contain your name")
                    break

        # Sequential/repeated character check
        if cls._has_sequential_chars(password, 4):
            errors.append("Password should not contain 4 or more sequential characters")

        if cls._has_repeated_chars(password, 4):
            errors.append("Password should not contain 4 or more repeated characters")

        return (len(errors) == 0, errors)

    @classmethod
    def _has_sequential_chars(cls, password: str, min_length: int = 4) -> bool:
        """Check for sequential characters like 'abcd' or '1234'."""
        password_lower = password.lower()

        for i in range(len(password_lower) - min_length + 1):
            is_sequential = True
            for j in range(min_length - 1):
                if ord(password_lower[i + j + 1]) != ord(password_lower[i + j]) + 1:
                    is_sequential = False
                    break
            if is_sequential:
                return True

        return False

    @classmethod
    def _has_repeated_chars(cls, password: str, min_length: int = 4) -> bool:
        """Check for repeated characters like 'aaaa'."""
        for i in range(len(password) - min_length + 1):
            if len(set(password[i:i + min_length])) == 1:
                return True
        return False

    @classmethod
    def get_strength_score(cls, password: str) -> Tuple[int, str]:
        """
        Calculate password strength score (0-100).

        Returns:
            Tuple of (score, strength_label)
        """
        score = 0

        # Length bonus
        length = len(password)
        if length >= 8:
            score += 10
        if length >= 12:
            score += 10
        if length >= 16:
            score += 10
        if length >= 20:
            score += 10

        # Character variety
        if any(c.isupper() for c in password):
            score += 15
        if any(c.islower() for c in password):
            score += 15
        if any(c.isdigit() for c in password):
            score += 15
        if any(c in cls.SPECIAL_CHARS for c in password):
            score += 15

        # Uniqueness
        unique_ratio = len(set(password)) / len(password) if password else 0
        score += int(unique_ratio * 10)

        # Penalties
        if password.lower() in cls.COMMON_PASSWORDS:
            score = min(score, 20)

        if cls._has_sequential_chars(password, 3):
            score -= 10

        if cls._has_repeated_chars(password, 3):
            score -= 10

        score = max(0, min(100, score))

        # Determine label
        if score >= 80:
            label = "Strong"
        elif score >= 60:
            label = "Good"
        elif score >= 40:
            label = "Fair"
        elif score >= 20:
            label = "Weak"
        else:
            label = "Very Weak"

        return (score, label)

    @classmethod
    def check_breach_hash(cls, password: str) -> bool:
        """
        Check first 5 chars of SHA-1 hash against HaveIBeenPwned API.

        Note: This is a placeholder. Actual implementation would call:
        https://api.pwnedpasswords.com/range/{first5}

        Returns:
            True if password appears in breach database
        """
        # Generate SHA-1 hash
        sha1_hash = hashlib.sha1(password.encode()).hexdigest().upper()
        prefix = sha1_hash[:5]
        suffix = sha1_hash[5:]

        # In production, would call:
        # response = httpx.get(f"https://api.pwnedpasswords.com/range/{prefix}")
        # return suffix in response.text

        # Placeholder - always return False
        return False

    @classmethod
    def generate_requirements_message(cls) -> str:
        """Generate user-friendly password requirements message."""
        requirements = [f"At least {cls.MIN_LENGTH} characters"]

        if cls.REQUIRE_UPPERCASE:
            requirements.append("At least one uppercase letter")
        if cls.REQUIRE_LOWERCASE:
            requirements.append("At least one lowercase letter")
        if cls.REQUIRE_DIGIT:
            requirements.append("At least one number")
        if cls.REQUIRE_SPECIAL:
            requirements.append("At least one special character")

        return "Password requirements: " + ", ".join(requirements)
