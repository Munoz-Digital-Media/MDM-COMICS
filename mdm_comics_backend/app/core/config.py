"""
Application configuration

SECURITY: Defaults are fail-safe for production.
- DEBUG defaults to False
- SECRET_KEY and DATABASE_URL have no defaults (will fail if not set)
- Runtime validation catches insecure configurations
"""
import json
import os
import logging
from typing import List, Optional, Union
from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# Default CORS origins
DEFAULT_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    "https://mdmcomics.com",
    "https://www.mdmcomics.com",
    # Railway development/staging
    "https://mdm-comics-frontend-development.up.railway.app",
    "https://mdm-comics-frontend-staging.up.railway.app",
]


class Settings(BaseSettings):
    # App - defaults are PRODUCTION safe
    APP_NAME: str = "MDM Comics"
    DEBUG: bool = False  # SECURE DEFAULT: off in production
    ENVIRONMENT: str = "production"  # Explicit env marker

    # Database - NO DEFAULT (will fail if not set)
    DATABASE_URL: str

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def convert_database_url(cls, v):
        """Convert Railway's postgres:// URL to asyncpg format."""
        if not v:
            return v
        # Railway provides postgres:// or postgresql://, but we need postgresql+asyncpg://
        if v.startswith("postgres://"):
            v = v.replace("postgres://", "postgresql+asyncpg://", 1)
        elif v.startswith("postgresql://") and "+asyncpg" not in v:
            v = v.replace("postgresql://", "postgresql+asyncpg://", 1)
        return v

    # Auth - NO DEFAULT SECRET KEY (will fail if not set)
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # CORS - accepts JSON array or comma-separated string
    # Using str type to prevent pydantic-settings from trying to parse as JSON
    CORS_ORIGINS: Union[str, List[str]] = DEFAULT_CORS_ORIGINS

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v):
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            if not v or v.strip() == "":
                return DEFAULT_CORS_ORIGINS
            # Try JSON first
            if v.startswith("["):
                try:
                    return json.loads(v)
                except json.JSONDecodeError:
                    pass
            # Fallback to comma-separated
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        return v

    # Storage (S3 compatible)
    S3_BUCKET: str = "mdm-comics"
    S3_REGION: str = "us-east-1"
    S3_ACCESS_KEY: str = ""
    S3_SECRET_KEY: str = ""
    S3_ENDPOINT: str = ""  # Leave empty for AWS, set for R2/MinIO

    # ML Model
    ML_MODEL_PATH: str = "app/ml/models/grade_estimator.pt"
    ML_CONFIDENCE_THRESHOLD: float = 0.7

    # Metron Comic Database API
    METRON_USERNAME: str = ""
    METRON_PASSWORD: str = ""
    METRON_API_BASE: str = "https://metron.cloud/api"

    # Stripe Payments
    STRIPE_SECRET_KEY: str = ""
    STRIPE_PUBLISHABLE_KEY: str = ""
    STRIPE_WEBHOOK_SECRET: str = ""
    STRIPE_CURRENCY: str = "usd"

    # PriceCharting API
    PRICECHARTING_API_TOKEN: str = ""

    # Redis (for webhook idempotency, caching)
    REDIS_URL: str = ""

    # Database Pool Configuration (P2-7)
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10
    DB_POOL_RECYCLE: int = 3600  # 1 hour

    # Stock Cleanup Scheduler (P0-1)
    STOCK_CLEANUP_INTERVAL_MINUTES: int = 5
    STOCK_CLEANUP_ENABLED: bool = True

    # Rate Limiting (P1-3)
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_DEFAULT: str = "100/minute"
    RATE_LIMIT_AUTH: str = "5/minute"
    RATE_LIMIT_CHECKOUT: str = "10/minute"

    # Cookie/CSRF Settings (P1-5)
    COOKIE_DOMAIN: str = ""  # Empty = auto-detect from request
    COOKIE_SECURE: bool = True  # Set to False for local dev without HTTPS
    COOKIE_SAMESITE: str = "lax"  # "strict", "lax", or "none"
    CSRF_SECRET_KEY: str = ""  # If empty, derives from SECRET_KEY

    # Feature Flags (P3-12)
    UNDER_CONSTRUCTION: bool = True

    # UPS Shipping Integration v1.28.0
    UPS_CLIENT_ID: str = ""
    UPS_CLIENT_SECRET: str = ""
    UPS_ACCOUNT_NUMBER: str = ""
    UPS_USE_SANDBOX: bool = False
    SHIPPING_TRACKING_SYNC_ENABLED: bool = True
    SHIPPING_TRACKING_SYNC_INTERVAL_SECONDS: int = 300
    SHIPPING_RATE_QUOTE_TTL_MINUTES: int = 30
    SHIPPING_ORIGIN_NAME: str = "MDM Comics"
    SHIPPING_ORIGIN_ADDRESS: str = ""
    SHIPPING_ORIGIN_CITY: str = ""
    SHIPPING_ORIGIN_STATE: str = ""
    SHIPPING_ORIGIN_ZIP: str = ""
    SHIPPING_ORIGIN_PHONE: str = ""

    # Alerting (PagerDuty)
    PAGERDUTY_ROUTING_KEY: str = ""
    PAGERDUTY_ENABLED: bool = False

    # User Management System v1.0.0
    # PII Encryption (Fernet key - generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
    PII_ENCRYPTION_KEY: str = ""
    PII_PEPPER: str = ""  # If empty, derived from SECRET_KEY

    # Account Lockout (constitution_cyberSec.json §8)
    ACCOUNT_LOCKOUT_MAX_ATTEMPTS: int = 5
    ACCOUNT_LOCKOUT_DURATION_MINUTES: int = 15
    ACCOUNT_LOCKOUT_PROGRESSIVE: bool = True
    ACCOUNT_LOCKOUT_MAX_MINUTES: int = 1440  # 24 hours

    # Session Management
    SESSION_MAX_CONCURRENT: int = 5  # Max active sessions per user
    SESSION_IDLE_TIMEOUT_MINUTES: int = 60
    SESSION_ABSOLUTE_TIMEOUT_HOURS: int = 24

    # Password Policy (NIST 800-63B)
    PASSWORD_MIN_LENGTH: int = 12
    PASSWORD_REQUIRE_UPPERCASE: bool = True
    PASSWORD_REQUIRE_LOWERCASE: bool = True
    PASSWORD_REQUIRE_DIGIT: bool = True
    PASSWORD_REQUIRE_SPECIAL: bool = True
    PASSWORD_HISTORY_COUNT: int = 5

    # Email Settings (for verification, password reset)
    EMAIL_FROM: str = "noreply@mdmcomics.com"
    EMAIL_SMTP_HOST: str = ""
    EMAIL_SMTP_PORT: int = 587
    EMAIL_SMTP_USER: str = ""
    EMAIL_SMTP_PASSWORD: str = ""
    EMAIL_VERIFICATION_TOKEN_HOURS: int = 24
    PASSWORD_RESET_TOKEN_MINUTES: int = 30

    # DSAR (GDPR/CCPA Compliance)
    DSAR_PROCESSING_DAYS: int = 30
    DSAR_DATA_RETENTION_DAYS: int = 7  # Keep exported data files for 7 days

    # ===== OUTREACH SYSTEM v1.5.0 =====

    # Application URL (for unsubscribe links, email templates)
    APP_URL: str = "https://mdmcomics.com"

    # Email (SendGrid)
    SENDGRID_API_KEY: str = ""
    SENDGRID_FROM_EMAIL: str = "hello@mdmcomics.com"
    SENDGRID_FROM_NAME: str = "MDM Comics"
    SENDGRID_NEWSLETTER_TEMPLATE_ID: str = ""
    SENDGRID_TRANSACTIONAL_TEMPLATE_ID: str = ""
    SENDGRID_WEBHOOK_SIGNING_KEY: str = ""

    # Social (Bluesky)
    BLUESKY_HANDLE: str = ""
    BLUESKY_APP_PASSWORD: str = ""

    # Social (Meta - Facebook/Instagram) v1.5.1
    META_ACCESS_TOKEN: str = ""  # Long-lived page access token
    META_PAGE_ID: str = ""  # Facebook Page ID
    META_INSTAGRAM_ACCOUNT_ID: str = ""  # Instagram Business Account ID (Phase 2+)

    # AI (OpenAI)
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4-turbo-preview"

    # Job Queue (ARQ) - uses main REDIS_URL if not set
    ARQ_REDIS_URL: str = ""

    # Marketing Feature Flags (all default to False for safety)
    MARKETING_NEWSLETTER_ENABLED: bool = False
    MARKETING_SOCIAL_ENABLED: bool = False
    MARKETING_AI_ENHANCEMENT_ENABLED: bool = False

    # Template directory
    MARKETING_TEMPLATE_DIR: str = "app/templates/marketing"

    # ===== DATA ACQUISITION PIPELINE v1.7.0 =====
    # Pipeline Scheduler (automated data enrichment)
    PIPELINE_SCHEDULER_ENABLED: bool = True  # Enable/disable all pipeline jobs
    PIPELINE_COMIC_ENRICHMENT_INTERVAL_MINUTES: int = 30
    PIPELINE_FUNKO_PRICE_CHECK_INTERVAL_MINUTES: int = 60
    PIPELINE_DLQ_RETRY_INTERVAL_MINUTES: int = 15

    # GCD (Grand Comics Database) Import - v1.8.0
    # SQLite dump path: local dev uses assets/, Railway uses mounted volume
    GCD_DUMP_PATH: str = "/data/gcd/2025-12-01.db"  # Railway volume mount
    GCD_DUMP_S3_URL: str = "https://mdm-comics-assets.s3.us-east-2.amazonaws.com/2025-12-01.db"  # S3 fallback
    GCD_IMPORT_BATCH_SIZE: int = 1000  # Records per batch during import
    GCD_IMPORT_ENABLED: bool = False  # Master toggle for GCD import job
    GCD_IMPORT_MAX_RECORDS: int = 0  # 0 = unlimited, >0 = subset import for validation

    # ===== MULTI-SOURCE ENRICHMENT v1.10.0 =====
    # Comic Vine API (200 req/hr - non-commercial only)
    COMIC_VINE_API_KEY: str = ""

    # Source Rotator Settings
    ENRICHMENT_HEDGE_DELAY_MS: int = 300  # Delay before backup request
    ENRICHMENT_MAX_FALLBACK_ATTEMPTS: int = 3  # Max sources to try per request
    ENRICHMENT_CIRCUIT_RECOVERY_SECONDS: int = 300  # Wait before retrying failed source

    # Scraper Settings (conservative to avoid bans)
    SCRAPER_MIN_DELAY_SECONDS: float = 2.0  # Minimum delay between scrape requests
    SCRAPER_USER_AGENT: str = "MDMComicsBot/1.0 (+https://mdmcomics.com/bot)"

    # ===== THE RACK FACTOR v1.5.2 =====
    # Newsletter and Social Media Content Engine
    RACK_FACTOR_ENABLED: bool = True  # Master toggle for The Rack Factor
    RACK_FACTOR_LOGO_URL: str = ""  # URL to The Rack Factor logo for emails
    RACK_FACTOR_NEWSLETTER_DAY: int = 4  # 0=Mon, 4=Fri (Friday newsletter)
    RACK_FACTOR_NEWSLETTER_HOUR: int = 10  # 10 AM EST
    RACK_FACTOR_SOCIAL_HOUR: int = 16  # 4:30 PM EST daily posts
    RACK_FACTOR_SOCIAL_MINUTE: int = 30
    RACK_FACTOR_DAILY_TOP_MOVERS: int = 5  # Top 5 up + Top 5 down
    RACK_FACTOR_WEEKLY_TOP_MOVERS: int = 10  # Top 10 in newsletter

    @model_validator(mode="after")
    def validate_production_config(self):
        """Runtime validation to catch insecure production configurations."""
        if self.ENVIRONMENT == "production":
            errors = []

            # Check DEBUG
            if self.DEBUG:
                errors.append(
                    "DEBUG=True is forbidden in production. "
                    "Set DEBUG=false or ENVIRONMENT=development"
                )

            # Check SECRET_KEY
            insecure_secrets = [
                "your-secret-key",
                "change-in-production",
                "secret",
                "password",
                "changeme",
            ]
            if any(bad in self.SECRET_KEY.lower() for bad in insecure_secrets):
                errors.append(
                    "Insecure SECRET_KEY detected in production. "
                    "Generate a secure key: python -c 'import secrets; print(secrets.token_urlsafe(32))'"
                )

            # Check DATABASE_URL
            if "localhost" in self.DATABASE_URL or "127.0.0.1" in self.DATABASE_URL:
                errors.append(
                    "Localhost DATABASE_URL detected in production. "
                    "Configure proper database connection."
                )

            # NASTY-004: Require explicit PII encryption keys in production
            # Changing SECRET_KEY would break PII decryption for existing records
            if not self.PII_ENCRYPTION_KEY:
                errors.append(
                    "PII_ENCRYPTION_KEY is required in production. "
                    "Generate with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
                )
            if not self.PII_PEPPER:
                errors.append(
                    "PII_PEPPER is required in production. "
                    "Generate with: python -c 'import secrets; print(secrets.token_urlsafe(32))'"
                )

            # NASTY-001: Block localhost CORS origins in production (not just warn)
            cors_errors = []
            for origin in self.CORS_ORIGINS:
                if origin == "*":
                    cors_errors.append("Wildcard '*' CORS origin is forbidden in production")
                elif "localhost" in origin or "127.0.0.1" in origin:
                    cors_errors.append(f"Localhost CORS origin '{origin}' is forbidden in production")

            if cors_errors:
                errors.extend(cors_errors)

            if errors:
                raise ValueError(
                    "PRODUCTION SECURITY VIOLATIONS:\n" + "\n".join(f"  - {e}" for e in errors)
                )

        return self

    class Config:
        env_file = ".env"
        case_sensitive = True


# Try to load settings, provide helpful error on failure
try:
    settings = Settings()
except Exception as e:
    # In development, allow fallback defaults
    if os.getenv("ENVIRONMENT", "development") == "development":
        logger.warning(
            "Settings validation failed, using development defaults. "
            "Set DATABASE_URL and SECRET_KEY in .env file."
        )
        # Create with development fallbacks
        os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost:5432/mdm_comics")
        os.environ.setdefault("SECRET_KEY", "dev-only-secret-key-not-for-production")
        os.environ.setdefault("ENVIRONMENT", "development")
        settings = Settings()
    else:
        raise
