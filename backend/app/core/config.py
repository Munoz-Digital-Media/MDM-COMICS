"""
Application configuration
"""
import json
from typing import List
from pydantic import field_validator
from pydantic_settings import BaseSettings

# Default CORS origins
DEFAULT_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
]


class Settings(BaseSettings):
    # App
    APP_NAME: str = "MDM Comics"
    DEBUG: bool = True

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://postgres:password@localhost:5432/mdm_comics"

    # Auth
    SECRET_KEY: str = "your-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # CORS - accepts JSON array or comma-separated string
    CORS_ORIGINS: List[str] = DEFAULT_CORS_ORIGINS

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

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
