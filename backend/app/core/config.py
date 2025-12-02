"""
Application configuration
"""
from typing import List
from pydantic_settings import BaseSettings


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
    
    # CORS
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ]
    
    # Storage (S3 compatible)
    S3_BUCKET: str = "mdm-comics"
    S3_REGION: str = "us-east-1"
    S3_ACCESS_KEY: str = ""
    S3_SECRET_KEY: str = ""
    S3_ENDPOINT: str = ""  # Leave empty for AWS, set for R2/MinIO
    
    # ML Model
    ML_MODEL_PATH: str = "app/ml/models/grade_estimator.pt"
    ML_CONFIDENCE_THRESHOLD: float = 0.7
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
