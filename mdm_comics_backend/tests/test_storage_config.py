from app.core.config import Settings


def test_s3_bucket_and_region_aliases(monkeypatch):
    """Ensure S3 env aliases are honored when canonical names are absent."""
    # Use alias names only to ensure they override defaults
    monkeypatch.delenv("S3_BUCKET", raising=False)
    monkeypatch.setenv("S3_BUCKET_NAME", "alias-bucket")

    monkeypatch.delenv("S3_REGION", raising=False)
    monkeypatch.setenv("AWS_REGION", "us-west-2")

    settings = Settings(_env_file=None)

    assert settings.S3_BUCKET == "alias-bucket"
    assert settings.S3_REGION == "us-west-2"
