import importlib
import os

import app.core.config as config


def _restore_env(env_snapshot):
    """Restore a snapshot of specific env vars and reload settings."""
    for key, value in env_snapshot.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    importlib.reload(config)


def test_s3_bucket_and_region_aliases(monkeypatch):
    """Ensure S3 env aliases are honored and override defaults."""
    # Snapshot current env so we can put it back after the test
    keys = ["S3_BUCKET", "S3_BUCKET_NAME", "S3_REGION", "AWS_REGION"]
    snapshot = {k: os.environ.get(k) for k in keys}

    try:
        # Use alias names only to ensure they override defaults
        monkeypatch.delenv("S3_BUCKET", raising=False)
        monkeypatch.setenv("S3_BUCKET_NAME", "alias-bucket")

        monkeypatch.delenv("S3_REGION", raising=False)
        monkeypatch.setenv("AWS_REGION", "us-west-2")

        importlib.reload(config)

        assert config.settings.S3_BUCKET == "alias-bucket"
        assert config.settings.S3_REGION == "us-west-2"
    finally:
        _restore_env(snapshot)
