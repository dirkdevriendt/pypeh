import pytest

from pypeh.core.models.settings import S3Config


@pytest.mark.core
class TestCustomEnv:
    def test_custom_env_prefix(self, monkeypatch):
        monkeypatch.setenv("MYBUCKET_BUCKET_NAME", "my-test-bucket")
        monkeypatch.setenv("MYBUCKET_AWS_REGION", "eu-central-1")

        config_base = S3Config(env_prefix="MYBUCKET_")
        config = config_base.make_settings()

        assert config.bucket_name == "my-test-bucket"
        assert config.aws_region == "eu-central-1"

    def test_override_env(self, monkeypatch):
        monkeypatch.setenv("MYBUCKET_BUCKET_NAME", "my-test-bucket")
        override = {"aws_region": "eu-central-1"}

        config_base = S3Config(env_prefix="MYBUCKET_", config_dict=override)
        config = config_base.make_settings()

        assert config.bucket_name == "my-test-bucket"
        assert config.aws_region == "eu-central-1"
