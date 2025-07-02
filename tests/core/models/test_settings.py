import pytest

from pypeh.core.models.settings import S3Config
from pypeh.adapters.outbound.persistence.connections import (
    S3Connection,
    FileConnection,
)
from tests.test_utils.dirutils import get_absolute_path


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


@pytest.mark.core
class TestConnectionDict:
    def test_small_example(self, monkeypatch):
        monkeypatch.setenv("TEST_BUCKET_NAME", "my-test-bucket")
        monkeypatch.setenv("TEST_AWS_REGION", "eu-central-1")
        source = "./config/test.yaml"
        path = get_absolute_path(source)
        connection_dict = {
            "connection_name_1": S3Connection(
                config=S3Config(env_prefix="TEST_"),
            ),
            "connection_name_2": FileConnection(path=path),
        }

        # namespace could be "peh" or "https://w3id.org/peh"
        # need to figure out what the optimal pattern is here
        importmap = {
            "namespace": "connection_name_1",
            "another_namespace": "connection_name_2",
        }
        # should lead to the ability to load a dataset, specifying where to find a particular namespace
