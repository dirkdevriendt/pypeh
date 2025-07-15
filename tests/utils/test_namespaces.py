import pytest

from pypeh.core.utils.namespaces import ImportMap
from pypeh.core.models.settings import LocalFileConfig, S3Config, ImportConfig, ValidatedImportConfig


@pytest.mark.core
class TestImportMap:
    def test_importmap_basic(self):
        import_map = ImportMap()
        import_map["https://www.example.org/"] = "s3://this.file"
        import_map["https://www.example.org/exception"] = "s3://another.file"
        # anything within the example.org namespace should be directed to this.file
        assert import_map["https://www.example.org/regular"] == "s3://this.file"
        # except when a concrete exception has been added to the ImportMap
        assert import_map["https://www.example.org/exception"] == "s3://another.file"
        assert import_map["https://"] is None
        assert import_map["https://test"] is None
        assert set(import_map.keys()) == set(
            [
                "https://www.example.org/",
                "https://www.example.org/exception",
            ]
        )
        assert set(import_map.values()) == set(["s3://another.file", "s3://this.file"])


@pytest.mark.core
class TestConfigMap:
    def test_import_config(self, monkeypatch):
        monkeypatch.setenv("TEST_BUCKET_NAME", "my-test-bucket")
        monkeypatch.setenv("TEST_AWS_REGION", "eu-central-1")
        connection_map = {"my_s3_bucket": S3Config(env_prefix="TEST_"), "my_file_system": LocalFileConfig()}
        import_map = {
            "https://www.example.org/test": "my_s3_bucket",
            "https://www.example.org": "my_file_system",
        }

        import_config = ImportConfig(
            connection_map=connection_map,
            import_map=import_map,
        )
        assert isinstance(import_config, ImportConfig)
        validated_config = import_config.to_validated_import_config()
        assert isinstance(validated_config, ValidatedImportConfig)
