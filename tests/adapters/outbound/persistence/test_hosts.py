import pytest

from peh_model.peh import EntityList

from pypeh.adapters.outbound.persistence.hosts import FileIO, DirectoryIO, S3StorageProvider
from pypeh.core.models.settings import S3Config

from tests.test_utils.dirutils import get_absolute_path


class TestFileIO:
    @pytest.mark.core
    @pytest.mark.parametrize(
        "file_path, expected_format",
        [
            ("example.yaml", "yaml"),
            ("config.json", "json"),
            ("data.TXT", "txt"),
            ("no_extension", ""),
        ],
    )
    def test_get_format(self, file_path, expected_format):
        assert FileIO.get_format(file_path) == expected_format

    @pytest.mark.core
    def test_basic(self):
        source = get_absolute_path("./input/config_basic/_Reference_YAML/observable_entities.yaml")
        fio = FileIO()
        data = fio.load(source)
        assert isinstance(data, EntityList)

    @pytest.mark.core
    def test_unknown_format(self):
        pass

    @pytest.mark.core
    def test_incompatible_file(self):
        pass


class TestDirectoryIO:
    @pytest.mark.core
    def test_basic(self):
        directory_io = DirectoryIO()
        source = get_absolute_path("./input/config_basic")
        all_data = list(directory_io.load(source, maxdepth=10))
        assert len(all_data) > 1

    @pytest.mark.core
    def test_unknown_format(self):
        directory_io = DirectoryIO()
        source = get_absolute_path("./input/unknown_format")
        all_data = list(directory_io.load(source))
        assert len(all_data) == 0

    @pytest.mark.core
    def test_incompatible_file(self):
        directory_io = DirectoryIO()
        source = get_absolute_path("./input/wrong_input")
        with pytest.raises(TypeError):
            _ = list(directory_io.load(source))

    @pytest.mark.core
    def test_walk(self):
        source = get_absolute_path("./input/config_basic")
        directory_io = DirectoryIO()
        i = 0
        for _ in directory_io.file_system.walk(source):
            i += 1
        assert i > 1


@pytest.mark.s3
class TestS3Adapter:
    def test_basic(self, monkeypatch):
        monkeypatch.setenv("MYBUCKET_BUCKET_NAME", "my-test-bucket")
        override = {"aws_region": "eu-central-1"}

        config_base = S3Config(env_prefix="MYBUCKET_", config_dict=override)
        settings = config_base.make_settings()
        s3io = S3StorageProvider(settings)


class TestDatabaseAdapter:
    @pytest.mark.core
    def test_basic(self):
        # check ability to connect to pre-existing sql-db
        assert True


class TestWebserviceAdapter:
    @pytest.mark.core
    def test_basic(self):
        # use mock here
        assert True

    @pytest.mark.core
    def test_yaml(self):
        pass

    @pytest.mark.core
    def test_json(self):
        pass
