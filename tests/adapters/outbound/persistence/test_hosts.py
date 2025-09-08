import pytest

from pathlib import Path
from peh_model.peh import EntityList

from pypeh.adapters.outbound.persistence.hosts import (
    FileIO,
    DirectoryIO,
    LocalStorageProvider,
    S3StorageProvider,
)
from pypeh.core.models.settings import LocalFileConfig, LocalFileSettings, S3Config, S3Settings
from pypeh.core.session.connections import ConnectionManager

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


@pytest.mark.dataframe
class TestFileIOCsv:
    def test_basic(self):
        source = get_absolute_path("./input/config_basic/_Tabular_Data/sampling_data_to_import.csv")
        fio = FileIO()
        data = fio.load(source, raise_if_empty=False)
        from polars import DataFrame

        assert isinstance(data, DataFrame)


@pytest.mark.dataframe
class TestFileIOExcel:
    def test_basic(self):
        source = get_absolute_path("./input/config_basic/_Tabular_Data/sampling_data_to_import.xlsx")
        fio = FileIO()
        data = fio.load(source)
        assert isinstance(data, dict)


class TestDirectoryIO:
    @pytest.mark.dataframe
    def test_basic(self):
        directory_io = DirectoryIO()
        source = get_absolute_path("./input/config_basic")
        all_data = list(directory_io.load(source, maxdepth=10))
        assert len(all_data) > 1

    @pytest.mark.core
    def test_no_root(self):
        source = get_absolute_path("./input/config_basic/_Reference_YAML")
        directory_io = DirectoryIO()
        all_data = directory_io.load(source)
        assert len(all_data) > 0

    @pytest.mark.core
    def test_unknown_format(self):
        source = Path("input/unknown_format")
        path = Path(get_absolute_path(str(source)))
        root = path.parents[len(source.parts) - 1]

        directory_io = DirectoryIO(root=str(root))
        all_data = list(directory_io.load(str(source)))
        assert len(all_data) == 0

    @pytest.mark.core
    def test_incompatible_file(self):
        source = Path("input/wrong_input")
        path = Path(get_absolute_path(str(source)))
        root = path.parents[len(source.parts) - 1]

        directory_io = DirectoryIO(root=str(root))
        with pytest.raises(TypeError):
            _ = list(directory_io.load(str(source)))

    @pytest.mark.core
    def test_walk(self):
        source = Path("input/config_basic")
        path = Path(get_absolute_path(str(source)))
        root = path.parents[len(source.parts) - 1]

        directory_io = DirectoryIO(root=str(root))
        i = 0
        for _ in directory_io.walk(str(source), format="yaml"):
            i += 1
        assert i > 1


@pytest.mark.core
class TestLocalStorageProvider:
    def test_with_settings(self):
        rel_root_folder = "./input/config_basic/_Reference_YAML"
        abs_root_folder = get_absolute_path(rel_root_folder)
        source_file = "observable_entities.yaml"
        config = LocalFileConfig(config_dict={"root_folder": abs_root_folder})
        settings = config.make_settings(_env_file=None)
        provider = ConnectionManager._create_adapter(settings)

        data = provider.load(source_file, format="yaml")
        assert data is not None
        assert isinstance(data, EntityList)


@pytest.mark.s3
class TestS3StorageProvider:
    def test_basic(self, monkeypatch):
        monkeypatch.setenv("MYBUCKET_BUCKET_NAME", "my-test-bucket")
        monkeypatch.setenv("MYBUCKET_ENDPOINT_URL", "http://endpoint-example.local")
        override = {"aws_region": "eu-central-1"}

        config_base = S3Config(env_prefix="MYBUCKET_", config_dict=override)
        settings = config_base.make_settings()
        s3io = S3StorageProvider(settings)
        assert s3io is not None


@pytest.mark.core
class TestDatabaseProvider:
    def test_basic(self):
        # check ability to connect to pre-existing sql-db
        assert True


@pytest.mark.core
class TestWebserviceProvider:
    def test_basic(self):
        # use mock here
        assert True

    def test_yaml(self):
        pass

    def test_json(self):
        pass


class TestHostFactory:
    @pytest.mark.core
    def test_factory_local(self):
        file_settings = LocalFileSettings()
        file_service = ConnectionManager._create_adapter(file_settings)
        assert isinstance(file_service, LocalStorageProvider)

    @pytest.mark.s3
    def test_factory_s3(self):
        file_settings = S3Settings(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_session_token="test",
            bucket_name="TEST",
        )
        file_service = ConnectionManager._create_adapter(file_settings)
        assert isinstance(file_service, S3StorageProvider)
