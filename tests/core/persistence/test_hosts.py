import pytest

from pypeh.core.persistence.hosts import FileIO, DirectoryIO, DatabaseAdapter, WebServiceAdapter


from tests.utils.dirutils import get_absolute_path


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
        assert True

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
        all_data = list(directory_io.load(source))
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
