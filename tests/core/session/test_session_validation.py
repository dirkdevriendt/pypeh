import pytest
import pathlib

from peh_model.peh import DataLayout

from pypeh import Session
from pypeh.core.models.validation_errors import ValidationError

from tests.test_utils.dirutils import get_absolute_path


@pytest.mark.dataframe
class TestSessionValidation:
    def test_invalid_file(self, monkeypatch):
        monkeypatch.setenv("DEFAULT_PERSISTED_CACHE_TYPE", "LocalFile")
        monkeypatch.setenv("DEFAULT_PERSISTED_CACHE_ROOT_FOLDER", get_absolute_path("./input/validation_config"))
        excel_path = get_absolute_path("./input/validation_files/invalid_excel.xlsx")
        assert pathlib.Path(excel_path).is_file()

        session = Session()
        result = session.load_tabular_data(source=excel_path)
        assert isinstance(result, ValidationError)
        assert result.type == "File Processing Error"

    def test_valid_file(self, monkeypatch):
        monkeypatch.setenv("DEFAULT_PERSISTED_CACHE_TYPE", "LocalFile")
        monkeypatch.setenv("DEFAULT_PERSISTED_CACHE_ROOT_FOLDER", get_absolute_path("./input/validation_config"))
        excel_path = get_absolute_path("./input/validation_files/valid_excel_wrong_format.xlsx")

        session = Session()
        result = session.load_tabular_data(source=excel_path)
        assert isinstance(result, dict)
        assert len(result) == 1

    def test_invalid_sheets(self, monkeypatch):
        monkeypatch.setenv("DEFAULT_PERSISTED_CACHE_TYPE", "LocalFile")
        monkeypatch.setenv("DEFAULT_PERSISTED_CACHE_ROOT_FOLDER", get_absolute_path("./input/validation_config"))
        excel_path = get_absolute_path("./input/validation_files/valid_excel_wrong_format.xlsx")

        session = Session()
        session.load_persisted_cache()
        layout = session.cache.get("TEST_DATA_LAYOUT", "DataLayout")
        assert layout is not None
        assert isinstance(layout, DataLayout)
        result = session.load_tabular_data(source=excel_path, validation_layout=layout)
        assert isinstance(result, ValidationError)
        assert result.type == "File Processing Error"
