import pytest
import pathlib

from peh_model.peh import DataLayout

from pypeh import Session
from pypeh.core.models.internal_data_layout import DataImportConfig, ObservationResultProxy, SectionImportConfig
from pypeh.core.models.validation_errors import ValidationError
from pypeh.core.models.settings import LocalFileConfig

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

    def test_load_data_collection_basic(self):
        session = Session(
            connection_config=[
                LocalFileConfig(
                    label="local_file",
                    config_dict={
                        "root_folder": get_absolute_path("./input/load_data_collection_basic"),
                    },
                ),
            ],
            default_connection="local_file",
            load_from_default_connection="",
        )
        import_config = DataImportConfig(
            data_layout_id="peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA",
            section_map=[
                SectionImportConfig(
                    data_layout_section_id="SAMPLE_METADATA_SECTION_SAMPLE",
                    observation_ids=[
                        "peh:VALIDATION_TEST_SAMPLE_METADATA",
                    ],
                ),
                SectionImportConfig(
                    data_layout_section_id="SAMPLE_METADATA_SECTION_SAMPLETIMEPOINT_BSS",
                    observation_ids=[
                        "peh:VALIDATION_TEST_SAMPLE_TIMEPOINT",
                    ],
                ),
            ],
        )
        result = session.load_tabular_data_collection(
            source="validation_test_03_data.xlsx", import_config=import_config, connection_label="local_file"
        )
        assert isinstance(result, dict)
        assert "peh:VALIDATION_TEST_SAMPLE_METADATA" in result
        observation_result = result["peh:VALIDATION_TEST_SAMPLE_METADATA"]
        assert isinstance(observation_result, ObservationResultProxy)
        assert observation_result.observed_values.shape == (1, 7)
        assert "peh:VALIDATION_TEST_SAMPLE_TIMEPOINT" in result
        observation_result = result["peh:VALIDATION_TEST_SAMPLE_TIMEPOINT"]
        assert isinstance(observation_result, ObservationResultProxy)
        assert observation_result.observed_values.shape == (1, 4)

    def test_invalid_sheets(self, monkeypatch):
        monkeypatch.setenv("DEFAULT_PERSISTED_CACHE_TYPE", "LocalFile")
        monkeypatch.setenv("DEFAULT_PERSISTED_CACHE_ROOT_FOLDER", get_absolute_path("./input/validation_config"))
        excel_path = get_absolute_path("./input/validation_files/valid_excel_wrong_format.xlsx")

        session = Session()
        session.load_persisted_cache()
        layout = session.cache.get("TEST_DATA_LAYOUT", "DataLayout")
        assert layout is not None
        assert isinstance(layout, DataLayout)
        result = session.load_tabular_data(source=excel_path, data_layout=layout)
        assert isinstance(result, ValidationError)
        assert result.type == "File Processing Error"

    def test_multiple_connections(self):
        session = Session(
            connection_config=[
                LocalFileConfig(
                    label="local_file_validation_config",
                    config_dict={
                        "root_folder": get_absolute_path("./input/default_localfile_data"),
                    },
                ),
                LocalFileConfig(
                    label="local_file_validation_files",
                    config_dict={
                        "root_folder": get_absolute_path("./input/validation_files"),
                    },
                ),
            ],
            default_connection="local_file_validation_config",
        )
        session.load_persisted_cache()
        observation = session.cache.get("peh:OBSERVATION_ADULTS_ANALYTICALINFO", "Observation")
        assert observation.id == "peh:OBSERVATION_ADULTS_ANALYTICALINFO"
        layout = session.cache.get("TEST_DATA_LAYOUT", "DataLayout")
        assert isinstance(layout, DataLayout)
        data = session.load_tabular_data(
            source="multi_connection_valid_excel.xlsx",
            connection_label="local_file_validation_files",
            data_layout=layout,
        )
        assert isinstance(data, dict)
        assert len(data) == 1


class TestMapping:
    def test_layout_map(self):
        session = Session(
            connection_config=[
                LocalFileConfig(
                    label="local_file_validation_config",
                    config_dict={
                        "root_folder": get_absolute_path("./input/layout_obsprop_map"),
                    },
                ),
            ],
            default_connection="local_file_validation_config",
        )
        session.load_persisted_cache()
        data_layout_id = "peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA"
        data_layout = session.cache.get(data_layout_id, "DataLayout")
        assert isinstance(data_layout, DataLayout)
        ret = session.layout_section_elements_to_observable_property_value_types(data_layout, flatten=True)
        assert isinstance(ret, dict)
        for value in ret.values():
            assert value in set(["decimal", "string", "integer", "float", "boolean"])
