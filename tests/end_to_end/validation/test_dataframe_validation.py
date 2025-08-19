import pytest
import peh_model.peh as peh

from tests.test_utils.dirutils import get_absolute_path

from pypeh import Session
from pypeh.core.services.dataops import ValidationService
from pypeh.core.models.validation_errors import ValidationErrorReport


@pytest.mark.end_to_end
class TestSessionDefaultLocalFile:
    def test_end_to_end_dataframe_validation(self, monkeypatch):
        from pypeh.adapters.outbound.validation.pandera_adapter.dataops import DataFrameAdapter

        monkeypatch.setenv("DEFAULT_PERSISTED_CACHE_TYPE", "LocalFile")
        monkeypatch.setenv("DEFAULT_PERSISTED_CACHE_ROOT_FOLDER", get_absolute_path("./input/test_01/config"))

        session = Session()
        session.load_persisted_cache()

        layout = session.cache.get("peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA", "DataLayout")
        assert isinstance(layout, peh.DataLayout)
        observation = session.cache.get("peh:VALIDATION_TEST_SAMPLE_METADATA", "Observation")
        assert isinstance(observation, peh.Observation)
        observable_property_dict = {}
        for op in session.cache.get_all("ObservableProperty"):
            assert isinstance(op.id, str)
            assert isinstance(op, peh.ObservableProperty)
            observable_property_dict[op.id] = op

        excel_path = get_absolute_path("./input/test_01/validation_test_01_data.xlsx")
        data_dict = session.load_tabular_data(
            source=excel_path,
            validation_layout=layout,
        )
        assert isinstance(data_dict, dict)
        data_df = data_dict["SAMPLE"]
        assert data_df is not None
        validation_service = ValidationService(outbound_adapter=DataFrameAdapter(), cache=session.cache)
        validation_result = validation_service._validate_data(
            data_df, observation=observation, observable_property_dict=observable_property_dict
        )

        report_to_check = list(validation_result.values())[0]

        assert isinstance(report_to_check, ValidationErrorReport)
        assert report_to_check.total_errors == 1
        assert report_to_check.groups[-1].errors[-1].type == "check_categorical"
