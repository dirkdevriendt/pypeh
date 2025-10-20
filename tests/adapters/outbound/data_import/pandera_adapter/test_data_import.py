import pytest
import random

from pypeh.core.interfaces.outbound.dataops import DataImportInterface
from pypeh.core.models.internal_data_layout import ObservationResultProxy


@pytest.mark.dataframe
class TestDataImport:
    @pytest.fixture(scope="class")
    def df(self):
        import polars as pl

        df = pl.DataFrame(
            {
                "id_sample": [i + 1 for i in range(10)],
                "obs_1": [random.randint(0, 100) for _ in range(10)],
                "obs_2": [random.uniform(0, 1) for _ in range(10)],
            }
        )
        return df

    def test_splice_dataframe(self, df):
        data_layout_element_labels = ["id_sample", "obs_1"]
        identifying_layout_element_label = "id_sample"
        entity_id_list = [1, 4, 5]
        adapter = DataImportInterface.get_default_adapter_class()
        result = adapter()._raw_data_to_observation_results(
            raw_data=df,
            data_layout_element_labels=data_layout_element_labels,
            identifying_layout_element_label=identifying_layout_element_label,
            entity_id_list=entity_id_list,
        )
        assert isinstance(result, ObservationResultProxy)
        observed_df = result.observed_values
        assert observed_df.shape == (len(entity_id_list), len(data_layout_element_labels))
        assert observed_df.columns == data_layout_element_labels
        assert set(observed_df["id_sample"].to_list()) == set(entity_id_list)
