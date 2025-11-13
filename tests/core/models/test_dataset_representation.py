import pytest

from pypeh.core.cache.containers import CacheContainerFactory, CacheContainerView
from pypeh.core.models.internal_data_layout import DatasetSeries, ElementToObservableProperty, InternalDataLayout
from pypeh.core.models.constants import ObservablePropertyValueType
from pypeh.adapters.outbound.persistence.hosts import DirectoryIO
from pypeh.core.cache.utils import load_entities_from_tree

from tests.test_utils.dirutils import get_absolute_path


class TestInternalDataLayout:
    @pytest.fixture(scope="class")
    def get_cache(self):
        source = get_absolute_path("input")
        container = CacheContainerFactory.new()
        host = DirectoryIO()
        roots = host.load(source, format="yaml")
        for root in roots:
            for entity in load_entities_from_tree(root):
                container.add(entity)

        return CacheContainerView(container)

    def test_bimap(self, get_cache):
        section_id = "SAMPLE_METADATA_SECTION_SAMPLE"
        section = get_cache.get(section_id, "DataLayoutSection")
        bimap = ElementToObservableProperty.from_peh(data_layout_section=section)
        expected = {"id_sample": "peh:adults_id_subject", "matrix": "peh:adults_u_matrix"}
        for expected_key, expected_value in expected.items():
            assert bimap.get_by_key(expected_key) == expected_value
            assert bimap.get_by_value(expected_value) == expected_key

        schema = bimap.collect_schema(get_cache)
        expected_schema = {
            "id_sample": ObservablePropertyValueType.STRING,
            "matrix": ObservablePropertyValueType.STRING,
        }
        assert len(schema) == len(bimap)
        for key, value in expected_schema.items():
            assert schema[key] == value

    def test_internal_data_layout(self, get_cache):
        layout_id = "peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA"
        layout = get_cache.get(layout_id, "DataLayoutLayout")
        internal_layout = InternalDataLayout.from_peh(data_layout=layout)
        schema = internal_layout.collect_schema(get_cache)
        expected_schema = {
            "SAMPLE": {"id_sample": ObservablePropertyValueType.STRING, "matrix": ObservablePropertyValueType.STRING},
            "SAMPLETIMEPOINT_BS": {
                "id_sample": ObservablePropertyValueType.STRING,
                "adults_u_crt": ObservablePropertyValueType.DECIMAL,
            },
        }
        for key, subschema in expected_schema.items():
            for subkey, value in subschema.items():
                assert schema[key][subkey] == value

    def test_dataset_series(self, get_cache):
        cache_view = get_cache
        layout_id = "peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA"
        layout = get_cache.get(layout_id, "DataLayoutLayout")
        all_sections = set()
        for section in layout.sections:
            section_id = section.id
            if section.id is not None:
                all_sections.add(section_id)
        dataset_series = DatasetSeries.from_peh_datalayout(
            layout,
            cache_view=cache_view,
        )
        assert isinstance(dataset_series, DatasetSeries)
        assert dataset_series.described_by == "peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA"
        for dataset in dataset_series.parts.values():
            assert dataset.described_by in all_sections

        schema = dataset_series.get_type_annotations()
        expected_schema = {
            "SAMPLE": {"id_sample": ObservablePropertyValueType.STRING, "matrix": ObservablePropertyValueType.STRING},
            "SAMPLETIMEPOINT_BS": {
                "id_sample": ObservablePropertyValueType.STRING,
                "adults_u_crt": ObservablePropertyValueType.DECIMAL,
            },
        }
        for key, subschema in expected_schema.items():
            for subkey, value in subschema.items():
                assert schema[key][subkey] == value
