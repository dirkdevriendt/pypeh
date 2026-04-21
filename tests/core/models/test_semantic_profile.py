import pytest

from datetime import datetime
from rdflib import Graph

from pypeh.core.cache.containers import (
    CacheContainerFactory,
    CacheContainerView,
)
from pypeh.core.models.internal_data_layout import DatasetSeries
from pypeh.adapters.persistence.hosts import DirectoryIO
from pypeh.core.cache.utils import load_entities_from_tree

from pypeh.core.models.semantic_profile import CSVWDatasetSchema
from pypeh.core.utils.rdf_graph_builder import GraphBuilder
from tests.test_utils.dirutils import get_absolute_path


@pytest.mark.core
class TestSemanticProfile:
    @pytest.fixture(scope="class")
    def get_cache(self) -> CacheContainerView:
        source = get_absolute_path("input")
        container = CacheContainerFactory.new()
        host = DirectoryIO()
        roots = host.load(source, format="yaml")
        for root in roots:
            for entity in load_entities_from_tree(root):
                container.add(entity)

        return CacheContainerView(container)

    def test_dataset_contained_in_schema(self, get_cache):
        cache_view = get_cache
        rdf_graph_builder = GraphBuilder()

        layout_id = "peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA"
        layout = get_cache.get(layout_id, "DataLayoutLayout")
        dataset_series = DatasetSeries.from_peh_datalayout(
            layout,
            cache_view=cache_view,
        )
        assert isinstance(dataset_series, DatasetSeries)
        creator = "https://orcid.org/000-000-000X"
        created = datetime.now()
        prov = {"creator": creator, "created": created}
        ret = rdf_graph_builder.build_series(
            dataset_series,
            provenance_info=prov,
            schema_profile=CSVWDatasetSchema,
        )
        assert isinstance(ret, Graph)
