import pytest

from peh_model.peh import ObservableProperty, Observation

from pypeh.core.cache.containers import CacheContainerFactory, CacheContainerView
from pypeh.core.cache.utils import load_entities_from_tree
from pypeh.adapters.outbound.persistence.hosts import DirectoryIO

from tests.test_utils.dirutils import get_absolute_path


@pytest.mark.core
class TestCache:
    @pytest.fixture(scope="class")
    def container(self):
        source = get_absolute_path("../../input/roundtrip")
        container = CacheContainerFactory.new()
        host = DirectoryIO()
        roots = host.load(source, format="yaml")
        for root in roots:
            for entity in load_entities_from_tree(root):
                container.add(entity)

        return container

    def test_filter_cache(self, container):
        assert all(isinstance(i, (ObservableProperty, Observation)) for i in container.get_all())
        filter = list(container.get_all(entity_type="Observation"))
        assert len(filter) > 1

    def test_cache_view(self, container):
        cache_view = CacheContainerView(container)
        assert cache_view.exists("OBSERVATION_ADULTS_CONSIDERATIONS", "Observation")

        container_subset = ["OBSERVATION_ADULTS_CONSIDERATIONS", "OBSERVATION_ADULTS_BLOOD_QUEST", "adults_id_subject"]
        cache_view = CacheContainerView(container, container_subset)
        ret = list(cache_view.get_all("Observation"))
        assert set(entity.id for entity in ret) == set(
            ["OBSERVATION_ADULTS_CONSIDERATIONS", "OBSERVATION_ADULTS_BLOOD_QUEST"]
        )

        container_subset = {
            "Observation": ["OBSERVATION_ADULTS_CONSIDERATIONS", "OBSERVATION_ADULTS_BLOOD_QUEST"],
            "ObservableProperty": ["adults_id_subject"],
        }
        cache_view = CacheContainerView(container, container_subset)
        assert len(list(cache_view.get_all())) == 3
