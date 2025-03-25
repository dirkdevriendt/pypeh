from pypeh.core.cache.dataview import BaseView, ImportMapEntityLoader, get_dataview, DataView
from pypeh.core.cache.containers import MappingContainer
from pypeh.core.persistence.hosts import FileIO
from pypeh.core.models import peh

from tests.utils.dirutils import get_absolute_path


class TestLoading:
    def test_baseview(self):
        data_view = get_dataview(importmap={})
        assert data_view._loader is not None
        assert isinstance(data_view._storage, MappingContainer)
        source = get_absolute_path("./input/observation_results.yaml")
        # create LinkMLRepository
        data_view = BaseView()
        yaml_loader = ImportMapEntityLoader(FileIO(), data_view)
        yaml_loader.load_greedily(source)

        entity = data_view.view_entity("INQUIRE_IT_PERSON_Q1_RESULT1", "ObservationResult")
        assert isinstance(entity, peh.ObservationResult)
