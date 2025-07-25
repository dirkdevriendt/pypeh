import pytest

from peh_model import peh

from pypeh.core.cache.dataview import (
    BaseView,
    ImportMapEntityLoader,
    get_dataview,
    get_importmapview,
)
from pypeh.core.cache.containers import MappingContainer
from pypeh.adapters.outbound.persistence.hosts import FileIO
from pypeh.adapters.outbound.persistence.serializations import YamlIO
from pypeh.core.cache.utils import load_entities_from_tree
from pypeh.core.models.proxy import TypedLazyProxy

from tests.test_utils.dirutils import get_absolute_path


class TestLoading:
    @pytest.mark.core
    def test_baseview(self):
        source = get_absolute_path("./input/config_basic/observation_results.yaml")
        data_view = BaseView()
        yaml_loader = ImportMapEntityLoader(FileIO())
        root_stream = yaml_loader.load(source)
        _ = data_view._add_root_stream(root_stream, None)

        entity = data_view.view_entity("peh:INQUIRE_IT_PERSON_Q1_RESULT1", "ObservationResult")
        assert isinstance(entity, peh.ObservationResult)

    @pytest.mark.core
    def test_load_entities_from_tree(self):
        base_view = BaseView()
        yaml_loader = YamlIO()
        source = get_absolute_path("./input/config_basic/observable_properties.yaml")
        root = yaml_loader.load(source)

        all_entities = []
        assert isinstance(root, peh.YAMLRoot)
        for entity in load_entities_from_tree(root, create_proxy=base_view.create_proxy):
            if isinstance(entity, TypedLazyProxy):
                all_entities.append(entity)
        assert len(all_entities) > 0

    @pytest.mark.core
    def test_importview(self):
        sources = [
            "./input/config_basic/observable_properties.yaml",
            # "./input/config_basic/observable_entities.yaml",
            # "./input/config_basic/observation_results.yaml",
        ]
        sources = [get_absolute_path(s) for s in sources]
        import_view = get_importmapview(importmap={"peh": sources})
        # request entity from file:
        entity_id = "peh:ParticipantID"
        entity_type = "ObservableProperty"
        ret = import_view.request_entity(entity_id, entity_type)
        assert ret is not None
        assert ret.id == entity_id
        assert isinstance(ret, getattr(peh, entity_type))
        # return entry should have been popped from storage
        assert not import_view._storage.exists(entity_id, entity_type)

    @pytest.mark.core
    def test_dataview(self):
        sources = [
            "./input/config_basic/observable_properties.yaml",
            "./input/config_basic/observation_results.yaml",
            "./input/config_basic/observable_entities.yaml",
        ]
        sources = [get_absolute_path(s) for s in sources]
        data_view = get_dataview(importmap={"peh": sources})
        assert data_view._loader is not None
        entity_id = "peh:ParticipantID"
        entity_type = "ObservableProperty"
        ret = data_view.request_entity(entity_id, entity_type)
        assert ret is not None
        assert ret.id == entity_id
        assert isinstance(ret, getattr(peh, entity_type))
        assert data_view._storage.exists(entity_id, entity_type)

        # check peh:IT_HH_01
        # should have been lazy loaded when loading in results first
        # and only then encountered actual object within observable_entities
        entity_id = "peh:IT_HH_01"
        entity_type = "StudyEntity"
        ret = data_view.request_entity(entity_id, entity_type)
        assert ret is not None
        assert ret.id == entity_id
        assert isinstance(ret, getattr(peh, entity_type))

        # check indirectly
        # peh:PRIMARY_QUEST_HEALTH
        entity_id = "peh:PRIMARY_QUEST_HEALTH"
        entity_type = "Grouping"
        ret = data_view.request_entity(entity_id, entity_type)
        assert ret is not None
        assert ret.id == entity_id
        assert isinstance(ret, TypedLazyProxy)

    @pytest.mark.core
    def test_load_reference(self):
        sources = [
            "./input/config_basic/observable_properties.yaml",
            "./input/config_basic/observable_entities.yaml",
            "./input/config_basic/observation_results.yaml",
        ]
        sources = [get_absolute_path(s) for s in sources]
        data_view = get_dataview(importmap={"peh": sources})
        assert isinstance(data_view._storage, MappingContainer)

        # test importmapentityloader
        assert data_view._loader is not None
        assert data_view._cache_viewer is not None
        assert data_view._cache_viewer._loader is not None
        for s in sources:
            data_view._cache_viewer._loader.load(s)
