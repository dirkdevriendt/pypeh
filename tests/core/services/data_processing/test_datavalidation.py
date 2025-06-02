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
from pypeh.adapters.outbound.persistence.formats import YamlIO, load_entities_from_tree
from pypeh.core.models.proxy import TypedLazyProxy
from pypeh.core.api import read_yaml

from tests.test_utils.dirutils import get_absolute_path

from pypeh.adapters.inbound.data_processing.console_adapter import ConsoleAdapter
from pypeh.adapters.outbound.persistence.hosts import DirectoryIO
from pypeh.adapters.outbound.validation.pandera_adapter.dataops import DataOpsAdapter

from pypeh.core.services.data_processing import ValidationService

import copy

class TestDataValidation:
    @pytest.mark.validation
    def test_integration_datavalidation(self):
        service = ValidationService(
            inbound_adapter=ConsoleAdapter(),
            persistence_adapter=DirectoryIO(),
            validation_adapter=DataOpsAdapter(),
        )

        sources = [
            "./input/config_datavalidation/observable_properties.yaml",
            "./input/config_datavalidation/observations.yaml",
        ]
        sources = [get_absolute_path(s) for s in sources]
        observation_data_view = get_dataview(importmap={"peh": sources})
        observation = observation_data_view.request_entity("peh:OBSERVATION_ADULTS_URINE_LAB", "Observation")

        op_id_list = []
        for oep_set in observation.observation_design.observable_entity_property_sets:
            op_id_list.extend(oep_set.identifying_observable_property_id_list + oep_set.required_observable_property_id_list + oep_set.optional_observable_property_id_list)
        observable_property_dict = {op_id: observation_data_view.request_entity(op_id, "ObservableProperty") for op_id in set(op_id_list)}

        data = {"id_subject": [1, 2], "matrix": ["UM", "UM"], "sg": [1.1, 1.0], "crt": [0.88, 0.90]}

        validation_result = service.validate_data(data, observation, observable_property_dict)
        print(validation_result)


    @pytest.mark.validation
    def test_unit_dataview_datavalidation(self):
        sources = [
            "./input/config_datavalidation/observable_properties.yaml",
            "./input/config_datavalidation/observations.yaml",
            "./input/config_datavalidation/data_layout.yaml",
        ]
        sources = [get_absolute_path(s) for s in sources]
        data_view = get_dataview(importmap={"peh": sources})
        assert data_view._loader is not None

        entity_id = "peh:PARC_ALIGNED_STUDIES_LAYOUT_ADULTS"
        entity_type = "DataLayout"
        ret = data_view.request_entity(entity_id, entity_type)
        assert ret is not None
        assert ret.id == entity_id
        assert isinstance(ret, getattr(peh, entity_type))
        assert data_view._storage.exists(entity_id, entity_type)

        # check peh:adults_id_subject
        # should have been lazy loaded when loading in results first
        # and only then encountered actual object within observable_entities
        entity_id = "peh:adults_id_subject"
        entity_type = "ObservableProperty"
        ret = data_view.request_entity(entity_id, entity_type)
        assert ret is not None
        assert ret.id == entity_id
        assert isinstance(ret, getattr(peh, entity_type))

        # check peh:OBSERVATION_ADULTS_ANALYTICALINFO
        # should have been lazy loaded when loading in results first
        # and only then encountered actual object within observable_entities
        entity_id = "peh:OBSERVATION_ADULTS_ANALYTICALINFO"
        entity_type = "Observation"
        ret = data_view.request_entity(entity_id, entity_type)
        assert ret is not None
        assert ret.id == entity_id
        assert isinstance(ret, getattr(peh, entity_type))

