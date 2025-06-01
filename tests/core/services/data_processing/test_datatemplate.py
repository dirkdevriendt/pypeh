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
from pypeh.adapters.outbound.export.xlsx_adapter.adapter import XlsxExportAdapter

from pypeh.core.services.data_processing import DataTemplateService

class TestLoadingDataTemplate:
    @pytest.mark.core
    def test_integration_dataview_datatemplate(self):
        service = DataTemplateService(
            inbound_adapter=ConsoleAdapter(),
            persistence_adapter=DirectoryIO(),
            export_adapter=XlsxExportAdapter(),
        )
        #service.create_empty_data_template("./input/config_datatemplate/", "./result/config_datatemplate/test_result_template.xlsx")

        sources = [
            "./input/config_datatemplate/observable_properties.yaml",
            "./input/config_datatemplate/observations.yaml",
            "./input/config_datatemplate/data_layout.yaml",
        ]
        sources = [get_absolute_path(s) for s in sources]
        data_view = get_dataview(importmap={"peh": sources})

        service.create_empty_data_template(data_view, "peh:PARC_ALIGNED_STUDIES_LAYOUT_ADULTS", "./tests/core/services/data_processing/result/config_datatemplate/test_result_template.xlsx")

    @pytest.mark.core
    def test_unit_dataview_datatemplate(self):
        sources = [
            "./input/config_datatemplate/observable_properties.yaml",
            "./input/config_datatemplate/observations.yaml",
            "./input/config_datatemplate/data_layout.yaml",
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

