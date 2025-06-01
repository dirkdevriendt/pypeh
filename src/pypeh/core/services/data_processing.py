from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Mapping

from pypeh.core.interfaces.inbound.data_processing import DataProcessingInterface
from pypeh.core.interfaces.outbound.dataops import ExportInterface, ExportTypeEnum
from pypeh.core.interfaces.outbound.dataops import ValidationInterface
from pypeh.core.interfaces.outbound.persistence import PersistenceInterface

from peh_model import peh

if TYPE_CHECKING:
    from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class ValidationService():
    def __init__(self, inbound_adapter: DataProcessingInterface, persistence_adapter: PersistenceInterface, validation_adapter: ValidationInterface):
        self.inbound_adapter = inbound_adapter
        self.persistence_adapter = persistence_adapter
        self.validation_adapter = validation_adapter

    def validate(self):
        pass

class DataTemplateService():
    def __init__(self, inbound_adapter: DataProcessingInterface, persistence_adapter: PersistenceInterface, export_adapter: ExportInterface):
        self.inbound_adapter = inbound_adapter
        self.persistence_adapter = persistence_adapter
        self.export_adapter = export_adapter

    def create_empty_data_template(self, config, data_layout_id, target_path):
        #export_info = self.inbound_adapter.get_export_info(project_name="p", data_layout="l", target_filepath="output.xlsx")
        #target = export_info["target_filepath"]
        #config = self.persistence_adapter.load(config_source)
        observable_property_dict = {op.id: op for op in list(config.view_all()) if isinstance(op, peh.ObservableProperty)}
        self.export_adapter.export(export_type=ExportTypeEnum.EmptyDataTemplate, config=config, observable_property_dict=observable_property_dict,
                                   data_layout_id=data_layout_id, target_path=target_path)
