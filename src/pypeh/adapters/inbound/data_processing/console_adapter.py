from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Mapping

from pypeh.core.interfaces.inbound.data_processing import DataProcessingInterface
from peh_model import peh

if TYPE_CHECKING:
    from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class ConsoleAdapter(DataProcessingInterface):
    """
    Console Adapter for performing data processing steps
    """
    def get_export_info(self, project_name: str = None, data_layout: str = None, target_filepath: str = None):
        project_name = project_name or input("Enter Project Name: ")
        data_layout = data_layout or input("Enter Data Layout Name: ")
        target_filepath = target_filepath or input("Enter Output File Path: ")
        # Alternative: pass domain objects: peh.DataLayout, peh.DataExtract, target: FilePath
        return {
            "project_name": project_name,
            "data_layout": data_layout,
            "target_filepath": target_filepath,
        }

    def create_empty_data_template(self, project_name: str, config_path: str, target_path: str):
        raise NotImplementedError

    def create_data_dictionary(self, project_name: str, config_path: str, target_path: str):
        raise NotImplementedError

    def validate_data(self, project_name: str, config_path: str, data_layout: str, data_path: str):
        # if data_path contains tabular data  > _validate_tabular_data
        # if data_path contains observed values > _validate_observed_values
        raise NotImplementedError

    def _validate_tabular_data(self, data: Mapping, project: Mapping, variables: Mapping):
        # data: List[List[]], project: peh.Project, variables: List[ObservableProperty]
        raise NotImplementedError

    def _validate_observed_values(self, data: Mapping, project: Mapping, variables: Mapping):
        # data: List[peh.ObservedValue], project: peh.Project, variables: List[ObservableProperty]
        raise NotImplementedError

    def import_data(self, project_name: str, config_path: str, data_layout: str, data_path: str):
        raise NotImplementedError

    def extract_data(self, project_name: str, config_path: str, data_layout: str, data_extract: str, target_path: str):
        raise NotImplementedError
