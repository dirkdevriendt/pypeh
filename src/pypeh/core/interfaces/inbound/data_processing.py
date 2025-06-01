"""
Services that provide Validation features use this interface for external users to access.
"""

import logging

from abc import abstractmethod
from typing import TYPE_CHECKING

from pypeh.core.abc import Interface
from peh_model import peh

if TYPE_CHECKING:
    from typing import List

logger = logging.getLogger(__name__)

class DataProcessingInterface(Interface):

    @abstractmethod
    def create_empty_data_template(self, project_name: str, config_path: str, target_path: str):
        raise NotImplementedError

    @abstractmethod
    def create_data_dictionary(self, project_name: str, config_path: str, target_path: str):
        raise NotImplementedError

    @abstractmethod
    def validate_data(self, project_name: str, config_path: str, data_layout: str, data_path: str):
        raise NotImplementedError

    @abstractmethod
    def import_data(self, project_name: str, config_path: str, data_layout: str, data_path: str):
        raise NotImplementedError

    @abstractmethod
    def extract_data(self, project_name: str, config_path: str, data_layout: str, data_extract: str, target_path: str):
        raise NotImplementedError
