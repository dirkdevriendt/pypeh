"""
Each of these Interface subclasses provides a protocol on how
the corresponding Adapter subclass should be implemented.

Usage: TODO: add usage info

"""

import logging

from abc import abstractmethod
from typing import TYPE_CHECKING, Mapping

from pypeh.core.abc import Interface, DataTransferObject

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class DataOpsInterface(Interface):
    @abstractmethod
    def process(self, dto: DataTransferObject):
        pass


class DataValidationInterface(DataOpsInterface):
    def validate(self, data: Mapping, config: Mapping):
        pass

    def process(self, dto:DataTransferObject):
        # apply model to metadata
        return self.validate(dto.data, dto.metadata)


class DataEnrichmentInterface(DataOpsInterface):
    pass


class DataSummaryInterface(DataOpsInterface):
    pass
