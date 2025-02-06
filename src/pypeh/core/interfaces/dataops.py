"""
Each of these Interface subclasses provides a protocol on how
the corresponding Adapter subclass should be implemented.

Usage: TODO: add usage info

"""

import logging

from abc import abstractmethod

from pypeh.core.abc import Interface, DataTransferObject

logger = logging.getLogger(__name__)


class DataOpsInterface(Interface):
    @abstractmethod
    def process(self, data: DataTransferObject, config: DataTransferObject):
        pass


class DataValidationInterface(DataOpsInterface):
    pass


class DataEnrichmentInterface(DataOpsInterface):
    pass


class DataSummaryInterface(DataOpsInterface):
    pass
