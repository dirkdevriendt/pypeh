"""
This part of the code should contain the reusable part
of the PyHBM library, aka, the part that would otherwise
be copied across projects. Ideally, it also contains a
specification on what the custom part should look like.
"""

import logging

from pypeh.core.abc import DataTransferObject
from pypeh.core.interfaces.dataops import (
    DataOpsInterface,
    DataValidationInterface,
    DataEnrichmentInterface,
    DataSummaryInterface,
)
from pypeh.core.interfaces.persistence import PersistenceInterface

logger = logging.getLogger(__name__)


class DataOpsAdapter(DataOpsInterface):
    def process(self, data: DataTransferObject, config: DataTransferObject):
        pass


class DataValidationAdapter(DataValidationInterface):
    """
    Pandas specific implementation of the
    DataValidationInterface abstract base class.
    """

    def process(self, data: DataTransferObject, config: DataTransferObject):
        pass


class DataEnrichmentAdapter(DataEnrichmentInterface):
    """
    Pandas specific implementation of the
    DataEnrichmentInterface abstract base class.
    """

    def process(self, data: DataTransferObject, config: DataTransferObject):
        pass


class DataSummaryAdapter(DataSummaryInterface):
    """
    Pandas specific implementation of the
    DataSummaryInterface abstract base class.
    """

    def process(self, data: DataTransferObject, config: DataTransferObject):
        pass


class DataPersistenceAdapter(PersistenceInterface):
    """
    Pandas/dataframe specific implementation of the
    PesistenceInterface abstract base class.
    """

    pass
