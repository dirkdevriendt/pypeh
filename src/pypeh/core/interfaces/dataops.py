"""
Each of these Interface subclasses provides a protocol on how
the corresponding Adapter subclass should be implemented.

Usage: TODO: add usage info

"""

import logging

from typing import TYPE_CHECKING, Mapping

from pypeh.core.abc import Interface, DataTransferObject

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class DataOpsInterface(Interface):
    """
    Example of DataOps methods
    def validate(self, data: Mapping, config: Mapping):
        pass

    def summarize(self, dat: Mapping, config: Mapping):
        pass
    """

    def process(self, dto: DataTransferObject, command: str):
        method = getattr(self, command, None)
        if method and callable(method):
            return method(dto.data, dto.metadata)
        else:
            raise ValueError(f"Unknown command for DataOpsInterface: {command}")
