"""
Each of these Interface subclasses provides a protocol on how
the corresponding Adapter subclass should be implemented.

Usage: TODO: add usage info

"""

from __future__ import annotations

import logging

from abc import abstractmethod
from typing import TYPE_CHECKING

from pypeh.core.models.dto import DataTransferObject
from pypeh.core.models.validation_dto import ValidationConfig

if TYPE_CHECKING:
    from typing import Sequence, Optional
    from pypeh.core.models.validation_errors import ValidationErrorReport

logger = logging.getLogger(__name__)


class OutDataOpsInterface:
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


class ValidationInterface(OutDataOpsInterface):
    @abstractmethod
    def validate(self, data: dict[str, Sequence], config: ValidationConfig) -> ValidationErrorReport:
        raise NotImplementedError

    def process(self, dto: DataTransferObject, command: Optional[str] = None):
        if command is not None:
            raise NotImplementedError
        else:
            return self.validate(dto.data, ValidationConfig.model_validate(dto.metadata))
