"""
Each of these Interface subclasses provides a protocol on how
the corresponding Adapter subclass should be implemented.

Usage: TODO: add usage info

"""

from __future__ import annotations

import logging

from abc import abstractmethod
from peh_model.peh import DataLayout, EntityList
from typing import TYPE_CHECKING, TypeVar, Generic, cast, List

from pypeh.core.models.settings import FileSystemSettings
from pypeh.core.models.validation_dto import ValidationConfig
from pypeh.adapters.outbound.persistence.hosts import HostFactory

if TYPE_CHECKING:
    from typing import Sequence
    from pypeh.core.models.validation_errors import ValidationErrorReport

logger = logging.getLogger(__name__)

T_DataType = TypeVar("T_DataType")


class OutDataOpsInterface:
    """
    Example of DataOps methods
    def validate(self, data: Mapping, config: Mapping):
        pass

    def summarize(self, dat: Mapping, config: Mapping):
        pass
    """

    pass


class ValidationInterface(OutDataOpsInterface):
    @abstractmethod
    def validate(self, data: dict[str, Sequence], config: ValidationConfig) -> ValidationErrorReport:
        raise NotImplementedError


class DataImportInterface(OutDataOpsInterface, Generic[T_DataType]):
    @abstractmethod
    def import_data(self, source: str, config: FileSystemSettings) -> T_DataType | List[T_DataType]:
        raise NotImplementedError

    def import_data_layout(
        self,
        source: str,
        config: FileSystemSettings,
        **kwargs,
    ) -> DataLayout | List[DataLayout]:
        provider = HostFactory.create(config)
        layout = provider.load(source)
        if isinstance(layout, EntityList):
            layout = layout.layouts
        if isinstance(layout, list):
            if not all(isinstance(item, DataLayout) for item in layout):
                me = "Imported layouts should all be DataLayout instances"
                logger.error(me)
                raise TypeError(me)
            return cast(List[DataLayout], layout)

        elif isinstance(layout, DataLayout):
            return layout

        else:
            me = f"Imported layout should be a DataLayout instance not {type(layout)}"
            logger.error(me)
            raise TypeError(me)
