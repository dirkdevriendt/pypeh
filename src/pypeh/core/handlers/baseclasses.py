from __future__ import annotations

import logging

from typing import TYPE_CHECKING
from pathlib import Path

from pypeh.core.interfaces import dataops, persistence
from pypeh.core.abc import Handler, Context
from pypeh.core.models.constants import AdapterEnum
from pypeh.core.persistence.formats import JsonIO
from pypeh.core.persistence.hosts import WebServiceAdapter
from pypeh.core.utils import resolve_identifiers

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class DataOpsHandler(Handler):
    def __init__(self, adapter: dataops.DataOpsInterface):
        super().__init__()
        self._adapter = adapter

    def handle(self, context: Context) -> bool:
        raise NotImplementedError

    @property
    def adapter(self):
        return self._adapter

    def map(self, context: Context) -> None:
        pass

    @classmethod
    def create(cls, engine: AdapterEnum) -> "DataOpsHandler":
        if engine == AdapterEnum.DATAFRAME:
            try:
                # TODO: currently always DataValidationAdapter, adapt logic
                from dataframe_adapter.dataops import DataValidationAdapter
            except ImportError:
                logging.error(f"The {engine} requires the 'dataframe_adapter' module. Please install it.")
                raise ImportError(f"The {engine} requires the 'dataframe_adapter' module. Please install it.")
            return DataOpsHandler(adapter=DataValidationAdapter())

        raise ValueError(f"Unsupported command type or engine: {engine}")


class PersistenceHandler(Handler):
    def __init__(self, adapter: persistence.PersistenceInterface, fn: str):
        self._adapter = adapter
        self._fn = fn  # load or dump

    @property
    def adapter(self):
        return self._adapter

    def handle(self, context: Context) -> bool:
        raise NotImplementedError

    def map(self, context: Context) -> None:
        pass

    @classmethod
    def create(cls, engine: AdapterEnum, fn: str) -> "PersistenceHandler":
        if engine == AdapterEnum.DATAFRAME:
            try:
                from dataframe_adapter.persistence import CsvIOImpl
            except ImportError:
                logging.error(f"The {engine} requires the 'dataframe_adapter' module. Please install it.")
                raise ImportError(f"The {engine} requires the 'dataframe_adapter' module. Please install it.")
            adapter = CsvIOImpl()
        else:
            raise NotImplementedError
        return cls(adapter, fn)


class ExtractionHandler(Handler):
    def __init__(self):
        super().__init__()

    def handle(self, context: Context) -> bool:
        raise NotImplementedError

    def map(self, context: Context) -> None:
        pass

    @classmethod
    def create(cls, engine: AdapterEnum):
        raise NotImplementedError


class ManifestHandler(PersistenceHandler):
    @classmethod
    def create(cls, root: str, fn: str) -> "ManifestHandler":
        resolved_root = resolve_identifiers.resource_path(root)
        if isinstance(resolved_root, Path):
            adapter = JsonIO()
        else:
            adapter = WebServiceAdapter()

        return cls(adapter, fn)

    def handle(self, context: Context) -> bool:
        if self._fn == "load":
            command_meta = context.metadata.command
            if command_meta is not None:
                # TODO: fix typing issue
                base_dir = command_meta.root  # type: ignore
            else:
                return False
            # TODO: fix typing issue, identifier can't be set to None
            data = self._adapter.load(base_dir=base_dir, identifier=None)  # type: ignore
            # push data to Context

        elif self._fn == "dump":
            pass
        else:
            raise NotImplementedError

        return True

    def map(self, context: Context) -> None:
        pass


class DataSetHandler(DataOpsHandler):
    def handle(self, context: Context) -> bool:
        return True
