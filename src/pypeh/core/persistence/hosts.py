"""
# TODO: add in support for other ManifestRepositories: ROCrate, ...
"""

from __future__ import annotations

import logging
import requests
import os

from typing import TYPE_CHECKING

from pypeh.core.interfaces.persistence import PersistenceInterface
from pypeh.core.cache.containers import CacheContainer, MappingContainer
from pypeh.core.persistence import formats
from pypeh.core.models import constants

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from typing import Optional, Callable, Any, Type, Dict
    from pydantic import BaseModel

FORMAT_MAPPING = {
    "json": formats.JsonIO,
    "yaml": formats.YamlIO,
    "yml": formats.YamlIO,
    "csv": formats.CsvIO,
    "xlsx": formats.ExcelIO,
    "xls": formats.ExcelIO,
}


class RemoteRepository(PersistenceInterface):
    _adapters: Dict[str, Type[formats.IOAdapter]] = FORMAT_MAPPING

    def load(self, source: str, format: str = "json", **kwargs) -> Any:
        is_pid = kwargs.get("is_pid", False)

        try:
            response = requests.get(source, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            raise

        data = None
        if is_pid:
            data = response.json()
            response_code = data.get("responseCode")  # type: ignore
            # check additional error codes defined by the API
            if response_code == 2:
                raise ValueError(f"Unexpected error during handle resolution of {source}.")
            elif response_code == 100:
                raise ValueError(f"Handle not found: {source}.")
            elif response_code == 200:
                raise ValueError(f"Values Not Found. The handle {source} exists but has no values.")
            elif response_code == 1:
                return True
            else:
                return False

        if format is None:
            # TODO: add auto format determination method
            raise NotImplementedError
        if data is None:
            if format.lower() == "json":
                data = response.json()
            else:
                data = response.text

        adapter_cls = self._adapters.get(format.lower())
        if adapter_cls is None:
            raise ValueError(f"No adapter registered for file extension: .{format}")
        adapter = adapter_cls()
        return adapter.load(data, **kwargs)

    def dump(self, entity: Any, destination: str) -> None:
        raise NotImplementedError


class FileIO(PersistenceInterface):
    """Adapter for loading from file."""

    _adapters: Dict[str, Type[formats.IOAdapter]] = FORMAT_MAPPING

    @classmethod
    def get_format(cls, path: str) -> str:
        return os.path.splitext(path)[1].lower().lstrip(".")

    def load(self, source: str, format: Optional[str] = None, **kwargs) -> Any:
        """Load data from file using the appropriate adapter."""
        if format is None:
            format = self.get_format(source)
        adapter_cls = self._adapters.get(format.lower())
        if adapter_cls is None:
            raise ValueError(f"No adapter registered for file extension: .{format}")
        adapter = adapter_cls()
        return adapter.load(source, **kwargs)

    @classmethod
    def dump(cls, destination: str, entity: BaseModel, **kwargs) -> None:
        raise NotImplementedError
