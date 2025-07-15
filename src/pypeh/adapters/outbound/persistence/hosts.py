"""
# TODO: add in support for other ManifestRepositories: ROCrate, ...
"""

from __future__ import annotations

import fsspec
import json
import logging
import os
import requests

from abc import abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Generic

from pypeh.core.interfaces.outbound.persistence import PersistenceInterface
from pypeh.adapters.outbound.persistence import formats
from pypeh.core.models.typing import T_Dataclass
from pypeh.core.models.settings import LocalFileSettings, S3Settings, FileSystemSettings

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from typing import Optional, Any, Dict, List, Generator, Type, Union
    from pydantic import BaseModel

    from pypeh.core.models.transform import FieldMapping


class HostAdapter(PersistenceInterface):
    pass


class WebServiceAdapter(HostAdapter):
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

        adapter = formats.IOAdapterFactory.create(format.lower())
        return adapter.load(data, **kwargs)

    def dump(self, entity: Any, destination: str) -> None:
        raise NotImplementedError


## fsspec-based file interactions: local or cloud


class FileIO(HostAdapter):
    def __init__(self, file_system: fsspec.AbstractFileSystem | None = None):
        self.file_system = file_system

    @classmethod
    def get_format(cls, path: Union[str, Path]) -> str:
        return os.path.splitext(str(path))[1].lower().lstrip(".")

    def load(self, source: Union[str, Path], format: Optional[str] = None, **kwargs) -> Any:
        """Load data from file using the appropriate adapter."""
        if format is None:
            format = self.get_format(source)
        adapter = formats.IOAdapterFactory.create(format.lower())
        open_func = self.file_system.open if self.file_system is not None else fsspec.open

        with open_func(source, adapter.read_mode) as f:
            try:
                return adapter.load(f, **kwargs)  # type: ignore ## fsspec does not provide type hints
            except Exception as e:
                logger.error(f"Error in FileIO: {e}")
                raise

    def dump(self, destination: str, entity: BaseModel, **kwargs) -> None:
        raise NotImplementedError


class DirectoryIO(HostAdapter):
    supported_formats = formats.IOAdapterFactory._adapters.keys()

    def __init__(self, protocol: str = "file", **storage_options):
        self.file_system = fsspec.filesystem(protocol, **storage_options)

    def walk(
        self, source: Union[str, Path], format: Optional[str] = None, maxdepth: int | None = None, **load_options
    ) -> Generator[Any, None, None]:
        """
        Yield data loaded from files in a directory and its subdirectories.
        This implementation assumes that all supported file formats (jsonn, yaml, csv, xslx, xls)
        should be loaded.
        """
        file_io = FileIO(file_system=self.file_system)
        for root, _, files in self.file_system.walk(source, maxdepth=maxdepth):
            for file in files:
                file_path = os.path.join(root, file)

                if format is not None:
                    yield file_io.load(file_path, format=format, **load_options)

                else:
                    inferred_format = FileIO.get_format(file_path)
                    if inferred_format in self.supported_formats:
                        yield file_io.load(file_path, format=inferred_format, **load_options)
                    else:
                        continue  # Skip unsupported formats

    def load(self, source: Union[str, Path], format: Optional[str] = None, maxdepth: int = 1, **load_options) -> Any:
        path = Path(source)

        if path.is_file():
            file_io = FileIO(file_system=self.file_system)
            return file_io.load(source, **load_options)
        elif path.is_dir():
            return list(self.walk(path, maxdepth=maxdepth, **load_options))
        else:
            logger.error(f"Source {source} could not be resolved as a path")
            raise ValueError

    def dump(self, destination: str, entities: List[BaseModel], **kwargs) -> None:
        pass


class S3StorageProvider(DirectoryIO):
    def __init__(self, settings: S3Settings, **storage_options):
        session_kwargs = {**storage_options, **settings.to_s3fs()}
        self.bucket = settings.bucket_name
        super().__init__(protocol="s3", storage_options=session_kwargs)

    ## TODO: check how should bucket names, ... be dealt with?


## initial implementation to database adapter: UNDER CONSTRUCTION


class DatabaseAdapter(HostAdapter, Generic[T_Dataclass]):
    def __init__(self, registry: ResourceRegistry, connection: Optional[Any] = None, **kwargs):
        self.config = kwargs
        self.conn = connection

    @abstractmethod
    def connect(self, **kwargs) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    @contextmanager
    def connection(self, **kwargs) -> Generator[None, None, None]:
        try:
            self.connect(**kwargs)
            yield
        finally:
            self.disconnect()

    @abstractmethod
    def query(self, resource_type: str, query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get(self, resource_type: str, resource_id: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def save(self, resource_type: str, data: Dict[str, Any]) -> str:
        pass

    @abstractmethod
    def update(self, resource_type: str, resource_id: str, data: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def delete(self, resource_type: str, resource_id: str) -> None:
        pass

    def load(self, source: str, target_class: Optional[Type[T_Dataclass]] = None, **kwargs) -> T_Dataclass:
        if "/" not in source:
            raise ValueError(f"Invalid source format: {source}. Expected 'resource_type/resource_id'")

        resource_type, resource_id = source.split("/", 1)
        data = self.get(resource_type, resource_id)

        if target_class is None:
            return data  # type: ignore

        # Use the model validation from your existing code
        if hasattr(target_class, "model_validate"):
            return target_class.model_validate(data)  # type: ignore
        else:
            # Fall back to your existing validation methods
            from pypeh.adapters.outbound.persistence.formats import validate_dataclass, validate_pydantic
            from dataclasses import is_dataclass

            if is_dataclass(target_class):
                return validate_dataclass(json.dumps(data), target_class)  # type: ignore
            else:
                return validate_pydantic(json.dumps(data), target_class)  # type: ignore

    def dump(self, destination: str, entity: Union[Dict[str, Any], BaseModel], **kwargs) -> None:
        raise NotImplementedError


class ResourceRegistry:
    def __init__(self):
        self.resources = {}

    def register_resource(
        self,
        resource_type: str,
        endpoint: Optional[str] = None,
        field_mapping: Optional[FieldMapping] = None,
        id_field: str = "id",
    ):
        self.resources[resource_type] = {
            "endpoint": endpoint or resource_type,
            "mapping": field_mapping or FieldMapping(),
            "id_field": id_field,
        }


class HostFactory:
    @classmethod
    def default(cls):
        return WebServiceAdapter()

    @classmethod
    def create(cls, settings: FileSystemSettings | None, **kwargs) -> HostAdapter:
        if settings is None:
            return cls.default()

        else:
            if isinstance(settings, S3Settings):
                return S3StorageProvider(settings, **kwargs)
            elif isinstance(settings, LocalFileSettings):
                return FileIO()
            else:
                me = f"No adapter registered for settings: {type(settings)}"
                logger.error(me)
                raise ValueError(me)
