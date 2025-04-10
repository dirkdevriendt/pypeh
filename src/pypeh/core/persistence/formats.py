from __future__ import annotations

import logging
import json

from dataclasses import is_dataclass
from io import IOBase
from linkml_runtime.loaders import YAMLLoader, JSONLoader
from linkml_runtime.dumpers import YAMLDumper, JSONDumper
from pydantic import TypeAdapter, BaseModel
from typing import TYPE_CHECKING, Mapping, TypeVar, Union, List, Dict, TextIO, Sequence, Any

from pypeh.core.interfaces.persistence import PersistenceInterface
from pypeh.core.models.peh import EntityList, NamedThing, YAMLRoot, NamedThingId


if TYPE_CHECKING:
    from typing import Optional, Callable, Type

logger = logging.getLogger(__name__)


T_dataclass = TypeVar("T_dataclass", bound=Union[EntityList, BaseModel])
T_root = Union[YAMLRoot, NamedThingId]
T_RootStream = Union[T_root, Mapping[Any, T_root], Sequence[T_root]]
IOLike = Union[str, List, List[Dict], TextIO]


def load_entities_from_tree(root: T_RootStream, create_proxy: Optional[Callable] = None):
    if isinstance(root, NamedThing):
        yield root
    if isinstance(root, YAMLRoot):
        # if isinstance(root, NamedThing) or isinstance(root, EntityList): # TODO decide which one we need
        for property_name in list(root._keys()):
            property = getattr(root, property_name)
            if property is not None:
                if isinstance(property, list):
                    yield from load_entities_from_tree(property, create_proxy=create_proxy)
                elif isinstance(property, dict):
                    yield from load_entities_from_tree(list(property.values()), create_proxy=create_proxy)
                else:
                    yield from load_entities_from_tree(property, create_proxy=create_proxy)
    if isinstance(root, NamedThingId) and create_proxy:
        proxy = create_proxy(root)
        yield proxy
    if isinstance(root, Mapping):
        root = list(root.values())
    if isinstance(root, list):
        for entity in root:
            yield from load_entities_from_tree(entity, create_proxy=create_proxy)


def validate_dataclass(
    json_data: IOLike,
    target_class: Type[T_dataclass],
) -> T_dataclass:
    """
    Validate JSON data against a dataclass using Pydantic's TypeAdapter.
    """
    # Create TypeAdapter for the target dataclass
    adapter = TypeAdapter(target_class)
    # Validate and return instance
    if isinstance(json_data, str):
        return adapter.validate_json(json_data)
    elif isinstance(json_data, IOBase):
        json_data = json.load(json_data)
    return adapter.validate_python(json_data)


def validate_pydantic(
    json_data: IOLike,
    target_class: BaseModel,
) -> BaseModel:
    """
    Validate JSON data against a dataclass using Pydantic's TypeAdapter.
    """
    if isinstance(json_data, str):
        return target_class.model_validate_json(json_data)
    elif isinstance(json_data, IOBase):
        json_data = json.load(json_data)

    return target_class.model_validate(json_data)


class IOAdapter(PersistenceInterface):
    """Adapter for loading from file."""

    def load(self, source: str) -> Any:
        raise NotImplementedError

    def dump(self, destination: str, entity: BaseModel) -> None:
        raise NotImplementedError


class JsonIO(IOAdapter):
    """
    Adapter for loading from json file/stream.
    Assuming jsonfiles can be directly loaded by linkml
    """

    def load(self, source: IOLike, target_class: Type[T_dataclass] = EntityList, **kwargs) -> Any:
        """
        Load JSON data from a file-like object (e.g., a context manager).
        # TODO: test with: fake_file = StringIO('{"key": "value"}')

        """
        if issubclass(target_class, EntityList):
            return JSONLoader().load(source, target_class)
        elif is_dataclass(target_class):
            return validate_dataclass(source, target_class)
        elif issubclass(target_class, BaseModel):
            return validate_pydantic(source, target_class)
        else:
            raise NotImplementedError

    def dump(self, destination: str, entity: BaseModel, **kwargs) -> None:
        # LinkML-based version JSONDumper().dump
        with open(destination, "w") as f:
            json.dump(entity.model_dump(), f, indent=2)


class YamlIO(IOAdapter):
    """
    Adapter for loading from Yaml file/stream
    Assuming yaml files can be directly loaded by linkml
    """

    def load(
        self, source: IOLike, target_class: Type[T_dataclass] = EntityList, **kwargs
    ) -> Union[BaseModel, YAMLRoot]:
        """
        Load YAML data from a file-like object (e.g., a context manager).
        Args:
        """
        if issubclass(target_class, EntityList):
            return YAMLLoader().load(source, target_class)
        else:
            raise NotImplementedError

    def dump(self, destination: str, entity: EntityList, fn: Callable = YAMLDumper().dump, **kwargs):
        raise NotImplementedError


class CsvIO(IOAdapter):
    """
    Public interace for the Csv Adapter
    Actual implementation is in dataframe_adapter
    """

    def load(self, source: str, **kwargs):
        try:
            from dataframe_adapter.persistence import CsvIOImpl
        except ImportError:
            message = "The CsvIO class requires the 'dataframe_adapter' module. Please install it."
            logging.error(message)
            raise ImportError(message)
        return CsvIOImpl().load(source, **kwargs)

    def dump(self, source: str, **kwargs):
        pass


class ExcelIO(IOAdapter):
    """
    Public interface for Excel repository
    Actual implementation is in dataframe_adapter
    """

    # source = StringIO(response.text)
    # df = pd.read_csv(source)

    def load(self, source: str, **kwargs):
        try:
            from dataframe_adapter.persistence import ExcelIOImpl
        except ImportError:
            message = "The ExcelIO class requires the 'dataframe_adapter' module. Please install it."
            logging.error(message)
            raise ImportError(message)
        return ExcelIOImpl().load(source, **kwargs)

    def dump(self, source: str, **kwargs):
        pass


class IOAdapterFactory:
    _adapters = {
        "json": JsonIO,
        "yaml": YamlIO,
        "yml": YamlIO,
        "csv": CsvIO,
        "xlsx": ExcelIO,
        "xls": ExcelIO,
    }

    @classmethod
    def register_adapter(cls, format: str, adapter_class: Type[IOAdapter]) -> None:
        cls._adapters[format.lower()] = adapter_class

    @classmethod
    def create(cls, format: str, **kwargs) -> IOAdapter:
        adapter_class = cls._adapters.get(format.lower())
        if not adapter_class:
            raise ValueError(f"No adapter registered for dataformat: {format}")
        return adapter_class(**kwargs)
