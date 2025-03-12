from __future__ import annotations

import logging
import json

from typing import TYPE_CHECKING
from linkml_runtime.loaders import YAMLLoader
from linkml_runtime.dumpers import YAMLDumper
from linkml_runtime.utils.yamlutils import YAMLRoot

from pypeh.core.interfaces.persistence import PersistenceInterface

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pydantic import BaseModel
    from typing import Optional, Callable, Any, Type


class FileSystem(PersistenceInterface):
    """Adapter for loading from file."""

    @classmethod
    def load(cls, base_dir: str, identifier: str, transform_to_target: Optional[Callable]) -> Any:
        raise NotImplementedError

    @classmethod
    def dump(cls, identifier: str, entity: BaseModel) -> None:
        raise NotImplementedError


class JsonFileSystem(FileSystem):
    """Adapter for loading from json file."""

    def __init__(self, to_repo: Optional[Callable] = None, from_repo: Optional[Callable] = None):
        self.from_repo = from_repo
        self.to_repo = to_repo

    def load(self, base_dir: str, identifier: Optional[str] = None) -> Any:
        if identifier is not None:
            path = base_dir + "/" + identifier
        else:
            path = base_dir
        with open(path, "r") as f:
            data = json.load(f)
        if self.from_repo is not None:
            return self.from_repo(data)
        else:
            return data

    def dump(self, identifier: str, entity: BaseModel) -> None:
        with open(identifier, "w") as f:
            json.dump(entity.model_dump(), f, indent=2)


class YamlFileSystem(FileSystem):
    """Adapter for loading from Yaml file"""

    pass


class LinkMLRepository(PersistenceInterface):
    from_repo = YAMLLoader().load  # source, target_class, base_dir
    to_repo = YAMLDumper().dump  # element: Union[BaseModel, YAMLRoot], to_file: str, **_

    def load(self, base_dir: str, identifier: str, target_class: Type[YAMLRoot] = YAMLRoot):
        return self.from_repo(identifier, target_class, base_dir)

    def dump(self, identifier: str, entity: YAMLRoot):
        return self.to_repo(entity, identifier)


class LinkMLFileSystem(LinkMLRepository):
    pass
