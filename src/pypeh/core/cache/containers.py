"""
This module provides functionality for creating and manipulating an in-memory tree-like
representation of data.

Usage:
    Use this module to define in-memory data structures containing data adhering to the
    PEH-model.

"""

from __future__ import annotations

import logging

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Dict, Type, TYPE_CHECKING

from pypeh.core.models.peh import NamedThing

if TYPE_CHECKING:
    from typing import Optional

logger = logging.getLogger(__name__)


class Proxy(NamedThing):
    pass


class CacheContainer(ABC):
    """Abstract base class for storage backends"""

    @abstractmethod
    def add(self, entity: NamedThing) -> None:
        """Store an entity"""
        pass

    @abstractmethod
    def get(self, entity_type: str, entity_id: str) -> NamedThing:
        """Retrieve an entity"""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all stored data"""
        pass

    @abstractmethod
    def exists(self, entity_type: str, entity_id: str) -> bool:
        """Clear all stored data"""
        pass


class MappingContainer(CacheContainer):
    def __init__(self):
        self._storage: Dict[str, Dict[str, NamedThing]] = defaultdict(dict)

    def add(self, entity: NamedThing) -> None:
        return self._add_object(entity.__class__.__name__, entity.id, entity)

    def _add_object(self, entity_class_name: str, entity_id: str, entity: NamedThing) -> None:
        self._storage[entity_class_name][entity_id] = entity

    def exists(self, entity_type: str, entity_id: str) -> bool:
        return entity_type in self._storage.keys() and entity_id in self._storage[entity_type].keys()

    def get(self, entity_type: str, entity_id: str) -> Optional[NamedThing]:
        if self.exists(entity_type, entity_id):
            return self._storage[entity_type][entity_id]
        else:
            logging.debug(f"Storage error: Object of class '{entity_type}' with id '{entity_id}' not found.")
            return None

    def clear(self) -> None:
        self._storage.clear()


class CacheContainerFactory:
    _default_container: Type[CacheContainer] = MappingContainer

    @classmethod
    def set_default_container(cls, container_class: Type[CacheContainer]):
        cls._default_container = container_class

    @classmethod
    def new(cls) -> CacheContainer:
        return cls._default_container()
