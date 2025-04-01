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
from typing import Dict, Type, TYPE_CHECKING

from pypeh.core.models.peh import NamedThing
from pypeh.core.cache.utils import T_NamedThingLike, get_entity_type
from pypeh.core.models.proxy import TypedLazyProxy

if TYPE_CHECKING:
    from typing import Optional, Generator

logger = logging.getLogger(__name__)


class CacheContainer(ABC):
    """Abstract base class for storage backends"""

    def __init__(self):
        self._storage = None

    @abstractmethod
    def add(self, entity: T_NamedThingLike) -> None:
        """Store an entity"""
        pass

    @abstractmethod
    def get(self, entity_id: str, entity_type: str) -> T_NamedThingLike:
        """Retrieve an entity"""
        pass

    @abstractmethod
    def get_all(self) -> Generator[T_NamedThingLike, None, None]:
        """Retrieve all entities"""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all stored data"""
        pass

    @abstractmethod
    def exists(self, entity_id: str, entity_type: str) -> bool:
        """Clear all stored data"""
        pass

    @abstractmethod
    def pop(self, entity_id: str, entity_type: str) -> T_NamedThingLike:
        """Return entry and delete from storage"""
        pass


class MappingContainer(CacheContainer):
    def __init__(self):
        self._storage: Dict[str, T_NamedThingLike] = dict()

    def _add_object(self, entity: T_NamedThingLike, entity_id: str, entity_type: str) -> None:
        self._storage[entity_id] = entity

    def exists(self, entity_id: str, entity_type: str) -> bool:
        return entity_id in self._storage.keys()

    def _get(self, entity_id: str, entity_type: str) -> Optional[T_NamedThingLike]:
        if self.exists(entity_id, entity_type):
            return self._storage[entity_id]

    def add(self, entity: T_NamedThingLike) -> None:
        class_name = get_entity_type(entity)
        container_entity = self._get(entity.id, class_name)
        if container_entity is not None:
            if isinstance(container_entity, NamedThing):
                return
            if isinstance(entity, TypedLazyProxy):
                return
        return self._add_object(entity, entity.id, class_name)

    def get(self, entity_id: str, entity_type: str) -> Optional[T_NamedThingLike]:
        ret = self._get(entity_id, entity_type)
        if ret is None:
            message = f"Storage error: Object of class '{entity_type}' with id '{entity_id}' not found."
            logging.debug(message)
        return ret

    def clear(self) -> None:
        self._storage.clear()

    def pop(self, entity_id: str, entity_type: str) -> Optional[T_NamedThingLike]:
        return self._storage.pop(entity_id, None)

    def get_all(self) -> Generator[T_NamedThingLike, None, None]:
        for entity_id in self._storage.keys():
            yield self._storage[entity_id]

    def __repr__(self):
        return self._storage.__repr__()


class CacheContainerFactory:
    _default_container: Type[CacheContainer] = MappingContainer

    @classmethod
    def set_default_container(cls, container_class: Type[CacheContainer]):
        cls._default_container = container_class

    @classmethod
    def new(cls) -> CacheContainer:
        return cls._default_container()
