"""
Contains functionality to query the content of a CacheContainer
"""

from __future__ import annotations

import logging

from dataclasses import dataclass, field
from pathlib import Path
from typing import Union, Optional, Protocol, TYPE_CHECKING

from pypeh.core.models.peh import NamedThing, YAMLRoot
from pypeh.core.cache.containers import CacheContainer, CacheContainerFactory

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass


def get_entity_type(entity: YAMLRoot) -> str:
    return entity.__class__.__name__


class EntityLoader(Protocol):
    lazy_loading = True

    def load_entity(self, entity_id: str): ...


@dataclass
class DataView:
    # TODO: add roll back ability

    _storage: CacheContainer = field(default_factory=CacheContainerFactory.new)
    _loader: Optional[EntityLoader] = None

    def set_loader(self, loader: EntityLoader):
        self._loader = loader

    def request_entity(self, entity_type: str, entity_id: str) -> Optional[NamedThing]:
        if self._loader is not None:
            entity = self._storage.get(entity_type, entity_id)
            if entity is None:
                entity = self._loader.load_entity(entity_id)
                if entity is None:
                    logger.error(f"Could not resolve entity with id {entity_id}")
                    raise ValueError(f"Could not resolve eneity with id {entity_id}")
        else:
            logger.error("_loader for DataView was not set correctly.")
            raise NotImplementedError("_loader for DataView was not set correctly. Report this as a bug.")
        return entity

    def get_entity(self, entity_type: Union[type, str], entity_id: str) -> Optional[NamedThing]:
        """Retrieve an entity from storage"""
        if isinstance(entity_type, type):
            entity_type = entity_type.__name__
        return self._storage.get(entity_type, entity_id)


class ReferencedEntityLoader(EntityLoader):
    def __init__(self, viewer: DataView, root: Optional[Union[str, Path]] = None):
        self.root = root
        self.viewer = viewer
        self.viewer.set_loader(self)

    def resolve_identifier(self, entity_id: str) -> Optional[NamedThing]:
        # TODO: requires access to namespace context
        pass

    def load_entity(self, entity_id: str) -> Optional[NamedThing]:
        """Save an entity to storage"""
        entity = self.resolve_identifier(entity_id)
        if entity is not None:
            entity_type = get_entity_type(entity)
            if not self.viewer._storage.exists(entity_type, entity.id):
                self.viewer._storage.add(entity)

        return entity


def get_dataview(storage_container="default") -> DataView:
    data_view = DataView()
    _ = ReferencedEntityLoader(data_view)

    return data_view
