"""
Contains functionality to query the content of a CacheContainer
"""

from __future__ import annotations

from abc import abstractmethod
import logging

from pathlib import Path
from typing import Union, Optional, Protocol, TYPE_CHECKING, TypeVar, Generic

from pypeh.core.models.peh import NamedThing
from pypeh.core.cache.containers import CacheContainer, CacheContainerFactory
from pypeh.core.models import proxy
from pypeh.core.persistence.hosts import FileIO, RemoteRepository
from pypeh.core.persistence.formats import YamlIO, load_entities_from_tree

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pypeh.core.interfaces.persistence import PersistenceInterface
    from typing import Dict, Generator

T_BaseView = TypeVar("T_BaseView", bound="BaseView")
T_DataView = TypeVar("T_DataView", bound="DataView")
T_ImportMapView = TypeVar("T_ImportMapView", bound="ImportMapView")


def get_entity_type(entity: NamedThing) -> str:
    # TODO: extend get_entity_type for proxy objects!!!
    if isinstance(entity, proxy.TypedLazyProxy):
        return entity.expected_type.__name__
    return entity.__class__.__name__


class EntityLoader(Protocol):
    lazy_loading = True

    def load_entity(self, entity_id: str, entity_type: Optional[str] = None): ...

    def load_greedily(self, source: str): ...


class BaseView:
    def __init__(
        self,
        storage: CacheContainer = CacheContainerFactory.new(),
        loader: Optional[EntityLoader] = None,
    ):
        self._storage = storage
        self._loader = loader

    def set_loader(self, loader: EntityLoader):
        self._loader = loader

    def create_proxy(self, entity: str) -> proxy.TypedLazyProxy:
        name = entity.__class__.__name__
        expected_type = proxy.CLASS_REFERENCES.get(name, None)
        if expected_type is None:
            logger.error(f"No mapping for object of type {name}")
            raise ValueError

        def _proxy_loader(entity, expected_type):
            return self.request_entity(entity, expected_type)

        return proxy.TypedLazyProxy(entity, expected_type, _proxy_loader)

    def request_entity(self, entity_id: str, entity_type: str) -> Optional[NamedThing]:
        entity = self._storage.get(entity_id, entity_type)
        if entity is None:
            if self._loader is not None:
                entity = self._loader.load_entity(entity_id, entity_type)
            else:
                raise NotImplementedError

        return entity

    def view_entity(self, entity_id: str, entity_type: Union[type, str]) -> Optional[NamedThing]:
        """should return view only object"""
        if isinstance(entity_type, type):
            entity_type = entity_type.__name__
        return self._storage.get(entity_id, entity_type)

    def view_all(self) -> Generator[NamedThing, None, None]:
        yield from self._storage.get_all()


class ImportMapView(BaseView):
    def __init__(
        self,
        storage: CacheContainer = CacheContainerFactory.new(),
        loader: Optional[ImportMapEntityLoader] = None,
        importmap: Dict[str, str] = {},
    ):
        super().__init__(storage, loader)
        self._import_map = importmap
        self._namespace_loaded = {}

    def _extract_namespace(self, entity_id: str) -> Optional[str]:
        """
        Assuming entity IDs are in the format namespace:identifier
        """
        if ":" in entity_id:
            return entity_id.split(":", 1)[0]
        return None

    def _is_loaded(self, namespace: str) -> bool:
        return self._namespace_loaded.get(namespace, False)

    def request_entity(self, entity_id: str, entity_type: str) -> Optional[NamedThing]:
        try:
            if self._loader is None:
                raise NotImplementedError
            namespace = self._extract_namespace(entity_id)
            if namespace in self._import_map:
                if not self._is_loaded(namespace):
                    _ = self._loader.load_greedily(self._import_map[namespace])
                    self._namespace_loaded[namespace] = True

                return super().request_entity(entity_id, entity_type)
        except NotImplementedError:
            e = "Loader for ImportMapView was not set correctly. Report this as a bug."
            logger.error(e)
            raise NotImplementedError(e)


class DataView(BaseView):
    # TODO: add roll back ability
    def __init__(
        self,
        storage: CacheContainer = CacheContainerFactory.new(),
        loader: Optional[DataViewEntityLoader] = None,
        importmap_viewer: Optional[ImportMapView] = None,
    ):
        super().__init__(storage, loader)
        self._importmap_viewer = importmap_viewer

    def request_entity(self, entity_id: str, entity_type: str) -> Optional[NamedThing]:
        if self._importmap_viewer is not None:
            entity = self._importmap_viewer.request_entity(entity_id, entity_type)
            if entity is not None:
                self._storage.add(entity)
                return entity
        try:
            entity = super().request_entity(entity_id, entity_type)
        except NotImplementedError:
            e = "Loader for DataView was not set correctly. Report this as a bug."
            logger.error(e)
            raise NotImplementedError(e)

        if entity is None:
            logger.error(f"Could not resolve entity with id {entity_id}")
            raise ValueError(f"Could not resolve eneity with id {entity_id}")

        return entity


class EntityLoaderABC(EntityLoader, Generic[T_BaseView]):
    def __init__(
        self,
        persistence_interface: PersistenceInterface,
        viewer: BaseView,
        root: Optional[Union[str, Path]] = None,
    ):
        self.root = root
        self.viewer = viewer
        # LOADER FOR VIEWER HAS TO BE SET EXPLICITLY
        self.viewer.set_loader(self)
        self.persistence_interface = persistence_interface

    def load_greedily(self, source: str):
        raise NotImplementedError

    @abstractmethod
    def load_entity(self, entity_id: str, entity_type: Optional[str] = None) -> Optional[NamedThing]:
        raise NotImplementedError


class DataViewEntityLoader(EntityLoaderABC[T_DataView]):
    def resolve_identifier(self, entity_id: str) -> Optional[NamedThing]:
        # loc = identifier_to_locator(entity_id, LocationEnum.PID)
        # TODO: requires access to namespace context
        # distinguish between local and remote entries
        # dealing with lazy_loading -> use proxyObject?

        # could represent single item or list of items
        pass

    def load_greedily(self, source: str):
        return super().load_greedily(source)

    def load_entity(self, entity_id: str, entity_type: Optional[str] = None) -> Optional[NamedThing]:
        """Save an entity to storage"""
        entity = self.resolve_identifier(entity_id)
        if entity is not None:
            entity_type = get_entity_type(entity)
            if not self.viewer._storage.exists(entity.id, entity_type):
                self.viewer._storage.add(entity)

        return entity


class ImportMapEntityLoader(EntityLoaderABC[T_ImportMapView]):
    def load_greedily(self, source: str) -> bool:
        root = self.persistence_interface.load(source)
        for entity in load_entities_from_tree(root, self.viewer.create_proxy):
            if entity is not None:
                entity_type = get_entity_type(entity)
                if not self.viewer._storage.exists(entity.id, entity_type):
                    self.viewer._storage.add(entity)
        return True

    def load_entity(self, entity_id: str, entity_type: Optional[str] = None) -> Optional[NamedThing]:
        if entity_type is None:
            entity_type = entity_id.__class__.__name__
        if self.viewer._storage.exists(entity_type, entity_id):
            return self.viewer._storage.pop(entity_id, entity_type)


def get_dataview(storage_container="default", importmap: Optional[Dict] = None) -> DataView:
    # TODO: extend: add storage_container logic: currently always dict
    data_view = DataView()
    # init EntityLoader linked to DataView
    _ = DataViewEntityLoader(RemoteRepository(), data_view)
    if importmap is not None:
        importmap_view = ImportMapView(importmap=importmap)
        _ = ImportMapEntityLoader(FileIO(), importmap_view)

    return data_view
