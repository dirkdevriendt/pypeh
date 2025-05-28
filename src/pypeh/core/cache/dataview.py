"""
Contains functionality to query the content of a CacheContainer
"""

from __future__ import annotations

import logging

from typing import Union, Optional, Protocol, TYPE_CHECKING, Sequence
from peh_model.peh import NamedThing

from pypeh.core.models.typing import T_RootStream
from pypeh.core.cache.containers import CacheContainer, CacheContainerFactory
from pypeh.core.models import proxy
from pypeh.adapters.outbound.persistence.hosts import FileIO, WebServiceAdapter
from pypeh.adapters.outbound.persistence.formats import load_entities_from_tree

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pypeh.core.interfaces.outbound.persistence import PersistenceInterface
    from pypeh.core.models.typing import T_NamedThingLike
    from typing import Dict, Generator, List, Callable


class EntityLoader(Protocol):
    lazy_loading = True

    def load(self, source: str) -> T_RootStream: ...

    def load_entity(self, entity_id: str, entity_type: Optional[str] = None): ...


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

        def _proxy_loader():
            return self.request_entity(entity, expected_type)

        return proxy.TypedLazyProxy(entity, expected_type, _proxy_loader)

    def request_entity(self, entity_id: str, entity_type: str) -> Optional[T_NamedThingLike]:
        entity = self._storage.get(entity_id, entity_type)
        if entity is None or isinstance(entity, proxy.TypedLazyProxy):
            if self._loader is not None:
                ret = self._loader.load_entity(entity_id, entity_type)
                if ret is not None:
                    entity = ret
            else:
                raise NotImplementedError

        return entity

    def view_entity(self, entity_id: str, entity_type: Union[type, str]) -> Optional[T_NamedThingLike]:
        """should return view only object"""
        if isinstance(entity_type, type):
            entity_type = entity_type.__name__
        ret = self._storage.get(entity_id, entity_type)
        if ret is None:
            raise KeyError
        return ret

    def view_all(self) -> Generator[T_NamedThingLike, None, None]:
        yield from self._storage.get_all()

    def _add_root_stream(self, root_stream: T_RootStream, create_proxy_loader: Optional[Callable]) -> bool:
        for entity in load_entities_from_tree(root_stream, create_proxy_loader):
            if entity is not None:
                self._storage.add(entity)
        return True


class CacheView(BaseView):
    def __init__(
        self,
        storage: CacheContainer = CacheContainerFactory.new(),
        importmap: Dict[str, Union[str, List[str]]] = {},
        loader: Optional[ImportMapEntityLoader] = None,
        create_proxy_fn: Optional[Callable[[str], proxy.TypedLazyProxy]] = None,
    ):
        self._import_map = importmap
        super().__init__(storage, loader)
        self._namespace_loaded = set()
        self._create_proxy_fn = create_proxy_fn

    def _extract_namespace(self, entity_id: str) -> Optional[str]:
        """
        Assuming entity IDs are in the format namespace:identifier
        """
        if ":" in entity_id:
            return entity_id.split(":", 1)[0]
        return None

    def _is_namespace_loaded(self, namespace: str) -> bool:
        return namespace in self._namespace_loaded

    def _load(self, source: Union[List[str], str]) -> bool:
        if self._loader is None:
            e = "Loader for CacheView was not set correctly. Report this as a bug."
            logger.error(e)
            raise NotImplementedError(e)

        if isinstance(source, Sequence):
            for s in source:
                root_stream = self._loader.load(s)
                _ = self._add_root_stream(root_stream, self._create_proxy_fn)
        else:
            root_stream = self._loader.load(source)
            _ = self._add_root_stream(root_stream, self._create_proxy_fn)

        return True

    def request_entity(self, entity_id: str, entity_type: str) -> Optional[T_NamedThingLike]:
        namespace = self._extract_namespace(entity_id)
        if namespace in self._import_map:
            if not self._is_namespace_loaded(namespace):
                source = self._import_map[namespace]
                _ = self._load(source)
                self._namespace_loaded.add(namespace)
            return self._storage.pop(entity_id, entity_type)

        return None


class DataView(BaseView):
    # TODO: add roll back ability
    def __init__(
        self,
        storage: CacheContainer = CacheContainerFactory.new(),
        loader: Optional[DataViewEntityLoader] = None,
        cache_viewer: Optional[CacheView] = None,
    ):
        super().__init__(storage, loader)
        self._cache_viewer = None
        if cache_viewer is not None:
            self.set_cache_viewer(cache_viewer)

    def set_cache_viewer(self, cache_viewer: CacheView) -> bool:
        self._cache_viewer = cache_viewer
        cache_viewer._create_proxy_fn = self.create_proxy
        return True

    def _add_root_stream_to_cache(self, root_stream: T_RootStream) -> bool:
        if self._cache_viewer is None:
            raise ValueError("CacheView was not correctly initialized.")
        return self._cache_viewer._add_root_stream(root_stream, self.create_proxy)

    def add(self, root_stream: T_RootStream) -> bool:
        return self._add_root_stream(root_stream, self.create_proxy)

    def request_entity(self, entity_id: str, entity_type: str) -> Optional[T_NamedThingLike]:
        if self._cache_viewer is not None:
            entity = self._cache_viewer.request_entity(entity_id, entity_type)
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
            raise ValueError(f"Could not resolve entity with id {entity_id}")

        return entity


class EntityLoaderABC(EntityLoader):
    def __init__(
        self,
        persistence_interface: PersistenceInterface,
    ):
        self.persistence_interface = persistence_interface

    def load(self, source: str) -> T_RootStream:
        return self.persistence_interface.load(source)

    def load_entity(self, entity_id: str, entity_type: Optional[str] = None) -> Optional[NamedThing]:
        raise NotImplementedError


class DataViewEntityLoader(EntityLoaderABC):
    def resolve_identifier(self, entity_id: str) -> str:
        # loc = identifier_to_locator(entity_id, LocationEnum.PID)
        # TODO: requires access to namespace context
        # distinguish between local and remote entries
        # dealing with lazy_loading -> use proxyObject?

        # could represent single item or list of items
        return ""

    def load(self, source: str) -> T_RootStream:
        source_url = self.resolve_identifier(source)
        return super().load(source_url)


class ImportMapEntityLoader(EntityLoaderABC):
    def load(self, source: str) -> T_RootStream:
        return super().load(source)


def get_importmapview(storage_container="default", importmap: Dict[str, Union[str, List[str]]] = {}) -> CacheView:
    loader = ImportMapEntityLoader(FileIO())
    importmap_view = CacheView(importmap=importmap, loader=loader)

    return importmap_view


def get_dataview(storage_container="default", importmap: Optional[Dict] = None) -> DataView:
    # TODO: extend: add storage_container logic: currently always dict
    importmap_view = None
    if importmap is not None:
        importmap_view = get_importmapview(importmap=importmap)

    loader = DataViewEntityLoader(WebServiceAdapter())
    data_view = DataView(loader=loader, cache_viewer=importmap_view)

    return data_view
