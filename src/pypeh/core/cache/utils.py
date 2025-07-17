from __future__ import annotations

from peh_model.peh import NamedThing, NamedThingId, YAMLRoot

from typing import TYPE_CHECKING, Optional, Callable, Mapping

from pypeh.core.models.proxy import TypedLazyProxy
from pypeh.core.models.typing import T_NamedThingLike
from pypeh.core.models.typing import T_RootStream

if TYPE_CHECKING:
    pass


def get_entity_type(entity: T_NamedThingLike) -> str:
    if isinstance(entity, TypedLazyProxy):
        return entity.expected_type.__name__
    return entity.__class__.__name__


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
