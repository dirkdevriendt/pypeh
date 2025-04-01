from __future__ import annotations

from typing import TYPE_CHECKING, Union

from pypeh.core.models.peh import NamedThing
from pypeh.core.models.proxy import TypedLazyProxy

if TYPE_CHECKING:
    pass

T_NamedThingLike = Union[NamedThing, TypedLazyProxy]


def get_entity_type(entity: T_NamedThingLike) -> str:
    if isinstance(entity, TypedLazyProxy):
        return entity.expected_type.__name__
    return entity.__class__.__name__
