from __future__ import annotations

from typing import TYPE_CHECKING

from pypeh.core.models.proxy import TypedLazyProxy
from pypeh.core.models.typing import T_NamedThingLike

if TYPE_CHECKING:
    pass


def get_entity_type(entity: T_NamedThingLike) -> str:
    if isinstance(entity, TypedLazyProxy):
        return entity.expected_type.__name__
    return entity.__class__.__name__
