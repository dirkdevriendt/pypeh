from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pypeh.core.models.peh import YAMLRoot


def get_entity_type(entity: YAMLRoot) -> str:
    return entity.__class__.__name__
