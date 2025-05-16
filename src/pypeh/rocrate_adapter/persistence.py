from __future__ import annotations

import logging

from typing import TYPE_CHECKING

from pypeh.core.interfaces.persistence import PersistenceInterface


if TYPE_CHECKING:
    from typing import Any, Generator

logger = logging.getLogger(__name__)


class ROCrateAdapter(PersistenceInterface):
    """Adapter for loading from file."""

    def load(self, source: str, **kwargs) -> Generator[Any, None, None]:
        raise NotImplementedError

    def dump(self, destination: str, entity: Any, **kwargs) -> bool:
        raise NotImplementedError
