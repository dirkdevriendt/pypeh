from __future__ import annotations

import logging

from abc import abstractmethod
from typing import TYPE_CHECKING

from pypeh.core.abc import Interface

if TYPE_CHECKING:
    from typing import Any

logger = logging.getLogger(__name__)


class PersistenceInterface(Interface):
    @abstractmethod
    def load(self, base_dir: str, identifier: str, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def dump(self, entity: Any, identifier: str, **kwargs) -> None:
        raise NotImplementedError


class RepositoryInterface(PersistenceInterface):
    def __init__(self):
        self.engine = None
