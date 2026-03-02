from __future__ import annotations

import logging

from abc import abstractmethod
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from peh_model.peh import EntityList

logger = logging.getLogger(__name__)


class PersistenceInterface:
    @abstractmethod
    def load(self, source: str, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def dump(self, source: EntityList, destination: str, **kwargs) -> None:
        raise NotImplementedError


class RepositoryInterface(PersistenceInterface):
    def __init__(self):
        self.engine = None
