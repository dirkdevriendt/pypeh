from __future__ import annotations

import logging

from abc import abstractmethod
from typing import TYPE_CHECKING

from pypeh.core.abc import Interface

if TYPE_CHECKING:
    from typing import Union
    from pydantic import BaseModel
    from pypeh.core.models.peh import YAMLRoot

logger = logging.getLogger(__name__)


class PersistenceInterface(Interface):
    @abstractmethod
    def load(self, source: str, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def dump(self, destination: str, entity: Union[str, BaseModel, YAMLRoot], **kwargs) -> None:
        raise NotImplementedError


class RepositoryInterface(PersistenceInterface):
    def __init__(self):
        self.engine = None
