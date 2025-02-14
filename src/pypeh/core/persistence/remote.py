"""
# TODO: add in support for other ManifestRepositories: ROCrate, ...
"""

from __future__ import annotations

import logging
import requests

from typing import TYPE_CHECKING

from pypeh.core.persistence.local import LinkMLRepository
from pypeh.core.interfaces.persistence import PersistenceInterface

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from typing import Optional, Callable, Any


class RemoteRepository(PersistenceInterface):
    def __init__(self, to_repo: Optional[Callable] = None, from_repo: Optional[Callable] = None):
        self.from_repo = from_repo
        self.to_repo = to_repo

    def identifier_to_locator(self, identifier: str) -> str:
        return identifier

    def load(self, base_dir: str, identifier: str) -> Any:
        # differentiate handling of PID / URI / CURIE ->

        try:
            response = requests.get(identifier, headers={"Accept": "application/json"}, timeout=10)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {e}")
            raise

        if self.from_repo is not None:
            return self.from_repo(data)
        else:
            return data

    def dump(self, entity: Any, identifier: str, transform: Optional[Callable] = None) -> None:
        raise NotImplementedError


class LinkMLRemoteRepository(LinkMLRepository):
    pass
