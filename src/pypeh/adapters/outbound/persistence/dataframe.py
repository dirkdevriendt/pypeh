from __future__ import annotations

import logging

from pathlib import Path
from typing import TYPE_CHECKING, Union, IO

from pypeh.adapters.outbound.persistence.formats import IOAdapter

if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


class CsvIOImpl(IOAdapter):
    def load(self, source: Union[str, Path, IO[str]], **kwargs):
        raise NotImplementedError

    def dump(self, destination: str, **kwargs):
        raise NotImplementedError


class ExcelIOImpl(IOAdapter):
    def load(self, source: Union[str, Path, IO[str]], **kwargs):
        raise NotImplementedError

    def dump(self, destination: str, **kwargs):
        raise NotImplementedError
