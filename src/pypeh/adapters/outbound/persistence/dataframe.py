from __future__ import annotations

import logging

from typing import TYPE_CHECKING

from pypeh.adapters.outbound.persistence.formats import IOAdapter

if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


class CsvIOImpl(IOAdapter):
    def load(self, source: str, **kwargs):
        raise NotImplementedError

    def dump(self, destination: str, **kwargs):
        raise NotImplementedError


class ExcelIOImpl(IOAdapter):
    def load(self, source: str, **kwargs):
        raise NotImplementedError

    def dump(self, destination: str, **kwargs):
        raise NotImplementedError
