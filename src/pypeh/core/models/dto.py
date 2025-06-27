from __future__ import annotations

from typing import TYPE_CHECKING
from abc import ABC

if TYPE_CHECKING:
    from typing import Sequence, Mapping


class DataTransferObject(ABC):
    def __init__(self, data: dict[str, Sequence], metadata: Mapping):
        self.data = data
        self.metadata = metadata
