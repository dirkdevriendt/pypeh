from __future__ import annotations

import logging
import pandas as pd

from typing import TYPE_CHECKING

from pypeh.core.interfaces.persistence import PersistenceInterface

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class PersistenceAdapter(PersistenceInterface):
    def load(self, file_path: str, **kwargs):
        pass

    def dump(self, data_frame: pd.DataFrame, file_path: str, **kwargs) -> None:
        pass


class ExcelSerializer(PersistenceAdapter):
    def load(self, file_path: str, **kwargs):
        data = pd.read_excel(file_path, **kwargs)
        return data

    def dump(self, data_frame: pd.DataFrame, file_path: str, **kwargs) -> None:
        data_frame.to_excel(file_path, **kwargs)


class CsvSerializer(PersistenceAdapter):
    def load(self, file_path: str, **kwargs):
        data = pd.read_csv(file_path, **kwargs)
        return data

    def dump(self, data_frame: pd.DataFrame, file_path: str, **kwargs) -> None:
        data_frame.to_csv(file_path, **kwargs)
