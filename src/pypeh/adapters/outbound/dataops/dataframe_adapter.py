from __future__ import annotations

import logging
import polars as pl
from typing import TYPE_CHECKING

from pypeh.core.interfaces.outbound.dataops import OutDataOpsInterface

if TYPE_CHECKING:
    from typing import Any

logger = logging.getLogger(__name__)


class DataFrameAdapter(OutDataOpsInterface[pl.DataFrame]):
    data_format = pl.DataFrame

    def get_element_labels(self, data: pl.DataFrame) -> list[str]:
        return data.columns

    def get_element_values(self, data: pl.DataFrame, element_label: str) -> set[str]:
        return set(data.get_column(element_label))

    def subset(
        self,
        data: pl.DataFrame,
        element_group: list[str],
        id_group: list[tuple[Any]] | None = None,
        identifying_elements: list[str] | None = None,
    ) -> pl.DataFrame:
        if id_group is None:
            ret = data.select(element_group)
        else:
            assert identifying_elements is not None
            ret = data.filter(pl.struct(identifying_elements).is_in(id_group)).select(element_group)

        return ret

    def relabel(self, data: pl.DataFrame, element_mapping: dict[str, str]) -> pl.DataFrame:
        return data.rename(element_mapping)
