from __future__ import annotations

import logging
import polars as pl
from typing import TYPE_CHECKING

from polars.datatypes import DataType
from enum import Enum

from pypeh.core.interfaces.outbound.dataops import OutDataOpsInterface
from pypeh.core.models.constants import ObservablePropertyValueType

if TYPE_CHECKING:
    from typing import Any

logger = logging.getLogger(__name__)


class DataFrameAdapter(OutDataOpsInterface[pl.DataFrame]):
    data_format = pl.DataFrame

    def get_element_labels(self, data: pl.DataFrame) -> list[str]:
        return data.columns

    def get_element_values(
        self, data: pl.DataFrame, element_label: str, as_list=False
    ) -> list[str] | set[str]:
        if as_list:
            return data.get_column(element_label).to_list()
        return set(data.get_column(element_label))

    def check_element_has_empty_values(
        self, data: pl.DataFrame, element_label: str
    ) -> bool:
        return data.select(pl.col(element_label).is_null().any()).item()

    def check_element_has_only_empty_values(
        self, data: pl.DataFrame, element_label: str
    ) -> bool:
        return data.select(pl.col(element_label).is_null().all()).item()

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
            ret = data.filter(
                pl.struct(identifying_elements).is_in(id_group)
            ).select(element_group)

        return ret

    def relabel(
        self, data: pl.DataFrame, element_mapping: dict[str, str]
    ) -> pl.DataFrame:
        return data.rename(element_mapping)

    def type_mapper(
        self, peh_value_type: str | ObservablePropertyValueType
    ) -> type[DataType]:
        if isinstance(peh_value_type, Enum):
            peh_value_type = peh_value_type.value

        match peh_value_type:
            case "string":
                return pl.String
            case "boolean":
                return pl.Boolean
            case "date":
                return pl.Date
            case "datetime":
                return pl.Datetime
            case "decimal":
                return pl.Float64
            case "integer":
                return pl.Int64
            case "float":
                return pl.Float64
            case _:
                return pl.String

    def normalize_input(self, data: pl.DataFrame) -> pl.LazyFrame:
        return data.lazy()

    def normalize_output(
        self, data: pl.DataFrame | pl.LazyFrame
    ) -> pl.DataFrame:
        if isinstance(data, pl.LazyFrame):
            return data.collect()
        return data
