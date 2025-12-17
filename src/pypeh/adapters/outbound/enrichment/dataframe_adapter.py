from __future__ import annotations

import logging

import polars as pl

from polars.datatypes import DataType
from enum import Enum

from pypeh.adapters.outbound.dataops.dataframe_adapter import DataFrameAdapter
from pypeh.core.interfaces.outbound.dataops import (
    DataEnrichmentInterface,
)
from pypeh.core.models.constants import ObservablePropertyValueType
from pypeh.core.models.graph import Node
from pypeh.core.models.internal_data_layout import JoinSpec


logger = logging.getLogger(__name__)


class DataFrameEnrichmentAdapter(DataFrameAdapter, DataEnrichmentInterface[pl.DataFrame]):
    data_format = pl.DataFrame

    def _enrich_data(
        self, data: pl.DataFrame, enrichment_config: dict
    ) -> pl.DataFrame: ...  # Implementation of data enrichment logic

    def _get_function_from_name(self, function_name: str):
        # Placeholder for actual function retrieval logic
        pass

    def select_field(self, dataset: pl.LazyFrame, field_label: str):
        return pl.col(field_label)

    def apply_joins(self, datasets, join_specs: list[list[JoinSpec]], node: Node) -> pl.LazyFrame:
        for join_spec in join_specs:
            assert isinstance(join_spec, list)
            if len(join_spec) == 1:
                single_join = join_spec[0]
                dataset = datasets.get(single_join.left_dataset, None)
                assert dataset is not None
                left_on = single_join.left_element
                right_on = single_join.right_element
                other_dataset = datasets.get(single_join.right_dataset, None)
                assert other_dataset is not None
                dataset = dataset.join(other_dataset, left_on=left_on, right_on=right_on)
            else:
                raise NotImplementedError

        return dataset

    def apply_map(self, ds: pl.LazyFrame, map_fn, new_field_name: str, output_dtype, base_fields: list[str], **kwargs):
        struct_expr = pl.struct(list(kwargs.values()))
        ds2 = ds.with_columns(
            struct_expr.map_batches(
                lambda s: map_fn(
                    **{arg_name: s.struct.field(expr.meta.output_name()) for arg_name, expr in kwargs.items()}
                ),
                return_dtype=output_dtype,
            ).alias(new_field_name)
        )
        return ds2.select([*base_fields, new_field_name])

    def collect(self, datasets: dict[str, pl.LazyFrame]):
        for dataset in datasets.values():
            if isinstance(dataset, pl.LazyFrame):
                dataset = dataset.collect()

    def type_mapper(self, peh_value_type: str | ObservablePropertyValueType) -> type[DataType]:
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
