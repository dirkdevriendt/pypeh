from __future__ import annotations

import logging

import polars as pl

from pypeh.adapters.outbound.dataops.dataframe_adapter import DataFrameAdapter
from pypeh.core.interfaces.outbound.dataops import (
    DataEnrichmentInterface,
)
from pypeh.core.models.graph import Node
from pypeh.core.models.internal_data_layout import JoinSpec


logger = logging.getLogger(__name__)


class DataFrameEnrichmentAdapter(
    DataFrameAdapter, DataEnrichmentInterface[pl.DataFrame]
):
    data_format = pl.DataFrame

    def select_field(self, dataset: pl.LazyFrame, field_label: str):
        return pl.col(field_label)

    def apply_joins(
        self, datasets, join_specs: list[list[JoinSpec]], node: Node
    ) -> pl.LazyFrame:
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
                dataset = dataset.join(
                    other_dataset, left_on=left_on, right_on=right_on
                )
            else:
                raise NotImplementedError

        return dataset

    def apply_map(
        self,
        ds: pl.LazyFrame,
        map_fn,
        new_field_name: str,
        output_dtype,
        base_fields: list[str],
        **kwargs,
    ):
        struct_expr = pl.struct(list(kwargs.values()))
        aliased_exprs = {
            arg_name: expr.alias(arg_name) for arg_name, expr in kwargs.items()
        }
        struct_expr = pl.struct(list(aliased_exprs.values()))
        mapped = struct_expr.map_batches(
            lambda s: map_fn(
                **{name: s.struct.field(name) for name in aliased_exprs}
            ),
            return_dtype=output_dtype,
        ).alias(new_field_name)

        ds2 = ds.with_columns(mapped)
        existing = set(ds2.columns)
        safe_fields = [f for f in base_fields if f in existing]

        if new_field_name not in safe_fields:
            safe_fields.append(new_field_name)

        seen = set()
        unique_fields = []
        for f in safe_fields:
            if f not in seen:
                seen.add(f)
                unique_fields.append(f)

        return ds2.select(unique_fields)

    def collect(self, datasets: dict[str, pl.LazyFrame]):
        for dataset in datasets.values():
            if isinstance(dataset, pl.LazyFrame):
                dataset = dataset.collect()
