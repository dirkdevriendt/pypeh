from __future__ import annotations

import logging

import polars as pl

from pypeh.adapters.dataops.dataframe_adapter import DataFrameAdapter
from pypeh.core.interfaces.dataops import (
    DataEnrichmentInterface,
)


logger = logging.getLogger(__name__)


class DataFrameEnrichmentAdapter(
    DataFrameAdapter, DataEnrichmentInterface[pl.DataFrame]
):
    data_format = pl.DataFrame

    def select_field(self, dataset: pl.LazyFrame, field_label: str):
        return pl.col(field_label)

    def apply_map(
        self,
        ds: pl.LazyFrame,
        map_fn,
        new_field_name: str,
        output_dtype,
        base_fields: list[str],
        **kwargs,
    ):
        aliased_exprs = {}
        scalar_kwargs = {}
        for arg_name, value in kwargs.items():
            if isinstance(value, pl.Expr):
                aliased_exprs[arg_name] = value.alias(arg_name)
            else:
                scalar_kwargs[arg_name] = value

        if len(aliased_exprs) > 0:
            struct_expr = pl.struct(list(aliased_exprs.values()))
            mapped = struct_expr.map_batches(
                lambda s: map_fn(
                    **{name: s.struct.field(name) for name in aliased_exprs},
                    **scalar_kwargs,
                ),
                return_dtype=output_dtype,
            ).alias(new_field_name)
        else:
            mapped = pl.lit(map_fn(**scalar_kwargs), dtype=output_dtype).alias(
                new_field_name
            )

        ds2 = ds.with_columns(mapped)
        existing = ds2.collect_schema().names()
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
