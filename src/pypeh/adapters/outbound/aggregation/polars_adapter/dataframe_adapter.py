from itertools import chain
from typing import Callable

import polars as pl

from pypeh.core.interfaces.outbound.dataops import AggregationInterface
from pypeh.adapters.outbound.dataops.dataframe_adapter import DataFrameAdapter
import pypeh.adapters.outbound.aggregation.polars_adapter.statistics as stats


class DataFrameAggregationAdapter(DataFrameAdapter, AggregationInterface[pl.DataFrame]):
    data_format = pl.DataFrame

    def calculate_for_strata(
        self,
        df: pl.LazyFrame,
        stratifications: list[list[str]] | None,
        value_col: str,
        stat_builders: list[str],
        **kwargs,
    ) -> pl.DataFrame:
        if not stratifications:
            return self._calculate_for_stratum(
                df=df,
                group_cols=None,
                value_col=value_col,
                stat_builders=stat_builders,
                **kwargs,
            ).collect()

        summary_dfs = []
        for strat in stratifications:
            summary_df = self._calculate_for_stratum(
                df=df,
                group_cols=strat,
                value_col=value_col,
                stat_builders=stat_builders,
                **kwargs,
            )
            summary_dfs.append(summary_df)

        combined_summary = pl.concat(summary_dfs, how="diagonal").collect()
        return combined_summary

    def _calculate_for_stratum(
        self, df: pl.LazyFrame, group_cols: list[str] | None, value_col: str, stat_builders: list[str], **kwargs
    ) -> pl.LazyFrame:
        # Each stat builder returns a list of expressions, so we need to flatten
        exprs = list(chain.from_iterable(self._get_stat_function(expr)(value_col, **kwargs) for expr in stat_builders))

        if not group_cols:
            return df.select(exprs)

        return df.group_by(group_cols).agg(exprs).with_columns(pl.lit(group_cols).alias("stratification"))

    def _get_stat_function_from_name(self, function_name: str):
        return getattr(stats, function_name)

    def _get_stat_function(self, fn: str | Callable):
        if isinstance(fn, str):
            return self._get_stat_function_from_name(fn)
        elif callable(fn):
            return fn
        else:
            raise ValueError(f"Invalid function specification: {fn}")
