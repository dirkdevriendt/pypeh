from functools import reduce
from itertools import chain
from typing import Callable

import polars as pl

from pypeh.core.interfaces.outbound.dataops import AggregationInterface
from pypeh.adapters.outbound.dataops.dataframe_adapter import DataFrameAdapter
import pypeh.adapters.outbound.aggregation.polars_adapter.statistics as stats


class DataFrameAggregationAdapter(
    DataFrameAdapter, AggregationInterface[pl.DataFrame]
):
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
        self,
        df: pl.LazyFrame,
        group_cols: list[str] | None,
        value_col: str,
        stat_builders: list[str],
        result_aliases: list[str] | None = None,
        **kwargs,
    ) -> pl.LazyFrame:
        if result_aliases is not None:
            exprs = list(
                chain.from_iterable(
                    self._get_stat_function(expr)(
                        value_col, result_alias=result_alias, **kwargs
                    )
                    for expr, result_alias in zip(
                        stat_builders, result_aliases
                    )
                )
            )
        else:
            exprs = list(
                chain.from_iterable(
                    self._get_stat_function(expr)(value_col, **kwargs)
                    for expr in stat_builders
                )
            )

        if not group_cols:
            return df.select(exprs)

        return df.group_by(group_cols).agg(exprs)

    def _get_stat_function_from_name(self, function_name: str):
        return getattr(stats, function_name)

    def _get_stat_function(self, fn: str | Callable):
        if isinstance(fn, str):
            return self._get_stat_function_from_name(fn)
        elif callable(fn):
            return fn
        else:
            raise ValueError(f"Invalid function specification: {fn}")

    def group_results(
        self,
        results_to_collect: list[pl.LazyFrame],
        strata: list[str] | None = None,
    ) -> pl.DataFrame:
        if strata is None:
            ret = pl.concat(results_to_collect, how="horizontal")
        else:
            ret = reduce(
                lambda left, right: left.join(right, on=strata, how="inner"),
                results_to_collect,
            )
        return ret.collect()

    def _calculate_frequency(
        self,
        df: pl.LazyFrame,
        group_cols: list[str] | None,
        value_col: str,
        result_aliases: list[str] = ["value", "frequency"],
    ) -> pl.LazyFrame:
        if group_cols:
            cols = group_cols + [value_col]
        else:
            cols = [value_col]

        fn = self._get_stat_function_from_name("frequency_table")(
            cols, result_aliases=result_aliases
        )
        return fn(df).collect()
