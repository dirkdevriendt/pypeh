from __future__ import annotations

import logging

import polars as pl

from pypeh.core.interfaces.outbound.dataops import (
    DataEnrichmentInterface,
)


logger = logging.getLogger(__name__)


class DataFrameAdapter(DataEnrichmentInterface[pl.DataFrame]):
    data_format = pl.DataFrame

    def _enrich_data(
        self, data: pl.DataFrame, enrichment_config: dict
    ) -> pl.DataFrame: ...  # Implementation of data enrichment logic

    def _get_function_from_name(self, function_name: str):
        # Placeholder for actual function retrieval logic
        pass
