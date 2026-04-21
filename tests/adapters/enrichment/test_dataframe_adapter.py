import pytest


@pytest.mark.dataframe
class TestDataFrameEnrichmentAdapter:
    def test_apply_map_supports_mixed_column_and_scalar_kwargs(self):
        import polars as pl

        from pypeh.adapters.enrichment.dataframe_adapter import (
            DataFrameEnrichmentAdapter,
        )

        adapter = DataFrameEnrichmentAdapter()
        ds = pl.DataFrame({"x": [1, 2, 3]}).lazy()

        result = adapter.apply_map(
            ds=ds,
            map_fn=lambda x, offset: x + offset,
            new_field_name="y",
            output_dtype=pl.Int64,
            base_fields=["x"],
            x=pl.col("x"),
            offset=2,
        ).collect()

        assert result.columns == ["x", "y"]
        assert result["y"].to_list() == [3, 4, 5]

    def test_apply_map_supports_scalar_only_kwargs(self):
        import polars as pl

        from pypeh.adapters.enrichment.dataframe_adapter import (
            DataFrameEnrichmentAdapter,
        )

        adapter = DataFrameEnrichmentAdapter()
        ds = pl.DataFrame({"x": [1, 2, 3]}).lazy()

        result = adapter.apply_map(
            ds=ds,
            map_fn=lambda offset: offset * 2,
            new_field_name="const",
            output_dtype=pl.Int64,
            base_fields=["x"],
            offset=2,
        ).collect()

        assert result.columns == ["x", "const"]
        assert result["const"].to_list() == [4, 4, 4]
