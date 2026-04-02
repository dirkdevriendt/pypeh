import pytest
import re

from pypeh.core.interfaces.outbound.dataops import ValidationInterface
from pypeh.core.models.constants import ValidationErrorLevel
from pypeh.core.models.validation_dto import (
    ColumnValidation,
    ValidationConfig,
    ValidationDesign,
    ValidationExpression,
)
from pypeh.core.models.validation_errors import ValidationError


@pytest.mark.dataframe
class TestCustomChecks:
    @pytest.fixture(scope="class")
    def fake_data(self):
        import numpy as np

        np.random.seed(42)
        values = np.random.lognormal(mean=2.0, sigma=0.3, size=499)
        # Add a clear outlier
        outlier = np.array([5000.0])
        data = np.concatenate([values, outlier])
        return data

    def test_tukey_range(self, fake_data):
        # FAKE DATASET
        import polars as pl
        import pandera.polars as pa
        from pypeh.adapters.outbound.validation.pandera_adapter.check_functions import (
            tukey_range_check_log,
        )

        df = pl.LazyFrame({"x": fake_data})
        result = tukey_range_check_log(
            data=pa.PolarsData(df, key="x"), arg_values=None
        ).collect()
        num_outliers = result.select((~pl.col(result.columns[0])).sum()).item()

        assert num_outliers == 1

    def test_from_config(self, fake_data):
        import polars as pl

        adapter_cls = ValidationInterface.get_default_adapter_class()
        adapter = adapter_cls()

        data = pl.DataFrame({"id": list(range(500)), "x": fake_data})
        config = ValidationConfig(
            name="test_config",
            columns=[
                ColumnValidation(
                    unique_name="x",
                    data_type="float",
                    required=True,
                    nullable=False,
                    validations=[
                        ValidationDesign(
                            name="tukey range check log",
                            error_message="Outlier detected",
                            error_level=ValidationErrorLevel.ERROR,
                            expression=ValidationExpression(
                                command="tukey_range_check_log",
                            ),
                        )
                    ],
                )
            ],
            identifying_column_names=["id"],
        )
        report = adapter._validate(data=data, config=config)
        assert report.total_errors == 1
        assert report.error_counts[ValidationErrorLevel.ERROR] == 1
        first_group = report.groups[0]
        error = first_group.errors[0]
        assert isinstance(error, ValidationError)
        assert re.search(r".*outlier detected.*", error.message, re.IGNORECASE)
