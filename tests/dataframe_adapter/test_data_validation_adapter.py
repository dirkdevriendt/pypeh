import pytest

from pypeh.dataframe_adapter.dataops import DataOpsAdapter

from pypeh.core.models.constants import ValidationErrorLevel
from pypeh.core.models.validation_dto import (
    ValidationExpression,
    ValidationDesign,
    ColumnValidation,
    ValidationConfig,
)


@pytest.mark.dataframe
class TestDataOpsAdapter:
    def test_validate(self):
        adapter = DataOpsAdapter()

        data = {
            "col1": [1, 2, 3, None],
            "col2": [2, 3, 1, None],
        }

        config = ValidationConfig(
            name="test_config",
            columns=[
                ColumnValidation(
                    unique_name="col1",
                    data_type="integer",
                    required=True,
                    nullable=False,
                    validations=[
                        ValidationDesign(
                            name="name",
                            error_level=ValidationErrorLevel.ERROR,
                            expression=ValidationExpression(
                                command="is_greater_than",
                                arg_columns=["col1"],
                            ),
                        )
                    ],
                )
            ],
            identifying_column_names=["col1"],
            validations=[
                ValidationDesign(
                    name="name",
                    error_level=ValidationErrorLevel.ERROR,
                    expression=ValidationExpression(
                        command="is_greater_than",
                        arg_columns=["col2"],
                        subject=["col1"],
                    ),
                )
            ],
        )

        result = adapter.validate(data, config)

        assert result is not None
        assert len(result) == 1
