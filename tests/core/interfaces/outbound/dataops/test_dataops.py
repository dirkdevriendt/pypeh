import pytest
import abc

from typing import Protocol

from pypeh.core.models.validation_errors import ValidationReport
from pypeh.core.models.constants import ValidationErrorLevel
from pypeh.core.models.validation_dto import (
    ValidationExpression,
    ValidationDesign,
    ColumnValidation,
    ValidationConfig,
)

class DataOpsProtocol(Protocol):
    def validate(self, data, config) -> ValidationReport: ...


class TestDataOps(abc.ABC):
    """Abstract base class for testing dataops adapters."""

    @abc.abstractmethod
    def get_adapter(self) -> DataOpsProtocol:
        """Return the adapter implementation to test."""
        raise NotImplementedError

    def test_validate(self):
        adapter = self.get_adapter()

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


@pytest.mark.dataframe
class TestDataFrameDataOps(TestDataOps):
    def get_adapter(self) -> DataOpsProtocol:
        try:
            from pypeh.adapters.outbound.validation.pandera_adapter import dataops as dfops

            return dfops.DataOpsAdapter()
        except ImportError:
            pytest.skip("Necessary modules not installed")


@pytest.mark.other
class TestUnknownDataOps(TestDataOps):
    def get_adapter(self) -> DataOpsProtocol:
        raise NotImplementedError