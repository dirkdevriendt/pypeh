import pytest

from pypeh.dataframe_adapter.dataops import DataValidationAdapter


class TestDataValidationAdapter:
    def test_validate(self):
        adapter = DataValidationAdapter()

        data = {
            "col1": [1, 2, 3, None],
            "col2": [2, 3, 1, None],
        }

        config = {
            "name": "test_config",
            "columns": [
                {
                    "unique_name": "col1",
                    "data_type": "integer",
                    "required": True,
                    "nullable": False,
                    "validations": [
                        {
                            "name": "name",
                            "error_level": "error",
                            "expression": {
                                "command": "is_greater_than",
                                "arg_columns": ["col2"],
                            },
                        }
                    ],
                }
            ],
            "identifying_column_names": ["col1"],
            "validations": [
                {
                    "name": "name",
                    "error_level": "error",
                    "expression": {
                        "command": "is_greater_than",
                        "arg_columns": ["col2"],
                        "subject": ["col1"],
                    },
                }
            ],
        }

        result = adapter.validate(data, config)

        assert result is not None
        assert len(result) == 1
