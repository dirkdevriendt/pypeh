import pytest

from pypeh.dataframe_adapter.validation.parsers import (
    parse_validation_expression,
    parse_validation_design,
    parse_columns,
    parse_config,
)
from pypeh.dataframe_adapter.validation.check_functions import decimals_precision


@pytest.mark.parametrize(
    "input_data, expected_output",
    [
        (
            {
                "command": "is_greater_than",
                "arg_columns": ["col1"],
            },
            {"command": "is_greater_than", "arg_values": None, "subject": None, "arg_columns": ["col1"]},
        ),
        (
            {
                "command": "conjunction",
                "arg_expressions": [
                    {
                        "command": "is_greater_than",
                        "arg_columns": ["col1"],
                    },
                    {
                        "command": "is_less_than",
                        "arg_columns": ["col2"],
                    },
                ],
            },
            {
                "check_case": "conjunction",
                "expressions": [
                    {"command": "is_greater_than", "arg_values": None, "subject": None, "arg_columns": ["col1"]},
                    {"command": "is_less_than", "arg_values": None, "subject": None, "arg_columns": ["col2"]},
                ],
            },
        ),
        (
            {
                "command": "disjunction",
                "arg_expressions": [
                    {
                        "command": "is_greater_than",
                        "arg_columns": ["col1"],
                    },
                    {"command": "is_less_than", "subject": ["col2"], "arg_values": [10]},
                ],
            },
            {
                "check_case": "disjunction",
                "expressions": [
                    {"command": "is_greater_than", "arg_values": None, "subject": None, "arg_columns": ["col1"]},
                    {"command": "is_less_than", "arg_values": [10], "subject": ["col2"], "arg_columns": None},
                ],
            },
        ),
        (
            {
                "conditional_expression": {
                    "command": "disjunction",
                    "arg_expressions": [
                        {
                            "command": "is_greater_than",
                            "arg_columns": ["col1"],
                        },
                        {"command": "is_less_than", "subject": ["col2"], "arg_values": [10]},
                    ],
                },
                "command": "is_equal_to",
                "arg_values": [5],
            },
            {
                "check_case": "conditional",
                "expressions": [
                    {
                        "check_case": "disjunction",
                        "expressions": [
                            {
                                "command": "is_greater_than",
                                "arg_values": None,
                                "subject": None,
                                "arg_columns": ["col1"],
                            },
                            {"command": "is_less_than", "arg_values": [10], "subject": ["col2"], "arg_columns": None},
                        ],
                    },
                    {"command": "is_equal_to", "arg_values": [5], "subject": None, "arg_columns": None},
                ],
            },
        ),
    ],
)
def test_parse_validation_expression(input_data, expected_output):
    result = parse_validation_expression(input_data)
    assert result == expected_output


@pytest.mark.parametrize(
    "columns, expected_output",
    [
        (
            [{"unique_name": "col1", "data_type": "string", "required": True, "nullable": False, "validations": []}],
            [
                {
                    "id": "col1",
                    "data_type": "string",
                    "required": True,
                    "nullable": False,
                    "unique": False,
                    "checks": None,
                }
            ],
        ),
        (
            [
                {
                    "unique_name": "col1",
                    "data_type": "string",
                    "required": True,
                    "nullable": False,
                    "validations": [
                        {
                            "name": "name",
                            "error_level": "error",
                            "expression": {
                                "command": "is_greater_than",
                                "arg_columns": ["col1"],
                            },
                        }
                    ],
                }
            ],
            [
                {
                    "id": "col1",
                    "data_type": "string",
                    "required": True,
                    "nullable": False,
                    "unique": False,
                    "checks": [
                        {
                            "name": "name",
                            "error_level": "error",
                            "command": "is_greater_than",
                            "arg_values": None,
                            "subject": None,
                            "arg_columns": ["col1"],
                        }
                    ],
                }
            ],
        ),
    ],
)
def test_parse_columns(columns, expected_output):
    result = parse_columns(columns)

    assert result == expected_output


@pytest.mark.parametrize(
    "config, expected_output",
    [
        (
            {
                "name": "test_config",
                "columns": [
                    {
                        "unique_name": "col1",
                        "data_type": "string",
                        "required": True,
                        "nullable": False,
                        "validations": [
                            {
                                "name": "name",
                                "error_level": "error",
                                "expression": {
                                    "command": "is_greater_than",
                                    "arg_columns": ["col1"],
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
            },
            {
                "name": "test_config",
                "columns": [
                    {
                        "id": "col1",
                        "data_type": "string",
                        "required": True,
                        "nullable": False,
                        "unique": False,
                        "checks": [
                            {
                                "name": "name",
                                "error_level": "error",
                                "command": "is_greater_than",
                                "arg_values": None,
                                "subject": None,
                                "arg_columns": ["col1"],
                            }
                        ],
                    }
                ],
                "ids": ["col1"],
                "checks": [
                    {
                        "name": "name",
                        "error_level": "error",
                        "command": "is_greater_than",
                        "arg_values": None,
                        "arg_columns": ["col2"],
                        "subject": ["col1"],
                    }
                ],
            },
        ),
    ],
)
def test_parse_config(config, expected_output):
    result = parse_config(config)

    assert result == expected_output
