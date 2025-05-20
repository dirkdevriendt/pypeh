from typing import Mapping, Sequence
import importlib

from pypeh.core.models.validation_errors import ValidationReport


def parse_single_expression(expression: Mapping) -> Mapping:
    command = expression.get("command")
    try:
        command = getattr(importlib.import_module("pypeh.dataframe_adapter.validation.check_functions"), command)
    except AttributeError:
        pass
    return {
        "command": command,
        "arg_values": expression.get("arg_values"),
        "arg_columns": expression.get("arg_columns"),
        "subject": expression.get("subject"),
    }


def parse_validation_expression(expression: Mapping) -> Mapping:
    keys = expression.keys()
    if "conditional_expression" in keys:
        case = "conditional"
        exp_1 = parse_validation_expression(expression["conditional_expression"])
        expression.pop("conditional_expression")
        exp_2 = parse_validation_expression(expression)
        return {
            "check_case": case,
            "expressions": [exp_1, exp_2],
        }
    if expression.get("command") in ("conjunction", "disjunction"):
        case = expression.get("command")
        exp_1 = parse_validation_expression(expression["arg_expressions"][0])
        exp_2 = parse_validation_expression(expression["arg_expressions"][1])
        return {
            "check_case": case,
            "expressions": [exp_1, exp_2],
        }
    return parse_single_expression(expression)


def parse_validation_design(validation_design: Mapping) -> Mapping:
    return {
        "name": validation_design.get("name"),
        "error_level": validation_design.get("error_level"),
    } | parse_validation_expression(validation_design.get("expression"))


def parse_columns(columns: Sequence[Mapping]) -> Mapping:
    parsed_columns = []
    for column in columns:
        parsed_columns.append(
            {
                "id": column.get("unique_name"),
                "data_type": column.get("data_type"),
                "required": column.get("required"),
                "nullable": column.get("nullable"),
                "unique": False,
                "checks": [parse_validation_design(check) for check in column.get("validations")]
                if column.get("validations")
                else None,
            }
        )
    return parsed_columns


def parse_config(config: Mapping) -> Mapping:
    return {
        "name": config.get("name"),
        "columns": parse_columns(config.get("columns")),
        "ids": config.get("identifying_column_names"),
        "checks": [parse_validation_design(check) for check in config.get("validations")]
        if config.get("validations")
        else None,
    }


def parse_error_report(error_report: Mapping) -> ValidationReport:
    # TODO: Implement the parsing logic for the error report
    return error_report
