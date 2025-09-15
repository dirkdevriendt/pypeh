from __future__ import annotations

import logging
import uuid

from decimal import Decimal, getcontext
from pydantic import BaseModel, field_validator
from typing import Any, Dict, Generator, Sequence

from pypeh.core.models.constants import ValidationErrorLevel
from peh_model import pydanticmodel_v2 as pehs
from peh_model import peh


logger = logging.getLogger(__name__)


def get_max_decimal_value():
    ctx = getcontext()
    precision = ctx.prec
    emax = ctx.Emax

    max_digits = "9" * precision
    max_value = Decimal(f"{max_digits}E{emax}")
    return max_value


def convert_peh_validation_error_level_to_validation_dto_error_level(peh_validation_error_level: str | None):
    if peh_validation_error_level is None:
        return ValidationErrorLevel.ERROR
    else:
        match peh_validation_error_level:
            case "info":
                return ValidationErrorLevel.INFO
            case "warning":
                return ValidationErrorLevel.WARNING
            case "error":
                return ValidationErrorLevel.ERROR
            case "fatal":
                return ValidationErrorLevel.FATAL
            case _:
                raise ValueError(f"Invalid Error level encountered: {peh_validation_error_level}")


def convert_peh_value_type_to_validation_dto_datatype(peh_value_type: str):
    # TODO: fix for "categorical" ?
    # TODO: review & extend potential input values
    # valid input values: "string", "boolean", "date", "datetime", "decimal", "integer", "float"
    # valid return values: 'date', 'datetime', 'boolean', 'decimal', 'integer', 'varchar', 'float', or 'categorical'
    if peh_value_type is None:
        return None
    else:
        match peh_value_type:
            case "decimal":
                logger.info("Casting decimal to float")
                return "float"
            case "boolean" | "date" | "datetime" | "float" | "string" | "integer":
                return peh_value_type
            case _:
                raise ValueError(f"Invalid data type encountered: {peh_value_type}")


def cast_to_peh_value_type(value: Any, peh_value_type: str | None) -> Any:
    # valid input values: "string", "boolean", "date", "datetime", "decimal", "float"
    if peh_value_type is None:
        return value
    else:
        match peh_value_type:
            case "string":
                return str(value)
            case "boolean":
                return bool(value)
            case "date":
                return str(value)  # FIXME
            case "datetime":
                return str(value)  # FIXME
            case "decimal":
                logger.info("Casting decimal as float")
                return float(value)
            case "integer":
                return int(value)
            case "float":
                return float(value)
            case _:
                return str(value)


class ValidationExpression(BaseModel):
    conditional_expression: ValidationExpression | None = None
    arg_expressions: list[ValidationExpression] | None = None
    command: str
    arg_values: list[Any] | None = None
    arg_columns: list[str] | None = None
    subject: list[str] | None = None

    @field_validator("command", mode="before")
    @classmethod
    def command_to_str(cls, v):
        if v is None:
            return "conjunction"
        elif isinstance(v, peh.PermissibleValue):
            return v.text
        elif isinstance(v, str):
            return v
        elif isinstance(v, peh.ValidationCommand):
            return str(v)
        else:
            logger.error(f"No conversion defined for {v} of type {v.__class__}")
            raise NotImplementedError

    @classmethod
    def from_peh(
        cls,
        expression: peh.ValidationExpression | pehs.ValidationExpression,
        observable_property_varname_dict: dict | None = None,
    ) -> "ValidationExpression":
        conditional_expression = getattr(expression, "validation_condition_expression")
        conditional_expression_instance = None
        if conditional_expression is not None:
            conditional_expression_instance = ValidationExpression.from_peh(
                conditional_expression, observable_property_varname_dict=observable_property_varname_dict
            )
        arg_expressions = getattr(expression, "validation_arg_expressions")
        arg_expression_instances = None
        if arg_expressions is not None:
            arg_expression_instances = [
                ValidationExpression.from_peh(
                    nested_expr, observable_property_varname_dict=observable_property_varname_dict
                )
                for nested_expr in arg_expressions
            ]
        validation_command = getattr(expression, "validation_command", "conjunction")
        arg_values = getattr(expression, "validation_arg_values", None)
        source_paths = getattr(expression, "validation_subject_source_paths", None)
        arg_type = None
        if source_paths:
            arg_types = set()
            if observable_property_varname_dict is not None:
                for source_path in source_paths:
                    obs_prop = observable_property_varname_dict.get(source_path, None)
                    if source_path is None:
                        me = f"Could not find validation_subject_source_path with varname {source_path}"
                        logger.error(me)
                        raise ValueError(me)
                    new_arg_type = getattr(obs_prop, "value_type", str)
                    assert new_arg_type is not None
                    arg_types.add(new_arg_type)
            if len(arg_types) != 1:
                logger.error("More than one type corresponds to the ObservableProperties in validation_source_paths.")
                raise ValueError
            arg_type = arg_types.pop()
        if arg_values is not None:
            assert isinstance(arg_values, Sequence)
            try:
                arg_values = [cast_to_peh_value_type(arg_value, arg_type) for arg_value in arg_values]
            except Exception as e:
                logger.error(f"Could not cast values in {arg_values} to {arg_type}: {e}")
                raise
        return cls(
            conditional_expression=conditional_expression_instance,
            arg_expressions=arg_expression_instances,
            command=validation_command,
            arg_values=arg_values,
            arg_columns=getattr(expression, "validation_arg_source_paths"),
            subject=getattr(expression, "validation_subject_source_paths"),
        )


class ValidationDesign(BaseModel):
    name: str
    error_level: ValidationErrorLevel
    expression: ValidationExpression

    @classmethod
    def from_peh(
        cls,
        validation_design: peh.ValidationDesign | pehs.ValidationDesign,
        observable_property_varname_dict: dict | None = None,
    ) -> "ValidationDesign":
        error_level = getattr(validation_design, "error_level", None)
        error_level = convert_peh_validation_error_level_to_validation_dto_error_level(error_level)
        expression = getattr(validation_design, "validation_expression", None)
        if expression is None:
            print(validation_design)
            raise AttributeError
        expression = ValidationExpression.from_peh(
            expression, observable_property_varname_dict=observable_property_varname_dict
        )
        name = getattr(validation_design, "validation_name", None)
        if name is None:
            name = str(uuid.uuid4())
        return cls(
            name=name,
            error_level=error_level,
            expression=expression,
        )

    @classmethod
    def list_from_metadata(cls, metadata: list[Any]) -> list["ValidationDesign"]:
        expression_list = []
        numeric_commands = set(
            [
                "min",
                "max",
                "is_equal_to",
                "is_greater_than_or_equal_to",
                "is_greater_than",
                "is_equal_to_or_both_missing",
                "is_less_than_or_equal_to",
                "is_less_than",
                "is_not_equal_to",
                "is_not_equal_to_and_not_both_missing",
            ]
        )
        for metadatum in metadata:
            arg_type = "string"
            if metadatum.field.lower() in numeric_commands:
                if metadatum.value is not None:
                    try:
                        # NOTE: type conversion here is useless unless using Baseclass.model_construct() to avoid validation
                        arg_type = "float"
                        typed_metadata_value = cast_to_peh_value_type(metadatum.value, arg_type)
                    except Exception as e:
                        logger.error(
                            f"could not cast ValidationExpression argument {metadatum.value} to {arg_type}: {e}"
                        )
                        raise

            generate = False
            match metadatum.field.lower():
                case "min":
                    validation_command = peh.ValidationCommand.is_greater_than_or_equal_to
                    generate = True
                case "max":
                    validation_command = peh.ValidationCommand.is_less_than_or_equal_to
                    generate = True
                case "is_equal_to":
                    validation_command = peh.ValidationCommand.is_equal_to
                    generate = True
                case "is_equal_to_or_both_missing":
                    validation_command = peh.ValidationCommand.is_equal_to_or_both_missing
                    generate = True
                case "is_greater_than_or_equal_to":
                    validation_command = peh.ValidationCommand.is_greater_than_or_equal_to
                    generate = True
                case "is_greater_than":
                    validation_command = peh.ValidationCommand.is_greater_than
                    generate = True
                case "is_less_than_or_equal_to":
                    validation_command = peh.ValidationCommand.is_less_than_or_equal_to
                    generate = True
                case "is_less_than":
                    validation_command = peh.ValidationCommand.is_less_than
                    generate = True
                case "is_not_equal_to":
                    validation_command = peh.ValidationCommand.is_not_equal_to
                    generate = True
                case "is_not_equal_to_and_not_both_missing":
                    validation_command = peh.ValidationCommand.is_not_equal_to_and_not_both_missing
                    generate = True

            if generate:
                expression_list.append(
                    ValidationExpression.from_peh(
                        pehs.ValidationExpression.model_construct(
                            **{
                                "validation_command": validation_command,
                                "validation_arg_values": [typed_metadata_value],
                            }
                        )
                    )
                )

        return [
            cls(name=metadatum.field.lower(), error_level=ValidationErrorLevel.ERROR, expression=expression)
            for expression in expression_list
        ]


class ColumnValidation(BaseModel):
    unique_name: str
    data_type: str
    required: bool
    nullable: bool
    validations: list[ValidationDesign] | None = None

    @classmethod
    def from_peh(
        cls, column_name: str, observable_property: peh.ObservableProperty | pehs.ObservableProperty, varname_dict: dict
    ) -> "ColumnValidation":
        required = observable_property.default_required
        nullable = not required
        validations = []
        assert isinstance(observable_property.value_type, str)
        data_type = convert_peh_value_type_to_validation_dto_datatype(observable_property.value_type)
        if validation_designs := getattr(observable_property, "validation_designs", None):
            validations.extend(
                [
                    ValidationDesign.from_peh(vd, observable_property_varname_dict=varname_dict)
                    for vd in validation_designs
                ]
            )
        if value_metadata := getattr(observable_property, "value_metadata", None):
            validations.extend(ValidationDesign.list_from_metadata(value_metadata))
        if getattr(observable_property, "categorical", None):
            validations.append(
                ValidationDesign.from_peh(
                    peh.ValidationDesign(
                        validation_name="check_categorical",
                        validation_expression=peh.ValidationExpression(
                            validation_command=peh.ValidationCommand.is_in,
                            validation_arg_values=[
                                vo.key for vo in getattr(observable_property, "value_options", None)
                            ],
                        ),
                        validation_error_level=peh.ValidationErrorLevel.error,
                    ),
                    observable_property_varname_dict=varname_dict,
                )
            )

        assert isinstance(required, bool)
        return cls(
            unique_name=column_name,
            data_type=data_type,
            required=required,
            nullable=nullable,
            validations=validations,
        )


class ValidationConfig(BaseModel):
    name: str
    columns: list[ColumnValidation]
    identifying_column_names: list[str] | None = None
    validations: list[ValidationDesign] | None = None

    @classmethod
    def from_peh(
        cls,
        oep_set: peh.ObservableEntityPropertySet | pehs.ObservableEntityPropertySet,
        oep_set_name: str,
        observable_property_dict: Dict[str, peh.ObservableProperty | pehs.ObservableProperty],
    ) -> "ValidationConfig":
        if isinstance(oep_set.required_observable_property_id_list, list) and isinstance(
            oep_set.optional_observable_property_id_list, list
        ):
            all_op_ids = (
                oep_set.identifying_observable_property_id_list
                + oep_set.required_observable_property_id_list
                + oep_set.optional_observable_property_id_list
            )
        else:
            raise TypeError
        observable_property_varname_dict = {op.varname: op for op in observable_property_dict.values()}
        columns = [
            ColumnValidation.from_peh(op_id, observable_property_dict[op_id], observable_property_varname_dict)
            for op_id in all_op_ids
            if op_id in observable_property_dict
        ]

        # Optional: log or raise error if some op_ids are missing
        missing = set(all_op_ids) - observable_property_dict.keys()
        if missing:
            raise ValueError(f"Missing observable properties for IDs: {missing}")

        assert isinstance(oep_set.identifying_observable_property_id_list, list)
        return cls(
            name=oep_set_name,
            columns=columns,
            identifying_column_names=oep_set.identifying_observable_property_id_list,
            validations=[],  # TODO: handle dataset-level validations if any
        )

    @classmethod
    def from_observation(
        cls,
        observation: peh.Observation | pehs.Observation,
        observable_property_dict: dict[str, peh.ObservableProperty | peh.ObservableProperty],
    ) -> Generator[tuple[str, ValidationConfig], None, None]:
        observation_design = observation.observation_design
        observable_entity_property_sets = getattr(observation_design, "observable_entity_property_sets", None)
        if observable_entity_property_sets is None:
            logger.error(
                "Cannot generate a ValidationConfig from an ObservationDesign that does not contain observable_entity_property_sets"
            )
            raise AttributeError
        for cnt, oep_set in enumerate(observable_entity_property_sets):
            oep_set_name = f"{oep_set}_{cnt:0>2}"
            yield (
                oep_set_name,
                ValidationConfig.from_peh(
                    oep_set,
                    oep_set_name,
                    observable_property_dict,
                ),
            )


class ValidationDTO(BaseModel):
    config: ValidationConfig
    data: dict[str, Any]
