from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Mapping, Sequence

from pypeh.core.interfaces.inbound.data_processing import DataProcessingInterface
from pypeh.core.interfaces.outbound.dataops import ExportInterface, ExportTypeEnum
from pypeh.core.interfaces.outbound.dataops import ValidationInterface
from pypeh.core.interfaces.outbound.persistence import PersistenceInterface

from peh_model import peh
from pypeh.core.cache.dataview import DataView
from pypeh.core.models.validation_dto import ValidationErrorLevel, ValidationExpression, ValidationDesign, ColumnValidation, ValidationConfig

if TYPE_CHECKING:
    from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

def convert_peh_validation_error_level_to_validation_dto_error_level(peh_validation_error_level: str):
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
    # valid input values: "string", "boolean", "date", "datetime", "decimal"
    # valid return values: 'date', 'datetime', 'boolean', 'decimal', 'integer', 'varchar' or 'categorical'
    if peh_value_type is None:
        return None
    else:
        match peh_value_type:
            case "string":
                return "varchar"
            case "boolean" | "date" | "datetime" | "decimal":
                return peh_value_type
            case _:
                raise ValueError(f"Invalid data type encountered: {peh_value_type}")

def convert_peh_validation_expression_to_validation_dto_expression(peh_validation_expression: peh.ValidationExpression):
    # TODO: convert source paths
    if peh_validation_expression is None:
        return None
    else:
        return ValidationExpression(
            subject=peh_validation_expression.validation_subject_source_paths,
            command=str(peh_validation_expression.validation_command),
            conditional_expression=convert_peh_validation_expression_to_validation_dto_expression(peh_validation_expression.validation_condition_expression),
            arg_values=peh_validation_expression.validation_arg_values,
            arg_columns=peh_validation_expression.validation_arg_source_paths,
            arg_expressions=[convert_peh_validation_expression_to_validation_dto_expression(expr) for expr in peh_validation_expression.validation_arg_expressions]
        )

def convert_peh_validation_design_to_validation_dto_design(peh_validation_design: peh.ValidationDesign):
    return ValidationDesign(
        name=peh_validation_design.validation_name if peh_validation_design.validation_name else str(uuid.uuid4()),
        error_level=convert_peh_validation_error_level_to_validation_dto_error_level(peh_validation_design.validation_error_level),
        expression=convert_peh_validation_expression_to_validation_dto_expression(peh_validation_design.validation_expression),
    )

def convert_peh_observable_property_to_validation_dto_column_validation(peh_observable_property_column_name, peh_observable_property: peh.ObservableProperty):
    # TODO: check required vs nullable
    # TODO: check unique
    # TODO: check ObservableProperty.default_significantdecimals, ObservableProperty.default_immutable
    # TODO: add validations for peh_observable_property.default_zeroallowed
    # TODO: add validations for value_metadata: min, max, etc
    return ColumnValidation(
        unique_name=peh_observable_property_column_name,
        data_type=convert_peh_value_type_to_validation_dto_datatype(peh_observable_property.value_type),
        required=peh_observable_property.default_required,
        nullable=not(peh_observable_property.default_required),
        unique=False,
        validations=[convert_peh_validation_design_to_validation_dto_design(opv) for opv in peh_observable_property.validation_designs]
    )

def convert_peh_validation_config_to_validation_dto_config(oep_set: peh.ObservableEntityPropertySet, oep_set_name:str, observable_property_dict: dict[str, peh.ObservableProperty]):
    # TODO: Add dataset level validations
    return ValidationConfig(
        name=oep_set_name,
        columns=[
            convert_peh_observable_property_to_validation_dto_column_validation(op_id, observable_property_dict[op_id])
            for op_id in oep_set.required_observable_property_id_list + oep_set.optional_observable_property_id_list
        ],
        identifying_column_names=oep_set.identifying_observable_property_id_list,
        validations=[]
    )

class ValidationService():
    def __init__(self, inbound_adapter: DataProcessingInterface, persistence_adapter: PersistenceInterface, validation_adapter: ValidationInterface):
        self.inbound_adapter = inbound_adapter
        self.persistence_adapter = persistence_adapter
        self.validation_adapter = validation_adapter

    def validate_data(self, data: dict[str, Sequence], observation: peh.Observation, observable_property_dict: dict[str, peh.ObservableProperty]):
        result_dict = {}
        for cnt, oep_set in enumerate(observation.observation_design.observable_entity_property_sets):
            oep_set_name = f"{oep_set}_{cnt:0>2}"
            validation_dto_config = convert_peh_validation_config_to_validation_dto_config(oep_set, oep_set_name, observable_property_dict)
            print(validation_dto_config)
            result_dict[oep_set_name] = self.validation_adapter.validate(data, validation_dto_config)

class DataTemplateService():
    def __init__(self, inbound_adapter: DataProcessingInterface, persistence_adapter: PersistenceInterface, export_adapter: ExportInterface):
        self.inbound_adapter = inbound_adapter
        self.persistence_adapter = persistence_adapter
        self.export_adapter = export_adapter

    def create_empty_data_template(self, config: DataView, data_layout_id: str, target_path: str):
        #export_info = self.inbound_adapter.get_export_info(project_name="p", data_layout="l", target_filepath="output.xlsx")
        #target = export_info["target_filepath"]
        #config = self.persistence_adapter.load(config_source)
        observable_property_dict = {op.id: op for op in list(config.view_all()) if isinstance(op, peh.ObservableProperty)}
        self.export_adapter.export(export_type=ExportTypeEnum.EmptyDataTemplate, config=config, observable_property_dict=observable_property_dict,
                                   data_layout_id=data_layout_id, target_path=target_path)
