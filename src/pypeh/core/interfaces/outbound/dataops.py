"""
Each of these Interface subclasses provides a protocol on how
the corresponding Adapter subclass should be implemented.

Usage: TODO: add usage info

"""

from __future__ import annotations
import importlib

import logging

from abc import abstractmethod
from collections import defaultdict
from peh_model import peh
from typing import TYPE_CHECKING, Generic, cast, List

from pypeh.core.cache.containers import CacheContainerView
from pypeh.core.models.constants import ObservablePropertyValueType
from pypeh.core.models.internal_data_layout import Dataset, DatasetSchemaElement, DatasetSeries
from pypeh.core.models.typing import T_DataType
from pypeh.core.models.settings import FileSystemSettings
from pypeh.core.models import validation_dto
from pypeh.core.session.connections import ConnectionManager

if TYPE_CHECKING:
    from typing import Sequence, Any
    from pypeh.core.models.validation_errors import ValidationErrorReport

logger = logging.getLogger(__name__)


class OutDataOpsInterface(Generic[T_DataType]):
    """
    Example of DataOps methods
    def validate(self, data: Mapping, config: Mapping):
        pass

    def summarize(self, dat: Mapping, config: Mapping):
        pass
    """

    @abstractmethod
    def _join_data(
        self,
        data: T_DataType,
        other_data: list[T_DataType],
        join_on: list[str],
        subset_fields_other: list[list[str]],
    ) -> T_DataType:
        raise NotImplementedError

    @abstractmethod
    def select_field(self, dataset, field_label: str):
        raise NotImplementedError

    @abstractmethod
    def get_element_labels(self, data: T_DataType) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_element_values(self, data: T_DataType, element_label: str, as_list=True) -> set[str] | list[str]:
        raise NotImplementedError

    @abstractmethod
    def subset(
        self,
        data: T_DataType,
        element_group: list[str],
        id_group: list[tuple[Any]] | None = None,
        identifying_elements: list[str] | None = None,
    ) -> T_DataType: ...

    def relabel(self, data: T_DataType, element_mapping: dict[str, str]) -> T_DataType: ...

    @abstractmethod
    def collect(self, datasets: dict):
        raise NotImplementedError

    @abstractmethod
    def type_mapper(self, peh_value_type: str | ObservablePropertyValueType):
        raise NotImplementedError


class ValidationInterface(OutDataOpsInterface, Generic[T_DataType]):
    @abstractmethod
    def _validate(
        self, data: dict[str, Sequence] | T_DataType, config: validation_dto.ValidationConfig
    ) -> ValidationErrorReport:
        raise NotImplementedError

    @classmethod
    def get_default_adapter_class(cls):
        try:
            adapter_module = importlib.import_module(
                "pypeh.adapters.outbound.validation.pandera_adapter.validation_adapter"
            )
            adapter_class = getattr(adapter_module, "DataFrameValidationAdapter")
        except Exception as e:
            logger.error("Exception encountered while attempting to import a Pandera-based DataFrameAdapter")
            raise e
        return adapter_class

    def build_column_validation(
        self,
        dataset_schema_element: DatasetSchemaElement,
        type_annotations: dict[str, dict[str, ObservablePropertyValueType]],
        cache_view: CacheContainerView,
        dataset_label: str | None = None,
    ) -> validation_dto.ColumnValidation:
        validations = []
        observable_property_id = dataset_schema_element.observable_property_id
        observable_property = cache_view.get(observable_property_id, "ObservableProperty")
        assert isinstance(
            observable_property, peh.ObservableProperty
        ), f"ObservableProperty with id {observable_property_id} not found"
        required = observable_property.default_required
        nullable = not required

        if validation_designs := getattr(observable_property, "validation_designs", None):
            validations.extend(
                [
                    validation_dto.ValidationDesign.from_peh(
                        vd, type_annotations=type_annotations, dataset_label=dataset_label
                    )
                    for vd in validation_designs
                ]
            )
        if value_metadata := getattr(observable_property, "value_metadata", None):
            validations.extend(
                validation_dto.ValidationDesign.list_from_metadata(
                    value_metadata, type_annotations=type_annotations, dataset_label=dataset_label
                )
            )
        if getattr(observable_property, "categorical", None):
            value_options = getattr(observable_property, "value_options", None)
            assert (
                value_options is not None
            ), f"ObservableProperty {observable_property} lacks `value_options` for categorical type"
            assert dataset_schema_element.data_type == ObservablePropertyValueType.STRING
            validation_arg_values: list[str] = [str(vo.key) for vo in value_options]
            expr = validation_dto.ValidationExpression(
                command="is_in",
                arg_values=validation_arg_values,
                arg_columns=None,
                subject=None,
            )
            validation = validation_dto.ValidationDesign(
                name="check_categorical",
                error_level=validation_dto.ValidationErrorLevel.ERROR,
                expression=expr,
            )
            validations.append(validation)

        assert dataset_schema_element.data_type.value != "decimal"
        # transformation using context_magic
        assert isinstance(required, bool)
        return validation_dto.ColumnValidation(
            unique_name=dataset_schema_element.label,
            data_type=dataset_schema_element.data_type.value,
            required=required,
            nullable=nullable,
            validations=validations,
        )

    def collect_column_validations(
        self,
        dataset: Dataset,
        type_annotations: dict[str, dict[str, ObservablePropertyValueType]],
        cache_view: CacheContainerView,
    ) -> list[validation_dto.ColumnValidation]:
        column_validations: list[validation_dto.ColumnValidation] = []
        non_empty_columns = dataset.non_empty
        assert non_empty_columns is not None
        for non_empty_column in non_empty_columns:
            dataset_schema_element = dataset.get_schema_element_by_label(non_empty_column)
            assert dataset_schema_element is not None
            column_validation = self.build_column_validation(
                dataset_schema_element=dataset_schema_element,
                cache_view=cache_view,
                type_annotations=type_annotations,
            )
            column_validations.append(column_validation)

        return column_validations

    def build_dataset_level_validations(
        self,
        dataset: Dataset,
        dataset_series: DatasetSeries | None = None,
        cache_view: CacheContainerView | None = None,
    ) -> list[validation_dto.ValidationDesign] | None:
        dataset_level_validations: list[validation_dto.ValidationDesign] = []
        if cache_view is None:
            raise NotImplementedError

        if dataset_series is not None:
            type_annotations = dataset_series.get_type_annotations()
        else:
            type_annotations = {dataset.label: dataset.get_type_annotations()}
        layout_section_id = dataset.described_by
        if layout_section_id is None:
            return None
        assert isinstance(layout_section_id, str)
        layout_section = cache_view.get(layout_section_id, "DataLayoutSection")
        assert layout_section is not None
        assert isinstance(layout_section, peh.DataLayoutSection)
        if layout_section.validation_designs:
            for vd in layout_section.validation_designs:
                assert isinstance(vd, peh.ValidationDesign)
                dataset_validation = validation_dto.ValidationDesign.from_peh(vd, type_annotations)
                # For an expression that relies on a field reference spec for its arguments, set the validation arguments
                # as the actual values from the dataset (e.g. for an "is_in" check on a foreign key relation)
                validation_expression = vd.validation_expression
                assert isinstance(validation_expression, peh.ValidationExpression)
                if validation_expression.validation_arg_contextual_field_references:
                    arg_values = validation_expression.validation_arg_values
                    assert isinstance(arg_values, list)
                    for ref in validation_expression.validation_arg_contextual_field_references:
                        assert isinstance(ref, peh.ContextualFieldReference)
                        dataset_label = ref.dataset_label
                        assert dataset_label is not None
                        field_label = ref.field_label
                        assert field_label is not None
                        if dataset_series is None:
                            assert dataset_label == dataset.label
                            dependent_dataset = dataset
                        else:
                            dependent_dataset = dataset_series[dataset_label]
                        assert dependent_dataset is not None
                        # fix this at the Interface level
                        column_arg_values = self.get_element_values(dependent_dataset.data, field_label, as_list=True)
                        arg_values.extend(column_arg_values)
                    dataset_validation.expression.arg_values = arg_values
                    dataset_validation.expression.arg_columns = None
                    dataset_level_validations.append(dataset_validation)

        return dataset_level_validations

    @classmethod
    def merge_contextual_field_reference_dependencies(
        cls, column_validations: list[validation_dto.ColumnValidation]
    ) -> dict[str, set[str]]:
        dependent_contextual_field_references = defaultdict(set)
        for column_validation in column_validations:
            if column_validation.validations is not None:
                for validation_design in column_validation.validations:
                    dependency_dict = validation_design.dependent_contextual_field_references
                    if dependency_dict is not None:
                        for ds, fields in dependency_dict.items():
                            dependent_contextual_field_references[ds].update(fields)

        return dependent_contextual_field_references

    def build_validation_config(
        self,
        dataset: Dataset,
        dataset_series: DatasetSeries | None = None,
        cache_view: CacheContainerView | None = None,
    ) -> validation_dto.ValidationConfig:
        column_validations = []
        dataset_validations = []
        if cache_view is None:
            raise NotImplementedError("The absence of a CacheView is currently not supported")

        identifying_column_names = dataset.get_primary_keys()
        if identifying_column_names is not None:
            identifying_column_names = list(identifying_column_names)

        if dataset_series is None:
            type_annotations = {dataset.label: dataset.get_type_annotations()}
        else:
            type_annotations = dataset_series.get_type_annotations()
        column_validations = self.collect_column_validations(
            dataset=dataset,
            type_annotations=type_annotations,
            cache_view=cache_view,
        )
        dependent_contextual_field_references = self.merge_contextual_field_reference_dependencies(column_validations)
        dataset_validations = self.build_dataset_level_validations(
            dataset=dataset,
            dataset_series=dataset_series,
            cache_view=cache_view,
        )

        return validation_dto.ValidationConfig(
            name=dataset.label,
            columns=column_validations,
            identifying_column_names=identifying_column_names,
            validations=dataset_validations,
            dependent_contextual_field_references=dependent_contextual_field_references,
        )

    def validate(
        self,
        dataset: Dataset[T_DataType],
        dependent_dataset_series: DatasetSeries[T_DataType] | None = None,
        cache_view: CacheContainerView | None = None,
    ) -> ValidationErrorReport:
        assert dataset.data is not None
        assert cache_view is not None
        to_validate = dataset.data
        assert to_validate is not None

        validation_config = self.build_validation_config(
            dataset,
            dependent_dataset_series,
            cache_view,
        )
        # check whether data requires join to perform validation (cross DataLayoutSection validation)
        join_required = False
        dependent_contextual_field_references = validation_config.dependent_contextual_field_references
        if dependent_contextual_field_references is not None:
            join_required = len(dependent_contextual_field_references) > 0

        if join_required:
            if dependent_dataset_series is None:
                me = "`dependent_data` is required to perform all validations with `ValidationInterface`"
                logger.error(me)
                raise ValueError(me)
            else:
                assert (
                    dependent_contextual_field_references is not None
                ), "dependent_contextual_field_references in `ValidationInterface.validate` should not be None"
                assert dependent_dataset_series is not None
                identifying_field_labels = dataset.schema.primary_keys
                assert identifying_field_labels is not None
                all_other_data = []
                subset_fields_other = []

                for dataset_label, dependent_field_labels in dependent_contextual_field_references.items():
                    other_dataset = dependent_dataset_series[dataset_label]
                    assert other_dataset is not None
                    other_data = other_dataset.data
                    assert other_data is not None
                    all_other_data.append(other_data)
                    field_subset = [*identifying_field_labels, *dependent_field_labels]
                    subset_fields_other.append(field_subset)
                    to_validate = self._join_data(
                        data=to_validate,
                        other_data=all_other_data,
                        join_on=list(identifying_field_labels),
                        subset_fields_other=subset_fields_other,
                    )

        ret = self._validate(to_validate, validation_config)

        return ret


class DataEnrichmentInterface(OutDataOpsInterface, Generic[T_DataType]):
    @abstractmethod
    def _enrich_data(self, data: dict[str, Sequence] | T_DataType, config: dict) -> T_DataType:
        raise NotImplementedError

    @classmethod
    def get_default_adapter_class(cls):
        try:
            adapter_module = importlib.import_module("pypeh.adapters.outbound.enrichment.dataframe_adapter")
            adapter_class = getattr(adapter_module, "DataFrameEnrichmentAdapter")
        except Exception as e:
            logger.error("Exception encountered while attempting to import enrichment DataFrameAdapter")
            raise e
        return adapter_class

    @abstractmethod
    def apply_joins(self, dataset, datasets, join_specs, node): ...

    @abstractmethod
    def apply_map(self, dataset, map_fn, field_label, output_dtype, base_fields, **kwargs): ...

    @abstractmethod
    def map_type(self, peh_value_type: str): ...


class DataImportInterface(OutDataOpsInterface, Generic[T_DataType]):
    @classmethod
    def get_default_adapter_class(cls):
        try:
            adapter_module = importlib.import_module(
                "pypeh.adapters.outbound.validation.pandera_adapter.validation_adapter"
            )
            adapter_class = getattr(adapter_module, "DataFrameValidationAdapter")
        except Exception as e:
            logger.error("Exception encountered while attempting to import a Pandera-based DataFrameAdapter")
            raise e
        return adapter_class

    @abstractmethod
    def import_data(self, source: str, config: FileSystemSettings) -> T_DataType | List[T_DataType]:
        raise NotImplementedError

    def import_data_layout(
        self,
        source: str,
        config: FileSystemSettings,
        **kwargs,
    ) -> peh.DataLayout | List[peh.DataLayout]:
        provider = ConnectionManager._create_adapter(config)
        layout = provider.load(source)
        if isinstance(layout, peh.EntityList):
            layout = layout.layouts
        if isinstance(layout, list):
            if not all(isinstance(item, peh.DataLayout) for item in layout):
                me = "Imported layouts should all be DataLayout instances"
                logger.error(me)
                raise TypeError(me)
            return cast(List[peh.DataLayout], layout)

        elif isinstance(layout, peh.DataLayout):
            return layout

        else:
            me = f"Imported layout should be a DataLayout instance not {type(layout)}"
            logger.error(me)
            raise TypeError(me)

    def _extract_observed_value_provenance(self) -> bool:
        return True

    def _normalize_observable_properties(self) -> bool:
        raise NotImplementedError

    def _raw_data_to_observation_data(
        self,
        raw_data: T_DataType,
        data_layout_element_labels: list[str],
        identifying_layout_element_label: str,
        entity_id_list: list[str] | None = None,
    ) -> T_DataType:
        raise NotImplementedError
