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
from typing import TYPE_CHECKING, Callable, Generic

from pypeh.core.cache.containers import CacheContainerView
from pypeh.core.models.constants import ObservablePropertyValueType
from pypeh.core.models.internal_data_layout import Dataset, DatasetSchemaElement, DatasetSeries, ContextIndexProtocol
from pypeh.core.models.typing import T_DataType
from pypeh.core.models import graph, validation_dto
from pypeh.core.utils.function_utils import _extract_callable

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

    @classmethod
    def get_default_adapter_class(cls):
        try:
            adapter_module = importlib.import_module("pypeh.adapters.outbound.dataops.dataframe_adapter")
            adapter_class = getattr(adapter_module, "DataFrameAdapter")
        except Exception as e:
            logger.error("Exception encountered while attempting to import dataops DataFrameAdapter")
            raise e
        return adapter_class

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
    def check_element_has_empty_values(self, data: T_DataType, element_label: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def check_element_has_only_empty_values(self, data: T_DataType, element_label: str) -> bool:
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

    def extract_labeled_observable_property_specifications(
        self, observation: peh.Observation, cache_view: CacheContainerView
    ) -> dict[str, peh.ObservablePropertySpecification]:
        ret = {}
        observation_design_id = observation.observation_design
        assert isinstance(observation_design_id, str)
        observation_design = cache_view.get(observation_design_id, "ObservationDesign")
        assert observation_design is not None
        observable_property_specs = observation_design.observable_property_specifications
        assert observable_property_specs is not None
        for observable_property_spec in observable_property_specs:
            observable_property = cache_view.get(observable_property_spec.observable_property, "ObservableProperty")
            assert isinstance(observable_property, peh.ObservableProperty)
            ret[observable_property.ui_label] = observable_property_spec
        return ret

    def get_dataset_by_observation_id(self, dataset_series: DatasetSeries, observation_id: str) -> Dataset:
        gen = dataset_series.get_datasets_by_observation(observation_id)
        dataset = next(gen)
        try:
            _ = next(gen)
            raise AssertionError("Expected only one dataset, but generator yielded more")
        except StopIteration:
            pass
        return dataset

    @abstractmethod
    def normalize_input(self, data: T_DataType) -> T_DataType:
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
        column_has_only_empty_values: bool = False,
        allow_incomplete: bool = False,
    ) -> validation_dto.ColumnValidation:
        # Set default validation check applicability flags
        apply_required_check = True
        apply_nullable_check = True
        apply_property_validation = True
        # Adapt the flags in accordance with the user request, project config and data
        if allow_incomplete:
            apply_required_check = False
            if column_has_only_empty_values:
                apply_nullable_check = False
                apply_property_validation = False

        validations = []
        observable_property_id = dataset_schema_element.observable_property_id
        observable_property = cache_view.get(observable_property_id, "ObservableProperty")
        assert isinstance(
            observable_property, peh.ObservableProperty
        ), f"ObservableProperty with id {observable_property_id} not found"

        if apply_required_check:
            required = observable_property.required
        else:
            required = False

        if apply_nullable_check:
            nullable = not required  # required and nullable are now checking the same thing
        else:
            nullable = True

        if apply_property_validation:
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
                # TODO: ADD CUSTOM CHECKS ON CATEGORICAL VARIABLES HERE
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
                    error_message=None,  # CUSTOM ERROR MESSAGE CANNOT BE PROVIDED OR SHOULD BE HARDCODED
                )
                validations.append(validation)

            if dataset_schema_element.data_type in [
                ObservablePropertyValueType.CATEGORICAL,
                ObservablePropertyValueType.STRING,
            ]:
                expr = validation_dto.ValidationExpression(
                    command="trailing_spaces",
                    arg_values=None,
                    arg_columns=None,
                    subject=None,
                )
                validation = validation_dto.ValidationDesign(
                    name="check_trailing_spaces",
                    error_level=validation_dto.ValidationErrorLevel.ERROR,
                    expression=expr,
                    error_message="Trailing/leading spaces were detected. Remove unnecessary white spaces.",
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
        allow_incomplete: bool = False,
    ) -> list[validation_dto.ColumnValidation]:
        column_validations: list[validation_dto.ColumnValidation] = []
        column_labels = dataset.get_element_labels()
        assert column_labels is not None
        for column_label in column_labels:
            dataset_schema_element = dataset.get_schema_element_by_label(column_label)
            assert dataset_schema_element is not None
            # Check whether the dataset has data in the column
            if dataset.data is None:
                column_has_only_empty_values = True
            else:
                column_has_only_empty_values = self.check_element_has_only_empty_values(
                    data=dataset.data,
                    element_label=column_label,
                )
            column_validation = self.build_column_validation(
                dataset_schema_element=dataset_schema_element,
                cache_view=cache_view,
                type_annotations=type_annotations,
                column_has_only_empty_values=column_has_only_empty_values,
                allow_incomplete=allow_incomplete,
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
        allow_incomplete: bool = False,
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
            allow_incomplete=allow_incomplete,
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
        allow_incomplete: bool = False,
    ) -> ValidationErrorReport:
        assert dataset.data is not None
        assert cache_view is not None
        to_validate = dataset.data
        assert to_validate is not None

        validation_config = self.build_validation_config(
            dataset=dataset,
            dataset_series=dependent_dataset_series,
            cache_view=cache_view,
            allow_incomplete=allow_incomplete,
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
    def apply_joins(self, datasets, join_specs, node): ...

    @abstractmethod
    def apply_map(self, dataset, map_fn, field_label, output_dtype, base_fields, **kwargs): ...

    @abstractmethod
    def map_type(self, peh_value_type: str): ...

    def build_callable(self, delayed_node: graph.Delayed) -> Callable:
        map_fn = delayed_node.map_fn
        arg_sources = delayed_node.arg_sources
        join_specs = delayed_node.join_specs
        output_dtype = self.map_type(delayed_node.output_dtype)

        def _apply(datasets: dict, *, node: graph.Node, base_fields: dict):
            """
            datasets: dict of dataset_label → dataset object (lazy or eager)
            parent_results: mapping from parent Node → computed result for this node
            """
            ds = datasets[node.dataset_label]
            # Apply all joins
            if join_specs:
                ds = self.apply_joins(
                    datasets=datasets,
                    join_specs=join_specs,
                    node=node,
                )

            # Build column expressions for the map function
            kwargs = {}
            for arg_name, parent_node in arg_sources.items():
                col_name = parent_node.field_label
                kwargs[arg_name] = self.select_field(ds, col_name)

            # Apply the map
            base_fields_subset = base_fields.get(node.dataset_label, None)
            assert base_fields_subset is not None
            out = self.apply_map(ds, map_fn, node.field_label, output_dtype, base_fields_subset, **kwargs)
            base_fields_subset.append(node.field_label)

            return out

        return _apply

    def compile_dependency_graph(self, dependency_graph: graph.Graph) -> graph.ExecutionPlan:
        sorted_nodes = dependency_graph.topological_sort()
        steps: list[graph.ExecutionStep] = []

        for node in sorted_nodes:
            delayed = dependency_graph.delayed_fns.get(node)
            if delayed is None:
                continue

            compute_fn = self.build_callable(delayed)
            steps.append(graph.ExecutionStep(node=node, compute=compute_fn))

        ret = graph.ExecutionPlan(steps)
        dependency_graph.execution_plan = ret

        return ret

    def compute_with_dependency_graph(self, dependency_graph: graph.Graph, datasets: dict[str, Dataset]):
        if dependency_graph.execution_plan is None:
            raise AssertionError("A dependency graph needs to be compiled first to set up an execution plan")

        raw_datasets = {label: dataset.data for label, dataset in datasets.items()}
        base_fields = {label: dataset.get_element_labels() for label, dataset in datasets.items()}

        dependency_graph.execution_plan.run(raw_datasets, base_fields)
        self.collect(datasets)
        for dataset_label in datasets:
            datasets[dataset_label].data = raw_datasets[dataset_label]

    def build_dependency_graph(
        self,
        observations: list[peh.Observation],
        context_index: ContextIndexProtocol,
        cache_view: CacheContainerView,
        join_spec_mapping: dict | None = None,
    ) -> graph.Graph:
        dependency_graph = graph.Graph()
        # the source_observations also need to be added to the dependency graph!!!!!
        for observation in observations:
            assert observation.observation_design is not None
            assert isinstance(observation.observation_design, peh.ObservationDesignId)
            observation_design = cache_view.get(observation.observation_design, "ObservationDesign")
            assert isinstance(observation_design, peh.ObservationDesign)
            for observable_property_spec in observation_design.observable_property_specifications:
                assert isinstance(
                    observable_property_spec.observable_property,
                    (peh.ObservableProperty, peh.ObservablePropertyId, str),
                )
                if isinstance(observable_property_spec.observable_property, (peh.ObservablePropertyId, str)):
                    observable_property = cache_view.get(
                        observable_property_spec.observable_property, "ObservableProperty"
                    )
                elif isinstance(observable_property_spec.observable_property, peh.ObservableProperty):
                    observable_property = observable_property_spec.observable_property
                assert observable_property is not None
                assert isinstance(observable_property, peh.ObservableProperty)
                target_contextual_field_ref = context_index.context_lookup(observation.id, observable_property.id)
                assert (
                    target_contextual_field_ref is not None
                ), f"Target contextual reference could not be found for property {observable_property.id} in observation {observation.id}."
                target_dataset_label, target_field_label = target_contextual_field_ref
                if observable_property.calculation_design is not None:
                    # EXTRA INFO FROM CALCULATION DESIGN AND UPDATE DEPENDENCY GRAPH
                    calculation_design = observable_property.calculation_design
                    assert isinstance(calculation_design, peh.CalculationDesign)
                    calculation_implementation = calculation_design.calculation_implementation
                    assert isinstance(calculation_implementation, peh.CalculationImplementation)
                    function_name = calculation_implementation.function_name
                    assert isinstance(function_name, str)
                    output_dtype = observable_property.value_type
                    assert output_dtype is not None

                    child = graph.Node(dataset_label=target_dataset_label, field_label=target_field_label)
                    dependency_graph.add_calculation_target(
                        child, function_name=function_name, result_dtype=output_dtype
                    )
                    function_kwargs = calculation_implementation.function_kwargs
                    assert function_kwargs is not None
                    for function_kwarg in function_kwargs:
                        assert isinstance(function_kwarg, peh.CalculationKeywordArgument)
                        source_field_ref = function_kwarg.contextual_field_reference
                        assert isinstance(source_field_ref, peh.ContextualFieldReference)
                        source_dataset_label = source_field_ref.dataset_label
                        assert source_dataset_label is not None
                        source_field_label = source_field_ref.field_label
                        assert source_field_label is not None
                        parent = graph.Node(dataset_label=source_dataset_label, field_label=source_field_label)
                        map_name = function_kwarg.mapping_name
                        assert map_name is not None
                        join_spec = None
                        if join_spec_mapping is not None:
                            join_spec = join_spec_mapping.get(
                                frozenset([target_dataset_label, source_dataset_label]), None
                            )
                            if join_spec is not None:
                                assert len(join_spec) == 1, "Complex JoinSpecs are not supported yet."
                        dependency_graph.add_calculation_source(
                            parent,
                            child,
                            map_name,
                            join_spec=join_spec,
                        )

        return dependency_graph

    def enrich(
        self,
        source_dataset_series: DatasetSeries,
        target_observations: list[peh.Observation],
        target_derived_from: list[peh.Observation],
        cache_view: CacheContainerView,
    ) -> DatasetSeries:
        # ADD TARGET OBSERVATION TO SOURCE_DATASET_SERIES
        for source_obs, target_observation in zip(target_derived_from, target_observations):
            # TODO: ENSURE PREREQUISTE IS MET: DATASETSERIES SPLIT INTO OBSERVATIONS
            source_dataset = self.get_dataset_by_observation_id(
                dataset_series=source_dataset_series, observation_id=source_obs.id
            )
            assert source_dataset is not None
            source_dataset_label = source_dataset.label
            labeled_observable_property_specifications = self.extract_labeled_observable_property_specifications(
                target_observation, cache_view=cache_view
            )
            source_dataset_series.add_observation(
                source_dataset_label,
                target_observation,
                labeled_observable_property_specifications=labeled_observable_property_specifications,
                cache_view=cache_view,
            )
        join_spec_mapping = source_dataset_series.resolve_all_joins()
        # BUILD DEPENDENCY GRAPH
        all_observations = []
        for nested_observations in source_dataset_series.observations.values():
            for observation_id in nested_observations:
                observation = cache_view.get(observation_id, "Observation")
                assert observation is not None
                all_observations.append(observation)

        dependency_graph = self.build_dependency_graph(
            observations=all_observations,
            context_index=source_dataset_series,
            join_spec_mapping=join_spec_mapping,
            cache_view=cache_view,
        )
        # EXECUTE THE DEFINED COMPUTATIONS
        self.compile_dependency_graph(dependency_graph=dependency_graph)
        self.compute_with_dependency_graph(
            dependency_graph=dependency_graph,
            datasets=source_dataset_series.parts,
        )
        # RETURN THE UPDATED SOURCE_DATASET_SERIES
        return source_dataset_series


class AggregationInterface(OutDataOpsInterface, Generic[T_DataType]):
    @abstractmethod
    def _calculate_for_stratum(
        self, df: T_DataType, group_cols: list[str] | None, value_col: str, stat_builders: list, **kwargs
    ) -> T_DataType:
        raise NotImplementedError

    @abstractmethod
    def calculate_for_strata(
        self,
        df: T_DataType,
        stratifications: list[list[str]] | None,
        value_col: str,
        stat_builders: list[str],
        **kwargs,
    ) -> T_DataType:
        raise NotImplementedError

    @abstractmethod
    def group_results(self, results_to_collect: list[T_DataType], strata: list[str] | None = None) -> T_DataType:
        raise NotImplementedError

    @classmethod
    def get_default_adapter_class(cls):
        try:
            adapter_module = importlib.import_module(
                "pypeh.adapters.outbound.aggregation.polars_adapter.dataframe_adapter"
            )

            adapter_class = getattr(adapter_module, "DataFrameAggregationAdapter")
        except Exception as e:
            logger.error("Exception encountered while attempting to import a Polars-based DataFrameAggregationAdapter")
            raise e
        return adapter_class

    def summarize(
        self,
        source_dataset_series: DatasetSeries,
        target_observations: list[peh.Observation],
        target_derived_from: list[peh.Observation],
        cache_view: CacheContainerView,
        id_factory: Callable[[], str] | None = None,
    ) -> DatasetSeries:
        # ADD TARGET OBSERVATION TO A NEW DATASET_SERIES
        aggregated_dataset_series: DatasetSeries = DatasetSeries(
            label=f"{source_dataset_series.label}_aggregated", id_factory=id_factory
        )
        assert len(target_observations) == len(target_derived_from)

        for source_obs, target_observation in zip(target_derived_from, target_observations):
            collected_results = []
            # FOR LOOP COMPUTES ALL SUMMARY STATS ASSOCIATED WITH A SINGLE SOURCE OBSERVABLE PROPERTY
            source_dataset = self.get_dataset_by_observation_id(
                dataset_series=source_dataset_series, observation_id=source_obs.id
            )
            source_data = source_dataset.data
            assert source_data is not None

            # COMPILE LIST OF LABELED OBSERVABLE PROPERTIES FOR TARGET
            if label := target_observation.ui_label:
                labeled_observable_property_specifications = self.extract_labeled_observable_property_specifications(
                    target_observation, cache_view=cache_view
                )
                target_dataset = aggregated_dataset_series.add_observation(
                    label,
                    target_observation,
                    labeled_observable_property_specifications=labeled_observable_property_specifications,
                    cache_view=cache_view,
                )
            else:
                raise ValueError(
                    f"Source observation {source_obs.id} lacks a `ui_label` which is required to add the target observation to the DatasetSeries"
                )

            # COMPILE STRATIFICATIONS
            stratification_ids = []
            # COMPILE AGGREGATION FUNCTION LIST
            map_fn_list = []
            map_fn_result_label_list = []
            # LOOP OVER ALL OBSERVABLE PROPERTY SPECIFICATIONS
            target_observation_design_id = target_observation.observation_design
            assert isinstance(target_observation_design_id, str)
            target_observation_design = cache_view.get(target_observation_design_id, "ObservationDesign")
            assert isinstance(target_observation_design, peh.ObservationDesign)
            observable_property_specs = target_observation_design.observable_property_specifications
            assert observable_property_specs is not None
            for observable_property_spec in observable_property_specs:
                assert isinstance(observable_property_spec, peh.ObservablePropertySpecification)
                observable_property_id = observable_property_spec.observable_property
                assert isinstance(observable_property_id, str)
                observable_property = cache_view.get(observable_property_id, "ObservableProperty")
                assert isinstance(observable_property, peh.ObservableProperty)
                specification_category = observable_property_spec.specification_category
                assert specification_category is not None
                identifying = (
                    str(specification_category) == peh.ObservablePropertySpecificationCategory.identifying.text
                )
                if identifying:
                    stratification_ids.append(observable_property_id)

                # EXTRACT CALCULATION DESIGN
                if observable_property.calculation_design is not None:
                    calculation_design = observable_property.calculation_design
                    assert isinstance(calculation_design, peh.CalculationDesign)
                    calculation_implementation = calculation_design.calculation_implementation
                    assert isinstance(calculation_implementation, peh.CalculationImplementation)
                    output_dtype = ObservablePropertyValueType(getattr(observable_property, "value_type", "string"))
                    assert output_dtype is not None
                    function_name = calculation_implementation.function_name
                    assert function_name is not None
                    map_fn = _extract_callable(function_name)
                    map_fn_list.append(map_fn)
                    if function_kwargs := calculation_implementation.function_kwargs:
                        for function_kwarg in function_kwargs:
                            assert isinstance(function_kwarg, peh.CalculationKeywordArgument)
                            source_field_ref = function_kwarg.contextual_field_reference
                            assert isinstance(source_field_ref, peh.ContextualFieldReference)
                            kwarg_source_observation_id = source_field_ref.dataset_label
                            assert kwarg_source_observation_id is not None
                            if kwarg_source_observation_id != source_obs.id:
                                raise ValueError(
                                    f"All CalculationKeywordArguments should refer to specified source observation: {source_obs.id}"
                                )
                            kwarg_source_observable_property_id = source_field_ref.field_label
                            assert kwarg_source_observable_property_id is not None
                            _, source_element_label = source_dataset_series.context_lookup(
                                kwarg_source_observation_id, kwarg_source_observable_property_id
                            )
                    if function_results := calculation_implementation.function_results:
                        for function_result in function_results:
                            # there can only be one such name !!!!!
                            if len(function_results) > 1:
                                raise NotImplementedError(
                                    "Only a single function result is currently supported by the AggregationInterface"
                                )
                            assert isinstance(function_result, peh.CalculationResult)
                            mapping_name = function_result.mapping_name
                            map_fn_result_label_list.append(mapping_name)

            # LOOKUP STRATIFICATION LABELS
            stratification_labels = None
            if len(stratification_ids) > 0:
                stratification_labels = []
                for strat_id in stratification_ids:
                    _, element_label = source_dataset_series.context_lookup(source_obs.id, strat_id)
                    stratification_labels.append(element_label)
            # COMPUTE SUMMARY STAT FOR SINGLE SOURCE ELEMENT
            assert source_element_label is not None
            target_data = self._calculate_for_stratum(
                df=self.normalize_input(source_data),
                group_cols=stratification_labels,
                value_col=source_element_label,
                stat_builders=map_fn_list,
                result_aliases=map_fn_result_label_list,
            )
            collected_results.append(target_data)

            target_data = self.group_results(collected_results, strata=stratification_labels)
            target_dataset.add_data(data=target_data)

        return aggregated_dataset_series
