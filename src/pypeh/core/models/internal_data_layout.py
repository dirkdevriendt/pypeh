from __future__ import annotations

import itertools
import logging
import uuid

from collections import defaultdict
from dataclasses import dataclass, field
from peh_model import peh
from typing import TYPE_CHECKING, Generic, Sequence

from pypeh.core.cache.containers import CacheContainer, CacheContainerView
from pypeh.core.models.typing import T_DataType
from pypeh.core.models.constants import ObservablePropertyValueType

if TYPE_CHECKING:
    from typing import Any
    from pypeh.core.interfaces.outbound.dataops import OutDataOpsInterface


logger = logging.getLogger(__name__)
CSVW_CONTEXT = {"csvw": "http://www.w3.org/ns/csvw#"}
DCAT_CONTEXT = {"dcat": "http://www.w3.org/ns/dcat#"}


@dataclass
class JoinSpec:
    left_element: str
    left_dataset: str
    right_element: str
    right_dataset: str


@dataclass
class DatasetSchemaElement:
    label: str
    observable_property_id: str
    data_type: ObservablePropertyValueType

    @classmethod
    def from_peh_data_layout_element(cls, data_layout_element: peh.DataLayoutElement, cache_view: CacheContainerView):
        label = data_layout_element.label
        assert label is not None
        observable_property_id = data_layout_element.observable_property
        assert observable_property_id is not None
        observable_property = cache_view.get(observable_property_id, "ObservableProperty")
        assert isinstance(observable_property, peh.ObservableProperty)
        data_type = getattr(observable_property, "value_type")

        return cls(
            label=label,
            observable_property_id=observable_property.id,
            data_type=ObservablePropertyValueType(data_type),
        )


@dataclass
class ElementReference:
    dataset_label: str = field(metadata={"id": "resource", "context": CSVW_CONTEXT})
    element_label: str = field(metadata={"id": "columnReference", "context": CSVW_CONTEXT})

    __metadata__ = {
        "id": "csvw:TableReference",
        "context": CSVW_CONTEXT,
    }


@dataclass
class ForeignKey:
    element_label: str = field(metadata={"id": "columnReference", "context": CSVW_CONTEXT})
    reference: ElementReference = field(metadata={"id": "reference", "context": CSVW_CONTEXT})

    __metadata__ = {
        "id": "csvw:ForeignKey",
        "context": CSVW_CONTEXT,
    }


@dataclass
class DatasetSchema:
    elements: dict[str, DatasetSchemaElement] = field(default_factory=dict)
    primary_keys: set[str] = field(default_factory=set)
    foreign_keys: dict[str, ForeignKey] = field(default_factory=dict)

    __metadata__ = {
        "id": "csvw:Schema",
        "context": CSVW_CONTEXT,
    }

    def __post_init__(self):
        self._type = self.get_type_annotations()
        self._elements_by_observable_property = self.build_observable_property_index()

    def get_type_annotations(self) -> dict[str, ObservablePropertyValueType]:
        ret: dict[str, ObservablePropertyValueType] = dict()
        for element in self.elements.values():
            data_type = element.data_type
            if data_type is not None:
                ret[element.label] = data_type

        return ret

    def build_observable_property_index(self) -> dict[str, str]:
        elements_by_observable_property: dict[str, str] = {}
        for element_label, element in self.elements.items():
            elements_by_observable_property[element.observable_property_id] = element_label
        return elements_by_observable_property

    def get_type(self, element_label: str) -> ObservablePropertyValueType:
        return self._type[element_label]

    def get_element_by_label(self, element_label: str) -> DatasetSchemaElement | None:
        return self.elements.get(element_label, None)

    def get_element_by_observable_property_id(self, observable_property_id: str) -> DatasetSchemaElement | None:
        element_label = self._elements_by_observable_property[observable_property_id]
        return self.get_element_by_label(element_label=element_label)

    def get_observable_property_ids(self) -> list[str]:
        return [element.observable_property_id for element in self.elements.values()]

    def get_element_labels(self) -> list[str]:
        return list(self.elements.keys())

    def add_observable_property(
        self,
        observable_property_id: str,
        data_type: ObservablePropertyValueType,
        element_label: str | None = None,
        is_primary_key: bool = False,
    ):
        if element_label is None:
            element_label = observable_property_id
        assert isinstance(data_type, ObservablePropertyValueType)
        new_element = DatasetSchemaElement(
            label=element_label,
            observable_property_id=observable_property_id,
            data_type=data_type,
        )
        self.elements[element_label] = new_element
        self._type[element_label] = data_type
        self._elements_by_observable_property[observable_property_id] = element_label
        if is_primary_key:
            self.primary_keys.add(element_label)

    # TODO: move method, this is probably not the right location
    def apply_context_to_expression(
        self,
        expression: peh.ValidationExpression,
        context: dict[str, dict[str, str]],
        this_dataset: str,  # temporary fix
    ):
        expression_stack = [expression]
        while expression_stack:
            expression = expression_stack.pop()
            assert isinstance(expression, peh.ValidationExpression)
            conditional_expression = expression.validation_condition_expression
            if conditional_expression is not None:
                assert isinstance(conditional_expression, peh.ValidationExpression)
                expression_stack.append(conditional_expression)
            arg_expressions = expression.validation_arg_expressions
            if arg_expressions is not None:
                for arg_expression in arg_expressions:
                    assert isinstance(arg_expression, peh.ValidationExpression)
                    expression_stack.append(arg_expression)

            # apply context
            arg_field_references = expression.validation_arg_contextual_field_references
            if arg_field_references is not None:
                for arg_ref in arg_field_references:
                    assert isinstance(arg_ref, peh.ContextualFieldReference)
                    field_label = arg_ref.field_label
                    if field_label in context:
                        retrieved_context = context[field_label]
                        if len(retrieved_context) == 1:
                            new_dataset_label = next(iter(retrieved_context))
                        else:
                            new_dataset_label = this_dataset
                        new_field_label = retrieved_context[new_dataset_label]
                        arg_ref.dataset_label = new_dataset_label
                        arg_ref.field_label = new_field_label

            subject_field_references = expression.validation_subject_contextual_field_references
            if subject_field_references is not None:
                for subject_field_ref in subject_field_references:
                    assert isinstance(subject_field_ref, peh.ContextualFieldReference)
                    field_label = subject_field_ref.field_label
                    if field_label in context:
                        retrieved_context = context[field_label]
                        if len(retrieved_context) == 1:
                            new_dataset_label = next(iter(retrieved_context))
                        else:
                            new_dataset_label = this_dataset
                        new_field_label = retrieved_context[new_dataset_label]
                        subject_field_ref.dataset_label = new_dataset_label
                        subject_field_ref.field_label = new_field_label

    def apply_context(
        self,
        context: dict[str, dict[str, str]],
        cache: CacheContainer,
        this_dataset: str,  # temporary fix
    ):
        """
        ObservableProperties require context for:
        - CalculationDesigns
        - ValidationDesigns
        """
        for schema_element in self.elements.values():
            observable_property_id = schema_element.observable_property_id
            observable_property = cache.get(observable_property_id, "ObservableProperty")
            assert isinstance(observable_property, peh.ObservableProperty)
            validation_designs = observable_property.validation_designs
            if validation_designs is not None:
                for validation_design in validation_designs:
                    assert isinstance(validation_design, peh.ValidationDesign)
                    expression = validation_design.validation_expression
                    assert isinstance(expression, peh.ValidationExpression)
                    self.apply_context_to_expression(expression, context, this_dataset=this_dataset)

            # CALCULATION DESIGN EXCLUDED FOR NOW
            # calculation_designs = observable_property.calculation_designs
            # if calculation_designs is not None:
            #    for calculation_design in calculation_designs:
            #        pass

    #### RESTRUCTURE DATASETSCHEMA ####

    def subset(self, element_group: Sequence[str]) -> DatasetSchema:
        elements = {}
        foreign_keys = {}
        primary_keys = set()

        for element_label in element_group:
            element = self.get_element_by_label(element_label)
            assert element is not None, f"Element with label {element_label} not found in schema"
            elements[element_label] = element
            if element_label in self.foreign_keys:
                foreign_keys[element_label] = self.foreign_keys[element_label]
            if self.primary_keys is not None:
                if element_label in self.primary_keys:
                    primary_keys.add(element_label)
            elements[element_label] = element

        return DatasetSchema(
            elements=elements,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
        )

    def relabel(self, element_mapping: dict[str, str]):
        elements: dict[str, DatasetSchemaElement] = dict()
        all_type_info: dict[str, ObservablePropertyValueType] = dict()
        elements_by_observable_property: dict[str, str] = {}
        primary_keys: set[str] | None = set()
        foreign_keys: dict[str, ForeignKey] = {}

        for element_label, new_element_label in element_mapping.items():
            schema_element = self.elements.pop(element_label)
            elements[new_element_label] = schema_element
            schema_element.label = new_element_label

            # _type dict
            type_info = self._type.pop(element_label)
            all_type_info[new_element_label] = type_info

            # _elements_by_observable_property
            observable_property_id = schema_element.observable_property_id
            element_label = self._elements_by_observable_property.pop(observable_property_id)
            elements_by_observable_property[observable_property_id] = element_label

            # primary_keys
            if self.primary_keys is not None:
                if element_label in self.primary_keys:
                    self.primary_keys.discard(element_label)
                    primary_keys.add(new_element_label)

            # foreign_keys
            if element_label in self.foreign_keys:
                foreign_key = self.foreign_keys.pop(element_label)
                foreign_keys[new_element_label] = foreign_key

        if len(self.elements) > 0:
            for element in self.elements:
                if element in elements:
                    raise ValueError("Schema element label {element} is non unique")
            elements = {**elements, **self.elements}
            all_type_info = {**all_type_info, **self._type}
            elements_by_observable_property = {
                **elements_by_observable_property,
                **self._elements_by_observable_property,
            }
            foreign_keys = {**foreign_keys, **self.foreign_keys}
            if self.primary_keys is not None:
                primary_keys = primary_keys | self.primary_keys
            else:
                assert len(primary_keys) == 0
                primary_keys = None

        num_elements = len(elements)
        assert len(elements_by_observable_property) == num_elements
        assert len(all_type_info) == num_elements
        assert len(foreign_keys) <= num_elements

        self.elements = elements
        self._elements_by_observable_property = elements_by_observable_property
        self._type = all_type_info
        self.foreign_keys = foreign_keys
        if primary_keys is not None:
            self.primary_keys = primary_keys

    def __len__(self):
        return len(self.elements)

    #### CONSTRUCT DATASETSCHEMA ####

    @classmethod
    def from_peh_data_layout_elements(
        cls, data_layout_elements: list[peh.DataLayoutElement], cache_view: CacheContainerView
    ):
        schema_elements = {}
        processed_foreign_keys = {}
        processed_primary_keys = set()
        for element in data_layout_elements:
            assert isinstance(element, peh.DataLayoutElement)
            element_label = element.label
            assert element_label is not None
            observable_property_id = element.observable_property
            assert observable_property_id is not None
            foreign_key = element.foreign_key_link
            if foreign_key is not None:
                assert isinstance(foreign_key, peh.DataLayoutElementLink)
                section_id = foreign_key.section
                assert isinstance(section_id, str)
                foreign_key_element_label = foreign_key.label
                assert foreign_key_element_label is not None
                section = cache_view.get(section_id, "DataLayoutSection")
                assert section is not None, f"section with id {section_id} cannot be found"
                assert section.ui_label is not None, f"ui_label is None for section {section.id}"
                foreign_key_object = ForeignKey(
                    element_label=element_label,
                    reference=ElementReference(
                        dataset_label=section.ui_label,
                        element_label=foreign_key_element_label,
                    ),
                )
                processed_foreign_keys[element_label] = foreign_key_object

            is_primary_key = element.is_observable_entity_key
            if is_primary_key is not None:
                if is_primary_key:
                    processed_primary_keys.add(element_label)

            schema_elements[element_label] = DatasetSchemaElement.from_peh_data_layout_element(element, cache_view)

        return cls(
            elements=schema_elements,
            foreign_keys=processed_foreign_keys,
            primary_keys=processed_primary_keys,
        )

    #### EXTRACT INFO FROM SCHEMA ####

    def detect_join(
        self,
        dataset_label: str,
        other_schema: DatasetSchema,
        other_dataset_label: str,
    ) -> list[JoinSpec] | None:
        # Case 1: A → B directly
        for col, fk in self.foreign_keys.items():
            if fk.reference.dataset_label == other_dataset_label:
                return [
                    JoinSpec(
                        left_element=fk.element_label,
                        left_dataset=dataset_label,
                        right_element=fk.reference.element_label,
                        right_dataset=other_dataset_label,
                    )
                ]

        # Case 2: B → A directly
        for col, fk in other_schema.foreign_keys.items():
            if fk.reference.dataset_label == dataset_label:
                return [
                    JoinSpec(
                        left_element=fk.reference.element_label,
                        left_dataset=dataset_label,
                        right_element=fk.element_label,
                        right_dataset=other_dataset_label,
                    )
                ]

        # Case 3: shared third dataset: requires two `JoinSpec`
        refs_a = {
            fk.reference.dataset_label: (fk.element_label, fk.reference.element_label)
            for fk in self.foreign_keys.values()
        }
        refs_b = {
            fk.reference.dataset_label: (fk.element_label, fk.reference.element_label)
            for fk in other_schema.foreign_keys.values()
        }

        shared = set(refs_a.keys()).intersection(set(refs_b.keys()))
        if shared:
            shared_label = next(iter(shared))
            a_col_local, a_other = refs_a[shared_label]
            b_col_local, b_other = refs_b[shared_label]

            return [
                JoinSpec(
                    left_element=a_col_local,
                    left_dataset=dataset_label,
                    right_element=a_other,
                    right_dataset=shared_label,
                ),
                JoinSpec(left_element=b_col_local, left_dataset="", right_element=b_other, right_dataset=shared_label),
            ]

        return None


@dataclass(kw_only=True)
class Resource:
    """
    Parent class for Dataset and DatasetSeries
    """

    label: str
    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict[str, Any] = field(default_factory=dict)

    __metadata__ = {
        "id": "dcat:resource",
        "context": DCAT_CONTEXT,
    }

    def add_metadata(self, metadata_key: str, metadata_value: Any) -> bool:
        if metadata_key in self.metadata:
            raise KeyError(f"{metadata_key} key already used in metadata mapping")
        self.metadata[metadata_key] = metadata_value

        return True

    @property
    def described_by(self) -> str | peh.NamedThingId | None:
        return self.metadata.get("described_by", None)


@dataclass(kw_only=True)
class Dataset(Resource, Generic[T_DataType]):
    schema: DatasetSchema = field(default_factory=DatasetSchema)
    data: T_DataType | None = field(default=None)
    part_of: DatasetSeries | None = field(default=None)
    observations: set[str] = field(default_factory=set)

    __metadata__ = {
        "id": "dcat:dataset",
        "context": DCAT_CONTEXT,
    }

    def get_type_annotations(self) -> dict[str, ObservablePropertyValueType]:
        return self.schema.get_type_annotations()

    #### CONSTRUCT DATASET ####

    @classmethod
    def from_peh_datalayout_section(
        cls,
        data_layout_section: peh.DataLayoutSection,
        cache_view: CacheContainerView,
        part_of_dataset_series: DatasetSeries | None = None,
    ) -> Dataset[T_DataType]:
        label = data_layout_section.ui_label
        assert label is not None
        elements = getattr(data_layout_section, "elements")
        if elements is not None:
            for element in elements:
                assert isinstance(element, peh.DataLayoutElement)
                observable_property_id = element.observable_property
                assert (
                    observable_property_id is not None
                ), f"could not find an observable_property field for {element.label}"

        schema = DatasetSchema.from_peh_data_layout_elements(elements, cache_view)

        ret: Dataset[T_DataType] = cls(
            label=label,
            schema=schema,
            part_of=part_of_dataset_series,
        )
        _ = ret.add_metadata("described_by", data_layout_section.id)

        return ret

    #### EXTRACT INFO FROM DATASET ####

    @property
    def non_empty(self):
        return self.metadata.get("non_empty_dataset_elements", None)

    def get_element_labels(self) -> list[str]:
        return self.schema.get_element_labels()

    def get_schema_element_by_label(self, element_label: str) -> DatasetSchemaElement | None:
        return self.schema.get_element_by_label(element_label)

    def get_schema_element_by_observable_property_id(self, observable_property_id: str) -> DatasetSchemaElement | None:
        return self.schema.get_element_by_observable_property_id(observable_property_id)

    def get_observable_property_ids(self) -> list[str]:
        return self.schema.get_observable_property_ids()

    def get_primary_keys(self) -> set[str] | None:
        return self.schema.primary_keys

    def resolve_join(self, other: Dataset) -> list[JoinSpec] | None:
        schema = self.schema
        assert schema is not None
        return schema.detect_join(dataset_label=self.label, other_schema=other.schema, other_dataset_label=other.label)

    #### RESTRUCTURE DATASET ####

    def subset(
        self,
        new_dataset_series_label: str,
        observation_groups: dict[str, list[peh.Observation]],
        dataops_adapter: OutDataOpsInterface,
    ) -> DatasetSeries:
        # TODO: schemas for all new datasets need to be properly relabeled
        ret = DatasetSeries(label=new_dataset_series_label, parts={})

        for dataset_label, observation_group in observation_groups.items():
            observable_properties = set()
            observation_ids: set[str] = set()
            for observation in observation_group:
                assert observation.id in self.observations
                observation_ids.add(observation.id)
                observation_design = observation.observation_design
                assert observation_design is not None
                for observable_property in getattr(observation_design, "identifying_observable_property_id_list", []):
                    observable_properties.add(observable_property)
                for observable_property in getattr(observation_design, "required_observable_property_id_list", []):
                    observable_properties.add(observable_property)
                for observable_property in getattr(observation_design, "optional_observable_property_id_list", []):
                    observable_properties.add(observable_property)

            element_group = []
            for observable_property_id in observable_properties:
                element = self.schema.get_element_by_observable_property_id(observable_property_id)
                assert element is not None
                element_group.append(element.label)
            assert len(element_group) > 0

            # split data
            data_subset = dataops_adapter.subset(
                data=self.data,
                element_group=element_group,
            )
            # split schema
            schema_subset = self.schema.subset(element_group)
            # add both to new dataset
            new_dataset = Dataset(
                schema=schema_subset,
                label=dataset_label,
                data=data_subset,
                observations=observation_ids,
                part_of=ret,
            )
            ret.parts[dataset_label] = new_dataset

        return ret

    def relabel(self, element_mapping: dict[str, str], dataops_adapter: OutDataOpsInterface) -> bool:
        # uniqueness check
        if len(set(element_mapping.values())) != len(element_mapping):
            raise ValueError("Not all values in the element_mapping are unique")
        # relabel schema
        _ = self.schema.relabel(element_mapping)
        # relabel dataset
        self.data = dataops_adapter.relabel(self.data, element_mapping)

        return True

    def add_observable_property(
        self,
        observable_property_id: str,
        data_type: ObservablePropertyValueType,
        element_label: str | None = None,
        is_primary_key: bool = False,
    ):
        return self.schema.add_observable_property(
            observable_property_id,
            data_type,
            element_label,
            is_primary_key,
        )

    def add_observable_property_from_cache(
        self,
        observable_property_id: str,
        cache_view: CacheContainerView,
        is_primary_key: bool,
    ):
        if observable_property_id in self.schema._elements_by_observable_property:
            # TODO: we assume observable_property_ids are unique within a dataset
            return
        observable_property = cache_view.get(observable_property_id, "ObservableProperty")
        assert observable_property is not None
        label = observable_property.ui_label
        assert label is not None
        data_type = ObservablePropertyValueType(getattr(observable_property, "value_type", "string"))
        self.add_observable_property(
            observable_property_id=observable_property_id,
            data_type=data_type,
            element_label=observable_property.ui_label,
            is_primary_key=is_primary_key,
        )

    def add_observation_to_index(self, observation_id: str):
        self.observations.add(observation_id)
        if self.part_of:
            self.part_of._register_observation(observation_id, self.label)

    def remove_observation_from_index(self, observation_id: str):
        self.observations.remove(observation_id)
        if self.part_of:
            self.part_of._unregister_observation(observation_id)

    def add_data(self, data: T_DataType, non_empty_dataset_elements: list[str] | None = None, overwrite: bool = True):
        if not overwrite:
            if self.data is not None:
                raise NotImplementedError()

        if len(self.schema) > 0:
            assert non_empty_dataset_elements is not None

            schema_check_result = self.contained_in_schema(non_empty_dataset_elements)
            assert schema_check_result

            self.metadata["non_empty_dataset_elements"] = non_empty_dataset_elements
            self.data = data

    #### VERIFY DATASET ####

    def contained_in_schema(self, element_labels: list[str] | None = None) -> bool:
        """
        Check if the columns from a set of data can be found amongst the labels defined in the dataset schema
        """
        raw_data_labels = set(element_labels)
        schema_labels = set(self.get_element_labels())
        label_diff = raw_data_labels.difference(schema_labels)
        assert (
            len(label_diff) == 0
        ), f"Data Schema Error: Element labels {label_diff} are not defined in the dataset schema"
        return True


@dataclass(kw_only=True)
class DatasetSeries(Resource, Generic[T_DataType]):
    parts: dict[str, Dataset[T_DataType]] = field(default_factory=dict)
    _obs_index: dict[str, str] = field(default_factory=dict)

    __metadata__ = {
        "id": "dcat:datasetSeries",
        "context": DCAT_CONTEXT,
    }

    def build_observation_index(self):
        idx = {obs: dataset.label for dataset in self.parts.values() for obs in dataset.observations}
        self._obs_index = idx

    def _register_observation(self, observation_id: str, dataset_label: str):
        self._obs_index[observation_id] = dataset_label

    def _unregister_observation(self, observation_id: str):
        self._obs_index.pop(observation_id, None)

    #### CONSTRUCT DATASETSERIES ####
    def apply_context(self, cache: CacheContainer):
        # THIS METHOD MODIFIES THE CACHE !!!
        context = self._get_validation_index()
        for dataset_label in self:
            dataset = self.get(dataset_label)
            assert dataset is not None
            dataset.schema.apply_context(context, cache, this_dataset=self.label)

    @classmethod
    def from_peh_datalayout(
        cls, data_layout: peh.DataLayout, cache_view: CacheContainerView, apply_context: bool = True
    ) -> DatasetSeries:
        parts = dict()
        sections = getattr(data_layout, "sections")
        if sections is None:
            raise ValueError("No sections found in DataLayout")
        for section in sections:
            label = getattr(section, "ui_label")
            parts[label] = Dataset.from_peh_datalayout_section(
                section,
                cache_view,
            )

        label = data_layout.ui_label
        if label is None:
            label = str(uuid.uuid4())
        ret = cls(label=label, parts=parts)
        for dataset in ret.parts.values():
            dataset.part_of = ret
        _ = ret.add_metadata("described_by", data_layout.id)

        if apply_context:
            ret.apply_context(cache=cache_view._container)

        return ret

    @classmethod
    def from_peh_data_import_config(
        cls,
        data_import_config: peh.DataImportConfig,
        cache_view: CacheContainerView,
        apply_context: bool = True,
    ) -> DatasetSeries:
        data_layout_id = data_import_config.layout
        assert data_layout_id is not None
        data_layout = cache_view.get(data_layout_id, "DataLayout")
        assert isinstance(data_layout, peh.DataLayout)
        ret = cls.from_peh_datalayout(data_layout, cache_view, apply_context=apply_context)

        # add Observation links
        section_mapping = data_import_config.section_mapping
        assert isinstance(section_mapping, peh.DataImportSectionMapping)
        section_mapping_links = section_mapping.section_mapping_links
        assert isinstance(section_mapping_links, list)
        for link in section_mapping_links:
            assert isinstance(link, peh.DataImportSectionMappingLink)
            section_id = link.section
            assert isinstance(section_id, str)
            layout_section = cache_view.get(section_id, "DataLayoutSection")
            assert isinstance(layout_section, peh.DataLayoutSection)
            dataset_label = layout_section.ui_label
            assert dataset_label is not None
            observation_ids = link.observation_id_list
            assert isinstance(observation_ids, list)
            assert observation_ids is not None
            dataset = ret[dataset_label]
            assert dataset is not None, f"Could not find dataset with label {dataset_label}"
            num_observations = len(observation_ids)
            dataset.observations = set(observation_ids)
            assert num_observations == len(dataset.observations)

        # add DataImportConfigId metadata
        ret.add_metadata("data_import_config_id", data_import_config.id)

        return ret

    def register_dataset(self, dataset: Dataset):
        dataset.part_of = self
        self.parts[dataset.label] = dataset

    def add_data(
        self,
        dataset_label: str,
        data: T_DataType,
        non_empty_dataset_elements: list[str] | None = None,
        overwrite: bool = True,
    ):
        dataset = self.parts.get(dataset_label, None)
        assert dataset is not None
        dataset.add_data(data=data, non_empty_dataset_elements=non_empty_dataset_elements, overwrite=overwrite)

    def add_observable_property(
        self,
        dataset_label: str,  # TODO: this should become an observation_id
        observable_property_id: str,
        data_type: ObservablePropertyValueType,
        element_label: str | None = None,
        is_primary_key: bool = False,
    ):
        dataset = self[dataset_label]
        assert dataset is not None
        return dataset.add_observable_property(
            observable_property_id,
            data_type,
            element_label,
            is_primary_key,
        )

    def add_empty_dataset(self, dataset_label: str) -> Dataset:
        dataset = Dataset(label=dataset_label)
        self.register_dataset(dataset)

        return dataset

    def add_observation(
        self,
        dataset_label: str,
        observation: peh.Observation,
        cache_view: CacheContainerView,
        data: T_DataType | None = None,
    ):
        if dataset_label not in self.parts:
            dataset = self.add_empty_dataset(dataset_label)
        else:
            dataset = self[dataset_label]
        assert dataset is not None
        observation_design = observation.observation_design
        assert isinstance(observation_design, peh.ObservationDesign)
        identifying_observable_properties = observation_design.identifying_observable_property_id_list
        assert identifying_observable_properties is not None
        for observable_property_id in identifying_observable_properties:
            dataset.add_observable_property_from_cache(
                observable_property_id=observable_property_id,
                cache_view=cache_view,
                is_primary_key=True,
            )

        required_observable_properties = observation_design.required_observable_property_id_list
        assert required_observable_properties is not None
        optional_observable_properties = observation_design.optional_observable_property_id_list
        assert optional_observable_properties is not None
        for observable_property_id_list in (required_observable_properties, optional_observable_properties):
            for observable_property_id in observable_property_id_list:
                dataset.add_observable_property_from_cache(
                    observable_property_id=observable_property_id,
                    cache_view=cache_view,
                    is_primary_key=False,
                )
        dataset.add_observation_to_index(observation.id)

        if data is not None:
            # TODO: this is still incomplete
            dataset.add_data(data=data)

    #### RESTRUCTURE DATASETSERIES ####

    def subset_dataset(
        self,
        dataset_label: str,
        new_dataset_series_label: str,
        observation_groups: dict[str, list[peh.Observation]],
        dataops_adapter: OutDataOpsInterface,
    ) -> DatasetSeries:
        """
        observation_groups: Contains the new `Dataset.label` as key, and the list[ObservationId] to be included in the new `Dataset`
        """

        dataset = self.get(dataset_label)
        assert dataset is not None
        return dataset.subset(new_dataset_series_label, observation_groups, dataops_adapter=dataops_adapter)

    def relabel_dataset(
        self, dataset_label: str, element_mapping: dict[str, str], dataops_adapter: OutDataOpsInterface
    ):
        dataset = self.get(dataset_label)
        assert dataset is not None
        return dataset.relabel(element_mapping, dataops_adapter=dataops_adapter)

    #### EXTRACT INFO FROM DATASETSERIES ####

    def get_type_annotations(self) -> dict[str, dict[str, ObservablePropertyValueType]]:
        ret: dict[str, dict[str, ObservablePropertyValueType]] = dict()
        for dataset in self.parts.values():
            label = dataset.label
            ret[label] = dataset.get_type_annotations()

        return ret

    def resolve_join(self, left_dataset_label: str, right_dataset_label: str) -> list[JoinSpec] | None:
        left = self.get(left_dataset_label)
        assert left is not None
        right = self.get(right_dataset_label)
        assert right is not None
        return left.resolve_join(right)

    def resolve_all_joins(self) -> dict[frozenset, list[JoinSpec] | None]:
        ret = {}
        for combo in itertools.combinations(self.parts, 2):
            key = frozenset(combo)
            ret[key] = self.resolve_join(*combo)
        return ret

    def matches_schema(self, raw_data_dict: dict[str, T_DataType], adapter: OutDataOpsInterface) -> bool:
        ret = True
        for dataset_label in self.parts:
            if dataset_label not in raw_data_dict:
                return False
            dataset = self[dataset_label]
            assert dataset is not None
            raw_data = raw_data_dict[dataset_label]
            raw_data_labels = set(adapter.get_element_labels(raw_data))
            ret = set(dataset.get_element_labels()) == raw_data_labels

        return ret

    def _get_validation_index(self) -> dict[str, dict[str, str]]:
        """
        # TEMPORARY SOLUTION !!!!
        ObservablePropertyId -> dataset_label, field_label
        We currently assume that each observable_property only occurs once in the DatasetSeries
        """
        field_ref_dict = defaultdict(dict)
        for dataset_label in self:
            dataset = self.get(dataset_label)
            assert dataset is not None
            for observable_property_id, element_label in dataset.schema._elements_by_observable_property.items():
                field_ref_dict[observable_property_id][dataset_label] = element_label

        return field_ref_dict

    def get_contextual_field_reference_index(self) -> dict[str, tuple[str, str] | None]:
        field_ref_dict = {}
        for dataset_label in self:
            dataset = self.get(dataset_label)
            assert dataset is not None
            for observable_property_id, element_label in dataset.schema._elements_by_observable_property.items():
                if observable_property_id in field_ref_dict:
                    field_ref_dict[observable_property_id] = None
                else:
                    field_ref_dict[observable_property_id] = (dataset_label, element_label)

        return field_ref_dict

    def get_dataset_by_observation(self, observation_id: str, rebuild_index: bool = False) -> Dataset | None:
        if rebuild_index:
            self.build_observation_index()
        dataset_label = self._obs_index.get(observation_id, None)
        if dataset_label is None:
            return
        return self[dataset_label]

    #### CORE FUNCTIONALITY ####

    @property
    def data_import_config(self) -> str | None:
        return self.metadata.get("data_import_config", None)

    @property
    def observations(self) -> dict[str, set[str]]:
        ret = {}
        for dataset_label in self:
            dataset = self[dataset_label]
            assert dataset is not None
            ret[dataset_label] = dataset.observations

        return ret

    def __len__(self):
        return len(self.parts)

    def get(self, key, default=None) -> Dataset | None:
        try:
            return self[key]
        except KeyError:
            return default

    def __getitem__(self, key: str) -> Dataset | None:
        return self.parts.get(key)

    def __setitem__(self, key: str, value: Dataset) -> None:
        self.parts[key] = value

    def update(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError("update expected at most 1 arguments, " "got %d" % len(args))
            assert len(args) == 1
            other = dict(args[0])
            for key in other:
                self.parts[key] = other[key]
        for key in kwargs:
            self.parts[key] = kwargs[key]

    def __iter__(self):
        return iter(self.parts)
