from __future__ import annotations

import logging
import uuid

from dataclasses import dataclass, field
from peh_model import peh
from typing import TYPE_CHECKING, Generic, Sequence

from pypeh.core.cache.containers import CacheContainerView
from pypeh.core.models.typing import T_DataType
from pypeh.core.models.constants import ObservablePropertyValueType, ValidationErrorLevel
from pypeh.core.models.validation_dto import ValidationDesign, ValidationExpression

if TYPE_CHECKING:
    from typing import Any
    from pypeh.core.interfaces.outbound.dataops import DataImportInterface


logger = logging.getLogger(__name__)
CSVW_CONTEXT = {"csvw": "http://www.w3.org/ns/csvw#"}
DCAT_CONTEXT = {"dcat": "http://www.w3.org/ns/dcat#"}


# TODO: Refactoring: relocate or add as a method on an appropriate class
def get_observations_from_data_import_config(data_import_config: peh.DataImportConfig, cache: CacheContainerView):
    observations = []
    for link in data_import_config.section_mapping.section_mapping_links:
        observations.extend([cache.get(observation_id, "Observation") for observation_id in link.observation_id_list])
    return observations


# TODO: Refactoring: relocate or add as a method on an appropriate class
# TODO: Refactoring: to be replaced with InternalDataLayout based implementation
# TODO: To be fixed: Does not account for observable_property_id reuse across datasets
def get_observable_property_id_to_dataset_label_dict(
    observable_property_id_list: Sequence[str], dependent_data: dict[str, dict[str, Sequence]] | dict[str, T_DataType]
):
    ret = {}
    for observable_property_id in observable_property_id_list:
        for k, v in dependent_data.items():
            if isinstance(v.observed_data, dict):
                dependent_property_id_list = v.observed_data.keys()
            elif hasattr(v.observed_data, "columns"):
                dependent_property_id_list = [str(c) for c in v.observed_data.columns]
            else:
                raise NotImplementedError(f"Unsupported observed_data type encountered while processing dataset {k}")

            if observable_property_id in dependent_property_id_list:
                ret[observable_property_id] = k
    return ret


@dataclass
class ObservationResultProxy(Generic[T_DataType]):
    observed_data: T_DataType
    observation_result_type: str = "measurement"
    id: uuid.UUID = uuid.uuid4()


class BiMap:
    def __init__(self):
        self.forward = {}
        self.backward = {}

    def insert(self, key, value):
        if key in self.forward or value in self.backward:
            raise ValueError("Duplicate key or value")
        self.forward[key] = value
        self.backward[value] = key

    def get_by_key(self, key):
        return self.forward.get(key)

    def get_by_value(self, value):
        return self.backward.get(value)

    def __repr__(self):
        return self.forward.__repr__()

    def __len__(self):
        return len(self.forward)


class ElementToObservableProperty(BiMap):
    """
    Bidirectional mapping of DataLayoutElements and ObservableProperties.
    Note this enforces DataLayoutElement.label uniqueness per DataLayoutSection
    """

    @classmethod
    def from_peh(cls, data_layout_section: peh.DataLayoutSection) -> ElementToObservableProperty:
        ret = cls()
        elements = getattr(data_layout_section, "elements")
        if elements is None:
            pass
        for element in elements:
            assert isinstance(element, peh.DataLayoutElement)
            observable_property_id = element.observable_property
            assert (
                observable_property_id is not None
            ), f"could not find an observable_property field for {element.label}"
            _ = ret.insert(element.label, observable_property_id)

        return ret

    def collect_schema(self, cache_view: CacheContainerView) -> dict[str, ObservablePropertyValueType]:
        ret = {}

        for element_label, observable_property_id in self.forward.items():
            observable_property = cache_view.get(observable_property_id, "ObservableProperty")
            assert observable_property is not None, f"could not find an observable_property for {element_label}"
            value_type = getattr(observable_property, "value_type")
            ret[element_label] = ObservablePropertyValueType(value_type)

        return ret


class InternalDataLayout(dict[str, ElementToObservableProperty]):
    # TODO: add entity tree: should have structure of observations

    @classmethod
    def from_peh(cls, data_layout: peh.DataLayout) -> InternalDataLayout:
        ret = cls()
        sections = getattr(data_layout, "sections")
        if sections is None:
            raise ValueError("No sections found in DataLayout")
        for section in sections:
            label = getattr(section, "ui_label")
            ret[label] = ElementToObservableProperty.from_peh(section)

        return ret

    def collect_schema(
        self, cache_view: CacheContainerView
    ) -> dict[str, dict[str, ObservablePropertyValueType]] | None:
        ret = {}
        try:
            for section_label, bimap in self.items():
                ret[section_label] = bimap.collect_schema(cache_view)

            return ret
        except Exception as _:
            logger.info("Could not infer schema with provided cache_view")
            return


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
    elements: list[DatasetSchemaElement]
    primary_keys: list[str] | None = None
    foreign_keys: list[ForeignKey] = field(default_factory=list)

    __metadata__ = {
        "id": "csvw:Schema",
        "context": CSVW_CONTEXT,
    }

    def __post_init__(self):
        self._index = {}
        for element in self.elements:
            self._index[element.label] = element
        self._type = self.get_type_annotations()

    def get_type_annotations(self) -> dict[str, ObservablePropertyValueType]:
        ret: dict[str, ObservablePropertyValueType] = dict()
        for element in self.elements:
            data_type = element.data_type
            if data_type is not None:
                ret[element.label] = data_type

        return ret

    def get_type(self, element_label: str) -> ObservablePropertyValueType:
        return self._type[element_label]

    def get_dataset_elements(self) -> list[str]:
        return [element.label for element in self.elements]

    def get_element(self, element_label: str) -> DatasetSchemaElement | None:
        return self._index.get(element_label, None)

    def get_observable_property_ids(self) -> list[str]:
        return [element.observable_property_id for element in self.elements]

    @classmethod
    def from_peh_data_layout_elements(
        cls, data_layout_elements: list[peh.DataLayoutElement], cache_view: CacheContainerView
    ):
        schema_elements = []
        processed_foreign_keys = []
        processed_primary_keys = []
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
                assert section is not None
                assert section.ui_label is not None
                processed_foreign_keys.append(
                    ForeignKey(
                        element_label=element_label,
                        reference=ElementReference(
                            dataset_label=section.ui_label,
                            element_label=foreign_key_element_label,
                        ),
                    )
                )
            is_primary_key = element.is_observable_entity_key
            if is_primary_key is not None:
                if is_primary_key:
                    processed_primary_keys.append(element_label)

            schema_elements.append(DatasetSchemaElement.from_peh_data_layout_element(element, cache_view))

        return cls(
            elements=schema_elements,
            foreign_keys=processed_foreign_keys,
            primary_keys=processed_primary_keys,
        )


@dataclass(kw_only=True)
class Resource:
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
    data: T_DataType | None = field(default=None)
    schema: DatasetSchema
    part_of: DatasetSeries | None = field(default=None)

    __metadata__ = {
        "id": "dcat:dataset",
        "context": DCAT_CONTEXT,
    }

    def get_type_annotations(self) -> dict[str, ObservablePropertyValueType]:
        return self.schema.get_type_annotations()

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

    @property
    def non_empty(self):
        return self.metadata.get("non_empty_dataset_elements", None)

    def get_schema_element(self, element_label: str) -> DatasetSchemaElement | None:
        return self.schema.get_element(element_label)

    def get_observable_property_ids(self) -> list[str]:
        return self.schema.get_observable_property_ids()


@dataclass(kw_only=True)
class DatasetSeries(Resource, Generic[T_DataType]):
    parts: dict[str, Dataset[T_DataType]]

    __metadata__ = {
        "id": "dcat:datasetSeries",
        "context": DCAT_CONTEXT,
    }

    @classmethod
    def from_peh_datalayout(cls, data_layout: peh.DataLayout, cache_view: CacheContainerView) -> DatasetSeries:
        parts = dict()
        label = data_layout.ui_label
        assert label is not None
        sections = getattr(data_layout, "sections")
        if sections is None:
            raise ValueError("No sections found in DataLayout")
        for section in sections:
            label = getattr(section, "ui_label")
            parts[label] = Dataset.from_peh_datalayout_section(
                section,
                cache_view,
            )

        ret = cls(label=label, parts=parts)
        for dataset in ret.parts.values():
            dataset.part_of = ret
        _ = ret.add_metadata("described_by", data_layout.id)

        return ret

    def get_type_annotations(self) -> dict[str, dict[str, ObservablePropertyValueType]]:
        ret: dict[str, dict[str, ObservablePropertyValueType]] = dict()
        for dataset in self.parts.values():
            label = dataset.label
            ret[label] = dataset.get_type_annotations()

        return ret

    def add_data(
        self, dataset_label: str, data: T_DataType, non_empty_dataset_elements: list[str] | None = None
    ) -> bool:
        dataset = self.parts.get(dataset_label, None)
        assert dataset is not None
        assert dataset.data is None
        observable_property_ids = dataset.get_observable_property_ids()

        if len(observable_property_ids) == 0:
            return False
        dataset.data = data
        dataset.metadata["non_empty_dataset_elements"] = non_empty_dataset_elements

        return True

    def get_identifier_validation_config_dict(
        self,
        data_import_adapter: DataImportInterface,
        cache_view: CacheContainerView,
    ) -> dict[str, list[ValidationDesign]]:
        ret: dict[str, list[ValidationDesign]] = dict()

        for dataset_label in self.parts:
            validation_designs = []
            dataset = self[dataset_label]
            assert dataset is not None
            schema = dataset.schema
            foreign_keys = schema.foreign_keys
            if foreign_keys is not None:
                for foreign_key in foreign_keys:
                    element_label = foreign_key.element_label
                    reference = foreign_key.reference
                    referenced_dataset = self[reference.dataset_label]
                    assert referenced_dataset is not None
                    referenced_data = referenced_dataset.data
                    assert referenced_data is not None
                    validation_arg_values = list(
                        data_import_adapter.get_element_values(
                            data=referenced_data, element_label=reference.element_label
                        )
                    )
                    assert (
                        len(validation_arg_values) > 0
                    ), f"No identifiers to validate against found in {reference.element_label}"
                    validation_name = validation_name = (
                        f"check_foreignkey_{dataset_label.replace(':', '_')}_{element_label}"
                    )
                    element = schema.get_element(element_label)
                    assert element is not None
                    element_observable_property = cache_view.get(element.observable_property_id, "ObservableProperty")
                    assert isinstance(element_observable_property, peh.ObservableProperty)
                    element_observable_property_short_name = element_observable_property.short_name
                    assert element_observable_property_short_name is not None
                    validation_design = ValidationDesign(
                        name=validation_name,
                        error_level=ValidationErrorLevel.ERROR,
                        expression=ValidationExpression(
                            command="is_in",
                            subject=[
                                element_label,
                            ],
                            arg_values=validation_arg_values,
                        ),
                    )

                    validation_designs.append(validation_design)

                ret[dataset_label] = validation_designs

        return ret

    @property
    def data_import_config(self) -> str | None:
        return self.metadata.get("data_import_config", None)

    def __len__(self):
        return len(self.parts)

    def __get__(self):
        return

    def __getitem__(self, key) -> Dataset | None:
        return self.parts.get(key)

    def __iter__(self):
        return iter(self.parts)
