from __future__ import annotations

import logging
import uuid

from dataclasses import dataclass, field
from peh_model import peh
from typing import TYPE_CHECKING, Generic, Sequence

from pypeh.core.cache.containers import CacheContainerView
from pypeh.core.models.typing import T_DataType
from pypeh.core.models.constants import ObservablePropertyValueType

if TYPE_CHECKING:
    from typing import Any


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

    def get_type_annotations(self) -> dict[str, ObservablePropertyValueType]:
        ret: dict[str, ObservablePropertyValueType] = dict()
        for element in self.elements:
            data_type = element.data_type
            if data_type is not None:
                ret[element.label] = data_type

        return ret

    @classmethod
    def from_peh_data_layout_elements(
        cls, data_layout_elements: list[peh.DataLayoutElement], cache_view: CacheContainerView
    ):
        schema_elements = []
        processed_foreign_keys = []
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

            schema_elements.append(DatasetSchemaElement.from_peh_data_layout_element(element, cache_view))

        return cls(
            elements=schema_elements,
        )


@dataclass(kw_only=True)
class Resource:
    label: str
    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict[str, str] = field(default_factory=dict)

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
    data: T_DataType | None = None
    schema: DatasetSchema

    __metadata__ = {
        "id": "dcat:dataset",
        "context": DCAT_CONTEXT,
    }

    def get_type_annotations(self) -> dict[str, ObservablePropertyValueType]:
        return self.schema.get_type_annotations()

    @classmethod
    def from_peh_datalayout_section(cls, data_layout_section: peh.DataLayoutSection, cache_view: CacheContainerView):
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

        ret = cls(
            label=label,
            schema=schema,
        )
        _ = ret.add_metadata("described_by", data_layout_section.id)

        return ret


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
            parts[label] = Dataset.from_peh_datalayout_section(section, cache_view)

        ret = cls(label=label, parts=parts)
        _ = ret.add_metadata("described_by", data_layout.id)

        return ret

    def get_type_annotations(self) -> dict[str, dict[str, ObservablePropertyValueType]]:
        ret: dict[str, dict[str, ObservablePropertyValueType]] = dict()
        for dataset in self.parts.values():
            label = dataset.label
            ret[label] = dataset.get_type_annotations()

        return ret

    def add_data(self, dataset_label: str, data: T_DataType) -> bool:
        dataset = self.parts.get(dataset_label, None)
        assert dataset is not None
        dataset.data = data

        return True

    @property
    def data_import_config(self) -> str | None:
        return self.metadata.get("data_import_config", None)
