from __future__ import annotations

import logging
import uuid

from dataclasses import dataclass
from peh_model import peh
from typing import TYPE_CHECKING, Generic

from pypeh.core.cache.containers import CacheContainerView
from pypeh.core.models.typing import T_DataType
from pypeh.core.models.constants import ObservablePropertyValueType

if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


# TEMP: to be replaced by peh.DataImportConfig
@dataclass
class SectionImportConfig:
    data_layout_section_id: str
    observation_ids: list[str]


# TEMP: to be replaced by peh.DataImportConfig
@dataclass
class DataImportConfig:
    data_layout_id: str
    section_map: list[SectionImportConfig]


@dataclass
class ObservationResultProxy(Generic[T_DataType]):
    observed_values: T_DataType
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
