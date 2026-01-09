"""
Each of these Interface subclasses provides a protocol on how
the corresponding Adapter subclass should be implemented.

Usage: TODO: add usage info

"""

from __future__ import annotations
import importlib

import logging

from abc import abstractmethod
from peh_model.peh import (
    DataLayout,
    DataLayoutSection,
    EntityList,
)
from typing import TYPE_CHECKING, Generic, cast, List

from pypeh.core.cache.containers import CacheContainerView
from pypeh.core.models.internal_data_layout import Dataset, DatasetSeries
from pypeh.core.models.typing import T_DataType
from pypeh.core.models.settings import FileSystemSettings
from pypeh.core.models import validation_dto
from pypeh.core.session.connections import ConnectionManager

if TYPE_CHECKING:
    from typing import Sequence, Mapping, Any
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
    def _join_dataset(
        self,
        identifying_observable_property_ids: list[str],
        dataset: Dataset[T_DataType],
        dependent_data: Mapping[str, Dataset[T_DataType]],
        dependent_observable_property_ids: set[str],
        observable_property_id_to_dataset_label_dict: dict[str, str],
    ) -> T_DataType:
        raise NotImplementedError

    @abstractmethod
    def select_field(self, dataset, field_label: str):
        raise NotImplementedError

    @abstractmethod
    def get_element_labels(self, data: T_DataType) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_element_values(self, data: T_DataType, element_label: str) -> set[str]:
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

    def validate(
        self,
        dataset: Dataset[T_DataType],
        dependent_dataset_series: DatasetSeries[T_DataType] | None = None,
        cache_view: CacheContainerView | None = None,
    ) -> ValidationErrorReport:
        assert dataset.data is not None
        to_validate: T_DataType = dataset.data
        type_annotations = dataset.get_type_annotations()
        # Get Dataset level validations if they exist for the describing DataLayoutSection
        dataset_validations = []

        layout_section_id = dataset.described_by
        if layout_section_id:
            assert cache_view is not None
            layout_section = cache_view.get(layout_section_id, "DataLayoutSection")
            assert layout_section is not None
            assert isinstance(layout_section, DataLayoutSection)
            if layout_section.validation_designs:
                for vd in layout_section.validation_designs:
                    dataset_validation = validation_dto.ValidationDesign.from_layout(vd, type_annotations, cache_view)
                    # For an expression that relies on a field reference spec for its arguments, set the validation arguments
                    # as the actual values from the dataset (e.g. for an "is_in" check on a foreign key relation)
                    if vd.validation_expression.validation_arg_contextual_field_references:
                        arg_values = vd.validation_expression.validation_arg_values
                        for ref in vd.validation_expression.validation_arg_contextual_field_references:
                            arg_values.extend(
                                dependent_dataset_series[ref.dataset_label].data.get_column(ref.field_label).to_list()
                            )
                        dataset_validation.expression.arg_values = arg_values
                        dataset_validation.expression.arg_columns = None
                    dataset_validations.append(dataset_validation)

        validation_config = validation_dto.ValidationConfig.from_dataset(
            dataset,
            dataset_validations,
            cache_view,
        )
        join_required = False
        # check whether data requires join to perform validation (cross DataLayoutSection validation)
        dependent_observable_property_ids = validation_config.dependent_observable_property_ids
        if dependent_observable_property_ids is not None:
            join_required = len(dependent_observable_property_ids) > 0

        if join_required:
            if dependent_dataset_series is None:
                me = f"`dependent_data` is required to perform all validations. One or more of the following `ObservableProperty`s cannot be found in the current `DataLayoutSection`: {', '.join(dependent_observable_property_ids)}"
                logger.error(me)
                raise ValueError(me)
            else:
                assert (
                    dependent_observable_property_ids is not None
                ), "dependent_observable_property_ids in `ValidationInterface.validate` should not be None"
                assert dependent_dataset_series is not None
                # TEMP: looping over datasets should not be necessary when contextual_field_references are implemented
                observable_property_id_to_dataset_label_dict = dict()
                for observable_property_id in dependent_observable_property_ids:
                    found = False
                    for dataset_label in dependent_dataset_series:
                        dependent_dataset = dependent_dataset_series[dataset_label]
                        assert dependent_dataset is not None
                        all_obs_props = set(dependent_dataset.get_observable_property_ids())
                        if observable_property_id in all_obs_props:
                            observable_property_id_to_dataset_label_dict[observable_property_id] = dataset_label
                            found = True
                            break
                    assert found, f"Did not find {observable_property_id}"

                identifying_obs_prop_id_list = dataset.schema.primary_keys
                assert (
                    identifying_obs_prop_id_list is not None
                ), "identifying_obs_prop_id_list in `ValidationInterface.validate` should not be None"

                to_validate = self._join_dataset(
                    identifying_observable_property_ids=identifying_obs_prop_id_list,
                    dataset=dataset,
                    dependent_data=dependent_dataset_series.parts,
                    dependent_observable_property_ids=dependent_observable_property_ids,
                    observable_property_id_to_dataset_label_dict=observable_property_id_to_dataset_label_dict,
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
    ) -> DataLayout | List[DataLayout]:
        provider = ConnectionManager._create_adapter(config)
        layout = provider.load(source)
        if isinstance(layout, EntityList):
            layout = layout.layouts
        if isinstance(layout, list):
            if not all(isinstance(item, DataLayout) for item in layout):
                me = "Imported layouts should all be DataLayout instances"
                logger.error(me)
                raise TypeError(me)
            return cast(List[DataLayout], layout)

        elif isinstance(layout, DataLayout):
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
