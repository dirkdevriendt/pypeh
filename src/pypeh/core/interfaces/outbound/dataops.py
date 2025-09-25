"""
Each of these Interface subclasses provides a protocol on how
the corresponding Adapter subclass should be implemented.

Usage: TODO: add usage info

"""

from __future__ import annotations
import importlib

import logging

from abc import abstractmethod
from peh_model.peh import DataLayout, ValidationDesign, EntityList, Observation, ObservableProperty
from typing import TYPE_CHECKING, Generic, cast, List

from pypeh.core.models.typing import T_DataType
from pypeh.core.models.settings import FileSystemSettings
from pypeh.core.models.validation_dto import ValidationConfig
from pypeh.core.session.connections import ConnectionManager

if TYPE_CHECKING:
    from typing import Sequence
    from pypeh.core.models.validation_errors import ValidationErrorReport

logger = logging.getLogger(__name__)


class OutDataOpsInterface:
    """
    Example of DataOps methods
    def validate(self, data: Mapping, config: Mapping):
        pass

    def summarize(self, dat: Mapping, config: Mapping):
        pass
    """

    pass


class ValidationInterface(OutDataOpsInterface, Generic[T_DataType]):
    @abstractmethod
    def _validate(self, data: dict[str, Sequence] | T_DataType, config: ValidationConfig) -> ValidationErrorReport:
        raise NotImplementedError

    @abstractmethod
    def _join_data(
        self,
        identifying_observable_property_ids: list[str],
        data: dict[str, Sequence] | T_DataType,
        dependent_data: dict[str, dict[str, Sequence]] | dict[str, T_DataType],
        dependent_observable_properties: set[str],
        observable_property_id_to_layout_section_label: dict[str, str],
    ) -> T_DataType:
        raise NotImplementedError

    @classmethod
    def get_default_adapter_class(cls):
        try:
            adapter_module = importlib.import_module("pypeh.adapters.outbound.validation.pandera_adapter.dataops")
            adapter_class = getattr(adapter_module, "DataFrameAdapter")
        except Exception as e:
            logger.error("Exception encountered while attempting to import a Pandera-based DataFrameAdapter")
            raise e
        return adapter_class

    def validate(
        self,
        data: dict[str, Sequence] | T_DataType,
        observation: Observation,
        observable_properties: List[ObservableProperty],
        dataset_validations: Sequence[ValidationDesign] | None = None,
        dependent_data: dict[str, dict[str, Sequence]] | dict[str, T_DataType] | None = None,
        observable_property_id_to_layout_section_label: dict[str, str]
        | None = None,  # TODO: this is a temporary quick fix
    ) -> ValidationErrorReport:
        observable_property_dict = {op.id: op for op in observable_properties}

        validation_config = ValidationConfig.from_observation(
            observation,
            observable_property_dict,
            dataset_validations,
        )
        to_validate = data
        join_required = False
        # check whether data requires join to perform validation (cross DataLayoutSection validation)
        dependent_observable_property_ids = validation_config.dependent_observable_property_ids
        if dependent_observable_property_ids is not None:
            # This test is obsolete, and should have been tested already when
            # constructing the list of dependent_observable_property_ids
            for obs_prop in dependent_observable_property_ids:
                if obs_prop not in observable_property_dict:
                    join_required = True
                    break

        if join_required:
            if dependent_data is None:
                me = f"`dependent_data` is required to perform all validations. One or more of the following `ObservableProperty`s cannot be found in the current `DataLayoutSection`: {', '.join(dependent_observable_property_ids)}"
                logger.error(me)
                raise ValueError(me)
            else:
                identifying_obs_prop_id_list = getattr(
                    observation.observation_design, "identifying_observable_property_id_list", None
                )
                assert (
                    identifying_obs_prop_id_list is not None
                ), "identitfying_obs_prop_id_list in `ValidationInterface.validate` should not be None"
                assert (
                    observable_property_id_to_layout_section_label is not None
                ), "observable_property_to_layout_section in `ValidationInterface.validate` should not be None"
                assert (
                    dependent_observable_property_ids is not None
                ), "dependent_observable_property_ids in `ValidationInterface.validate` should not be None"
                to_validate = self._join_data(
                    identifying_observable_property_ids=identifying_obs_prop_id_list,
                    data=data,
                    dependent_data=dependent_data,
                    dependent_observable_properties=dependent_observable_property_ids,
                    observable_property_id_to_layout_section_label=observable_property_id_to_layout_section_label,
                )

        ret = self._validate(to_validate, validation_config)

        return ret


class DataImportInterface(OutDataOpsInterface, Generic[T_DataType]):
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
