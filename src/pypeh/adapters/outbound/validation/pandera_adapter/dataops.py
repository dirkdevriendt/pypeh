"""
This part of the code should contain the reusable part
of the PyHBM library, aka, the part that would otherwise
be copied across projects. Ideally, it also contains a
specification on what the custom part should look like.
"""

from __future__ import annotations

import logging
import polars as pl

from contextlib import contextmanager
from dataguard import Validator, ErrorCollector
from peh_model.peh import DataLayout
from polars import DataFrame
from typing import TYPE_CHECKING, List, Dict

from pypeh.core.interfaces.outbound.dataops import (
    ValidationInterface,
    DataImportInterface,
)
from pypeh.core.models.internal_data_layout import Dataset
from pypeh.core.models.validation_errors import ValidationErrorReport, EntityLocation
from pypeh.core.models.validation_dto import ValidationConfig
from pypeh.core.session.connections import ConnectionManager
from pypeh.adapters.outbound.validation.pandera_adapter.parsers import parse_config, parse_error_report
from pypeh.adapters.outbound.persistence.hosts import FileIO

if TYPE_CHECKING:
    from typing import Mapping
    from pypeh.core.models.settings import FileSystemSettings

logger = logging.getLogger(__name__)


class DataFrameAdapter(ValidationInterface[DataFrame], DataImportInterface[DataFrame]):
    data_format = DataFrame

    def parse_configuration(self, config: ValidationConfig) -> Mapping:
        return parse_config(config)

    @contextmanager
    def get_error_collector(self):
        collector = ErrorCollector()
        try:
            yield collector
        finally:
            collector.clear_errors()

    def _validate(self, data: dict[str, list] | DataFrame, config: ValidationConfig) -> ValidationErrorReport:
        config_map = self.parse_configuration(config)
        validator = Validator.config_from_mapping(config=config_map, logger=logger)
        _ = validator.validate(data)

        with self.get_error_collector() as error_collector:
            report = parse_error_report(error_collector.get_errors())

        # Replace DataframeLocations with corresponding EntityLocation entries
        def get_data_item(data, row_index, column_name):
            if isinstance(data, dict):
                return data[column_name][row_index]
            if isinstance(data, DataFrame):
                return data.item(row_index, column_name)

        for group in report.groups:
            for error in group.errors:
                new_location_list = []
                assert error.locations is not None
                for location in error.locations:
                    row_ids = getattr(location, "row_ids", None)
                    key_columns = getattr(location, "key_columns", None)
                    if row_ids and key_columns:
                        entity_ids = [
                            tuple(get_data_item(data, row_id, id_obs_prop) for id_obs_prop in key_columns)
                            for row_id in row_ids
                        ]
                        new_location_list.append(
                            EntityLocation(
                                location_type="entity",
                                identifying_property_list=key_columns,
                                identifying_property_values=entity_ids,
                            )
                        )
                    else:
                        new_location_list.append(location)
                error.locations = new_location_list

        return report

    def _join_data(
        self,
        identifying_observable_property_ids: list[str],
        data: dict[str, list] | DataFrame,
        dependent_data: dict[str, dict[str, list]] | dict[str, DataFrame],
        dependent_observable_property_ids: set[str],
        observable_property_id_to_dataset_label_dict: dict[str, str],
    ) -> DataFrame:
        joined_data = data
        assert isinstance(joined_data, DataFrame), "joined_data in `DataFrameAdapter._join_data` should be a DataFrame"
        for dependent_obs_prop in dependent_observable_property_ids:
            dependent_section = observable_property_id_to_dataset_label_dict.get(dependent_obs_prop, None)
            if dependent_section is None:
                raise ValueError(f"Could not find data layout section for observable property {dependent_obs_prop}")
            other_result_proxy = dependent_data.get(dependent_section, None)
            if other_result_proxy is not None:
                other = other_result_proxy.observed_data
                assert isinstance(other, DataFrame), "other in `DataFrameAdapter._join_data` should be a DataFrame"
                joined_data = joined_data.join(other, on=identifying_observable_property_ids, how="left")
            else:
                raise ValueError(f"Did not find data section with label {dependent_section}")
        return joined_data

    # TEMP: to replace _join_data
    def _join_dataset(
        self,
        identifying_observable_property_ids: list[str],
        dataset: Dataset[DataFrame],
        dependent_data: dict[str, Dataset[DataFrame]],
        dependent_observable_property_ids: set[str],
        observable_property_id_to_dataset_label_dict: dict[str, str],
    ) -> DataFrame:
        joined_data = dataset.data
        assert isinstance(joined_data, DataFrame), "joined_data in `DataFrameAdapter._join_data` should be a DataFrame"
        for dependent_obs_prop in dependent_observable_property_ids:
            dependent_dataset = observable_property_id_to_dataset_label_dict.get(dependent_obs_prop, None)
            if dependent_dataset is None:
                raise ValueError(f"Could not find data layout section for observable property {dependent_obs_prop}")
            other_result_proxy = dependent_data[dependent_dataset]
            if other_result_proxy is not None:
                other = other_result_proxy.data
                assert isinstance(other, DataFrame), "other in `DataFrameAdapter._join_data` should be a DataFrame"
                joined_data = joined_data.join(other, on=identifying_observable_property_ids, how="left")
            else:
                raise ValueError(f"Did not find data section with label {dependent_dataset}")
        return joined_data

    def summarize(self, data: Mapping, config: Mapping):
        pass

    def import_data(self, source: str, config: FileSystemSettings, **kwargs) -> DataFrame | Dict[str, DataFrame]:
        provider = ConnectionManager._create_adapter(config)
        # format  = # should either be .csv or .xls/.xlsx
        # or provide additional info in kwargs
        format = FileIO.get_format(source)
        if format not in set(("csv", "xls", "xlsx")):
            # TODO: provide transformation function from format to dataframe
            logger.error("File format should either be .csv, .xls, or .xlsx")
            raise ValueError
        data = provider.load(source)
        if not isinstance(data, DataFrame):
            me = "Imported data is not a dataframe or dict of dataframes."
            if isinstance(data, dict):
                if not all(isinstance(d, DataFrame) for d in data.values()):
                    logger.error(me)
                    raise TypeError(me)
            else:
                logger.error(me)
                raise TypeError(me)
        return data

    def import_data_layout(
        self,
        source: str,
        config: FileSystemSettings,
        **kwargs,
    ) -> DataLayout | List[DataLayout]:
        return super().import_data_layout(source, config, **kwargs)

    def _normalize_observable_properties(self) -> bool:
        return True

    def _raw_data_to_observation_data(
        self,
        raw_data: DataFrame,
        data_layout_element_labels: list[str],
        identifying_layout_element_label: str,
        entity_id_list: list[str] | None = None,
    ) -> DataFrame:
        columns_to_select = [col for col in data_layout_element_labels if col in raw_data.columns]
        if not entity_id_list:
            ret = raw_data.select(columns_to_select)
        else:
            ret = raw_data.filter(pl.col(identifying_layout_element_label).is_in(entity_id_list)).select(
                columns_to_select
            )

        return ret

    def get_element_labels(self, data: DataFrame) -> list[str]:
        return data.columns

    def get_element_values(self, data: DataFrame, element_label: str) -> set[str]:
        return set(data.get_column(element_label))
