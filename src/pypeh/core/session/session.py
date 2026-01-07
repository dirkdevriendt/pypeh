from __future__ import annotations

import os
import importlib
import logging
import peh_model.peh as peh

from typing import TYPE_CHECKING, TypeVar, Sequence, Generic

from pypeh.core.cache.containers import CacheContainer, CacheContainerFactory, CacheContainerView
from pypeh.core.models.proxy import TypedLazyProxy
from pypeh.core.models.settings import (
    LocalFileConfig,
    ImportConfig,
    ConnectionConfig,
    ValidatedImportConfig,
    DEFAULT_CONNECTION_LABEL,
)
from pypeh.core.models.typing import T_NamedThingLike, T_DataType
from pypeh.core.models.validation_errors import (
    ValidationErrorReport,
    ValidationErrorReportCollection,
)
from pypeh.core.models.internal_data_layout import DatasetSeries, Dataset
from pypeh.core.interfaces.outbound.dataops import ValidationInterface, DataImportInterface
from pypeh.core.cache.utils import load_entities_from_tree
from pypeh.core.session.connections import ConnectionManager
from pypeh.core.utils.resolve_identifiers import is_url

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from polars import DataFrame
    from pydantic_settings import BaseSettings
    from typing import Sequence

T_AdapterType = TypeVar("T_AdapterType")


class Session(Generic[T_AdapterType, T_DataType]):
    _adapter_mapping: dict[str, T_AdapterType] = dict()

    def __init__(
        self,
        *,
        connection_config: ConnectionConfig | Sequence[ConnectionConfig] | None = None,
        default_connection: str | ConnectionConfig | None = None,
        env_file: str | None = None,
        load_from_default_connection: str | None = None,
    ):
        """
        Initializes a new pypeh Session.

        Args:
            connection_config (ConnectionConfig | Sequence[ConnectionConfig] | None):
                A (list of) ConnectionConfig instance(s). Allows you to setup connection to local
                or remote repositories.
                Required if a string-based default_connection is used.
            default_connection (str | ConnectionConfig | None):
                Specifies the default storage for the session. Can either be:
                    - A string key referring to a connection in connection_config,
                    - A ConnectionConfig instance to directly generate BaseSettings.
            load_from_default_connection: (str | None = None):
                Optional. Source to load from default connection on init.
        """
        connection_map, default_connection = self._normalize_configs(connection_config, default_connection)
        self.connection_manager: ConnectionManager = ConnectionManager(ValidatedImportConfig())
        validated_default_connection: BaseSettings | None = self._init_default_connection(default_connection, env_file)
        if connection_map is not None:
            import_config = ImportConfig(connection_map=connection_map).to_validated_import_config(_env_file=env_file)
            self.connection_manager = ConnectionManager(import_config)

        if validated_default_connection is not None:
            self.connection_manager._register_connection_label(DEFAULT_CONNECTION_LABEL, validated_default_connection)
        self.cache: CacheContainer = CacheContainerFactory.new()
        if load_from_default_connection is not None:
            _ = self.load_persisted_cache(source=load_from_default_connection)

    def _normalize_configs(
        self,
        connection_config,
        default_connection,
    ) -> tuple[dict[str, ConnectionConfig], ConnectionConfig | None]:
        """Validates and normalizes configs before init proceeds."""
        connection_map = {}
        # Handle missing connection_config
        if connection_config is None:
            if default_connection is None:
                default_connection = self._env_default_connection()
            elif isinstance(default_connection, str):
                raise ValueError("String value for default_connection requires a connection_config")
            elif not isinstance(default_connection, ConnectionConfig):
                logger.debug("All resources will be loaded as linked open data")
        else:
            if isinstance(connection_config, ConnectionConfig):
                connection_map = {connection_config.label: connection_config}
            elif isinstance(connection_config, Sequence):
                for config in connection_config:
                    if not isinstance(config, ConnectionConfig):
                        raise ValueError("connection_config argument is of wrong type")
                    connection_map[config.label] = config
            else:
                raise ValueError("connection_config argument is of wrong type")

        # Validate string cache references
        validated_default_connection = None
        if isinstance(default_connection, str):
            if default_connection not in connection_map:
                raise ValueError("Default connection string must refer to a key in connection_config")
            validated_default_connection = connection_map[default_connection]
        elif isinstance(default_connection, ConnectionConfig):
            if default_connection.namespaces is not None:
                logger.warning(
                    "default_connection has namespaces associated to it. These are ignored."
                    " Use the connection_config to achieve this"
                )
            validated_default_connection = default_connection

        return connection_map, validated_default_connection

    def _env_default_connection(self) -> ConnectionConfig | None:
        """Derives a default cache config from environment variables."""
        if os.environ.get("DEFAULT_PERSISTED_CACHE_TYPE", "").upper() == "LOCALFILE":
            return LocalFileConfig(env_prefix="DEFAULT_PERSISTED_CACHE_")

    def _init_default_connection(
        self,
        default_connection: ConnectionConfig | None,
        env_file: str | None,
    ) -> BaseSettings | None:
        """Creates the BaseSettings instance for the default cache."""
        if isinstance(default_connection, ConnectionConfig):
            return default_connection.make_settings(_env_file=env_file)
        return None

    def register_default_adapter(self, interface_functionality: str):
        adapter = None
        match interface_functionality:
            case "validation":
                adapter = ValidationInterface.get_default_adapter_class()
                self._adapter_mapping[interface_functionality] = adapter
            case "data_import":
                adapter = ValidationInterface.get_default_adapter_class()
                self._adapter_mapping[interface_functionality] = adapter
            case _:
                raise NotImplementedError()

        return adapter

    def register_adapter(self, interface_functionality: str, adapter: T_AdapterType):
        self._adapter_mapping[interface_functionality] = adapter

    def register_adapter_by_name(
        self,
        interface_functionality: str,
        adapter_module_name: str,
        adapter_class_name: str,
    ):
        try:
            adapter_module = importlib.import_module(adapter_module_name)
            adapter = getattr(adapter_module, adapter_class_name)
        except Exception as e:
            logger.error(
                f"Exception encountered while attempting to import the requested {interface_functionality} adapter: {adapter_module_name} - {adapter_class_name}"
            )
            raise e
        self.register_adapter(interface_functionality, adapter)

    def get_adapter(self, interface_functionality: str):
        adapter = self._adapter_mapping.get(interface_functionality)
        if adapter is None:
            adapter = self.register_default_adapter(interface_functionality)
        assert adapter is not None

        if isinstance(adapter, type):
            return adapter()
        else:
            return adapter

    def _root_to_cache(self, root: peh.EntityList) -> bool:
        for entity in load_entities_from_tree(root):
            _ = self.cache.add(entity)

        return True

    def _source_to_cache(self, roots: list | peh.EntityList) -> bool:
        if isinstance(roots, list):
            for root in roots:
                ret = self._root_to_cache(root)
                assert ret
        else:
            ret = self._root_to_cache(roots)

        return True

    def load_persisted_cache(self, source: str | None = None, connection_label: str | None = None):
        """Load all resources from either the default cache persistence location or from the provided
        connection into cache. The provided connection_label takes precedence over the default.
        Currently all resources should still be represented as yaml files.
        """
        # get host/connection
        # TODO: fix host calls with unified ConnectionManager
        if connection_label is None:
            logger.info("Using DEFAULT_CONNECTION_LABEL in absence of connection_label")
            connection_label = DEFAULT_CONNECTION_LABEL

        if source is None:
            # TEMP FIX: will only work with filesystems
            source = ""

        with self.connection_manager.get_connection(connection_label=connection_label) as connection:
            roots = connection.load(source, format="yaml")

        ret = self._source_to_cache(roots)
        assert ret

    def load_tabular_dataset_series(
        self,
        source: str,
        data_import_config: peh.DataImportConfig,
        connection_label: str | None = None,
    ) -> DatasetSeries[DataFrame]:
        cache_view = CacheContainerView(self.cache)
        assert isinstance(data_import_config, peh.DataImportConfig)
        data_layout_id = data_import_config.layout
        assert data_layout_id is not None
        data_layout = self.get_resource(data_layout_id, "DataLayout")
        assert isinstance(data_layout, peh.DataLayout)
        dataset_series = DatasetSeries.from_peh_datalayout(data_layout, cache_view=cache_view)
        dataset_series.add_metadata("data_import_config_id", data_import_config.id)
        data_schema = dataset_series.get_type_annotations()

        # TODO: fix host calls with unified ConnectionManager
        if is_url(source):
            raise NotImplementedError
        elif connection_label is not None:
            pass
        else:
            connection_label = DEFAULT_CONNECTION_LABEL

        with self.connection_manager.get_connection(connection_label=connection_label) as connection:
            data_dict = connection.load(source, validation_layout=data_layout, data_schema=data_schema)
        assert isinstance(data_dict, dict)
        import_adapter = self.get_adapter("data_import")
        for raw_dataset_label, raw_dataset in data_dict.items():
            assert isinstance(import_adapter, DataImportInterface)
            non_empty_dataset_elements = import_adapter.get_element_labels(raw_dataset)
            _ = dataset_series.add_data(
                dataset_label=raw_dataset_label, data=raw_dataset, non_empty_dataset_elements=non_empty_dataset_elements
            )

        return dataset_series

    def get_resource(self, resource_identifier: str, resource_type: str) -> T_NamedThingLike | None:
        """Get resource from cache"""
        ret = self.cache.get(resource_identifier, resource_type)
        if ret is None:
            logger.debug(f"No resource found with identifier {resource_identifier}")

        return ret

    def resolve_typed_lazy_proxy(self, proxy: TypedLazyProxy) -> peh.NamedThing:
        raise NotImplementedError()

    def load_resource(
        self,
        resource_identifier: str,
        resource_type: str,
        resource_path: str | None = None,
        connection_label: str | None = None,
    ) -> T_NamedThingLike | None:
        """Load resource into cache. First checks the cache,
        then configured persisted cache, and finally the `ImportConfig`"""
        # cache
        ret = self.get_resource(resource_identifier, resource_type)
        if ret is not None:
            return ret

        if connection_label is not None:
            with self.connection_manager.get_connection(connection_label=connection_label) as connection:
                # assuming connection points to a file-based system
                # loading entire directory
                logger.debug(f"Loading .yaml files recursively from {connection_label} root directory")
                if resource_path is None:
                    resource_path = ""
                    roots = connection.load(resource_path, format="yaml")
                else:
                    roots = connection.load(resource_path)
                ret = self._source_to_cache(roots)
                assert ret

            # resource should have been loaded into cache
            ret = self.get_resource(resource_identifier, resource_type)
            type_to_cast = getattr(peh, resource_type)
            assert isinstance(ret, type_to_cast)
        else:
            # TODO: use linked data approach
            raise NotImplementedError

        return ret

    def load_project(self, project_identifier: str, connection_label: str | None = None) -> T_NamedThingLike | None:
        return self.load_resource(project_identifier, resource_type="Project", connection_label=connection_label)

    def dump_resource(self, resource_identifier: str, resource_type: str, version: str | None) -> bool:
        return True

    def dump_project(self, project_identifier: str, version: str | None) -> bool:
        return self.dump_resource(project_identifier, resource_type="Project", version=version)

    def validate_tabular_dataset(
        self,
        data: Dataset[DataFrame],
        dependent_data: DatasetSeries[DataFrame] | None = None,
    ) -> ValidationErrorReport:
        assert data.data is not None, f"No data associated with {data.label}"
        cache_view = CacheContainerView(self.cache)
        validation_adapter = self.get_adapter("validation")
        assert isinstance(validation_adapter, ValidationInterface)
        return validation_adapter.validate(
            dataset=data,
            dependent_dataset_series=dependent_data,
            cache_view=cache_view,
        )

    def validate_tabular_dataset_series(
        self,
        dataset_series: DatasetSeries[DataFrame],
    ) -> ValidationErrorReportCollection:
        validation_result_dict = ValidationErrorReportCollection()
        for dataset_label in dataset_series:
            dataset = dataset_series[dataset_label]
            assert dataset is not None
            if dataset.data is None:
                continue
            validation_result = self.validate_tabular_dataset(data=dataset, dependent_data=dataset_series)
            assert isinstance(
                validation_result, ValidationErrorReport
            ), "validation_result in `Session.validate_tabular_dataset_series` should be a`ValidationErrorReport`"
            validation_result_dict[dataset_label] = validation_result

        # Catch no data in dataset_series case
        assert len(validation_result_dict) > 0, f"DatasetSeries with label {dataset_series.label} contains no data"

        return validation_result_dict
