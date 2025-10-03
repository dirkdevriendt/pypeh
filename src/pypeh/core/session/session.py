from __future__ import annotations

import os
import importlib
import logging
import peh_model.peh as peh

from typing import TYPE_CHECKING, TypeVar, Sequence, Dict, Generic

from pypeh.core.cache.containers import CacheContainer, CacheContainerFactory
from pypeh.core.models.constants import ObservablePropertyValueType
from pypeh.core.models.proxy import TypedLazyProxy
from pypeh.core.models.settings import (
    LocalFileConfig,
    ImportConfig,
    ConnectionConfig,
    ValidatedImportConfig,
    DEFAULT_CONNECTION_LABEL,
)
from pypeh.core.models.typing import T_NamedThingLike, T_DataType
from pypeh.core.models.validation_dto import ValidationConfig
from pypeh.core.models.validation_errors import (
    ValidationError,
    ValidationErrorLevel,
    ValidationErrorReport,
    ValidationErrorReportCollection,
)
from pypeh.core.interfaces.outbound.dataops import ValidationInterface
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
            case _:
                raise NotImplementedError()

        return adapter

    def register_adapter(self, interface_functionality: str, adapter: T_AdapterType):
        self._adapter_mapping[interface_functionality] = adapter

    def register_adapter_by_name(
        self,
        interface_functionality: str,
        adapter_module_name: str | None = None,
        adapter_class_name: str | None = None,
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

        return adapter()

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

    def load_tabular_data(
        self, source: str, connection_label: str | None = None, data_layout: peh.DataLayout | None = None
    ) -> dict[str, DataFrame] | ValidationError:
        """
        Load a binary resource and return its content as tabular data in a dataframe
        Args:
            source (str): A path or url pointing to the data to be loaded in.
            connection_label (str | None):
                Optional key pointing to the connection to be used to
                load in the data source. The connection_label should be a key of the provided
                connection_config.
            validation_layout: (DataLayout | None) Optional DataLayout object used for validation.
        """
        data_schema = None
        if data_layout is not None:
            data_schema = self.layout_section_elements_to_observable_property_value_types(
                layout=data_layout,
            )
        try:
            # TODO: fix host calls with unified ConnectionManager
            if is_url(source):
                raise NotImplementedError
            elif connection_label is not None:
                pass
            else:
                connection_label = DEFAULT_CONNECTION_LABEL

            with self.connection_manager.get_connection(connection_label=connection_label) as connection:
                return connection.load(source, validation_layout=data_layout, data_schema=data_schema)

        except Exception as e:
            return ValidationError(
                message=f"File could not be read or validated: {e}",
                type="File Processing Error",
                level=ValidationErrorLevel.FATAL,
            )

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

    def get_dataset_validations_dict(
        self,
        observation_list: Sequence[peh.Observation],
        layout: peh.DataLayout,
        dataset_mapping: Dict[str, Dict[str, str | int | Dict[str, Sequence[str]]]],
        data_dict: Dict[str, Dict[str, Sequence] | T_DataType],
    ) -> Dict[str, Sequence[peh.ValidationDesign]] | None:
        return ValidationConfig.get_dataset_validations_dict(observation_list, layout, dataset_mapping, data_dict)

    def get_dataset_identifier_consistency_validations_dict(
        self,
        observation_list: Sequence[peh.Observation],
        layout: peh.DataLayout,
        dataset_mapping: Dict[str, Dict[str, str | int | Dict[str, Sequence[str]]]],
        data_dict: Dict[str, Dict[str, Sequence] | T_DataType],
    ) -> Dict[str, Sequence[peh.ValidationDesign]] | None:
        return ValidationConfig.get_dataset_identifier_consistency_validations_dict(
            observation_list, layout, dataset_mapping, data_dict
        )

    def validate_tabular_data(
        self,
        data: dict[str, Sequence] | DataFrame,
        data_layout_section: peh.DataLayoutSection,
        dataset_validations: Sequence[peh.ValidationDesign] | None = None,
        dependent_data: dict[str, dict[str, Sequence]] | dict[str, DataFrame] | None = None,
        observable_property_id_to_layout_section_label: dict[str, str] | None = None,
    ) -> ValidationErrorReport:
        try:
            # Fetch and resolve Observation link from DataLayoutSection
            observation_id = data_layout_section.observation
            if observation_id is None:
                raise ValueError("The DataLayoutSection does not contain a reference to an Observation")
            assert isinstance(
                observation_id, str
            ), "observation_id in `Session.validate_tabular_data` should be a string"
            observation = self.cache.get(observation_id, "Observation")
            assert isinstance(
                observation, peh.Observation
            ), "observation in `Session.validate_tabular_data` should be an `Observation`"
            observable_property_ids = set()
            if observation.observation_design is None:
                raise ValueError(f"Specified observation {observation.id} has no ObservationDesign")
            # Extract observable properties from Observation
            # NOTE: these have to correspond to the observable properties in the DataLayoutSection elements
            observable_property_ids.update(
                observation.observation_design.identifying_observable_property_id_list,
                observation.observation_design.optional_observable_property_id_list,
                observation.observation_design.required_observable_property_id_list,
            )
            observable_properties = [
                op for op in self.cache.get_all("ObservableProperty") if op.id in observable_property_ids
            ]
            assert len(observable_properties) > 0

            validation_adapter = self.get_adapter("validation")
            return validation_adapter.validate(
                data=data,
                observation=observation,
                observable_properties=observable_properties,
                dataset_validations=dataset_validations,
                dependent_data=dependent_data,
                observable_property_id_to_layout_section_label=observable_property_id_to_layout_section_label,
            )

        except Exception as e:
            return ValidationErrorReport.from_runtime_error(e)

    def validate_tabular_data_collection(
        self,
        data_collection: dict[str, dict[str, Sequence]] | dict[str, DataFrame],
        data_layout: peh.DataLayout,
    ) -> ValidationErrorReportCollection:
        """
        data_collection: keys are `DataLayoutSection` labels
        """
        observable_property_to_layout_section = self.observable_property_to_layout_section(data_layout)
        result_dict = ValidationErrorReportCollection()
        assert isinstance(
            data_layout, peh.DataLayout
        ), "data_layout in `Session.validate_tabular_data_collection` should be a `DataLayout`"
        sections = data_layout.sections
        assert sections is not None

        for section in sections:
            if section is not None:
                assert isinstance(
                    section, peh.DataLayoutSection
                ), f"DataLayoutSection {section} wrong type. Should be a `DataLayoutSection`"
                section_label = section.ui_label
                observation_id = section.observation
                if observation_id is None:
                    continue
                assert isinstance(
                    observation_id, str
                ), "observation_id in `Session.validate_tabular_data_collection` should not be a string"
                assert isinstance(
                    section_label, str
                ), "section_label in `Session.validate_tabular_data_collection` should be a string"
                data = data_collection.get(section_label, None)
                assert data is not None, "data in `Session.validate_tabular_data_collection` should not be None"
                ret = self.validate_tabular_data(
                    data=data,
                    data_layout_section=section,
                    dependent_data=data_collection,
                    observable_property_id_to_layout_section_label=observable_property_to_layout_section,
                )
                assert isinstance(
                    ret, ValidationErrorReport
                ), "ret in `Session.validate_tabular_data_collection` should not be a`ValidationErrorReport`"
                result_dict[observation_id] = ret

        return result_dict

    def validate_tabular_data_collection_by_reference(
        self,
        data_collection_id: str,
        data_layout_id: str,
        data_collection_connection_label: str | None = None,
        data_layout_path: str | None = None,
        data_layout_connection_label: str | None = None,
    ) -> ValidationErrorReportCollection:
        # fetch data_layout
        data_layout = self.load_resource(
            resource_identifier=data_layout_id,
            resource_type="DataLayout",
            resource_path=data_layout_path,
            connection_label=data_layout_connection_label,
        )
        assert isinstance(
            data_layout, peh.DataLayout
        ), "data_layout in `Session.validate_tabular_data_collection_by_reference` should be a `peh.DataLayout`"
        # fetch data_collection
        data_collection = self.load_tabular_data(
            source=data_collection_id,
            connection_label=data_collection_connection_label,
            data_layout=data_layout,
        )
        assert isinstance(
            data_collection, dict
        ), "data_collection in `Session.validate_tabular_data_collection_by_reference` should be a dict"
        return self.validate_tabular_data_collection(
            data_collection=data_collection,
            data_layout=data_layout,
        )

    ### CREATE MAPPINGS BASED ON CACHE CONTENT ###

    def layout_section_elements_to_observable_property_value_types(
        self, layout: peh.DataLayout, flatten=False
    ) -> dict[str, ObservablePropertyValueType | dict[str, ObservablePropertyValueType]] | None:
        ret = {}

        sections = getattr(layout, "sections")
        if sections is None:
            raise ValueError("No sections found in DataLayout")
        for section in sections:
            label = getattr(section, "ui_label")
            elements = getattr(section, "elements")
            if elements is None:
                logger.info("DataLayout does not contain elements. Cannot determine observable_entity_value_types.")
                return None
            for element in elements:
                element_label = getattr(element, "label")
                observable_property_id = getattr(element, "observable_property")
                observable_property = self.cache.get(observable_property_id, "ObservableProperty")
                if observable_property is None:
                    logger.info(
                        f"Could not find {observable_property_id} in cache. Cannot determine observable_property_value_types. "
                    )
                    return None
                assert isinstance(label, str)
                value_type = getattr(observable_property, "value_type")
                if flatten:
                    ret[element_label] = ObservablePropertyValueType(value_type)
                else:
                    if label not in ret:
                        ret[label] = {}
                    ret[label][element_label] = ObservablePropertyValueType(value_type)

        return ret

    def observable_property_to_layout_section(self, layout: peh.DataLayout) -> dict[str, str]:
        ret = {}
        sections = layout.sections
        assert sections is not None
        for section in sections:
            assert isinstance(section, peh.DataLayoutSection)
            section_label = section.ui_label
            elements = section.elements
            assert elements is not None
            for element in elements:
                assert isinstance(element, peh.DataLayoutElement)
                observable_property = element.observable_property
                assert observable_property is not None
                assert isinstance(observable_property, str)
                ret[observable_property] = section_label

        return ret
