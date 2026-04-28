# Session API

`Session` is the main orchestration object in `pypeh`. It holds configured
connections, an in-memory cache of PEH model resources, optional namespace
management, and the adapters used for tabular data operations.

Import it from the package root:

```python
from pypeh import Session
```

## Create a Session

For simple local workflows, create an empty session and load resources later:

```python
session = Session()
session.load_persisted_cache(source="config")
```

For explicit local file storage, pass a `LocalFileConfig` and make it the
default connection:

```python
from pypeh import LocalFileConfig, Session

session = Session(
    connection_config=[
        LocalFileConfig(
            label="local_file",
            config_dict={"root_folder": "path/to/project"},
        ),
    ],
    default_connection="local_file",
)
```

You can also load the default persisted cache during initialization:

```python
session = Session(
    connection_config=[
        LocalFileConfig(
            label="local_file",
            config_dict={"root_folder": "path/to/project"},
        ),
    ],
    default_connection="local_file",
    load_from_default_connection="",
)
```

## Environment-Configured Default Cache

If `DEFAULT_PERSISTED_CACHE_TYPE=LocalFile` is set, `Session()` creates a
default local-file cache connection from environment variables with the
`DEFAULT_PERSISTED_CACHE_` prefix.

For example:

```bash
export DEFAULT_PERSISTED_CACHE_TYPE=LocalFile
export DEFAULT_PERSISTED_CACHE_ROOT_FOLDER=/path/to/project
```

Then:

```python
session = Session()
session.load_persisted_cache()
```

## Load PEH Resources

Use `load_persisted_cache` to load YAML resources into the session cache:

```python
session.load_persisted_cache(
    source="observations.yaml",
    connection_label="local_file",
)
```

Use `load_resource` when you need one resource by identifier and type:

```python
observation = session.load_resource(
    resource_identifier="peh:OBSERVATION_ADULTS_URINE_LAB",
    resource_type="Observation",
    resource_path="observations.yaml",
    connection_label="local_file",
)
```

You can retrieve already-cached resources with `get_resource`:

```python
observation = session.get_resource(
    "peh:OBSERVATION_ADULTS_URINE_LAB",
    "Observation",
)
```

## Load Tabular Data

`load_tabular_dataset_series` reads tabular data into a `DatasetSeries` using a
PEH `DataImportConfig`.

```python
from peh_model.peh import (
    DataImportConfig,
    DataImportSectionMapping,
    DataImportSectionMappingLink,
)

data_import_config = DataImportConfig(
    id="peh:IMPORT_CONFIG_SAMPLE_METADATA",
    layout="peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA",
    section_mapping=DataImportSectionMapping(
        section_mapping_links=[
            DataImportSectionMappingLink(
                section="SAMPLE_METADATA_SECTION_SAMPLE",
                observation_id_list=["peh:VALIDATION_TEST_SAMPLE_METADATA"],
            ),
        ]
    ),
)

dataset_series = session.load_tabular_dataset_series(
    source="sample_metadata.xlsx",
    data_import_config=data_import_config,
    connection_label="local_file",
)
```

The method checks loaded labels against the expected schema. By default, type
cast and schema errors are raised. Use `cast_error_policy="report"` or
`schema_error_policy="report"` to receive a `ValidationErrorReportCollection`
instead.

```python
result = session.load_tabular_dataset_series(
    source="sample_metadata.xlsx",
    data_import_config=data_import_config,
    connection_label="local_file",
    cast_error_policy="report",
    schema_error_policy="report",
)
```

Set `allow_incomplete=True` to allow missing labels while still reporting
undefined labels.

## Validate Tabular Data

Validate one dataset:

```python
report = session.validate_tabular_dataset(
    data=dataset_series["SAMPLE"],
    dependent_data=dataset_series,
)
```

Validate every dataset in a series:

```python
reports = session.validate_tabular_dataset_series(dataset_series)
```

Build validation configuration from a cached `DataLayout`:

```python
data_layout = session.load_resource(
    "peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA",
    "DataLayout",
)
validation_configs = session.build_validation_config(data_layout)
```

## Register Adapters

The session can use default adapters where available. To override a workflow,
register an adapter for its functionality key:

```python
session.register_adapter("validation", validation_adapter)
session.register_adapter("dataops", dataops_adapter)
session.register_adapter("enrichment", enrichment_adapter)
session.register_adapter("aggregate", aggregation_adapter)
```

You can also register by import path:

```python
session.register_adapter_by_name(
    "validation",
    "my_package.validation",
    "MyValidationAdapter",
)
```

## Enrichment and Aggregation

`enrich` and `aggregate` delegate to the registered adapter while passing a
cache view and the source and target observations.

```python
enriched = session.enrich(
    source_dataset_series=dataset_series,
    target_observations=target_observations,
    target_derived_from=source_observations,
)

summary = session.aggregate(
    source_dataset_series=dataset_series,
    target_observations=target_observations,
    target_derived_from=source_observations,
)
```

The target observation list and source observation list must have the same
length.

## Namespaces and Minting

Bind a `NamespaceManager` before minting new PEH resources:

```python
from peh_model.peh import ObservableProperty
from pypeh import NamespaceManager

namespace_manager = NamespaceManager(
    default_base_uri="https://w3id.org/example/id/"
)
session.bind_namespace_manager(namespace_manager)

observable_property = session.mint_and_cache(
    ObservableProperty,
    ui_label="cholesterol",
)
```

The minted resource is added to the session cache.
