import pytest
import abc

from datetime import date
from typing import Protocol, Any, Generic
from peh_model.peh import DataLayout

from pypeh.core.cache.containers import CacheContainerFactory, CacheContainerView
from pypeh.core.cache.utils import load_entities_from_tree
from pypeh.core.interfaces.outbound.dataops import T_DataType, ValidationInterface
from pypeh.core.models.internal_data_layout import (
    Dataset,
    DatasetSchema,
    DatasetSchemaElement,
    DatasetSeries,
    ElementReference,
    ForeignKey,
)
from pypeh.core.models.validation_errors import ValidationErrorReport
from pypeh.core.models.constants import ObservablePropertyValueType, ValidationErrorLevel
from pypeh.core.models.validation_dto import (
    ValidationExpression,
    ValidationDesign,
    ColumnValidation,
    ValidationConfig,
)
from pypeh.core.models.settings import LocalFileSettings
from pypeh.core.models.graph import Graph
from pypeh.adapters.outbound.persistence.hosts import DirectoryIO
from tests.test_utils.dirutils import get_absolute_path


class DataOpsProtocol(Protocol, Generic[T_DataType]):
    data_format: T_DataType

    def _validate(self, data, config) -> ValidationErrorReport: ...

    def validate(self, dataset, dependent_dataset_series, cache_view) -> ValidationErrorReport: ...

    def import_data(self, source, config) -> Any: ...

    def import_data_layout(self, source, config) -> Any: ...

    def build_validation_config(self, dataset, dataset_series, cache_view) -> ValidationConfig: ...


class TestValidation(abc.ABC):
    """Abstract base class for testing dataops adapters."""

    __test__ = False

    @abc.abstractmethod
    def get_adapter(self) -> DataOpsProtocol:
        """Return the adapter implementation to test."""
        raise NotImplementedError

    def get_container(self, path: str, is_file=True) -> CacheContainerView:
        source = get_absolute_path(path)
        container = CacheContainerFactory.new()
        host = DirectoryIO()
        roots = host.load(source, format="yaml")
        if is_file:
            roots = [roots]
        for root in roots:
            for entity in load_entities_from_tree(root):
                container.add(entity)

        return CacheContainerView(container)

    def get_container_validation_example_03(self) -> CacheContainerView:
        src_path = "./input/ValidationExamples/validation_test_03_corrected_config.yaml"
        cache_view = self.get_container(src_path)
        return cache_view

    def test_getting_default_adapter_from_interface(self):
        adapter_class = ValidationInterface.get_default_adapter_class()
        adapter = adapter_class()
        assert isinstance(adapter, ValidationInterface)
        assert isinstance(adapter, type(self.get_adapter()))

    @pytest.mark.parametrize(
        "config, data, expected_output",
        [
            # Simple validation using integers
            (
                ValidationConfig(
                    name="simple_validation_integer",
                    columns=[
                        ColumnValidation(
                            unique_name="col1",
                            data_type="integer",
                            required=True,
                            nullable=False,
                            validations=[
                                ValidationDesign(
                                    name="is_greater_than_other_column",
                                    error_level=ValidationErrorLevel.ERROR,
                                    expression=ValidationExpression(
                                        command="is_greater_than",
                                        arg_columns=["col2"],
                                    ),
                                ),
                                ValidationDesign(
                                    name="is_greater_than_number",
                                    error_level=ValidationErrorLevel.WARNING,
                                    expression=ValidationExpression(
                                        command="is_greater_than",
                                        arg_values=[2],
                                    ),
                                ),
                            ],
                        )
                    ],
                    identifying_column_names=["col1"],
                    validations=[],
                ),
                {
                    "col1": [3, 1],
                    "col2": [1, 2],
                },
                {
                    "name": "simple_validation_integer",
                    "total_errors": 2,
                    "errors_counts": {
                        ValidationErrorLevel.INFO: 0,
                        ValidationErrorLevel.WARNING: 1,
                        ValidationErrorLevel.ERROR: 1,
                        ValidationErrorLevel.FATAL: 0,
                    },
                },
            ),
            # Simple validation using integers at df level
            (
                ValidationConfig(
                    name="simple_validation_integer_df_level",
                    columns=[
                        ColumnValidation(
                            unique_name="col1",
                            data_type="integer",
                            required=True,
                            nullable=False,
                            validations=[],
                        )
                    ],
                    identifying_column_names=["col1"],
                    validations=[
                        ValidationDesign(
                            name="name",
                            error_level=ValidationErrorLevel.ERROR,
                            expression=ValidationExpression(
                                command="is_greater_than",
                                arg_columns=["col2"],
                                subject=["col1"],
                            ),
                        )
                    ],
                ),
                {
                    "col1": [3, 1],
                    "col2": [1, 2],
                },
                {
                    "name": "simple_validation_integer_df_level",
                    "total_errors": 1,
                    "errors_counts": {
                        ValidationErrorLevel.INFO: 0,
                        ValidationErrorLevel.WARNING: 0,
                        ValidationErrorLevel.ERROR: 1,
                        ValidationErrorLevel.FATAL: 0,
                    },
                },
            ),
            # disjunction validation using integers
            (
                ValidationConfig(
                    name="disjunction_validation",
                    columns=[
                        ColumnValidation(
                            unique_name="col1",
                            data_type="integer",
                            required=True,
                            nullable=False,
                            validations=[
                                ValidationDesign(
                                    name="name",
                                    error_level=ValidationErrorLevel.ERROR,
                                    expression=ValidationExpression(
                                        command="disjunction",
                                        arg_expressions=[
                                            ValidationExpression(
                                                command="is_greater_than",
                                                arg_columns=["col2"],
                                            ),
                                            ValidationExpression(
                                                command="is_less_than",
                                                subject=["col2"],
                                                arg_values=[0],
                                            ),
                                        ],
                                    ),
                                )
                            ],
                        )
                    ],
                    identifying_column_names=["col1"],
                    validations=[],
                ),
                {
                    "col1": [3, 1],
                    "col2": [1, 2],
                },
                {
                    "name": "disjunction_validation",
                    "total_errors": 1,
                    "errors_counts": {
                        ValidationErrorLevel.INFO: 0,
                        ValidationErrorLevel.WARNING: 0,
                        ValidationErrorLevel.ERROR: 1,
                        ValidationErrorLevel.FATAL: 0,
                    },
                },
            ),
            # Conjunction validation using integers
            (
                ValidationConfig(
                    name="conjunction_validation",
                    columns=[
                        ColumnValidation(
                            unique_name="col1",
                            data_type="integer",
                            required=True,
                            nullable=False,
                            validations=[
                                ValidationDesign(
                                    name="name",
                                    error_level=ValidationErrorLevel.WARNING,
                                    expression=ValidationExpression(
                                        command="conjunction",
                                        arg_expressions=[
                                            ValidationExpression(
                                                command="is_greater_than",
                                                arg_columns=["col2"],
                                            ),
                                            ValidationExpression(
                                                command="is_less_than",
                                                subject=["col2"],
                                                arg_values=[1],
                                            ),
                                        ],
                                    ),
                                )
                            ],
                        )
                    ],
                    identifying_column_names=["col1"],
                    validations=[],
                ),
                {
                    "col1": [3, 0],
                    "col2": [1, -1],
                },
                {
                    "name": "conjunction_validation",
                    "total_errors": 1,
                    "errors_counts": {
                        ValidationErrorLevel.INFO: 0,
                        ValidationErrorLevel.WARNING: 1,
                        ValidationErrorLevel.ERROR: 0,
                        ValidationErrorLevel.FATAL: 0,
                    },
                },
            ),
            # Simple validation using strings
            (
                ValidationConfig(
                    name="simple_validation_strings",
                    columns=[
                        ColumnValidation(
                            unique_name="col1",
                            data_type="string",
                            required=True,
                            nullable=False,
                            validations=[
                                ValidationDesign(
                                    name="name",
                                    error_level=ValidationErrorLevel.ERROR,
                                    expression=ValidationExpression(
                                        command="is_in",
                                        arg_values=["value1", "value2"],
                                    ),
                                )
                            ],
                        ),
                        ColumnValidation(
                            unique_name="col2",
                            data_type="integer",
                            required=True,
                            nullable=False,
                            validations=[],
                        ),
                    ],
                    identifying_column_names=["col1", "col2"],
                    validations=[],
                ),
                {
                    "col1": ["value1", "value2", "value3"],
                    "col2": [1, -1, None],
                },
                {
                    "name": "simple_validation_strings",
                    "total_errors": 2,
                    "errors_counts": {
                        ValidationErrorLevel.INFO: 0,
                        ValidationErrorLevel.WARNING: 0,
                        ValidationErrorLevel.ERROR: 2,
                        ValidationErrorLevel.FATAL: 0,
                    },
                },
            ),
            # Duplicated ID
            (
                ValidationConfig(
                    name="duplicate_id_validation",
                    columns=[
                        ColumnValidation(
                            unique_name="col1",
                            data_type="string",
                            required=True,
                            nullable=False,
                            validations=[],
                        ),
                        ColumnValidation(
                            unique_name="col2",
                            data_type="integer",
                            required=True,
                            nullable=False,
                            validations=[],
                        ),
                    ],
                    identifying_column_names=["col1", "col2"],
                    validations=[],
                ),
                {
                    "col1": ["value1", "value2", "value1"],
                    "col2": [1, -1, 1],
                },
                {
                    "name": "duplicate_id_validation",
                    "total_errors": 1,
                    "errors_counts": {
                        ValidationErrorLevel.INFO: 0,
                        ValidationErrorLevel.WARNING: 0,
                        ValidationErrorLevel.ERROR: 1,
                        ValidationErrorLevel.FATAL: 0,
                    },
                },
            ),
            # function implementation test
            (
                ValidationConfig(
                    name="function implementation test",
                    columns=[
                        ColumnValidation(
                            unique_name="col1",
                            data_type="float",
                            required=True,
                            nullable=False,
                            # decimals_precision
                            validations=[
                                ValidationDesign(
                                    name="fn",
                                    error_level=ValidationErrorLevel.ERROR,
                                    expression=ValidationExpression(
                                        command="decimals_precision",
                                        arg_values=[3],
                                    ),
                                )
                            ],
                        ),
                        ColumnValidation(
                            unique_name="col2",
                            data_type="integer",
                            required=True,
                            nullable=False,
                            validations=[],
                        ),
                    ],
                    identifying_column_names=["col2"],
                    validations=[],
                ),
                {
                    "col1": [1.234, 1.0, 1.123456],
                    "col2": [1, 2, 3],
                },
                {
                    "name": "function implementation test",
                    "total_errors": 1,
                    "errors_counts": {
                        ValidationErrorLevel.INFO: 0,
                        ValidationErrorLevel.WARNING: 0,
                        ValidationErrorLevel.ERROR: 1,
                        ValidationErrorLevel.FATAL: 0,
                    },
                },
            ),
            # conditional
            (
                ValidationConfig(
                    name="conditional test",
                    columns=[
                        ColumnValidation(
                            unique_name="col1",
                            data_type="integer",
                            required=True,
                            nullable=False,
                            validations=[
                                ValidationDesign(
                                    name="conditional",
                                    error_level=ValidationErrorLevel.ERROR,
                                    expression=ValidationExpression(
                                        conditional_expression=ValidationExpression(
                                            command="is_greater_than",
                                            arg_columns=["col2"],
                                        ),
                                        command="is_equal_to",
                                        arg_values=[5],
                                    ),
                                )
                            ],
                        ),
                        ColumnValidation(
                            unique_name="col2",
                            data_type="integer",
                            required=True,
                            nullable=False,
                            validations=[],
                        ),
                    ],
                    identifying_column_names=["col2"],
                    validations=[],
                ),
                {
                    "col1": [20, 1, 5],
                    "col2": [15, 20, 3],
                },
                {
                    "name": "conditional test",
                    "total_errors": 1,
                    "errors_counts": {
                        ValidationErrorLevel.INFO: 0,
                        ValidationErrorLevel.WARNING: 0,
                        ValidationErrorLevel.ERROR: 1,
                        ValidationErrorLevel.FATAL: 0,
                    },
                },
            ),
        ],
    )
    def test_validate(self, config, data, expected_output):
        adapter = self.get_adapter()
        result = adapter._validate(data, config)
        assert result is not None
        assert result.groups[0].name == expected_output.get("name")
        assert result.total_errors == expected_output.get("total_errors")
        assert result.error_counts == expected_output.get("errors_counts")

    def test_build_validation_config(self):
        adapter = self.get_adapter()
        cache_view = self.get_container_validation_example_03()
        data_layout = cache_view.get("peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA", "DataLayout")
        assert isinstance(data_layout, DataLayout)
        dataset_series = DatasetSeries.from_peh_datalayout(
            data_layout=data_layout, cache_view=cache_view, apply_context=True
        )
        dataset = dataset_series.get("SAMPLETIMEPOINT_BSS")
        assert dataset is not None
        dataset.metadata["non_empty_dataset_elements"] = ["id_sample", "chol", "chol_lod", "chol_loq"]
        validation_config = adapter.build_validation_config(
            dataset=dataset,
            dataset_series=dataset_series,
            cache_view=cache_view,
        )
        assert isinstance(validation_config, ValidationConfig)


class TestDataImport(abc.ABC):
    """Abstract base class for testing dataops adapters."""

    __test__ = False

    @abc.abstractmethod
    def get_adapter(self) -> DataOpsProtocol:
        """Return the adapter implementation to test."""
        raise NotImplementedError

    def test_import_data_layout(self):
        adapter = self.get_adapter()
        source = "./input/datalayout.yaml"
        path = get_absolute_path(source)
        config = LocalFileSettings()
        data = adapter.import_data_layout(path, config)
        if isinstance(data, list):
            assert all(isinstance(dl, DataLayout) for dl in data)
        else:
            assert isinstance(data, DataLayout)

    def test_import_csv(self):
        adapter = self.get_adapter()
        source = "./input/data.csv"
        path = get_absolute_path(source)
        config = LocalFileSettings()
        data = adapter.import_data(path, config)
        assert isinstance(data, adapter.data_format)

    def test_import_excel(self):
        adapter = self.get_adapter()
        source = "./input/data.xlsx"
        path = get_absolute_path(source)
        config = LocalFileSettings()
        data = adapter.import_data(path, config)
        assert all(isinstance(d, adapter.data_format) for d in data.values())

    @pytest.fixture(scope="function")
    def data_layout_container(self):
        source = get_absolute_path("./input")
        container = CacheContainerFactory.new()
        host = DirectoryIO()
        roots = host.load(source, format="yaml")
        for root in roots:
            for entity in load_entities_from_tree(root):
                container.add(entity)
        return container


class TestDatasetSeriesMods(abc.ABC):
    __test__ = False

    def get_adapter(self):
        raise NotImplementedError

    def verify_dataset_subset(self, dataset: Dataset, num_elements: int):
        raise NotImplementedError

    def verify_dataset_relabel(self, dataset: Dataset, expected_labels: list[str]):
        raise NotImplementedError

    @pytest.fixture(scope="function")
    def raw_data(self):
        raise NotImplementedError

    @pytest.fixture(scope="function")
    def dataset_series(self, raw_data) -> DatasetSeries:
        # Schema for urine_lab
        urine_lab_schema = DatasetSchema(
            elements={
                "id_subject": DatasetSchemaElement(
                    label="id_subject",
                    observable_property_id="id_subject",
                    data_type=ObservablePropertyValueType.STRING,
                ),
                "matrix": DatasetSchemaElement(
                    label="matrix",
                    observable_property_id="matrix",
                    data_type=ObservablePropertyValueType.STRING,
                ),
                "crt": DatasetSchemaElement(
                    label="crt",
                    observable_property_id="crt",
                    data_type=ObservablePropertyValueType.FLOAT,
                ),
                "crt_lod": DatasetSchemaElement(
                    label="crt_lod",
                    observable_property_id="crt_lod",
                    data_type=ObservablePropertyValueType.FLOAT,
                ),
                "crt_loq": DatasetSchemaElement(
                    label="crt_loq",
                    observable_property_id="crt_loq",
                    data_type=ObservablePropertyValueType.FLOAT,
                ),
                "sg": DatasetSchemaElement(
                    label="sg",
                    observable_property_id="sg",
                    data_type=ObservablePropertyValueType.FLOAT,
                ),
            },
            primary_keys={"id_subject"},
            foreign_keys={},
        )

        # Schema for analyticalinfo
        analyticalinfo_schema = DatasetSchema(
            elements={
                "id_subject": DatasetSchemaElement(
                    label="id_subject",
                    observable_property_id="id_subject",
                    data_type=ObservablePropertyValueType.STRING,
                ),
                "biomarkercode": DatasetSchemaElement(
                    label="biomarkercode",
                    observable_property_id="biomarkercode",
                    data_type=ObservablePropertyValueType.STRING,
                ),
                "matrix": DatasetSchemaElement(
                    label="matrix",
                    observable_property_id="matrix",
                    data_type=ObservablePropertyValueType.STRING,
                ),
                "labinstitution": DatasetSchemaElement(
                    label="labinstitution",
                    observable_property_id="labinstitution",
                    data_type=ObservablePropertyValueType.STRING,
                ),
            },
            primary_keys={"id_subject", "biomarkercode"},
            foreign_keys={
                "fk_subject": ForeignKey(
                    element_label="id_subject",
                    reference=ElementReference(
                        dataset_label="urine_lab",
                        element_label="id_subject",
                    ),
                )
            },
        )

        # --- DATASET INSTANCES ------------------------------------------------------

        urine_lab_dataset = Dataset(label="urine_lab", schema=urine_lab_schema, data=raw_data["urine_lab"])

        analyticalinfo_dataset = Dataset(
            label="analyticalinfo", schema=analyticalinfo_schema, data=raw_data["analyticalinfo"]
        )

        # --- DATASET SERIES ---------------------------------------------------------

        series = DatasetSeries(
            label="urine_study_series",
            parts={
                "urine_lab": urine_lab_dataset,
                "analyticalinfo": analyticalinfo_dataset,
            },
        )
        # Make the reverse link (Dataset.part_of)
        urine_lab_dataset.part_of = series
        analyticalinfo_dataset.part_of = series

        return series

    def test_subset_dataset(self, dataset_series):
        element_groups = {
            "urine_lab_part_one": ["id_subject", "matrix", "crt"],
            "urine_lab_part_two": ["id_subject", "crt_lod", "crt_loq", "sg"],
        }
        expected_labels = [*element_groups.keys(), "analyticalinfo"]
        num_primary_keys_urine_lab = len(dataset_series["urine_lab"].schema.primary_keys)
        _ = dataset_series.subset_dataset("urine_lab", element_groups, dataops_adapter=self.get_adapter())
        assert len(dataset_series.parts) == 3
        labels = set(dataset_series.parts)
        assert labels == set(expected_labels)
        # check schema
        for dataset_label in ["urine_lab_part_one", "urine_lab_part_two", "analyticalinfo"]:
            dataset = dataset_series.get(dataset_label)
            schema = dataset.schema
            assert schema is not None
            if dataset_label in element_groups:
                assert len(schema) == len(element_groups[dataset_label])
                assert len(schema.primary_keys) == num_primary_keys_urine_lab

            if dataset_label == "analyticalinfo":
                num_elements = 4
            else:
                num_elements = len(element_groups[dataset_label])
            self.verify_dataset_subset(dataset, num_elements=num_elements)

    def test_relabel_dataset(self, dataset_series):
        element_mapping = {
            "id_subject": "id_subject_new",
            "matrix": "matrix_new",
            "crt": "crt_new",
            "crt_loq": "crt_loq_new",
            "crt_lod": "crt_lod_new",
            "sg": "sg_new",
        }
        _ = dataset_series.relabel_dataset(
            "urine_lab", element_mapping=element_mapping, dataops_adapter=self.get_adapter()
        )
        dataset = dataset_series.get("urine_lab")
        for _, new_label in element_mapping.items():
            schema_element = dataset.get_schema_element_by_label(new_label)
            assert schema_element is not None
            assert schema_element.label == new_label

        self.verify_dataset_relabel(dataset, expected_labels=list(element_mapping.values()))


class TestEnrichment(abc.ABC):
    """Abstract base class for enrichment adapters."""

    __test__ = False

    @abc.abstractmethod
    def get_adapter(self) -> DataOpsProtocol:
        """Return the adapter implementation to test."""
        raise NotImplementedError

    def container(self, path: str) -> CacheContainerView:
        source = get_absolute_path(path)
        container = CacheContainerFactory.new()
        host = DirectoryIO()
        roots = host.load(source, format="yaml", maxdepth=3)
        for root in roots:
            for entity in load_entities_from_tree(root):
                container.add(entity)

        return CacheContainerView(container)

    def raw_data(self):
        raise NotImplementedError

    def raw_dataset_series(self) -> DatasetSeries:
        return DatasetSeries(
            label="raw_dataset_series",
            parts={
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED": Dataset(
                    label="peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED",
                    schema=DatasetSchema(
                        elements={
                            "peh:id_subject": DatasetSchemaElement(
                                label="peh:id_subject",
                                observable_property_id="peh:id_subject",
                                data_type=ObservablePropertyValueType.STRING,
                            ),
                            "peh:current_year": DatasetSchemaElement(
                                label="peh:current_year",
                                observable_property_id="peh:current_year",
                                data_type=ObservablePropertyValueType.INTEGER,
                            ),
                            "peh:current_month": DatasetSchemaElement(
                                label="peh:current_month",
                                observable_property_id="peh:current_month",
                                data_type=ObservablePropertyValueType.INTEGER,
                            ),
                            "peh:current_day": DatasetSchemaElement(
                                label="peh:current_day",
                                observable_property_id="peh:current_day",
                                data_type=ObservablePropertyValueType.INTEGER,
                            ),
                            "peh:N1Birthdate": DatasetSchemaElement(
                                label="peh:N1Birthdate",
                                observable_property_id="peh:N1Birthdate",
                                data_type=ObservablePropertyValueType.DATETIME,
                            ),
                            "peh:N1Birthweight": DatasetSchemaElement(
                                label="peh:N1Birthweight",
                                observable_property_id="peh:N1Birthweight",
                                data_type=ObservablePropertyValueType.INTEGER,
                            ),
                        }
                    ),
                ),
                "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED": Dataset(
                    label="peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED",
                    schema=DatasetSchema(
                        elements={
                            "peh:id_subject": DatasetSchemaElement(
                                label="peh:id_subject",
                                observable_property_id="peh:id_subject",
                                data_type=ObservablePropertyValueType.STRING,
                            ),
                            "peh:agemonths": DatasetSchemaElement(
                                label="peh:agemonths",
                                observable_property_id="peh:agemonths",
                                data_type=ObservablePropertyValueType.INTEGER,
                            ),
                            "peh:Todaysdate": DatasetSchemaElement(
                                label="peh:Todaysdate",
                                observable_property_id="peh:Todaysdate",
                                data_type=ObservablePropertyValueType.DATETIME,
                            ),
                        },
                        primary_keys=set(["peh:id_subject"]),
                        foreign_keys={
                            "peh:id_subject": ForeignKey(
                                element_label="peh:id_subject",
                                reference=ElementReference(
                                    dataset_label="peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED",
                                    element_label="peh:id_subject",
                                ),
                            )
                        },
                    ),
                ),
            },
        )

    @pytest.fixture(scope="function")
    def simple_graph(self):
        dataset_series = self.raw_dataset_series()
        join_spec_mapping = dataset_series.resolve_all_joins()
        src_path = "./input/ProcessingExamples/Enrichment_03_MULTI_STEP"
        cache_view = self.container(src_path)
        observations = list(cache_view.get_all("Observation"))
        return Graph.from_observations(observations, cache_view, join_spec_mapping=join_spec_mapping)

    def test_dependency_graph_compilation(self, simple_graph):
        dataset_series = self.raw_dataset_series()
        adapter = self.get_adapter()
        datasets = self.raw_data()
        execution_plan = simple_graph.compile(adapter=adapter)
        assert len(execution_plan) == 2
        simple_graph.compute(datasets=datasets, adapter=adapter)
        assert dataset_series.matches(datasets, adapter)


@pytest.mark.dataframe
class TestDataFrameDataOps(TestValidation, TestDataImport, TestDatasetSeriesMods):
    __test__ = True

    def get_adapter(self) -> DataOpsProtocol:
        try:
            from pypeh.adapters.outbound.validation.pandera_adapter import validation_adapter as dfops

            return dfops.DataFrameValidationAdapter()  # type: ignore
        except ImportError:
            pytest.skip("Necessary modules not installed")

    @pytest.fixture(scope="function")
    def raw_data(self):
        import polars as pl

        layout = {
            "urine_lab": pl.DataFrame(
                schema={
                    "id_subject": pl.String,
                    "matrix": pl.String,
                    "crt": pl.Float64,
                    "crt_lod": pl.Float64,
                    "crt_loq": pl.Float64,
                    "sg": pl.Float64,
                }
            ),
            "analyticalinfo": pl.DataFrame(
                schema={
                    "id_subject": pl.String,
                    "biomarkercode": pl.String,
                    "matrix": pl.String,
                    "labinstitution": pl.String,
                }
            ),
        }

        layout["urine_lab"] = pl.DataFrame(
            [
                {"id_subject": "001", "matrix": "urine", "crt": 1.2, "crt_lod": 0.1, "crt_loq": 0.2, "sg": 1.015},
                {"id_subject": "002", "matrix": "urine", "crt": 1.5, "crt_lod": 0.1, "crt_loq": 0.2, "sg": 1.020},
            ]
        )

        layout["analyticalinfo"] = pl.DataFrame(
            [
                {"id_subject": "001", "biomarkercode": "B001", "matrix": "urine", "labinstitution": "LabCorp"},
                {
                    "id_subject": "002",
                    "biomarkercode": "B002",
                    "matrix": "urine",
                    "labinstitution": "Quest Diagnostics",
                },
            ]
        )

        return layout

    def verify_dataset_subset(self, dataset: Dataset, num_elements: int):
        data = dataset.data
        assert data is not None
        assert data.shape[-1] == num_elements

    def verify_dataset_relabel(self, dataset: Dataset, expected_labels: list[str]):
        data = dataset.data
        assert data is not None
        labels = data.columns
        assert set(labels) == set(expected_labels)


@pytest.mark.dataframe
class TestDataFrameEnrichment(TestEnrichment):
    __test__ = True

    def get_adapter(self) -> DataOpsProtocol:
        try:
            from pypeh.adapters.outbound.enrichment import dataframe_adapter as dfops

            return dfops.DataFrameEnrichmentAdapter()  # type: ignore
        except ImportError:
            pytest.skip("Necessary modules not installed")

    def raw_data(self) -> dict:
        import polars as pl

        df_ingested = pl.DataFrame(
            {
                "peh:id_subject": [1, 2, 3, 4, 5],
                "peh:current_year": [2025, 2025, 2025, 2025, 2025],
                "peh:current_month": [12, 12, 12, 12, 12],
                "peh:current_day": [11, 11, 11, 11, 11],
                "peh:N1Birthdate": [
                    date(1990, 5, 21),
                    date(1985, 7, 14),
                    date(2000, 1, 3),
                    date(1995, 9, 30),
                    date(1988, 3, 12),
                ],
                "peh:N2Birthweight": [3.2, 2.8, 3.5, 4.0, 3.0],
            }
        )

        df_enriched = pl.DataFrame(
            {
                "peh:id_subject": [1, 2, 3, 4, 5],
            }
        )

        return {
            "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECTUNIQUE_INGESTED": df_ingested,
            "peh:ENRICHMENT_TEST_OBSERVATION_SUBJECT_ENRICHED": df_enriched,
        }


@pytest.mark.other
class TestUnknownDataOps(TestValidation):
    def get_adapter(self) -> DataOpsProtocol:
        raise NotImplementedError
