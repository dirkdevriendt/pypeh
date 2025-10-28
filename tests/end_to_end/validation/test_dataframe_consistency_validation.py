import pytest
import peh_model.peh as peh
import logging
from collections import defaultdict

from tests.test_utils.dirutils import get_absolute_path

from pypeh import Session
from pypeh.core.models.settings import LocalFileConfig
from pypeh.core.models.internal_data_layout import get_observations_from_data_import_config
from pypeh.core.models.validation_dto import ValidationConfig
from pypeh.core.models.validation_errors import ValidationErrorReport, ValidationErrorLevel

logger = logging.getLogger(__name__)


@pytest.mark.end_to_end_consistency
class TestConsistency:
    def test_entity_consistency(self, monkeypatch):
        session = Session(
            connection_config=[
                LocalFileConfig(
                    label="local_file_validation_config",
                    config_dict={
                        "root_folder": get_absolute_path("./input/test_06/config"),
                    },
                ),
                LocalFileConfig(
                    label="local_file_validation_files",
                    config_dict={
                        "root_folder": get_absolute_path("./input/test_06"),
                    },
                ),
            ],
            default_connection="local_file_validation_config",
        )
        session.load_persisted_cache()
        data_import_config = session.cache.get(
            "peh:IMPORT_CONFIG_CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA", "DataImportConfig"
        )
        assert data_import_config.id == "peh:IMPORT_CONFIG_CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA"
        assert isinstance(data_import_config, peh.DataImportConfig)
        data_dict = session.load_tabular_data_collection(
            source="validation_test_06_data.xlsx",
            connection_label="local_file_validation_files",
            data_import_config=data_import_config,
        )
        assert isinstance(data_dict, dict)
        assert len(data_dict) > 0

        observations = [
            observation
            for observation in get_observations_from_data_import_config(data_import_config, session.cache)
            if observation.id in data_dict.keys()
        ]

        id_validations_dict = ValidationConfig.get_dataset_identifier_consistency_validations_dict(
            observation_list=observations,
            data_import_config=data_import_config,
            data_dict=data_dict,
            cache_view=session.cache,
        )
        matrix_validations_dict = ValidationConfig.get_sample_matrix_validations_dict_from_section_labels(
            observation_list=observations,
            data_import_config=data_import_config,
            data_dict=data_dict,
            cache_view=session.cache,
        )

        data_collection_validations = defaultdict(list)
        for k, v in id_validations_dict.items():
            data_collection_validations[k].extend(v)
        for k, v in matrix_validations_dict.items():
            data_collection_validations[k].extend(v)

        validation_report_collection = session.validate_tabular_data_collection(
            data_collection=data_dict,
            observations=observations,
            data_collection_validations=data_collection_validations,
        )
        assert isinstance(validation_report_collection, dict)
        assert len(validation_report_collection) == 3
        assert isinstance(validation_report_collection["peh:VALIDATION_TEST_SAMPLE_SAMPLE"], ValidationErrorReport)

        sample_errors = validation_report_collection["peh:VALIDATION_TEST_SAMPLE_SAMPLE"]
        assert sample_errors.total_errors == 3
        assert sample_errors.error_counts[ValidationErrorLevel.WARNING] == 0
        assert sample_errors.error_counts[ValidationErrorLevel.ERROR] == 3
        assert len(sample_errors.unexpected_errors) == 0

        subject_errors = validation_report_collection["peh:VALIDATION_TEST_SAMPLE_SUBJECTUNIQUE"]
        assert subject_errors.total_errors == 0
        assert subject_errors.error_counts[ValidationErrorLevel.WARNING] == 0
        assert subject_errors.error_counts[ValidationErrorLevel.ERROR] == 0
        assert len(subject_errors.unexpected_errors) == 0

        labresult_errors = validation_report_collection["peh:VALIDATION_TEST_SAMPLE_SAMPLETIMEPOINT_BWB"]
        assert labresult_errors.total_errors == 1
        assert labresult_errors.error_counts[ValidationErrorLevel.WARNING] == 0
        assert labresult_errors.error_counts[ValidationErrorLevel.ERROR] == 1
        assert len(labresult_errors.unexpected_errors) == 0
