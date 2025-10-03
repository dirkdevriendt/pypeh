import pytest
import peh_model.peh as peh
import logging

from pypeh.core.models.constants import ValidationErrorLevel
from tests.test_utils.dirutils import get_absolute_path

from pypeh import Session
from pypeh.core.models.settings import LocalFileConfig
from pypeh.core.models.validation_errors import ValidationErrorReport

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
        layout = session.cache.get("peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA", "DataLayout")
        assert layout.id == "peh:CODEBOOK_v2.4_LAYOUT_SAMPLE_METADATA"
        assert isinstance(layout, peh.DataLayout)
        data_dict = session.load_tabular_data(
            source="validation_test_06_data.xlsx",
            connection_label="local_file_validation_files",
            data_layout=layout,
        )
        assert isinstance(data_dict, dict)
        assert len(data_dict) > 0

        # Configure the combination of data content and layout, specific to the observation dataset
        dataset_mapping = {
            "SAMPLE": {
                "layout_section_id": "peh:SAMPLE_METADATA_SECTION_SAMPLE",
                "observation_id": "peh:VALIDATION_TEST_SAMPLE_SAMPLE",
                "foreign_keys": {"id_subject": ["peh:VALIDATION_TEST_SAMPLE_SUBJECTUNIQUE", "id_subject"]},
            },
            "SUBJECTUNIQUE": {
                "layout_section_id": "peh:SAMPLE_METADATA_SECTION_SUBJECTUNIQUE",
                "observation_id": "peh:VALIDATION_TEST_SAMPLE_SUBJECTUNIQUE",
                "foreign_keys": {"id_participant": ["peh:VALIDATION_TEST_SAMPLE_SUBJECTUNIQUE", "id_subject"]},
            },
            "SAMPLETIMEPOINT_BWB": {
                "layout_section_id": "peh:SAMPLE_METADATA_SECTION_SAMPLETIMEPOINT_BWB",
                "observation_id": "peh:VALIDATION_TEST_SAMPLE_SAMPLETIMEPOINT_BWB",
            },
        }
        section_label_to_section_id = {
            "SAMPLE": "peh:SAMPLE_METADATA_SECTION_SAMPLE",
            "SUBJECTUNIQUE": "peh:SAMPLE_METADATA_SECTION_SUBJECTUNIQUE",
            "SAMPLETIMEPOINT_BWB": "peh:SAMPLE_METADATA_SECTION_SAMPLETIMEPOINT_BWB",
        }
        observable_property_id_to_layout_section_label = {
            "matrix": "SAMPLE",
        }

        observation_list = [session.cache.get(m["observation_id"], "Observation") for m in dataset_mapping.values()]
        consistency_validations_dict = session.get_dataset_validations_dict(
            observation_list=observation_list, layout=layout, dataset_mapping=dataset_mapping, data_dict=data_dict
        )
        id_validations_dict = session.get_dataset_identifier_consistency_validations_dict(
            observation_list=observation_list, layout=layout, dataset_mapping=dataset_mapping, data_dict=data_dict
        )

        def validate_set(set_key):
            sheet_label = set_key
            section_id = section_label_to_section_id[sheet_label]
            layout_section = session.get_resource(resource_identifier=section_id, resource_type="DataLayoutSection")
            assert layout_section is not None
            assert isinstance(layout_section, peh.DataLayoutSection)
            dataset_validations = []
            if consistency_validations := consistency_validations_dict.get(set_key, None):
                dataset_validations.extend(consistency_validations)
            if id_validations := id_validations_dict.get(set_key, None):
                dataset_validations.extend(id_validations)
            data_df = data_dict.get(sheet_label, None)
            assert data_df is not None

            validation_report = session.validate_tabular_data(
                data=data_df,
                data_layout_section=layout_section,
                dataset_validations=dataset_validations,
                dependent_data=data_dict,
                observable_property_id_to_layout_section_label=observable_property_id_to_layout_section_label,
            )

            assert isinstance(validation_report, ValidationErrorReport)
            assert validation_report.error_counts[ValidationErrorLevel.WARNING] == 0
            return validation_report

        report = validate_set("SAMPLE")
        assert report.error_counts[ValidationErrorLevel.ERROR] == 3
        assert len(report.unexpected_errors) == 0

        report = validate_set("SUBJECTUNIQUE")
        assert report.error_counts[ValidationErrorLevel.ERROR] == 0
        assert len(report.unexpected_errors) == 0

        report = validate_set("SAMPLETIMEPOINT_BWB")
        assert report.error_counts[ValidationErrorLevel.ERROR] == 1
        assert len(report.unexpected_errors) == 0
