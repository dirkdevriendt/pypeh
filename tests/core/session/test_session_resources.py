from peh_model.peh import Observation

from pypeh import Session
from pypeh.core.models.settings import LocalFileConfig

from tests.test_utils.dirutils import get_absolute_path


class TestSessionResource:
    def test_load_resource(self):
        session = Session(
            connection_config=[
                LocalFileConfig(
                    label="local_file",
                    config_dict={
                        "root_folder": get_absolute_path("./input/default_localfile_data"),
                    },
                ),
            ],
            default_connection=None,
        )
        resource_identifier = "peh:OBSERVATION_ADULTS_URINE_LAB"
        resource_type = "Observation"
        resource_path = "observations.yaml"
        connection_label = "local_file"
        ret = session.load_resource(resource_identifier, resource_type, resource_path, connection_label)
        assert isinstance(ret, Observation)
