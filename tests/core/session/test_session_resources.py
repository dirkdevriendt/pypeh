import pytest
import re
import yaml

from peh_model.peh import Observation, ObservableProperty, DerivedObservation

from pypeh import Session
from pypeh.core.models.settings import LocalFileConfig

from pypeh.core.utils.namespaces import NamespaceManager
from tests.test_utils.dirutils import get_absolute_path


def get_session(
    root_folder: str = "./input/default_localfile_data",
) -> Session:
    session = Session(
        connection_config=[
            LocalFileConfig(
                label="local_file",
                config_dict={
                    "root_folder": get_absolute_path(root_folder),
                },
            ),
        ],
        default_connection=None,
    )
    return session


@pytest.mark.core
class TestSessionResource:
    def test_load_resource(self):
        session = get_session()
        resource_identifier = "peh:OBSERVATION_ADULTS_URINE_LAB"
        resource_type = "Observation"
        resource_path = "observations.yaml"
        connection_label = "local_file"
        ret = session.load_resource(
            resource_identifier, resource_type, resource_path, connection_label
        )
        assert isinstance(ret, Observation)


@pytest.mark.core
class TestSessionDump:
    def test_dump_entity_list(self, tmp_path):
        session = get_session()
        assert isinstance(session, Session)
        session.load_persisted_cache(
            source="observations.yaml", connection_label="local_file"
        )
        dest = tmp_path / "out.yaml"
        session.dump_cache(
            output_path=dest,
            connection_label="local_file",
        )
        data = dest.read_bytes()
        assert data, "Dumped file is empty"
        test_data = yaml.safe_load(data.decode("utf-8"))
        assert isinstance(test_data, dict)
        assert "observations" in test_data


@pytest.mark.core
class TestSessionMint:
    def test_mint_and_cache(self):
        session = get_session()
        assert isinstance(session, Session)
        namespace_manager = NamespaceManager()
        namespace_manager.bind("test", "www.example.com")
        session.bind_namespace_manager(namespace_manager=namespace_manager)
        ret = session.mint_and_cache(
            ObservableProperty, namespace_key="test", ui_label="test"
        )
        next_instance = next(session.cache.get_all("ObservableProperty"))
        assert isinstance(next_instance, ObservableProperty)
        assert next_instance.id == ret.id

    def test_mint_and_cache_resource(self):
        session = get_session()
        assert isinstance(session, Session)
        namespace_manager = NamespaceManager(
            default_base_uri="https://w3id.org/example/id/"
        )
        session.bind_namespace_manager(namespace_manager=namespace_manager)
        ret = session.mint_and_cache(ObservableProperty, ui_label="test")
        next_instance = next(session.cache.get_all("ObservableProperty"))
        assert isinstance(next_instance, ObservableProperty)
        assert next_instance.id == ret.id
        pattern = r"^https://w3id\.org/example/id/observable-property/[0-9A-HJKMNP-TV-Z]{26}$"
        assert re.match(
            pattern, ret.id
        ), f"IRI did not match expected pattern: {ret.id}"


@pytest.mark.core
class TestSessionUnpack:
    def test_unpack_derived_observation_group(self):
        session = get_session("./input/unpack_resource")
        assert isinstance(session, Session)
        session.load_persisted_cache(
            source="unpack_derived_observation_group.yaml",
            connection_label="local_file",
        )
        count = 0
        for target, source in session.unpack_derived_observation_group(
            observation_group_id="example:this_group"
        ):
            assert isinstance(target, DerivedObservation)
            assert isinstance(source, Observation)
            count += 1
        assert count == 3
