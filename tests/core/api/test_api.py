import pytest
import pypeh


class TestCaptureApiParams:
    @pytest.mark.core
    def test_(self, get_mock_command):
        command_cls = get_mock_command
        command = command_cls.create(root=".")
        _ = command.get_task()
        assert command.metadata.params.root is not None


class TestVersion:
    @pytest.mark.core
    def test_pypeh_version(self):
        assert pypeh.__version__ == "0.0.1"

    @pytest.mark.core
    def test_peh_model_version(self):
        assert True

    @pytest.mark.core
    def test_peh_fdo_schema_version(self):
        assert True
