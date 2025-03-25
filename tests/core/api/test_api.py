import pypeh


class TestCaptureApiParams:
    def test_(self, get_mock_command):
        command_cls = get_mock_command
        command = command_cls.create(root=".")
        _ = command.get_task()
        assert command.metadata.params.root is not None


class TestVersion:
    def test_pypeh_version(self):
        assert pypeh.__version__ == "0.0.1"

    def test_peh_model_version(self):
        assert True

    def test_peh_fdo_schema_version(self):
        assert True
