import pytest

from pypeh.adapters.outbound.persistence.formats import IOAdapterFactory, IOAdapter, YamlIO

from pydantic import BaseModel
from tests.test_utils.dirutils import get_absolute_path


class MockAdapter(IOAdapter):
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class MockModel(BaseModel):
    pass


@pytest.mark.core
class TestIOAdapterFactory:
    @pytest.mark.parametrize(
        "format_name, expected_adapter",
        [
            ("json", "JsonIO"),
            ("yaml", "YamlIO"),
            ("csv", "CsvIO"),
            ("xlsx", "ExcelIO"),
            ("xls", "ExcelIO"),
        ],
    )
    def test_create_known_adapters(self, format_name, expected_adapter):
        adapter = IOAdapterFactory.create(format_name)
        assert adapter.__class__.__name__ == expected_adapter

    def test_create_unknown_adapter_raises_error(self):
        with pytest.raises(ValueError, match="No adapter registered for dataformat: unknown"):
            IOAdapterFactory.create("unknown")

    def test_register_adapter(self):
        IOAdapterFactory.register_adapter("mock", MockAdapter)
        adapter = IOAdapterFactory.create("mock", test_param=True)

        assert isinstance(adapter, MockAdapter)
        assert adapter.kwargs["test_param"]


@pytest.mark.core
class TestYamlIO:
    def test_basic(self):
        source = get_absolute_path("./input/config_basic/_Reference_YAML/observable_entities.yaml")
        yaml_io = YamlIO()
        yaml_io.load(source)

    def test_wrong_schema(self):
        source = get_absolute_path("./input/wrong_input/observable_entities.yaml")
        yaml_io = YamlIO()
        with pytest.raises(NotImplementedError):
            yaml_io.load(source, target_class=MockModel)

        source = get_absolute_path("./input/wrong_input/random.yaml")
        yaml_io = YamlIO()
        with pytest.raises(TypeError):
            yaml_io.load(source)


@pytest.mark.core
class TestJsonIO:
    def test_basic(self):
        pass

    def test_wrong_schema(self):
        pass


class TestCsvIO:
    pass


class TestXlsIO:
    pass
