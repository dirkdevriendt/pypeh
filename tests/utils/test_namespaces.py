import pytest

from pypeh.core.utils.namespaces import ImportMap


@pytest.mark.core
class TestImportMap:
    def test_importmap_basic(self):
        import_map = ImportMap()
        import_map["https://www.example.org/"] = "s3://this.file"
        import_map["https://www.example.org/exception"] = "s3://another.file"
        # anything within the example.org namespace should be directed to this.file
        assert import_map["https://www.example.org/regular"] == "s3://this.file"
        # except when a concrete exception has been added to the ImportMap
        assert import_map["https://www.example.org/exception"] == "s3://another.file"
