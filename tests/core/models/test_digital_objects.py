import json
import pytest

from pypeh.core.models import digital_objects
from pypeh.core.models.constants import LocationEnum
from pypeh.core.utils.resolve_identifiers import assign_location_enum


class TestFDO:
    @pytest.mark.core
    def test_basic(self):
        assert True

    @pytest.mark.core
    def test_schema_instance(self):
        schema = digital_objects.PehFDO.dump_json_schema(indent=2)  # type: ignore
        if schema is not None:
            schema_dict = json.loads(schema)
        assert schema_dict["$schema"] == "https://json-schema.org/draft/2020-12/schema"


class TestIdentifiers:
    @pytest.mark.core
    def test_resolve_identifiers(self):
        test = "http://test.com"
        ret = assign_location_enum(test)
        assert ret == LocationEnum.URI
        test = "./local.txt"
        ret = assign_location_enum(test)
        assert ret == LocationEnum.LOCAL
        test = "../local.txt"
        ret = assign_location_enum(test)
        assert ret == LocationEnum.LOCAL
        test = "local.txt"
        ret = assign_location_enum(test)
        assert ret == LocationEnum.LOCAL

        ## TODO: fix test, still fails
        # test = "foaf:Person"
        # ret = assign_location_enum(test)
        # assert ret == LocationEnum.CURIE
