import pytest

from pypeh.core.interfaces import persistence, dataops
from pypeh.core.abc import Interface, HandlerChain
from pypeh.core.handlers.baseclasses import DataOpsHandler
from pypeh.core.models.digital_objects import PehFDO
from pypeh.core.persistence.formats import JsonIO

from tests.utils.dirutils import get_input_path


# temp util
class DataOpsTestAdapter(dataops.DataOpsInterface):
    def process(self):
        return None


class TestInterfaceABC:
    @pytest.mark.core
    def test_basic(self):
        assert issubclass(persistence.RepositoryInterface, Interface)

    @pytest.mark.core
    def test_handler_chain(self):
        ll = HandlerChain()
        vh = DataOpsHandler(DataOpsTestAdapter())
        eh = DataOpsHandler(DataOpsTestAdapter())
        ll.head = vh
        vh.next = eh
        # iter
        counter = 0
        for _ in ll:
            counter += 1
        assert counter == 2


class TestPersistenceInterface:
    pass


class TestLocalPersistence:
    pass


class TestFileSystem:
    @pytest.mark.core
    def test_json_file_system(self):
        root = get_input_path("core/input/simple.json")
        adapter = JsonIO()
        with open(root, "r") as file:
            fdo = adapter.load(file, target_class=PehFDO)
        assert isinstance(fdo, PehFDO)
