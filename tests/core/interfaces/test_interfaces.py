from pypeh.core.interfaces import persistence, dataops
from pypeh.core.abc import Interface, HandlerChain
from pypeh.core.handlers.baseclasses import DataOpsHandler
from pypeh.core.models.digital_objects import PehFDO
from pypeh.core.persistence.local import JsonFileSystem


# temp util
class DataOpsTestAdapter(dataops.DataOpsInterface):
    def process(self):
        return None


class TestInterfaceABC:
    def test_basic(self):
        assert issubclass(persistence.RepositoryInterface, Interface)

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
    def test_json_file_system(self, get_input_path):
        root = get_input_path("core/input/simple.json")
        adapter = JsonFileSystem(from_repo=PehFDO.model_validate)
        fdo = adapter.load(root)
        assert isinstance(fdo, PehFDO)
