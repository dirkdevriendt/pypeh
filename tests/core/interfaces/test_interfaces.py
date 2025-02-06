from pypeh.core.interfaces import persistence, dataops
from pypeh.core.abc import Interface, HandlerChain
from pypeh.core.handlers.ingestion import ValidationHandler, EnrichmentHandler


# temp util
class DataOpsTestAdapter(dataops.DataOpsInterface):
    def process(self):
        return None


class TestInterfaceABC:
    def test_basic(self):
        assert issubclass(persistence.RepositoryInterface, Interface)

    def test_handler_chain(self):
        ll = HandlerChain()
        vh = ValidationHandler(DataOpsTestAdapter())
        eh = EnrichmentHandler(DataOpsTestAdapter())
        ll.head = vh
        vh.next = eh
        # iter
        counter = 0
        for _ in ll:
            counter += 1
        assert counter == 2
