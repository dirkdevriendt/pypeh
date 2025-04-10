from pypeh.core.handlers.baseclasses import ManifestHandler
from pypeh.core.persistence.formats import JsonIO
from pypeh.core.models.digital_objects import PehFDO

from tests.utils.dirutils import get_input_path


class TestPersistenceHandlers:
    def test_manifest_handler(self):
        root = "core/input/simple.json"
        abs_root = get_input_path(root)
        handler = ManifestHandler.create(abs_root, "load")
        assert isinstance(handler.adapter, JsonIO)
        with open(abs_root, "r") as file:
            fdo = handler.adapter.load(file, target_class=PehFDO)
        assert isinstance(fdo, PehFDO)
        # context = get_empty_context
        # fdo = handler.handle(context)
        # assert isinstance(fdo, PehFDO)
        # for adapter in [local.JsonFileSystem, remote.WebServiceAdapter]:
        #    handler = ManifestHandler(adapter)
        #    handler.handle()
