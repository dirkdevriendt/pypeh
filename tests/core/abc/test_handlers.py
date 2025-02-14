from pypeh.core.handlers.baseclasses import ManifestHandler
from pypeh.core.persistence.local import JsonFileSystem
from pypeh.core.models.digital_objects import PehFDO
from pypeh.core.persistence import remote, local


class TestPersistenceHandlers:
    def test_manifest_handler(self, get_input_path, get_empty_context):
        root = "core/input/simple.json"
        abs_root = get_input_path(root)
        handler = ManifestHandler.create(abs_root, "load")
        assert isinstance(handler.adapter, JsonFileSystem)
        fdo = handler.adapter.load(abs_root)
        assert isinstance(fdo, PehFDO)
        # context = get_empty_context
        # fdo = handler.handle(context)
        # assert isinstance(fdo, PehFDO)
        # for adapter in [local.JsonFileSystem, remote.RemoteRepository]:
        #    handler = ManifestHandler(adapter)
        #    handler.handle()
