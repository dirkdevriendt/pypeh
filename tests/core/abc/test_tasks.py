import pytest

from pypeh.core.commands.ingestion import ManifestIngestionCommand
from pypeh.core.tasks.ingestion import ManifestIngestionTask
from pypeh.core.abc import HandlerChain

from tests.test_utils.dirutils import get_input_path


class TestIngestionTask:
    @pytest.mark.core
    def test_manifest_task(self):
        root = get_input_path("core/input/simple.json")
        command = ManifestIngestionCommand.create(root=root)
        assert command.metadata.task_name == "ManifestIngestionTask"
        task = command.get_task()
        assert isinstance(task, ManifestIngestionTask)
        handlerchain = task.resolve()
        assert isinstance(handlerchain, HandlerChain)
