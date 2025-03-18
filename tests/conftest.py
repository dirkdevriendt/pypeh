import pathlib
import pytest

from typing import Callable, Optional


from pypeh.core.abc import Command, Task, HandlerChain, Context
from pypeh.core.cache import dataview
from pypeh.core.models.dto import CommandParams, ContextMetaData


@pytest.fixture(scope="function")
def get_empty_context() -> Optional[Context]:
    view = dataview.get_dataview()
    context = Context(data=view, metadata=ContextMetaData(command=None, executed_commands=[], namespaces={}))
    return context


class MockTask(Task):
    def resolve(self) -> HandlerChain:
        return HandlerChain()


class MockMetadataParams(CommandParams):
    root: str


class MockCommand(Command):
    params_model = MockMetadataParams

    @staticmethod
    def get_task_class() -> type:
        return MockTask


@pytest.fixture(scope="class")
def get_mock_command():
    return MockCommand
