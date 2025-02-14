import pathlib
import pytest

from typing import Callable, Optional


from pypeh.core.abc import Command, Task, HandlerChain, Context
from pypeh.core.cache import dataview
from pypeh.core.models.dto import CommandParams, ContextMetaData


@pytest.fixture(scope="session")
def tests_root() -> pathlib.Path:
    """Return the root directory of the tests folder."""
    return pathlib.Path(__file__).parent


@pytest.fixture(scope="session")
def input_root(tests_root: pathlib.Path) -> pathlib.Path:
    """Return the root directory for all test input files."""
    return tests_root / "input"


@pytest.fixture(scope="session")
def get_input_path() -> Callable[[str], str]:
    """
    Return a function that resolves paths to input files relative to the tests directory.
    """
    tests_dir = pathlib.Path(__file__).parent

    def _get_path(path: str) -> str:
        return str(tests_dir / path)

    return _get_path


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
