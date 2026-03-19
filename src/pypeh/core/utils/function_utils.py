import importlib

from typing import Callable


def _extract_callable(path: str) -> Callable:
    assert "." in path, "Could not split path into module and func_name"
    module_name, func_name = path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Module '{module_name}' could not be imported") from e
    try:
        return getattr(module, func_name)
    except AttributeError as e:
        raise AttributeError(f"Function '{func_name}' not found in module '{module_name}'") from e
