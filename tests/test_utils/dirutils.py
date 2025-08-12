import pathlib
import inspect


def get_tests_root() -> pathlib.Path:
    """Finds and returns the root directory of the 'tests' folder."""
    current_path = pathlib.Path(__file__).resolve()
    while current_path != current_path.anchor:  # .anchor handles drive letters & UNC roots
        if (current_path / "tests").is_dir():
            return current_path / "tests"
        current_path = current_path.parent
    raise FileNotFoundError("Could not find the 'tests' folder in the project hierarchy.")


def input_root(tests_root: pathlib.Path) -> pathlib.Path:
    """Return the root directory for all test input files."""
    return tests_root / "input"


def get_input_path(path: str) -> str:
    """Resolves paths to input files relative to the tests directory."""
    tests_dir = get_tests_root()
    return str(tests_dir / path)


def get_absolute_path(path: str) -> str:
    """Returns the absolute path of the input file given its relative path."""
    caller_frame = inspect.stack()[1]
    caller_path = pathlib.Path(caller_frame.filename).resolve().parent
    return str((caller_path / path).resolve())
