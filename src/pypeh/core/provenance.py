import logging
import platform

__version__ = "undefined"

try:
    from . import _version

    __version__ = _version.version
except ImportError:
    pass
logger = logging.getLogger(__name__)


class CURRENT_TREE_SEQUENCE:
    pass


def get_environment(extra_libs=None, include_tskit=True):
    """
    Returns a dictionary describing the environment in which tskit
    is currently running.

    This API is tentative and will change in the future when a more
    comprehensive provenance API is implemented.
    """
    env = {
        "os": {
            "system": platform.system(),
            "node": platform.node(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
        },
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
        },
    }
    
    return env


def get_provenance_dict(parameters=None):
    """
    Returns a dictionary encoding an execution of pypeh conforming to the
    pypeh provenance schema.
    """
    document = {
        "schema_version": "0.0.1",
        "software": {"name": "pypeh", "version": __version__},
        "parameters": parameters,
        "environment": get_environment()
    }
    return document