import logging
import platform

from pypeh import __version__

logger = logging.getLogger(__name__)


def get_environment():
    """
    Returns a dictionary describing the environment in which pypeh
    is currently running.

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
        "environment": get_environment(),
    }
    return document
