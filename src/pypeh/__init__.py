try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    # For Python < 3.8
    raise NotImplementedError(
        "pypeh requires Python 3.8+ because importlib.metadata is unavailable."
    )

from pypeh.core.session.session import Session
from pypeh.core.models.settings import LocalFileConfig, S3Config
from pypeh.core.utils.namespaces import NamespaceManager

__all__ = [
    "Session",
    "LocalFileConfig",
    "S3Config",
    "NamespaceManager",
]

try:
    __version__ = version("pypeh")
except PackageNotFoundError:
    __version__ = "0.0.0"
