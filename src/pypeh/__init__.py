try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    # For Python < 3.8
    raise NotImplementedError

from pypeh.core.session.session import Session
from pypeh.core.models.settings import LocalFileConfig, S3Config

__all__ = [
    "Session",
    "LocalFileConfig",
    "S3Config",
]

try:
    __version__ = version("pypeh")
except PackageNotFoundError:
    __version__ = "0.0.0"
