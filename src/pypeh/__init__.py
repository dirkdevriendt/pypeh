try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    # For Python < 3.8
    raise NotImplementedError

try:
    __version__ = version("pypeh")
except PackageNotFoundError:
    __version__ = "0.0.0"
