import pathlib

from abc import ABC

from pypeh.core.models.settings import S3Config


class BaseConnection(ABC):
    pass


class S3Connection(BaseConnection):
    def __init__(self, config: S3Config):
        self.settings = config.make_settings()

    def resolve(self, path: str) -> str:
        return f"s3://{self.settings.bucket_name}/{path}"


class DirectoryConnection(BaseConnection):
    def __init__(self, root: str | pathlib.Path):
        self.root = pathlib.Path(root)

    def resolve(self, path: str | pathlib.Path) -> pathlib.Path:
        return self.root / pathlib.Path(path)


class FileConnection(BaseConnection):
    def __init__(self, path: str | pathlib.Path):
        self.path = path

    def resolve(self) -> pathlib.Path:
        return pathlib.Path(self.path)


class ROCrateConnection(BaseConnection):
    def __init__(self, root: str | pathlib.Path):
        self.root = pathlib.Path(root)

    def resolve(self, path: str | pathlib.Path) -> pathlib.Path:
        return self.root / pathlib.Path(path)
