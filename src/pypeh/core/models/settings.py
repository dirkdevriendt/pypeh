from __future__ import annotations

import logging

from abc import abstractmethod
from pydantic_settings import BaseSettings, SettingsConfigDict, PydanticBaseSettingsSource
from pydantic import BaseModel, Field, ValidationError, field_validator
from typing import Optional

from pypeh.core.utils.namespaces import ImportMap

logger = logging.getLogger(__name__)


class FileSystemSettings(BaseSettings):
    pass


class LocalFileSettings(FileSystemSettings):
    pass


class S3Settings(FileSystemSettings):
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    aws_region: Optional[str] = "us-east-1"
    bucket_name: str

    model_config = SettingsConfigDict(env_file=".env")

    def to_s3fs(self):
        return {
            "key": self.aws_access_key_id,
            "secret": self.aws_secret_access_key,
            "token": self.aws_session_token,
            "client_kwargs": {"region_name": self.aws_region},
        }


class SettingsConfig(BaseModel):
    @abstractmethod
    def make_settings(self) -> FileSystemSettings:
        raise NotImplementedError


class LocalFileConfig(SettingsConfig):
    def make_settings(self) -> LocalFileSettings:
        return LocalFileSettings()


class S3Config(SettingsConfig):
    env_prefix: str = "S3_"
    config_dict: Optional[dict[str, str]] = Field(default_factory=dict)

    def make_settings(self) -> S3Settings:
        """
        Translate config dict into S3Settings with the option to
        specify the prefix to be used to find the correct details in the .env
        Needed in case we need to connect to different S3 buckets.
        """
        config_data = self.config_dict or {}

        class CustomisedSettings(S3Settings):
            @classmethod
            def settings_customise_sources(
                cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings
            ) -> tuple[PydanticBaseSettingsSource, ...]:
                return (
                    init_settings,
                    dotenv_settings,
                    env_settings,
                    file_secret_settings,
                )

        CustomisedSettings.model_config = SettingsConfigDict(env_prefix=self.env_prefix, env_file=".env")

        try:
            return CustomisedSettings(**config_data)
        except ValidationError as e:
            raise ValueError(f"Failed to load config with prefix '{self.env_prefix}': {e}") from e


class ImportConfig(BaseModel):
    connection_map: dict[str, SettingsConfig]
    import_map: dict[str, str]

    @field_validator("import_map", mode="after")
    @classmethod
    def dict_to_trie(cls, namespace_map: dict[str, str]) -> ImportMap:
        new_import_map = ImportMap()
        for key, value in namespace_map.items():
            new_import_map[key] = value
        return new_import_map
