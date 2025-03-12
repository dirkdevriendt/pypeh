from __future__ import annotations

import logging

from pypeh.core.models.dto import CommandParams
from pypeh.core.tasks.ingestion import ManifestIngestionTask, IngestionTask, DataIngestionTask
from pypeh.core.utils.resolve_identifiers import resource_path

from pathlib import Path
from pydantic import field_validator
from typing import TYPE_CHECKING, Union

from pypeh.core.abc import Command

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# TODO: add in definition of required information for all types of commands


class IngestionParams(CommandParams):
    root: Union[str, Path]

    @field_validator("root")
    def validate_root(cls, v):
        return resource_path(v)


class IngestionCommand(Command):
    params_model = IngestionParams

    @staticmethod
    def get_task_class():
        return IngestionTask


class ManifestIngestionParams(IngestionParams):
    root: str


class ManifestIngestionCommand(IngestionCommand):
    params_model = ManifestIngestionParams

    @staticmethod
    def get_task_class():
        return ManifestIngestionTask


class DataIngestionParams(CommandParams):
    load_data: bool = True
    lazy_loading: bool = True


class DataIngestionCommand(IngestionCommand):
    params_model = DataIngestionParams

    @staticmethod
    def get_task_class():
        return DataIngestionTask
