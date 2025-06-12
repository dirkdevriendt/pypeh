"""
This part of the code should contain the reusable part
of the PyHBM library, aka, the part that would otherwise
be copied across projects. Ideally, it also contains a
specification on what the custom part should look like.
"""

from __future__ import annotations

import logging

from peh_validation_library import Validator

# TODO: Remove this import once import from __init__.py is fixed
from peh_validation_library.error_report.error_collector import ErrorCollector
from typing import TYPE_CHECKING

from pypeh.core.interfaces.outbound.dataops import (
    DataOpsInterface,
)
from pypeh.adapters.outbound.validation.pandera_adapter.parsers import parse_config, parse_error_report
from pypeh.core.models.validation_errors import ValidationErrorReport
from pypeh.core.models.validation_dto import ValidationDTO, ValidationConfig

if TYPE_CHECKING:
    from typing import Mapping, Sequence

logger = logging.getLogger(__name__)


class DataOpsAdapter(DataOpsInterface):
    """
    DataOpsInterface has process method that can be called like this:
    `self.process(dto, "validate")`
    """

    def parse_configuration(self, config: ValidationConfig) -> Mapping:
        return parse_config(config)

    def get_error_collector(self):
        return ErrorCollector()

    def cleanup(self):
        self.get_error_collector().clear_errors()

    def validate(self, data: dict[str, Sequence], config: Mapping) -> ValidationErrorReport:
        config = self.parse_configuration(config)

        validator = Validator.config_from_mapping(config=config, logger=logger)

        validator.validate(data)

        return parse_error_report(self.get_error_collector().get_errors())

    def process(self, dto: ValidationDTO, action: str) -> ValidationErrorReport:
        return getattr(self, action)(dto.data, dto.config)

    def summarize(self, data: Mapping, config: Mapping):
        pass
