"""
This part of the code should contain the reusable part
of the PyHBM library, aka, the part that would otherwise
be copied across projects. Ideally, it also contains a
specification on what the custom part should look like.
"""

import logging
from typing import Mapping

from peh_validation_library import Validator

from typing import TYPE_CHECKING

from pypeh.core.interfaces.dataops import (
    DataOpsInterface,
)
from pypeh.dataframe_adapter.validation.parsers import parse_config, parse_error_report
from pypeh.core.models.validation_errors import ValidationReport

if TYPE_CHECKING:
    from typing import Mapping

logger = logging.getLogger(__name__)


class DataOpsAdapter(DataOpsInterface):
    """
    DataOpsInterface has process method that can be called like this:
    `self.process(dto, "validate")`
    """

    def validate(self, data: Mapping, config: Mapping) -> ValidationReport:
        config = parse_config(config)

        validator = Validator.build_validator(config, data, logger)
        return parse_error_report(validator.validate())

    def summarize(self, data: Mapping, config: Mapping):
        pass
