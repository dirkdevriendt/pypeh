import logging

from pypeh.core.abc import (
    Command,
    DataTransferObject,
    Handler,
)
from pypeh.core.interfaces import dataops, persistence

logger = logging.getLogger(__name__)


class IngestionHandler(Handler):
    def __init__(self, adapter: dataops.DataOpsInterface):
        super().__init__()
        self.adapter = adapter

    def handle(self, command: Command) -> DataTransferObject:
        raise NotImplementedError

    def load_data(self, data: DataTransferObject):
        raise NotImplementedError


class ValidationHandler(IngestionHandler):
    def __init__(self, adapter: dataops.DataValidationInterface):
        super().__init__(adapter)

    def handle(self, command: Command) -> DataTransferObject:
        raise NotImplementedError


class EnrichmentHandler(IngestionHandler):
    def __init__(self, adapter: dataops.DataEnrichmentInterface):
        super().__init__(adapter)

    def handle(self, command: Command) -> DataTransferObject:
        raise NotImplementedError


class PersistenceHandler(IngestionHandler):
    def __init__(self, adapter: persistence.PersistenceInterface):
        super().__init__(adapter)

    def handle(self, command: Command) -> DataTransferObject:
        raise NotImplementedError
