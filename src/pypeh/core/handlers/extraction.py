import logging

from pypeh.core.abc import Handler, Command, DataTransferObject

logger = logging.getLogger(__name__)


class SummaryStatHandler(Handler):
    def __init__(self):
        super().__init__()

    def handle(self, command: Command) -> DataTransferObject:
        raise NotImplementedError
