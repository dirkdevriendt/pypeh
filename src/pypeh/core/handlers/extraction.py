import logging

from pypeh.core.abc import Handler, Command, Response

logger = logging.getLogger(__name__)


class SummaryStatHandler(Handler):
    def __init__(self):
        super().__init__()

    def handle(self, command: Command) -> Response:
        raise NotImplementedError
