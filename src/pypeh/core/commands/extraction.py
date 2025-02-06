import logging

from pypeh.core.abc import Command, CommandBus

logger = logging.getLogger(__name__)


class ExtractCommand(Command):
    pass


class ExtractCommandBus(CommandBus):
    pass
