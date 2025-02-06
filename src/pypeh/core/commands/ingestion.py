import logging

from pypeh.core.abc import (
    Command,
    CommandBus,
    HandlerChain,
    HandlerChainFactory,
    DataTransferObject,
)

logger = logging.getLogger(__name__)


class IngestionCommand(Command):
    def __init__(self):
        super().__init__()


class IngestionCommandBus(CommandBus):
    def __init__(
        self,
        handlerchainfactory: HandlerChainFactory,
        # TODO: add in_memory_cache datastructure
    ):
        self.handlerchainfactory = handlerchainfactory

    def resolve(self, command: Command) -> HandlerChain:
        return self.handlerchainfactory.build_chain(command)

    def execute(self, command: Command) -> DataTransferObject:
        handler_chain = self.resolve(command)
        result = None
        for handler in handler_chain:
            result = handler.handle(command)
        return result
