from abc import ABC, abstractmethod
from typing import Any, Iterable, Iterator, TypeVar

T = TypeVar("T", bound="Handler")

class DataTransferObject(ABC):
    """
    DataTransferObject
    """
    def __init__(self, data: Any):
        self.data = data


class Command(DataTransferObject):
    """
    A command acts essentially as a data tranfer object passing parameters
    from the API call to the correct Handler.
    """
    provenance = None # information on how the command was called


class Handler(ABC):
    """
    Each Handler class should function as a collector class connecting all 
    the dependencies needed to execute a particular use case.
    The handler should be provided with those dependencies upon initialization.
    """
    @abstractmethod
    def handle(self, command: Command) -> DataTransferObject:
        raise NotImplementedError


class HandlerChain(ABC, Iterable[T]):
    """
    Iterable chaining together a series of Handlers.
    """
    def __iter__(self) -> Iterator[T]:
        raise NotImplementedError
    
    def __next__(self) -> T:
        raise NotImplementedError

class CommandBus(ABC):
    """
    A commandbus instance resolves the correct HandlerChain required to
    execute the Command.
    Each CommandBus class/instance should be paired with the correct 
    HandlerChainFactory class/instance.
    """
    handlerchain: HandlerChain = NotImplementedError

    @abstractmethod
    def resolve(self, command: Command) -> HandlerChain:
        raise NotImplementedError

    @abstractmethod
    def execute(self) -> DataTransferObject:
        raise NotImplementedError


class HandlerChainFactory(ABC):
    """
    The HandlerChainFactory takes care of validating all parameters passed by the command and 
    initializes the correct Handler subclass chaining them together into a HandlerChain and
    providing each Handler with the dependencies it needs based on the validated parameters.
    """
    @abstractmethod
    def get_chain(self, command: Command) -> HandlerChain:
        raise NotImplementedError

    @abstractmethod
    def get_handler(self, command: Command) -> Handler:
        raise NotImplementedError