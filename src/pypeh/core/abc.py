"""
This module defines abstract base classes (ABCs) for the pypeh project.


Usage:
    Use these abstract base classes as templates for creating concrete implementations
    in other modules. Each abstract base class should define the core methods and properties
    that must be implemented by any subclass.

"""

from abc import ABC, abstractmethod
from typing import Dict, Iterable, Iterator, TypeVar

T = TypeVar("T", bound="Handler")
sentinel = object()


class DataTransferObject(ABC):
    """
    DataTransferObject
    """

    def __init__(self):
        self.data: Dict = None
        self.schema_version: str = None

    @abstractmethod
    def resolve_schema_vesion(self):
        raise NotImplementedError


class Command(DataTransferObject):
    """
    A command acts essentially as a data tranfer object passing parameters
    from the API call to the correct Handler.
    """

    def __init__(self):
        super().__init__()
        self.config: Dict = None
        self.provenance: Dict = None


class Response(DataTransferObject):
    """
    A Response acts as a data tranfer object structuring the result of the API call.
    """

    def __init__(self):
        super().__init__()
        self.return_status: Dict = None


class Handler(ABC):
    """
    Each Handler class should function as a collector class connecting all
    the dependencies needed to execute a particular use case.
    The handler should be provided with those dependencies upon initialization.
    """

    def __init__(self):
        self.next: "Handler[T]" = sentinel

    @abstractmethod
    def handle(self, command: Command) -> DataTransferObject:
        raise NotImplementedError


class HandlerChain(Iterable[T]):
    """
    Iterable chaining together a series of Handlers.
    """

    def __init__(self, head: Handler = sentinel):
        self.head = head

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        ret = self.head
        if ret is not sentinel:
            self.head = self.head.next
            return ret
        else:
            raise StopIteration


class HandlerChainFactory(ABC):
    """
    The HandlerChainFactory takes care of validating all parameters passed by the command and
    initializes the correct Handler subclass chaining them together into a HandlerChain and
    providing each Handler with the dependencies it needs based on the validated parameters.
    """

    @abstractmethod
    def build_chain(self, command: Command) -> HandlerChain:
        raise NotImplementedError

    def get_handler(self, command: Command) -> Handler:
        return self.build_chain(command).head


class CommandBus(ABC):
    """
    A commandbus instance resolves the correct HandlerChain required to
    execute the Command.
    Each CommandBus class/instance should be paired with the correct
    HandlerChainFactory class/instance.
    """

    @abstractmethod
    def resolve(self, command: Command) -> HandlerChain:
        raise NotImplementedError

    @abstractmethod
    def execute(self) -> DataTransferObject:
        raise NotImplementedError


class Interface(ABC):
    """
    Each of the Interface subclasses provides the abstract base class that contains the guidelines on how
    the corresponding Adapter subclass should implement.
    """
