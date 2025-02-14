"""
This module defines abstract base classes (ABCs) for the pypeh project.


Usage:
    Use these abstract base classes as templates for creating concrete implementations
    in other modules. Each abstract base class should define the core methods and properties
    that must be implemented by any subclass.

"""

from __future__ import annotations

import logging

from abc import ABC, abstractmethod
from datetime import datetime
from pydantic import ValidationError
from typing import TYPE_CHECKING, TypeVar, Generic, Iterable


from pypeh.core.models.constants import TaskStatusEnum, ResponseStatusEnum
from pypeh.core.models.dto import CommandMetaData, CommandParams, ContextMetaData, MetaData, ResponseMetaData

if TYPE_CHECKING:
    from typing import Dict, Iterator, Any, Optional, Type, Union, ClassVar, Callable, List

T_Handler = TypeVar("T_Handler", bound="Handler")
T_Task = TypeVar("T_Task", bound="Task")
T_MetaData = TypeVar("T_MetaData", bound="MetaData")
sentinel = object()

logger = logging.getLogger(__name__)


class DataTransferObject(ABC, Generic[T_MetaData]):
    """
    DataTransferObject
    """

    metadata_model: ClassVar = NotImplemented

    def __init__(self, data: Any = None, metadata: T_MetaData = MetaData()):
        self.data = data
        self.metadata = metadata

    def transform_to(self, target_class: Type) -> "DataTransferObject":
        if isinstance(self, target_class):
            return self
        metadata = self.metadata.transform_to(target_class.metadata_model)
        new_instance = target_class(data=self.data, metadata=metadata)
        return new_instance


class Command(Generic[T_Task], DataTransferObject[CommandMetaData]):
    """
    A command acts essentially as a data tranfer object passing parameters
    from the API call to the correct Handler.
    """

    params_model: ClassVar[Type[CommandParams]] = CommandParams
    metadata_model: ClassVar[Type[CommandMetaData]] = CommandMetaData

    @classmethod
    def create(cls, fn: Callable = lambda: None, rank: int = 0, **params):
        metadata = cls.metadata_model(
            task_name=cls.get_task_class().__name__,
            calling_function=fn.__name__,
            module=fn.__module__,
            timestamp=datetime.now(),
            rank=rank,
            params=cls._validate_params(**params),
            status=TaskStatusEnum.PENDING,
        )
        return cls(metadata=metadata)

    @staticmethod
    @abstractmethod
    def get_task_class() -> Type[T_Task]:
        """Return the Task class this command requires."""
        pass

    @classmethod
    def _validate_params(cls, **params) -> CommandParams:
        try:
            # Validate metadata against the model
            return cls.params_model(**params)
        except ValidationError as e:
            error_msg = f"Invalid metadata for {cls.__name__}: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    def get_task(self, **kwargs) -> T_Task:
        """
        Create and return a task instance for this command.
        Validates metadata before creating the task.
        """
        task_class = self.get_task_class()
        return task_class(self, **kwargs)


class Context(DataTransferObject[ContextMetaData]):
    """
    The context acts as a data transfer object during workflow execution.
    """

    metadata_model: ClassVar[Type[ContextMetaData]] = ContextMetaData

    @classmethod
    def from_command(cls, command: Command) -> "Context":
        """Create a Context from a Command."""
        return command.transform_to(cls)  # type: ignore

    def add_task(self, command_type: Type[Command], **params):
        prev_rank = self.metadata.flush_command()
        command = command_type.create(rank=prev_rank + 1, **params)
        self.metadata.command = command.metadata
        task_class = command.get_task_class()

        return task_class(self)


class Response(DataTransferObject[ResponseMetaData]):
    """
    A Response acts as a data tranfer object structuring the result of the API call.
    """

    metadata_model: ClassVar[Type[ResponseMetaData]] = ResponseMetaData

    @property
    @abstractmethod
    def response_status(self):
        pass

    @classmethod
    def from_context(cls, context: Context) -> "Response":
        """Create a Response from a Context."""
        return context.transform_to(cls)  # type: ignore


class Handler(ABC):
    """
    The handler class is reponsible for connecting to a specific adapter
    that implements the logic required to solve a task
    AND the handler takes care of applying the peh specific model logic to
    the in- and output of the linked adapter.
    The handler should be provided with the needed dependencies upon initialization.
    """

    def __init__(self):
        self.next: "Handler" = sentinel  # type: ignore

    @abstractmethod
    def handle(self, context: Dict) -> bool:
        raise NotImplementedError

    @abstractmethod
    def map(self):
        # applies the transformation logic to context
        # to get context ready for next adapter
        pass

    @property
    @abstractmethod
    def adapter(self):
        pass


class HandlerChain(Iterable[T_Handler]):
    """
    Iterable chaining together a series of Handlers.
    """

    def __init__(self, head: Handler = sentinel):  # type: ignore
        self.head = head

    def __iter__(self) -> Iterator[T_Handler]:
        return self

    def __next__(self) -> T_Handler:
        ret = self.head
        if ret is not sentinel:
            self.head = self.head.next
            return ret  # type: ignore
        else:
            raise StopIteration

    @classmethod
    def create(cls, handlers: List[Handler]) -> "HandlerChain":
        if not handlers:
            return HandlerChain()

        for first, second in zip(handlers, handlers[1:]):
            first.next = second

        handlers[-1].next = sentinel  # type: ignore

        return HandlerChain(handlers[0])


class Task(ABC):
    """
    A Task instance resolves the correct HandlerChain required to
    execute the corresponding Command.
    """

    default_data_view_getter: ClassVar[Optional[Callable]] = None

    def __init__(self, dto: Union[Command, Context]):
        if isinstance(dto, Command):
            context = Context.from_command(dto)
        elif isinstance(dto, Context):
            context = dto
        else:
            logging.error(f"Data transfer object of type {type(dto)} was passed to Task()")
            raise ValueError
        self._context = context
        if self._context.data is None:
            if self.default_data_view_getter is not None:
                self._context.data = self.default_data_view_getter()
        self._context = context

    @property
    def command(self) -> Optional[CommandMetaData]:
        return self._context.metadata.command

    def _complete_task(self) -> bool:
        return True

    def complete(self, status: ResponseStatusEnum = ResponseStatusEnum.COMPLETED) -> Response:
        response = Response.from_context(self._context)
        response.metadata.complete(status=status)
        return response

    @abstractmethod
    def resolve(
        self,
    ) -> HandlerChain:
        raise NotImplementedError

    def execute(self) -> Context:
        handler_chain = self.resolve()
        for handler in handler_chain:
            _ = handler.handle(self._context)
            # Each handler can modify the context for subsequent handlers

        _ = self._complete_task()
        return self._context


class Interface(ABC):
    """
    Each of the Interface subclasses provides the abstract base class that contains the guidelines on how
    the corresponding Adapter subclass should implement.
    """
