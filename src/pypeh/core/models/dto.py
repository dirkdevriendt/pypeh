from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel
from typing import TYPE_CHECKING, Dict, List, Optional

from pypeh.core.models.constants import TaskStatusEnum, ResponseStatusEnum

if TYPE_CHECKING:
    from typing import Type


class CommandParams(BaseModel):
    pass


class MetaData(BaseModel):
    def transform_to(self, target_class: Type):
        raise NotImplementedError


class CommandMetaData(MetaData):
    task_name: str
    calling_function: str  # func.__name__
    module: str  # func.__module__
    timestamp: datetime
    rank: int
    params: CommandParams  # This will be replaced with specific params model
    status: TaskStatusEnum

    def transform_to(self, target_class: Type):
        if target_class == ContextMetaData:
            new_instance = target_class(
                command=self,
                executed_commands=list(),
                namespaces=dict(),
            )
            return new_instance
        else:
            raise NotImplementedError


class ContextMetaData(MetaData):
    command: Optional[CommandMetaData]
    executed_commands: List[CommandMetaData]
    namespaces: Dict[str, str]

    @classmethod
    def from_command(cls, command_metadata: CommandMetaData):
        command_metadata.transform_to(cls)

    def flush_command(self) -> int:
        rank = -1
        if self.command is not None:
            rank = self.command.rank
            if self.command.status != TaskStatusEnum.COMPLETED:
                raise AssertionError("Flusing metadata for incomplete task")
            self.executed_commands.append(self.command)
        self.command = None
        return rank

    def transform_to(self, target_class: Type):
        if target_class == ResponseMetaData:
            new_instance = target_class(executed_commands=self.executed_commands)
            return new_instance
        else:
            raise NotImplementedError

    def complete(self) -> ResponseMetaData:
        _ = self.flush_command()
        return self.transform_to(ResponseMetaData)


class ResponseMetaData(MetaData):
    executed_commands: List[CommandMetaData]
    status: Optional[ResponseStatusEnum] = None

    @classmethod
    def from_context(cls, context_metadata: ContextMetaData):
        context_metadata.transform_to(cls)

    def complete(self, status: ResponseStatusEnum = ResponseStatusEnum.COMPLETED) -> bool:
        self.status = status
        return True
