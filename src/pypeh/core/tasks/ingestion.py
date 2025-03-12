from __future__ import annotations

import logging

from typing import TYPE_CHECKING

from pydantic_core import ValidationError

from pypeh.core.abc import HandlerChain, Task
from pypeh.core.handlers.baseclasses import DataOpsHandler, ManifestHandler, PersistenceHandler
from pypeh.core.models.constants import AdapterEnum
from pypeh.core.cache.dataview import get_dataview

if TYPE_CHECKING:
    from typing import Callable, ClassVar, Union, Optional, List
    from pypeh.core.abc import Task, Context, Handler
    from pypeh.core.commands.ingestion import IngestionCommand

logger = logging.getLogger(__name__)


class IngestionTask(Task):
    default_data_view_getter: ClassVar[Callable] = get_dataview

    def __init__(self, dto: Union[IngestionCommand, Context], **kwargs):
        super().__init__(dto)

    def resolve(self, command: IngestionCommand) -> HandlerChain:
        return HandlerChain()


class ValidationTask(IngestionTask):
    # TODO: this provides an outline of how a task can be set up
    # This requires further implementation and testing to get this working

    def __init__(self, dto: Union[Context, IngestionCommand], adapter_chain: Optional[List[Handler]] = None, **kwargs):
        super().__init__(dto, **kwargs)
        self._adapter_chain = adapter_chain

    def resolve(self) -> HandlerChain:
        # TODO: complete logic; temporary example
        if self._adapter_chain is not None:
            chain_length = 3
            if len(self._adapter_chain) != chain_length:
                raise ValidationError(f"adapter_chain can only contain {chain_length} adapter.")
            ret = HandlerChain.create(self._adapter_chain)
        else:
            validation_handler = DataOpsHandler.create(AdapterEnum.DATAFRAME)
            enrichment_handler = DataOpsHandler.create(AdapterEnum.DATAFRAME)
            persistence_handler = PersistenceHandler.create(AdapterEnum.DATAFRAME, "dump")

            validation_handler.next = enrichment_handler
            enrichment_handler.next = persistence_handler

            ret = HandlerChain(head=validation_handler)

        return ret


class ManifestIngestionTask(IngestionTask):
    def __init__(self, dto: Union[Context, IngestionCommand], adapter_chain: Optional[List[Handler]] = None, **kwargs):
        super().__init__(dto, **kwargs)
        self._adapter_chain = adapter_chain

    def resolve(self) -> HandlerChain:
        # TODO: adapter methods need to be added to the adapter_chain!!
        if self._adapter_chain is not None:
            chain_length = 1
            if len(self._adapter_chain) != chain_length:
                raise ValidationError(f"adapter_chain can only contain {chain_length} adapter.")
            ret = HandlerChain.create(self._adapter_chain)
        else:
            command = self.command
            if command is not None:
                params = command.params
            root = getattr(params, "root")
            if root is not None:
                ingestion_handler = ManifestHandler.create(root, "load")

            ret = HandlerChain(head=ingestion_handler)
        # TODO: resolve all data locations in manifest file

        return ret


class DataIngestionTask(IngestionTask):
    def __init__(self, dto: Union[Context, IngestionCommand], **kwargs):
        super().__init__(dto, **kwargs)
        # ManifestIngestionTask needs to run first

    def resolve(self) -> HandlerChain:
        # TODO: load in all data from context requests
        # based on ingested manifest we can create the
        # different DataSetHandlers

        # create handler for each of the files in the dataset
        # d1 = DataSetHandler()
        # d2 = DataSetHandler()

        # self.manifest_loader.next = d1
        # d1.next = d2

        # return HandlerChain(head=self.manifest_loader)
        return HandlerChain()

    # TODO: define workflows, consisting of multiple linear HandlerChains
    # a single HandlerChain might depend on one or more other HandlerChains.
