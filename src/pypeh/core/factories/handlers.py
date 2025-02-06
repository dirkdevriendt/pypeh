import logging

from pypeh.core.abc import (
    Command,
    HandlerChainFactory,
    HandlerChain,
)
from pypeh.core.handlers.ingestion import ValidationHandler, EnrichmentHandler
from pypeh.core.commands.ingestion import IngestionCommand

logger = logging.getLogger(__name__)


class ValidationHandlerFactory:
    @classmethod
    def get_handler(cls, command: IngestionCommand) -> ValidationHandler:
        engine = None
        if isinstance(command, IngestionCommand):
            engine = command.config.get("engine")
            if engine == "dataframe" or engine == "pandas":
                try:
                    from dataframe_adapter.adapter import DataValidationAdapter
                except ImportError:
                    logging.error("The 'pandas' engine requires the 'dataframe_adapter' module. Please install it.")
                    raise ImportError("The 'pandas' engine requires the 'dataframe_adapter' module. Please install it.")
                return ValidationHandler(adapter=DataValidationAdapter())

        raise ValueError(f"Unsupported command type or engine: {engine}")


class EnrichmentHandlerFactory:
    @classmethod
    def get_handler(cls, command: IngestionCommand) -> EnrichmentHandler:
        engine = None
        if isinstance(command, IngestionCommand):
            engine = command.config.get("engine")
            if engine == "dataframe" or engine == "pandas":
                try:
                    from dataframe_adapter.adapter import DataValidationAdapter
                except ImportError:
                    logging.error("The 'pandas' engine requires the 'dataframe_adapter' module. Please install it.")
                    raise ImportError("The 'pandas' engine requires the 'dataframe_adapter' module. Please install it.")
                return EnrichmentHandler(adapter=DataValidationAdapter())

        raise ValueError(f"Unsupported command type or engine: {engine}")


class PersistenceHandlerFactory:
    @classmethod
    def get_handler(cls, command: IngestionCommand) -> EnrichmentHandler:
        engine = None
        if isinstance(command, IngestionCommand):
            engine = command.config.get("engine")
            if engine == "dataframe" or engine == "pandas":
                try:
                    from dataframe_adapter.adapter import DataPersistenceAdapter
                except ImportError:
                    logging.error("The 'pandas' engine requires the 'dataframe_adapter' module. Please install it.")
                    raise ImportError("The 'pandas' engine requires the 'dataframe_adapter' module. Please install it.")
                return EnrichmentHandler(adapter=DataPersistenceAdapter())

        raise ValueError(f"Unsupported command type or engine: {engine}")


class IngestionHandlerChainFactory(HandlerChainFactory):
    @classmethod
    def build_chain(cls, command: Command) -> HandlerChain:
        validation_handler = ValidationHandlerFactory.get_handler(command)
        enrichment_handler = EnrichmentHandlerFactory.get_handler(command)
        persistence_handler = PersistenceHandlerFactory.get_handler(command)

        validation_handler.next = enrichment_handler
        enrichment_handler.next = persistence_handler

        return HandlerChain(head=validation_handler)
