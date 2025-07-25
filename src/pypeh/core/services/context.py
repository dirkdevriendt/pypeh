from __future__ import annotations

import logging

from typing import TYPE_CHECKING

from pypeh.core.interfaces.inbound.dataops import InDataOpsInterface
from pypeh.core.interfaces.outbound.persistence import PersistenceInterface
from pypeh.core.cache.containers import CacheContainer, CacheContainerFactory
from pypeh.core.models.settings import ImportConfig
from pypeh.core.cache.utils import load_entities_from_tree
from pypeh.adapters.outbound.persistence.hosts import HostFactory

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class ContextService:
    def __init__(
        self,
        inbound_adapter: InDataOpsInterface | None = None,
        outbound_adapter: PersistenceInterface = HostFactory.create(settings=None),
        import_config: ImportConfig | None = None,
        cache: CacheContainer = CacheContainerFactory.new(),
    ):
        self.inbound_adapter = inbound_adapter
        self.outbound_adapter = outbound_adapter
        self.cache = cache
        if import_config is not None:
            self.import_config = import_config.to_validated_import_config()

    def _set_outbound_adapter(self, adapter: PersistenceInterface) -> bool:
        self.outbound_adapter = adapter
        return True

    def import_context(self, source: str) -> bool:
        connection_settings = None
        if self.import_config is not None:
            connection_settings = self.import_config.get_connection(source)
        if connection_settings is not None:
            adapter = HostFactory.create(connection_settings)  # issue is transform
            _ = self._set_outbound_adapter(adapter)

        root_stream = self.outbound_adapter.load(source)
        new_entities = []
        for entity in load_entities_from_tree(root_stream, create_proxy=None):
            new_entities.append(entity)
        _ = self.validate_new_context()

        for entity in new_entities:
            self.cache.add(entity)

        return True

    def validate_new_context(self) -> bool:
        # validation requirements
        # are identifiers dereferencable
        # are they all in the same format

        return True

    def update_context(self):
        pass

    def export_context(self):
        pass
