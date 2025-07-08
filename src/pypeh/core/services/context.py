from __future__ import annotations

import logging

from typing import TYPE_CHECKING

from pypeh.core.interfaces.inbound.dataops import InDataOpsInterface
from pypeh.core.interfaces.outbound.dataops import OutDataOpsInterface
from pypeh.core.cache.containers import CacheContainer, CacheContainerFactory


if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class ContextService:
    def __init__(
        self,
        inbound_adapter: InDataOpsInterface,
        outbound_adapter: OutDataOpsInterface,
        cache: CacheContainer = CacheContainerFactory.new(),
    ):
        self.inbound_adapter = inbound_adapter
        self.outbound_adapter = outbound_adapter
        self.cache = cache

    def import_context(self):
        # load in file

        # pass to cache: load_entities_from_tree

        # validate newly added context
        # does one need the entire context for validation?

        pass

    def validate_new_context(self):
        # validation requirements
        # are identifiers dereferencable
        # are they all in the same format

        pass

    def update_context(self):
        pass

    def export_context(self):
        pass
