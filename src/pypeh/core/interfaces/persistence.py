import logging

from pypeh.core.abc import Interface

logger = logging.getLogger(__name__)


class PersistenceInterface(Interface):
    pass


class RepositoryInterface(PersistenceInterface):
    def __init__(self):
        self.engine = None
