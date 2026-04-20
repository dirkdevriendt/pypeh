from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import psycopg2

from pypeh.adapters.outbound.persistence.hosts import DatabaseAdapter

if TYPE_CHECKING:
    from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class PostgreSQLAdapter(DatabaseAdapter):
    def connect(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5432,
        **kwargs,
    ) -> None:
        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
            **kwargs,
        )

    def disconnect(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def query(
        self, resource_type: str, query_params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError(
            "PostgreSQLAdapter.query is not implemented yet. "
            f"resource_type={resource_type!r}, query_params={query_params!r}."
        )

    def get(self, resource_type: str, resource_id: str) -> Dict[str, Any]:
        raise NotImplementedError(
            "PostgreSQLAdapter.get is not implemented yet. "
            f"resource_type={resource_type!r}, resource_id={resource_id!r}."
        )

    def save(self, resource_type: str, data: Dict[str, Any]) -> str:
        raise NotADirectoryError

    def update(
        self, resource_type: str, resource_id: str, data: Dict[str, Any]
    ) -> None:
        raise NotImplementedError(
            "PostgreSQLAdapter.update is not implemented yet. "
            f"resource_type={resource_type!r}, resource_id={resource_id!r}, "
            f"data_keys={list(data.keys())!r}."
        )

    def delete(self, resource_type: str, resource_id: str) -> None:
        raise NotADirectoryError
