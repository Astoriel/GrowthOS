"""PostgreSQL connector for DuckDB attachment."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb as _duckdb

logger = logging.getLogger(__name__)


class PostgresConnector:
    """Handles attaching a PostgreSQL database to DuckDB as a read-only source."""

    def __init__(self, url: str) -> None:
        self.url = url

    @property
    def configured(self) -> bool:
        """Return True if a PostgreSQL URL is set."""
        return bool(self.url)

    def attach_to(self, conn: _duckdb.DuckDBPyConnection) -> bool:
        """Install the postgres extension and attach the database.

        Returns True on success, False on failure.
        """
        if not self.url:
            return False
        try:
            conn.execute("INSTALL postgres; LOAD postgres;")
            conn.execute(f"ATTACH '{self.url}' AS pg (TYPE POSTGRES, READ_ONLY)")
            logger.info("PostgresConnector: attached '%s' as 'pg' (read-only)", self.url[:30])
            return True
        except Exception as exc:
            logger.error("PostgresConnector: failed to attach: %s", exc)
            return False
