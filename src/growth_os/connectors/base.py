"""Connector base types."""

from __future__ import annotations

from typing import Protocol


class QueryableConnector(Protocol):
    """Protocol for queryable data connectors."""

    def query(self, sql: str) -> list[dict]:
        """Run a query and return rows."""
