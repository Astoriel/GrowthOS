"""Describe a specific data table schema."""

from __future__ import annotations

from growth_os.services import CatalogService


def describe_table(service: CatalogService, table_name: str) -> str:
    """Return schema, sample rows, and column descriptions for a table."""
    return service.describe_table(table_name)
