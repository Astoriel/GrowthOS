"""List available data tables."""

from __future__ import annotations

from growth_os.services import CatalogService


def list_tables(service: CatalogService) -> str:
    """List all available tables in the connected data sources."""
    return service.list_tables()
