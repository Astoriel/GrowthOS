"""Backward-compatible schema exports."""

from growth_os.domain.models import ColumnInfo, TableInfo
from growth_os.ingestion.catalog import discover_table as _discover_table
from growth_os.ingestion.catalog import discover_tables, format_schema_for_prompt

__all__ = ["ColumnInfo", "TableInfo", "_discover_table", "discover_tables", "format_schema_for_prompt"]
