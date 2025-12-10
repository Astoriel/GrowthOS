"""Discovery and query services."""

from __future__ import annotations

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.ingestion.catalog import discover_table, discover_tables
from growth_os.presentation.markdown import format_table
from growth_os.services._helpers import build_tool_envelope, extract_sql_sources


class CatalogService:
    """Services related to catalog discovery and raw querying."""

    def __init__(self, connector: GrowthConnector):
        self.connector = connector

    def list_tables(self):
        """List available tables with a compact summary."""
        tables = discover_tables(self.connector)
        if not tables:
            body = (
                "No tables found. Set `GROWTH_DATA_DIR` to a folder with CSV files, "
                "attach PostgreSQL, or use bundled sample data."
            )
            return build_tool_envelope("Available Tables", body, self.connector, [])

        rows = []
        for table in tables:
            col_names = ", ".join(column.name for column in table.columns[:6])
            if len(table.columns) > 6:
                col_names += f" (+{len(table.columns) - 6} more)"
            rows.append(
                {
                    "Table": table.name,
                    "Rows": table.row_count,
                    "Columns": len(table.columns),
                    "Column Names": col_names,
                }
            )
        body = format_table(rows, "📊 Available Tables")
        return build_tool_envelope("Available Tables", body, self.connector, [table.name for table in tables])

    def describe_table(self, table_name: str):
        """Describe one table in detail."""
        table_info = discover_table(self.connector, table_name)
        rows = []
        for column in table_info.columns:
            rows.append(
                {
                    "Column": column.name,
                    "Type": column.dtype,
                    "Distinct": column.distinct_count,
                    "Null %": f"{column.null_percentage:.1f}%",
                    "Samples": ", ".join(column.sample_values[:3]),
                }
            )
        body = format_table(rows, f"📋 Table: `{table_name}` ({table_info.row_count:,} rows)")
        if table_info.sample_rows:
            body += "\n\n" + format_table(table_info.sample_rows, "Sample Rows (first 3)")
        return build_tool_envelope(f"Describe {table_name}", body, self.connector, [table_name])

    def run_query(self, sql: str, offset: int = 0, limit: int = 50):
        """Execute a custom read-only SQL query with optional pagination.

        Use offset/limit to page through large result sets.
        """
        results = self.connector.query(sql)
        if not results:
            body = "Query returned no results."
        else:
            total = len(results)
            page = results[offset: offset + limit]
            body = format_table(page, f"Query Results ({total:,} rows)")
            if total > limit or offset > 0:
                showing_to = min(offset + limit, total)
                body += f"\n\n_Showing rows {offset + 1}–{showing_to} of {total:,}._"
                if offset + limit < total:
                    body += f" Pass `offset={offset + limit}` for the next page."
        sources = extract_sql_sources(sql, self.connector)
        return build_tool_envelope("Query Results", body, self.connector, sources)
