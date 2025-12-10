"""Shared service helpers."""

from __future__ import annotations

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.domain.models import ToolEnvelope
from growth_os.ingestion.catalog import inspect_freshness


def build_tool_envelope(
    title: str,
    body: str,
    connector: GrowthConnector,
    sources: list[str],
    warnings: list[str] | None = None,
) -> ToolEnvelope:
    """Build a ToolEnvelope with basic trust metadata."""
    warnings = list(warnings or [])
    freshness = {report.table_name: report for report in inspect_freshness(connector)}
    min_dates = []
    max_dates = []

    for source in sources:
        report = freshness.get(source)
        if report is None:
            continue
        if report.min_date:
            min_dates.append(report.min_date)
        if report.max_date:
            max_dates.append(report.max_date)
        if report.status == "stale":
            warnings.append(f"`{source}` is somewhat stale.")
        elif report.status == "outdated":
            warnings.append(f"`{source}` is outdated.")

    date_range = ""
    if min_dates and max_dates:
        date_range = f"{min(min_dates)} to {max(max_dates)}"

    return ToolEnvelope(
        title=title,
        body=body,
        sources=sources,
        date_range=date_range,
        warnings=sorted(set(warnings)),
    )


def extract_sql_sources(sql: str, connector: GrowthConnector) -> list[str]:
    """Guess source tables from SQL text."""
    sql_upper = sql.upper()
    return [table for table in connector.get_tables() if table.upper() in sql_upper]
