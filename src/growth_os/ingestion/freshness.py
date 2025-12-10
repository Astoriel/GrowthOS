"""Freshness computation for ingested tables."""

from __future__ import annotations

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.domain.contracts import DATE_COLUMN_CANDIDATES
from growth_os.domain.models import FreshnessReport
from growth_os.ingestion.catalog import discover_tables
from growth_os.query.builder import safe_identifier


def compute_freshness(connector: GrowthConnector) -> list[FreshnessReport]:
    """Compute freshness for all tables that contain a recognised date column.

    Status thresholds:
    - fresh:    <= 2 days stale
    - stale:    3–14 days stale
    - outdated: > 14 days stale
    - unknown:  no date data available
    """
    reports: list[FreshnessReport] = []

    for table in discover_tables(connector):
        date_column = next(
            (col.name for col in table.columns if col.name in DATE_COLUMN_CANDIDATES),
            "",
        )
        if not date_column:
            continue

        safe_table = safe_identifier(table.name)
        safe_col = date_column.replace('"', '""')

        try:
            rows = connector.query(
                f"""
                SELECT
                    MIN(CAST("{safe_col}" AS DATE)) AS min_date,
                    MAX(CAST("{safe_col}" AS DATE)) AS max_date,
                    DATEDIFF('day', MAX(CAST("{safe_col}" AS DATE)), CURRENT_DATE) AS days_stale
                FROM {safe_table}
                """
            )
        except Exception:
            continue

        if not rows:
            continue

        row = rows[0]
        days_stale = row.get("days_stale")

        if days_stale is None:
            status = "unknown"
        elif days_stale <= 2:
            status = "fresh"
        elif days_stale <= 14:
            status = "stale"
        else:
            status = "outdated"

        reports.append(
            FreshnessReport(
                table_name=table.name,
                date_column=date_column,
                min_date=str(row["min_date"]) if row.get("min_date") is not None else None,
                max_date=str(row["max_date"]) if row.get("max_date") is not None else None,
                days_stale=days_stale,
                status=status,
            )
        )

    return reports
