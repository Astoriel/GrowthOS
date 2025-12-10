"""Catalog discovery and schema introspection."""

from __future__ import annotations

import logging

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.domain.contracts import CONTRACT_SPECS
from growth_os.domain.models import ColumnInfo, FreshnessReport, TableInfo, ValidationResult
from growth_os.query.builder import safe_identifier

logger = logging.getLogger(__name__)


def discover_tables(connector: GrowthConnector) -> list[TableInfo]:
    """Discover all available tables."""
    tables = connector.get_tables()
    result: list[TableInfo] = []
    for table_name in tables:
        try:
            result.append(discover_table(connector, table_name))
        except Exception as exc:  # pragma: no cover - logging path
            logger.error("Failed to discover table %s: %s", table_name, exc)
    return result


def discover_table(connector: GrowthConnector, table_name: str) -> TableInfo:
    """Discover detailed metadata for a single table."""
    table_name = safe_identifier(table_name)
    count_result = connector.query(f"SELECT COUNT(*) AS cnt FROM {table_name}")
    row_count = count_result[0]["cnt"] if count_result else 0
    col_result = connector.query(f"DESCRIBE {table_name}")
    columns: list[ColumnInfo] = []

    for col_row in col_result:
        col_name = col_row.get("column_name", col_row.get("Field", ""))
        col_type = col_row.get("column_type", col_row.get("Type", ""))
        safe_column = col_name.replace('"', '""')

        try:
            samples = connector.query(
                f"""
                SELECT DISTINCT CAST("{safe_column}" AS VARCHAR) AS val
                FROM {table_name}
                WHERE "{safe_column}" IS NOT NULL
                LIMIT 5
                """
            )
            sample_values = [str(sample["val"]) for sample in samples]
        except Exception:
            sample_values = []

        try:
            null_result = connector.query(
                f"""
                SELECT
                    ROUND(100.0 * SUM(CASE WHEN "{safe_column}" IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS null_pct,
                    COUNT(DISTINCT "{safe_column}") AS distinct_cnt
                FROM {table_name}
                """
            )
            null_pct = float(null_result[0]["null_pct"]) if null_result else 0.0
            distinct_cnt = int(null_result[0]["distinct_cnt"]) if null_result else 0
        except Exception:
            null_pct = 0.0
            distinct_cnt = 0

        columns.append(
            ColumnInfo(
                name=col_name,
                dtype=col_type,
                sample_values=sample_values,
                null_percentage=null_pct,
                distinct_count=distinct_cnt,
            )
        )

    try:
        sample_rows = connector.query(f"SELECT * FROM {table_name} LIMIT 3")
    except Exception:
        sample_rows = []

    return TableInfo(name=table_name, row_count=row_count, columns=columns, sample_rows=sample_rows)


def format_schema_for_prompt(tables: list[TableInfo]) -> str:
    """Format schema for prompt injection."""
    lines = ["## Available Tables\n"]
    for table in tables:
        lines.append(f"### TABLE: `{table.name}` ({table.row_count:,} rows)")
        lines.append("")
        lines.append("| Column | Type | Distinct | Nulls | Sample Values |")
        lines.append("|---|---|---|---|---|")
        for column in table.columns:
            samples = ", ".join(f"`{value}`" for value in column.sample_values[:3])
            lines.append(
                f"| {column.name} | {column.dtype} | {column.distinct_count:,} | "
                f"{column.null_percentage:.1f}% | {samples} |"
            )
        lines.append("")
    return "\n".join(lines)


def inspect_freshness(connector: GrowthConnector) -> list[FreshnessReport]:
    """Inspect freshness for all tables with a date-like column.

    Delegates to ingestion.freshness.compute_freshness.
    """
    from growth_os.ingestion.freshness import compute_freshness
    return compute_freshness(connector)


def validate_marketing_dataset(connector: GrowthConnector) -> ValidationResult:
    """Validate canonical marketing tables.

    Delegates to ingestion.validators.validate_all_contracts.
    """
    from growth_os.ingestion.validators import validate_all_contracts
    return validate_all_contracts(connector, CONTRACT_SPECS)
