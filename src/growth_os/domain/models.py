"""Shared domain models."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class ColumnInfo:
    """Metadata for a single database column."""

    name: str
    dtype: str
    sample_values: list[str] = field(default_factory=list)
    null_percentage: float = 0.0
    distinct_count: int = 0


@dataclass(slots=True)
class TableInfo:
    """Metadata for a single database table."""

    name: str
    row_count: int = 0
    columns: list[ColumnInfo] = field(default_factory=list)
    sample_rows: list[dict] = field(default_factory=list)


@dataclass(slots=True)
class FreshnessReport:
    """Freshness summary for a table."""

    table_name: str
    date_column: str
    min_date: str | None
    max_date: str | None
    days_stale: int | None
    status: str


@dataclass(slots=True)
class ValidationIssue:
    """Validation result entry."""

    table_name: str
    severity: str
    message: str


@dataclass(slots=True)
class ValidationResult:
    """Validation result envelope."""

    issues: list[ValidationIssue] = field(default_factory=list)
    freshness: list[FreshnessReport] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Return True when there are no error severity issues."""
        return not any(issue.severity == "error" for issue in self.issues)


@dataclass(slots=True)
class ToolEnvelope:
    """Presentation-ready tool output with trust metadata."""

    title: str
    body: str
    sources: list[str] = field(default_factory=list)
    date_range: str = ""
    warnings: list[str] = field(default_factory=list)
