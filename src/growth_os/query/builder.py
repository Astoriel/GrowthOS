"""Safe SQL construction helpers."""

from __future__ import annotations

from datetime import datetime
import re


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_identifier(value: str) -> str:
    """Validate and return a SQL identifier."""
    if not IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid SQL identifier: {value}")
    return value


def safe_sql_string(value: str) -> str:
    """Escape a SQL string literal."""
    return value.replace("'", "''")


def safe_date(value: str) -> str:
    """Validate a date literal in YYYY-MM-DD format."""
    datetime.strptime(value, "%Y-%m-%d")
    return value
