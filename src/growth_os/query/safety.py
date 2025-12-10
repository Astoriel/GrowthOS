"""AST-based SQL safety validation using sqlglot."""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp


class SQLSandboxError(Exception):
    """Raised when a query violates GrowthOS read-only rules."""


# Allowed top-level statement types
_ALLOWED_STATEMENT_TYPES = (
    exp.Select, exp.With, exp.Union, exp.Intersect, exp.Except, exp.Describe, exp.Show, exp.Command
)

# Write and DDL expression types that are forbidden anywhere in the AST
_FORBIDDEN_EXPRESSION_TYPES = (
    exp.Drop,
    exp.Delete,
    exp.Update,
    exp.Insert,
    exp.Create,
    exp.Alter,
    exp.TruncateTable,
    exp.Transaction,
    exp.Commit,
    exp.Rollback,
    exp.Grant,
    exp.Revoke,
    exp.Copy,
    exp.Export,
)


def validate_sql_ast(sql: str) -> None:
    """Validate that SQL is safe for read-only execution.

    Raises SQLSandboxError if the SQL contains write operations,
    DDL statements, multi-statement queries, or cannot be parsed.
    """
    if not sql or not sql.strip():
        raise SQLSandboxError("Empty SQL is not allowed.")

    try:
        statements = sqlglot.parse(sql, dialect="duckdb", error_level=sqlglot.ErrorLevel.RAISE)
    except sqlglot.errors.ParseError as exc:
        raise SQLSandboxError(f"SQL parse error: {exc}") from exc

    if not statements:
        raise SQLSandboxError("Empty SQL is not allowed.")

    if len(statements) > 1:
        raise SQLSandboxError(
            "Multiple SQL statements are not allowed. Only a single read-only query is permitted."
        )

    stmt = statements[0]

    if stmt is None:
        raise SQLSandboxError("Empty SQL is not allowed.")

    # Check top-level statement type
    if not isinstance(stmt, _ALLOWED_STATEMENT_TYPES):
        stmt_type = type(stmt).__name__.upper()
        raise SQLSandboxError(
            f"Forbidden SQL operation '{stmt_type}' detected. "
            "Only SELECT, WITH...SELECT, DESCRIBE, and SHOW are permitted."
        )

    # For SHOW/DESCRIBE/Command they are inherently read-only — no further walk needed
    if isinstance(stmt, (exp.Describe, exp.Show, exp.Command)):
        return

    # Walk entire AST looking for any write or DDL node
    for node in stmt.walk():
        if isinstance(node, _FORBIDDEN_EXPRESSION_TYPES):
            op_name = type(node).__name__.upper()
            raise SQLSandboxError(
                f"Forbidden SQL operation '{op_name}' detected inside the query. "
                "Only read-only queries are allowed."
            )


def allowed_sql_prefixes() -> tuple[str, ...]:
    """Return supported read-only SQL prefixes (kept for backward compatibility)."""
    return ("SELECT", "WITH", "DESCRIBE", "SHOW")
