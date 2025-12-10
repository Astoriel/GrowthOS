"""Comprehensive regression tests for AST-based SQL safety."""

from __future__ import annotations

import pytest

from growth_os.query.safety import SQLSandboxError, validate_sql_ast


# ---------------------------------------------------------------------------
# Valid queries — must NOT raise
# ---------------------------------------------------------------------------

class TestValidQueries:
    def test_simple_select(self):
        validate_sql_ast("SELECT 1")

    def test_select_from_table(self):
        validate_sql_ast("SELECT * FROM marketing_spend")

    def test_select_with_where(self):
        validate_sql_ast("SELECT channel, SUM(spend) FROM marketing_spend WHERE date >= '2024-01-01' GROUP BY 1")

    def test_cte_select(self):
        validate_sql_ast("WITH cte AS (SELECT * FROM marketing_spend) SELECT * FROM cte")

    def test_nested_subquery(self):
        validate_sql_ast("SELECT * FROM (SELECT channel, COUNT(*) AS cnt FROM marketing_spend GROUP BY 1) t")

    def test_union_all(self):
        validate_sql_ast("SELECT 1 AS n UNION ALL SELECT 2 AS n")

    def test_describe(self):
        validate_sql_ast("DESCRIBE marketing_spend")

    def test_show_tables(self):
        validate_sql_ast("SHOW TABLES")

    def test_select_with_case(self):
        validate_sql_ast(
            "SELECT CASE WHEN spend > 100 THEN 'high' ELSE 'low' END AS tier FROM marketing_spend"
        )

    def test_window_function(self):
        validate_sql_ast(
            "SELECT date, spend, AVG(spend) OVER (ORDER BY date ROWS 6 PRECEDING) AS rolling_avg "
            "FROM marketing_spend"
        )

    def test_multiline_cte(self):
        sql = """
        WITH
          a AS (SELECT * FROM marketing_spend),
          b AS (SELECT * FROM user_events)
        SELECT a.channel, COUNT(b.user_id) AS users
        FROM a
        LEFT JOIN b ON a.channel = b.utm_source
        GROUP BY 1
        """
        validate_sql_ast(sql)


# ---------------------------------------------------------------------------
# Classic write operations — must raise SQLSandboxError
# ---------------------------------------------------------------------------

class TestForbiddenWriteOperations:
    def test_drop_table(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("DROP TABLE marketing_spend")

    def test_delete(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("DELETE FROM marketing_spend WHERE spend < 0")

    def test_update(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("UPDATE marketing_spend SET spend = 0")

    def test_insert(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("INSERT INTO marketing_spend VALUES ('2024-01-01', 'google', 100)")

    def test_create_table(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("CREATE TABLE new_table AS SELECT * FROM marketing_spend")

    def test_alter_table(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("ALTER TABLE marketing_spend ADD COLUMN extra TEXT")

    def test_truncate_table(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("TRUNCATE TABLE marketing_spend")


# ---------------------------------------------------------------------------
# Multi-statement queries — must raise
# ---------------------------------------------------------------------------

class TestMultiStatement:
    def test_two_selects_semicolon(self):
        with pytest.raises(SQLSandboxError, match="Multiple"):
            validate_sql_ast("SELECT 1; SELECT 2")

    def test_select_then_drop(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("SELECT * FROM marketing_spend; DROP TABLE marketing_spend")

    def test_select_then_insert(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("SELECT 1; INSERT INTO t VALUES (1)")


# ---------------------------------------------------------------------------
# Bypass attempts — must raise
# ---------------------------------------------------------------------------

class TestBypassAttempts:
    def test_comment_before_drop(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("-- safe query\nDROP TABLE marketing_spend")

    def test_mixed_case_drop(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("DrOp TaBlE marketing_spend")

    def test_cte_wrapping_delete(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast(
                "WITH hack AS (DELETE FROM marketing_spend RETURNING *) "
                "SELECT * FROM hack"
            )

    def test_cte_wrapping_insert(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast(
                "WITH hack AS (INSERT INTO t VALUES (1) RETURNING id) "
                "SELECT * FROM hack"
            )

    def test_subquery_wrapping_update(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast(
                "SELECT * FROM (UPDATE marketing_spend SET spend = 0 RETURNING *) t"
            )

    def test_whitespace_padding(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("   \n\t  DROP   TABLE   marketing_spend  ")


# ---------------------------------------------------------------------------
# Edge cases — empty / bad SQL
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_string(self):
        with pytest.raises(SQLSandboxError, match="Empty"):
            validate_sql_ast("")

    def test_whitespace_only(self):
        with pytest.raises(SQLSandboxError, match="Empty"):
            validate_sql_ast("   \n  ")

    def test_semicolon_only(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast(";")


# ---------------------------------------------------------------------------
# Error messages contain useful context
# ---------------------------------------------------------------------------

class TestErrorMessages:
    def test_drop_message_is_informative(self):
        with pytest.raises(SQLSandboxError, match="(?i)drop"):
            validate_sql_ast("DROP TABLE marketing_spend")

    def test_multi_statement_message(self):
        with pytest.raises(SQLSandboxError, match="Multiple"):
            validate_sql_ast("SELECT 1; SELECT 2")
