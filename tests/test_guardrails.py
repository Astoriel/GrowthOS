"""Additional SQL safety guardrail tests and GrowthConnector value normalization."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.query.safety import SQLSandboxError, validate_sql_ast


class TestUnionReadQueries:
    def test_union_distinct_is_allowed(self):
        validate_sql_ast("SELECT id FROM a UNION SELECT id FROM b")

    def test_union_all_across_two_tables_is_allowed(self):
        validate_sql_ast(
            "SELECT channel, spend FROM marketing_spend WHERE date >= '2024-01-01' "
            "UNION ALL "
            "SELECT utm_source AS channel, revenue AS spend FROM user_events"
        )

    def test_intersect_is_allowed(self):
        validate_sql_ast(
            "SELECT channel FROM marketing_spend "
            "INTERSECT "
            "SELECT channel FROM marketing_spend WHERE spend > 0"
        )

    def test_except_is_allowed(self):
        validate_sql_ast(
            "SELECT channel FROM marketing_spend "
            "EXCEPT "
            "SELECT 'unknown' AS channel"
        )

    def test_union_cannot_smuggle_write_in_subquery_branch(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast(
                "SELECT * FROM marketing_spend "
                "UNION ALL "
                "SELECT * FROM (UPDATE marketing_spend SET spend = 0 RETURNING *) t"
            )

    def test_union_with_aggregates_is_allowed(self):
        validate_sql_ast(
            "SELECT 'spend' AS metric, SUM(spend) AS total FROM marketing_spend "
            "UNION ALL "
            "SELECT 'revenue' AS metric, SUM(revenue) AS total FROM user_events"
        )


class TestNestedSubqueries:
    def test_three_levels_of_nesting_is_allowed(self):
        validate_sql_ast(
            "SELECT * FROM ("
            "  SELECT channel, cnt FROM ("
            "    SELECT channel, COUNT(*) AS cnt FROM ("
            "      SELECT channel FROM marketing_spend"
            "    ) inner_q GROUP BY 1"
            "  ) mid_q"
            ") outer_q"
        )

    def test_subquery_in_where_clause_is_allowed(self):
        validate_sql_ast(
            "SELECT * FROM marketing_spend "
            "WHERE channel IN (SELECT DISTINCT utm_source FROM user_events)"
        )

    def test_correlated_subquery_is_allowed(self):
        validate_sql_ast(
            "SELECT user_id, revenue FROM user_events e "
            "WHERE revenue > ("
            "  SELECT AVG(revenue) FROM user_events "
            "  WHERE utm_source = e.utm_source"
            ")"
        )

    def test_subquery_in_select_list_is_allowed(self):
        validate_sql_ast(
            "SELECT channel, (SELECT MAX(spend) FROM marketing_spend) AS max_spend "
            "FROM marketing_spend GROUP BY 1"
        )

    def test_subquery_with_cte_is_allowed(self):
        validate_sql_ast(
            "WITH agg AS (SELECT channel, SUM(spend) AS total FROM marketing_spend GROUP BY 1) "
            "SELECT * FROM (SELECT * FROM agg WHERE total > 1000) filtered"
        )


class TestCTEWriteAttempts:
    def test_cte_wrapping_delete_raises(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast(
                "WITH evil AS (DELETE FROM marketing_spend RETURNING *) "
                "SELECT * FROM evil"
            )

    def test_cte_wrapping_update_raises(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast(
                "WITH updater AS (UPDATE marketing_spend SET spend = 0 RETURNING *) "
                "SELECT * FROM updater"
            )

    def test_chained_legitimate_ctes_are_allowed(self):
        validate_sql_ast(
            "WITH "
            "  base AS (SELECT channel, SUM(spend) AS total FROM marketing_spend GROUP BY 1), "
            "  ranked AS ("
            "    SELECT *, ROW_NUMBER() OVER (ORDER BY total DESC) AS rank FROM base"
            "  ) "
            "SELECT * FROM ranked WHERE rank <= 3"
        )

    def test_cte_with_join_is_allowed(self):
        validate_sql_ast(
            "WITH spend AS (SELECT channel, SUM(spend) AS s FROM marketing_spend GROUP BY 1), "
            "     events AS (SELECT utm_source, COUNT(*) AS cnt FROM user_events GROUP BY 1) "
            "SELECT s.channel, s.s, e.cnt "
            "FROM spend s LEFT JOIN events e ON s.channel = e.utm_source"
        )


class TestWhereClauseInjectionAttempts:
    def test_always_true_tautology_is_allowed(self):
        validate_sql_ast("SELECT * FROM marketing_spend WHERE 1=1")

    def test_or_tautology_pattern_is_allowed(self):
        validate_sql_ast(
            "SELECT * FROM marketing_spend WHERE channel = 'google' OR 1=1"
        )

    def test_null_comparison_injection_pattern_is_allowed(self):
        validate_sql_ast(
            "SELECT * FROM marketing_spend WHERE channel IS NOT NULL OR channel IS NULL"
        )

    def test_semicolon_injection_after_select_is_blocked(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("SELECT 1; DROP TABLE marketing_spend")

    def test_inline_comment_smuggling_drop_is_blocked(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast("SELECT 1 -- harmless comment\nDROP TABLE marketing_spend")

    def test_multi_statement_with_delete_after_comment_is_blocked(self):
        with pytest.raises(SQLSandboxError):
            validate_sql_ast(
                "SELECT * FROM marketing_spend; DELETE FROM marketing_spend WHERE 1=1"
            )


class TestNormalizeValue:
    def test_decimal_converts_to_float(self):
        result = GrowthConnector._normalize_value(Decimal("123.456"))
        assert result == 123.456
        assert isinstance(result, float)

    def test_decimal_zero_converts_to_float_zero(self):
        result = GrowthConnector._normalize_value(Decimal("0"))
        assert result == 0.0
        assert isinstance(result, float)

    def test_decimal_negative_converts_to_float(self):
        result = GrowthConnector._normalize_value(Decimal("-9.99"))
        assert result == pytest.approx(-9.99)
        assert isinstance(result, float)

    def test_datetime_converts_to_iso_string(self):
        result = GrowthConnector._normalize_value(datetime(2024, 1, 15, 10, 30))
        assert result == "2024-01-15T10:30:00"
        assert isinstance(result, str)

    def test_datetime_with_seconds_produces_correct_iso(self):
        result = GrowthConnector._normalize_value(datetime(2024, 6, 30, 23, 59, 59))
        assert result == "2024-06-30T23:59:59"

    def test_datetime_with_microseconds_produces_iso_string(self):
        result = GrowthConnector._normalize_value(datetime(2024, 6, 30, 23, 59, 59, 123456))
        assert isinstance(result, str)
        assert "2024-06-30" in result

    def test_date_converts_to_iso_string(self):
        result = GrowthConnector._normalize_value(date(2024, 1, 15))
        assert result == "2024-01-15"
        assert isinstance(result, str)

    def test_date_start_of_year_produces_correct_iso(self):
        result = GrowthConnector._normalize_value(date(2024, 1, 1))
        assert result == "2024-01-01"

    def test_uuid_converts_to_string(self):
        uid = UUID("12345678-1234-5678-1234-567812345678")
        result = GrowthConnector._normalize_value(uid)
        assert result == "12345678-1234-5678-1234-567812345678"
        assert isinstance(result, str)

    def test_bytes_decodes_to_utf8_string(self):
        result = GrowthConnector._normalize_value(b"hello bytes")
        assert result == "hello bytes"
        assert isinstance(result, str)

    def test_bytes_empty_decodes_to_empty_string(self):
        result = GrowthConnector._normalize_value(b"")
        assert result == ""
        assert isinstance(result, str)

    def test_none_passes_through_unchanged(self):
        result = GrowthConnector._normalize_value(None)
        assert result is None

    def test_int_passes_through_unchanged(self):
        result = GrowthConnector._normalize_value(42)
        assert result == 42
        assert isinstance(result, int)

    def test_int_zero_passes_through(self):
        result = GrowthConnector._normalize_value(0)
        assert result == 0

    def test_float_passes_through_unchanged(self):
        result = GrowthConnector._normalize_value(3.14)
        assert result == pytest.approx(3.14)
        assert isinstance(result, float)

    def test_string_passes_through_unchanged(self):
        result = GrowthConnector._normalize_value("regular string")
        assert result == "regular string"
        assert isinstance(result, str)

    def test_empty_string_passes_through(self):
        result = GrowthConnector._normalize_value("")
        assert result == ""

    def test_bool_true_passes_through(self):
        result = GrowthConnector._normalize_value(True)
        assert result is True

    def test_bool_false_passes_through(self):
        result = GrowthConnector._normalize_value(False)
        assert result is False
