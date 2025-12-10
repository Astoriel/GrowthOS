"""Tests for output formatters."""

from __future__ import annotations

from growth_os.core.formatters import (
    format_table,
    format_kpi_card,
    format_kpi_dashboard,
    format_insight,
    format_actions,
)


class TestFormatters:

    def test_format_table_basic(self):
        """Formats list of dicts as markdown table."""
        data = [
            {"Name": "Alice", "Score": 95},
            {"Name": "Bob", "Score": 87},
        ]
        result = format_table(data, "Test Table")
        assert "### Test Table" in result
        assert "| Name | Score |" in result
        assert "Alice" in result
        assert "Bob" in result

    def test_format_table_empty(self):
        """Empty data returns 'No data' message."""
        result = format_table([], "Empty")
        assert "No data" in result

    def test_format_kpi_card_positive(self):
        """KPI with positive change shows up arrow."""
        result = format_kpi_card("Revenue", "$125,400", 12.5)
        assert "▲" in result
        assert "+12.5%" in result

    def test_format_kpi_card_negative(self):
        """KPI with negative change shows down arrow."""
        result = format_kpi_card("CAC", "$42.30", -8.0)
        assert "▼" in result
        assert "-8.0%" in result

    def test_format_kpi_card_no_change(self):
        """KPI without change omits arrow."""
        result = format_kpi_card("Users", 1234)
        assert "▲" not in result
        assert "▼" not in result

    def test_format_kpi_dashboard(self):
        """Dashboard formats multiple KPIs."""
        metrics = [
            {"label": "Revenue", "value": "$100k", "change": 10.0},
            {"label": "Users", "value": 500, "change": -5.0},
        ]
        result = format_kpi_dashboard(metrics)
        assert "Key Metrics" in result
        assert "Revenue" in result
        assert "Users" in result

    def test_format_insight(self):
        """Insight has emoji marker."""
        result = format_insight("Revenue dropped 23%")
        assert "💡" in result
        assert "Revenue dropped" in result

    def test_format_actions(self):
        """Actions are numbered."""
        result = format_actions(["Do X", "Do Y", "Do Z"])
        assert "1. Do X" in result
        assert "2. Do Y" in result
        assert "3. Do Z" in result

    def test_format_actions_empty(self):
        """Empty actions returns empty string."""
        assert format_actions([]) == ""
