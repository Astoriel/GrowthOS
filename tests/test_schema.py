"""Tests for schema discovery."""

from __future__ import annotations

from growth_os.core.schema import discover_tables, format_schema_for_prompt


class TestSchemaDiscovery:
    """Test table introspection and schema formatting."""

    def test_discover_tables(self, connector):
        """Discovers all tables with metadata."""
        tables = discover_tables(connector)
        assert len(tables) >= 3
        names = [t.name for t in tables]
        assert "marketing_spend" in names
        assert "user_events" in names

    def test_table_has_columns(self, connector):
        """Each table has column metadata."""
        tables = discover_tables(connector)
        spend_table = next(t for t in tables if t.name == "marketing_spend")
        assert len(spend_table.columns) > 0
        col_names = [c.name for c in spend_table.columns]
        assert "date" in col_names
        assert "channel" in col_names
        assert "spend" in col_names

    def test_table_has_row_count(self, connector):
        """Tables have row counts."""
        tables = discover_tables(connector)
        spend_table = next(t for t in tables if t.name == "marketing_spend")
        assert spend_table.row_count > 0

    def test_columns_have_samples(self, connector):
        """Columns have sample values."""
        tables = discover_tables(connector)
        spend_table = next(t for t in tables if t.name == "marketing_spend")
        channel_col = next(c for c in spend_table.columns if c.name == "channel")
        assert len(channel_col.sample_values) > 0

    def test_format_for_prompt(self, connector):
        """Schema can be formatted for LLM prompt injection."""
        tables = discover_tables(connector)
        prompt = format_schema_for_prompt(tables)
        assert "marketing_spend" in prompt
        assert "Column" in prompt
        assert "Type" in prompt

    def test_table_has_sample_rows(self, connector):
        """Tables have sample rows."""
        tables = discover_tables(connector)
        spend_table = next(t for t in tables if t.name == "marketing_spend")
        assert len(spend_table.sample_rows) > 0
        assert len(spend_table.sample_rows) <= 3
