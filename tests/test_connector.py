"""Tests for GrowthOS core connector."""

from __future__ import annotations

import pytest

from growth_os.core.connector import GrowthConnector, SQLSandboxError


class TestDuckDBConnector:
    """Test DuckDB connector with CSV ingestion."""

    def test_csv_ingestion(self, connector):
        """CSVs are auto-ingested as tables."""
        tables = connector.get_tables()
        assert "marketing_spend" in tables
        assert "user_events" in tables
        assert "campaigns" in tables

    def test_query_returns_results(self, connector):
        """Basic query returns list of dicts."""
        results = connector.query("SELECT COUNT(*) as cnt FROM marketing_spend")
        assert len(results) == 1
        assert results[0]["cnt"] > 0

    def test_query_read_only(self, connector):
        """Forbidden SQL patterns are blocked."""
        with pytest.raises(SQLSandboxError, match="DROP"):
            connector.query("DROP TABLE marketing_spend")

    def test_query_no_insert(self, connector):
        """INSERT is blocked."""
        with pytest.raises(SQLSandboxError, match="INSERT"):
            connector.query("INSERT INTO marketing_spend VALUES (1,2,3)")

    def test_query_no_update(self, connector):
        """UPDATE is blocked."""
        with pytest.raises(SQLSandboxError, match="UPDATE"):
            connector.query("UPDATE marketing_spend SET spend = 0")

    def test_query_no_multi_statement(self, connector):
        """Multiple statements are blocked."""
        with pytest.raises(SQLSandboxError, match="Multiple"):
            connector.query("SELECT 1; SELECT 2")

    def test_single_csv_ingestion(self, connector, tmp_path):
        """Ingest a single CSV file."""
        csv_path = tmp_path / "test_table.csv"
        csv_path.write_text("id,name\n1,alice\n2,bob\n")
        name = connector.ingest_csv(str(csv_path))
        assert name == "test_table"
        results = connector.query("SELECT * FROM test_table")
        assert len(results) == 2

    def test_empty_connector(self, empty_connector):
        """Empty connector has no tables."""
        tables = empty_connector.get_tables()
        assert len(tables) == 0

    def test_sanitize_table_name(self):
        """Table names are sanitized."""
        assert GrowthConnector._sanitize_table_name("My File (1)") == "my_file__1_"
        assert GrowthConnector._sanitize_table_name("123data") == "t_123data"
        assert GrowthConnector._sanitize_table_name("normal_name") == "normal_name"
