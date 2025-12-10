"""Tests that bundled demo data stays current."""

from __future__ import annotations

from datetime import UTC, datetime

from growth_os.connectors import GrowthConnector
from growth_os.demo.sample_generator import generate_all_sample_data, generate_marketing_spend
from growth_os.ingestion import inspect_freshness
from growth_os.semantic.metrics import growth_summary


def test_marketing_spend_defaults_to_current_range():
    """Default demo spend data should end on the current day."""
    rows = generate_marketing_spend(days=14)
    max_date = max(row["date"] for row in rows)
    assert max_date == datetime.now(UTC).strftime("%Y-%m-%d")


def test_demo_data_produces_live_summary(tmp_path):
    """Generated demo data should feed non-empty recent summaries."""
    generate_all_sample_data(str(tmp_path))
    connector = GrowthConnector(data_dir=str(tmp_path))
    summary = connector.query(growth_summary("marketing_spend", "user_events"))
    assert any((row.get("current_value") or 0) > 0 for row in summary)


def test_demo_data_is_not_outdated(tmp_path):
    """Freshness checks on bundled demo data should not report outdated spend data."""
    generate_all_sample_data(str(tmp_path))
    connector = GrowthConnector(data_dir=str(tmp_path))
    reports = inspect_freshness(connector)
    spend_report = next(report for report in reports if report.table_name == "marketing_spend")
    assert spend_report.status in {"fresh", "stale"}
