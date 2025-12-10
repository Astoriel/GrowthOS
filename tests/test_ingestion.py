"""Tests for the ingestion layer — freshness, validators, mapping, loaders."""

from __future__ import annotations

import pytest

from growth_os.connectors.duckdb import GrowthConnector, reset_connector
from growth_os.ingestion.freshness import compute_freshness
from growth_os.ingestion.loaders import SourceRegistry
from growth_os.ingestion.mapping import apply_contract_aliases
from growth_os.ingestion.validators import validate_all_contracts


@pytest.fixture
def fresh_connector(tmp_path):
    """Connector with current-date data for freshness tests."""
    from growth_os.demo.sample_generator import generate_campaigns, generate_marketing_spend, generate_user_events

    reset_connector()
    generate_marketing_spend(days=7, output_path=str(tmp_path / "marketing_spend.csv"))
    generate_user_events(days=7, total_users=50, output_path=str(tmp_path / "user_events.csv"))
    generate_campaigns(output_path=str(tmp_path / "campaigns.csv"))
    conn = GrowthConnector(data_dir=str(tmp_path))
    yield conn
    reset_connector()


class TestFreshness:
    def test_returns_list(self, fresh_connector):
        reports = compute_freshness(fresh_connector)
        assert isinstance(reports, list)
        assert len(reports) > 0

    def test_marketing_spend_is_fresh(self, fresh_connector):
        reports = compute_freshness(fresh_connector)
        spend_report = next((r for r in reports if r.table_name == "marketing_spend"), None)
        assert spend_report is not None
        assert spend_report.status in ("fresh", "stale")

    def test_report_has_date_range(self, fresh_connector):
        reports = compute_freshness(fresh_connector)
        for report in reports:
            assert report.min_date is not None or report.days_stale is None
            assert report.date_column != ""

    def test_table_with_no_date_column_excluded(self, fresh_connector):
        """Tables without any date column candidate should be excluded."""
        fresh_connector.db.execute("CREATE TABLE nodates (id INT, val TEXT)")
        fresh_connector._tables.append("nodates")
        reports = compute_freshness(fresh_connector)
        assert not any(r.table_name == "nodates" for r in reports)


class TestValidators:
    def test_valid_dataset_has_no_errors(self, fresh_connector):
        result = validate_all_contracts(fresh_connector)
        errors = [i for i in result.issues if i.severity == "error"]
        assert errors == [], f"Unexpected errors: {errors}"

    def test_missing_table_raises_error(self, fresh_connector):
        fresh_connector.db.execute("DROP TABLE IF EXISTS campaigns")
        result = validate_all_contracts(fresh_connector)
        campaigns_errors = [i for i in result.issues if i.table_name == "campaigns" and i.severity == "error"]
        assert len(campaigns_errors) > 0

    def test_missing_required_column_raises_error(self, fresh_connector):
        fresh_connector.db.execute("ALTER TABLE marketing_spend DROP COLUMN spend")
        result = validate_all_contracts(fresh_connector)
        spend_errors = [i for i in result.issues if i.table_name == "marketing_spend" and i.severity == "error"]
        assert any("spend" in issue.message for issue in spend_errors)

    def test_freshness_included_in_result(self, fresh_connector):
        result = validate_all_contracts(fresh_connector)
        assert len(result.freshness) > 0


class TestAliasMapping:
    def test_alias_column_renamed_to_canonical(self, tmp_path):
        """If `cost` is present but `spend` is missing, it should be renamed."""
        from growth_os.demo.sample_generator import generate_marketing_spend

        reset_connector()
        generate_marketing_spend(days=7, output_path=str(tmp_path / "marketing_spend.csv"))
        conn = GrowthConnector(data_dir=str(tmp_path))

        # Rename 'spend' → 'cost' to simulate alias scenario
        conn.db.execute("ALTER TABLE marketing_spend RENAME COLUMN spend TO cost")

        remappings = apply_contract_aliases(conn)
        assert any("cost" in r and "spend" in r for r in remappings), f"No remapping applied: {remappings}"

        # After mapping, 'spend' column should exist again
        cols_result = conn.db.execute("DESCRIBE marketing_spend").fetchall()
        col_names = [r[0] for r in cols_result]
        assert "spend" in col_names
        reset_connector()

    def test_no_remapping_when_canonical_present(self, fresh_connector):
        """No remapping should happen when canonical columns are already present."""
        remappings = apply_contract_aliases(fresh_connector)
        assert remappings == []


class TestSourceRegistry:
    def test_register_and_load_csv(self, tmp_path):
        from growth_os.demo.sample_generator import generate_marketing_spend

        reset_connector()
        generate_marketing_spend(days=7, output_path=str(tmp_path / "marketing_spend.csv"))
        conn = GrowthConnector()  # empty connector

        registry = SourceRegistry()
        registry.register_csv(str(tmp_path))
        results = registry.load_all(conn)

        assert "marketing_spend" in results["csv"]
        tables = conn.get_tables()
        assert "marketing_spend" in tables
        reset_connector()

    def test_register_same_csv_twice(self, tmp_path):
        registry = SourceRegistry()
        registry.register_csv(str(tmp_path))
        registry.register_csv(str(tmp_path))
        assert len(registry.csv_dirs) == 1

    def test_from_settings_empty(self):
        registry = SourceRegistry.from_settings()
        # With default settings (no data dir set), registry should be empty or contain demo dir
        assert isinstance(registry, SourceRegistry)
