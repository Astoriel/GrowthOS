"""Tests for marketing analytics SQL templates."""

from __future__ import annotations

from growth_os.core import metrics as sql_templates


class TestSQLTemplates:
    """Test that SQL templates execute correctly against mock data."""

    def test_cac_by_channel(self, connector):
        """CAC query runs and returns channel data."""
        sql = sql_templates.cac_by_channel("marketing_spend", "user_events")
        results = connector.query(sql)
        assert len(results) > 0
        channels = [r["channel"] for r in results]
        assert any(ch in channels for ch in ["google_ads", "meta_ads"])

    def test_ltv_by_channel(self, connector):
        """LTV query runs and returns revenue data."""
        sql = sql_templates.ltv_by_channel("user_events")
        results = connector.query(sql)
        assert len(results) > 0
        assert "avg_ltv" in results[0]

    def test_cohort_retention(self, connector):
        """Cohort retention query runs."""
        sql = sql_templates.cohort_retention("user_events", "month")
        results = connector.query(sql)
        assert len(results) > 0
        assert "retention_pct" in results[0]

    def test_funnel_conversion(self, connector):
        """Funnel query runs with given steps."""
        sql = sql_templates.funnel_conversion(
            "user_events",
            ["signup", "activation", "purchase"],
        )
        results = connector.query(sql)
        assert len(results) == 3
        # Funnel should decrease
        users = [r["users"] for r in results]
        assert users[0] >= users[1] >= users[2]

    def test_channel_attribution(self, connector):
        """Attribution query runs."""
        sql = sql_templates.channel_attribution("marketing_spend", "user_events")
        results = connector.query(sql)
        assert len(results) > 0
        assert "roas" in results[0]

    def test_churn_analysis(self, connector):
        """Churn segmentation query runs."""
        sql = sql_templates.churn_analysis("user_events", 30)
        results = connector.query(sql)
        assert len(results) > 0
        segments = [r["segment"] for r in results]
        # Should have at least one segment
        assert len(segments) >= 1

    def test_growth_summary(self, connector):
        """Growth summary compares periods."""
        sql = sql_templates.growth_summary("marketing_spend", "user_events")
        results = connector.query(sql)
        assert len(results) > 0
        metrics = [r["metric"] for r in results]
        assert "Revenue" in metrics
        assert "Spend" in metrics
