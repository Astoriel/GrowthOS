"""Tests for workflow-level composite analysis methods in AnalysisService."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from growth_os.domain.models import ToolEnvelope
from growth_os.services.analysis_service import AnalysisService


class TestFunnelDiagnosis:
    STEPS = "signup,activation,purchase"

    def test_returns_tool_envelope(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_diagnosis("user_events", self.STEPS)
        assert isinstance(result, ToolEnvelope)

    def test_title_is_funnel_diagnosis(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_diagnosis("user_events", self.STEPS)
        assert result.title == "Funnel Diagnosis"

    def test_body_contains_funnel_conversion_section(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_diagnosis("user_events", self.STEPS)
        assert "Funnel Conversion" in result.body

    def test_body_contains_churn_signals_section(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_diagnosis("user_events", self.STEPS)
        assert "Churn Signals" in result.body

    def test_accepts_date_range_arguments(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_diagnosis(
            "user_events", self.STEPS,
            date_from="2020-01-01", date_to="2030-12-31",
        )
        assert isinstance(result, ToolEnvelope)
        assert result.title == "Funnel Diagnosis"

    def test_source_table_referenced_in_envelope(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_diagnosis("user_events", self.STEPS)
        assert "user_events" in result.sources

    def test_empty_funnel_data_returns_graceful_message(self, connector):
        service = AnalysisService(connector)
        with patch.object(connector, "query", return_value=[]):
            result = service.funnel_diagnosis("user_events", self.STEPS)
        assert isinstance(result, ToolEnvelope)
        assert result.title == "Funnel Diagnosis"
        assert "No funnel data found" in result.body

    def test_single_step_funnel_does_not_crash(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_diagnosis("user_events", "signup")
        assert isinstance(result, ToolEnvelope)


class TestChannelEfficiencyReview:
    def test_returns_tool_envelope(self, connector):
        service = AnalysisService(connector)
        result = service.channel_efficiency_review("marketing_spend", "user_events")
        assert isinstance(result, ToolEnvelope)

    def test_title_is_channel_efficiency_review(self, connector):
        service = AnalysisService(connector)
        result = service.channel_efficiency_review("marketing_spend", "user_events")
        assert result.title == "Channel Efficiency Review"

    def test_body_contains_section_header(self, connector):
        service = AnalysisService(connector)
        result = service.channel_efficiency_review("marketing_spend", "user_events")
        assert "Channel Efficiency Review" in result.body

    def test_body_contains_signal_column(self, connector):
        service = AnalysisService(connector)
        result = service.channel_efficiency_review("marketing_spend", "user_events")
        assert "Signal" in result.body

    def test_sources_reference_both_tables(self, connector):
        service = AnalysisService(connector)
        result = service.channel_efficiency_review("marketing_spend", "user_events")
        assert "marketing_spend" in result.sources
        assert "user_events" in result.sources

    def test_body_contains_roas_column(self, connector):
        service = AnalysisService(connector)
        result = service.channel_efficiency_review("marketing_spend", "user_events")
        assert "ROAS" in result.body

    def test_empty_cac_results_returns_valid_envelope(self, connector):
        service = AnalysisService(connector)
        with patch.object(connector, "query", return_value=[]):
            result = service.channel_efficiency_review("marketing_spend", "user_events")
        assert isinstance(result, ToolEnvelope)
        assert result.title == "Channel Efficiency Review"


class TestAnomalyExplanation:
    def test_returns_tool_envelope(self, connector):
        service = AnalysisService(connector)
        result = service.anomaly_explanation("marketing_spend", "spend", "date", lookback_days=30)
        assert isinstance(result, ToolEnvelope)

    def test_title_is_anomaly_explanation(self, connector):
        service = AnalysisService(connector)
        result = service.anomaly_explanation("marketing_spend", "spend", "date", lookback_days=30)
        assert result.title == "Anomaly Explanation"

    def test_source_table_referenced_in_envelope(self, connector):
        service = AnalysisService(connector)
        result = service.anomaly_explanation("marketing_spend", "spend", "date", lookback_days=30)
        assert "marketing_spend" in result.sources

    def test_no_anomalies_message_when_query_returns_empty(self, connector):
        service = AnalysisService(connector)
        with patch.object(connector, "query", return_value=[]):
            result = service.anomaly_explanation("marketing_spend", "spend", "date", lookback_days=30)
        assert result.title == "Anomaly Explanation"
        assert "No anomalies" in result.body

    def test_hypotheses_section_present_when_spike_detected(self, connector):
        fake_spike = [
            {
                "date": "2024-03-01",
                "daily_value": 9999.0,
                "avg_value": 100.0,
                "z_score": 3.5,
                "status": "🔴 Spike",
            }
        ]
        service = AnalysisService(connector)
        with patch.object(connector, "query", return_value=fake_spike):
            result = service.anomaly_explanation("marketing_spend", "spend", "date", lookback_days=30)
        assert result.title == "Anomaly Explanation"
        assert "spike" in result.body.lower()

    def test_hypotheses_section_present_when_drop_detected(self, connector):
        fake_drop = [
            {
                "date": "2024-03-05",
                "daily_value": 1.0,
                "avg_value": 500.0,
                "z_score": -3.1,
                "status": "🔴 Drop",
            }
        ]
        service = AnalysisService(connector)
        with patch.object(connector, "query", return_value=fake_drop):
            result = service.anomaly_explanation("marketing_spend", "spend", "date", lookback_days=30)
        assert result.title == "Anomaly Explanation"
        assert "drop" in result.body.lower()

    def test_spike_and_drop_hypotheses_both_appear(self, connector):
        mixed_anomalies = [
            {"date": "2024-03-01", "daily_value": 9000.0, "avg_value": 100.0, "z_score": 3.5, "status": "🔴 Spike"},
            {"date": "2024-03-10", "daily_value": 1.0, "avg_value": 100.0, "z_score": -3.1, "status": "🔴 Drop"},
        ]
        service = AnalysisService(connector)
        with patch.object(connector, "query", return_value=mixed_anomalies):
            result = service.anomaly_explanation("marketing_spend", "spend", "date", lookback_days=30)
        assert "spike" in result.body.lower()
        assert "drop" in result.body.lower()


class TestDetectDataDrift:
    def test_returns_tool_envelope(self, connector):
        service = AnalysisService(connector)
        result = service.detect_data_drift("marketing_spend", "spend", "date", lookback_days=7)
        assert isinstance(result, ToolEnvelope)

    def test_title_is_data_drift(self, connector):
        service = AnalysisService(connector)
        result = service.detect_data_drift("marketing_spend", "spend", "date", lookback_days=7)
        assert result.title == "Data Drift"

    def test_body_contains_data_drift_header(self, connector):
        service = AnalysisService(connector)
        result = service.detect_data_drift("marketing_spend", "spend", "date", lookback_days=7)
        assert "Data Drift" in result.body

    def test_source_table_referenced_in_envelope(self, connector):
        service = AnalysisService(connector)
        result = service.detect_data_drift("marketing_spend", "spend", "date", lookback_days=7)
        assert "marketing_spend" in result.sources

    def test_handles_empty_query_results_gracefully(self, connector):
        service = AnalysisService(connector)
        with patch.object(connector, "query", return_value=[]):
            result = service.detect_data_drift("marketing_spend", "spend", "date", lookback_days=7)
        assert isinstance(result, ToolEnvelope)
        assert result.title == "Data Drift"
        assert "Not enough data" in result.body

    def test_handles_single_row_result_as_insufficient_data(self, connector):
        single_row = [
            {"period": "current", "avg_val": 42.0, "total_val": 294.0, "n_rows": 7}
        ]
        service = AnalysisService(connector)
        with patch.object(connector, "query", return_value=single_row):
            result = service.detect_data_drift("marketing_spend", "spend", "date", lookback_days=7)
        assert isinstance(result, ToolEnvelope)
        assert result.title == "Data Drift"
        assert "Not enough data" in result.body

    def test_two_period_results_produce_comparison_body(self, connector):
        two_rows = [
            {"period": "current", "avg_val": 120.0, "total_val": 840.0, "n_rows": 7},
            {"period": "previous", "avg_val": 100.0, "total_val": 700.0, "n_rows": 7},
        ]
        service = AnalysisService(connector)
        with patch.object(connector, "query", return_value=two_rows):
            result = service.detect_data_drift("marketing_spend", "spend", "date", lookback_days=7)
        assert result.title == "Data Drift"
        assert "Data Drift" in result.body


class TestFunnelABComparison:
    STEPS = "signup,activation,purchase"

    def test_returns_tool_envelope(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_ab_comparison("user_events", self.STEPS)
        assert isinstance(result, ToolEnvelope)

    def test_title_is_funnel_ab_comparison(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_ab_comparison("user_events", self.STEPS)
        assert result.title == "Funnel A/B Comparison"

    def test_default_period_labels_appear_in_body(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_ab_comparison("user_events", self.STEPS)
        assert "Period A" in result.body
        assert "Period B" in result.body

    def test_custom_period_labels_appear_in_body(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_ab_comparison(
            "user_events", self.STEPS,
            period_a_label="Before", period_b_label="After",
        )
        assert "Before" in result.body
        assert "After" in result.body

    def test_body_contains_delta_column(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_ab_comparison("user_events", self.STEPS)
        assert "(pp)" in result.body

    def test_source_table_referenced_in_envelope(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_ab_comparison("user_events", self.STEPS)
        assert "user_events" in result.sources

    def test_empty_data_returns_graceful_no_data_message(self, connector):
        service = AnalysisService(connector)
        with patch.object(connector, "query", return_value=[]):
            result = service.funnel_ab_comparison("user_events", self.STEPS)
        assert isinstance(result, ToolEnvelope)
        assert result.title == "Funnel A/B Comparison"
        assert "No funnel data" in result.body

    def test_step_column_present_in_output(self, connector):
        service = AnalysisService(connector)
        result = service.funnel_ab_comparison("user_events", self.STEPS)
        assert "Step" in result.body
