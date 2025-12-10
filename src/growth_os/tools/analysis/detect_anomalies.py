"""Detect metric anomalies (spikes and drops)."""

from __future__ import annotations

from growth_os.services import AnalysisService


def detect_anomalies(service: AnalysisService, table: str, metric_column: str, date_column: str = "date", lookback_days: int = 30) -> str:
    """Detect statistical anomalies (spikes and drops) in a metric time series."""
    return service.detect_anomalies(table, metric_column, date_column, lookback_days)
