"""Analyze user churn and at-risk segments."""

from __future__ import annotations

from growth_os.services import AnalysisService


def analyze_churn(service: AnalysisService, events_table: str, inactive_days: int = 30) -> str:
    """Segment users by churn status and identify at-risk users by inactivity."""
    return service.analyze_churn(events_table, inactive_days)
