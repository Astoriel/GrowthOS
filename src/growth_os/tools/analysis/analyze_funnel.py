"""Analyze funnel conversion rates."""

from __future__ import annotations

from growth_os.services import AnalysisService


def analyze_funnel(service: AnalysisService, events_table: str, steps: str, date_from: str = "", date_to: str = "") -> str:
    """Analyze funnel conversion rates and identify the biggest drop-off step."""
    return service.analyze_funnel(events_table, steps, date_from, date_to)
