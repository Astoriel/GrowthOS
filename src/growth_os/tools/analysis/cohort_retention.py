"""Analyze cohort retention over time."""

from __future__ import annotations

from growth_os.services import AnalysisService


def cohort_retention(service: AnalysisService, events_table: str, period: str = "month") -> str:
    """Analyze cohort retention by week or month and compare to SaaS benchmarks."""
    return service.cohort_retention(events_table, period)
