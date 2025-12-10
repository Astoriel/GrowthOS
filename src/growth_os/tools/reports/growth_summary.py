"""Generate a growth summary report."""

from __future__ import annotations

from growth_os.services import ReportingService


def growth_summary(service: ReportingService, spend_table: str, events_table: str) -> str:
    """Generate a holistic growth summary covering key metrics across acquisition, retention, and revenue."""
    return service.growth_summary(spend_table, events_table)
