"""Generate an executive-level growth summary."""

from __future__ import annotations

from growth_os.services import ReportingService


def executive_summary(service: ReportingService, spend_table: str, events_table: str) -> str:
    """Produce a board-level executive summary with key growth KPIs and strategic recommendations."""
    return service.executive_summary(spend_table, events_table)
