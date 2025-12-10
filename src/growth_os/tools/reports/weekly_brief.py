"""Generate a weekly growth review brief."""

from __future__ import annotations

from growth_os.services import ReportingService


def weekly_growth_review(service: ReportingService, spend_table: str, events_table: str) -> str:
    """Produce a concise weekly growth review with channel performance, top wins, and next actions."""
    return service.weekly_growth_review(spend_table, events_table)
