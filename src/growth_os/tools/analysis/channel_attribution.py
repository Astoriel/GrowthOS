"""Analyze channel attribution and revenue contribution."""

from __future__ import annotations

from growth_os.services import AnalysisService


def channel_attribution(service: AnalysisService, spend_table: str, events_table: str) -> str:
    """Compute attributed revenue and ROAS per marketing channel."""
    return service.channel_attribution(spend_table, events_table)
