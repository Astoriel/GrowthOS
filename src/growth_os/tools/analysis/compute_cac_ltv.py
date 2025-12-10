"""Compute CAC and LTV by channel."""

from __future__ import annotations

from growth_os.services import AnalysisService


def compute_cac_ltv(service: AnalysisService, spend_table: str, events_table: str) -> str:
    """Calculate Customer Acquisition Cost and Lifetime Value by marketing channel."""
    return service.compute_cac_ltv(spend_table, events_table)
