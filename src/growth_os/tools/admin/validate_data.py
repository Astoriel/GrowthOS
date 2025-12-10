"""Validate loaded data against GrowthOS schema contracts."""

from __future__ import annotations

from growth_os.services import DiagnosticsService


def validate_data(service: DiagnosticsService) -> str:
    """Validate all loaded tables against GrowthOS schema contracts and report issues."""
    return service.validate_data()
