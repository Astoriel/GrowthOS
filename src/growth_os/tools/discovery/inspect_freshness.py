"""Inspect data freshness across all tables."""

from __future__ import annotations

from growth_os.services import DiagnosticsService


def inspect_freshness(service: DiagnosticsService) -> str:
    """Check data freshness for all loaded tables and flag stale sources."""
    return service.inspect_freshness()
