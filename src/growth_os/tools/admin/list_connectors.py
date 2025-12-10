"""List configured data connectors and their status."""

from __future__ import annotations

from growth_os.services import DiagnosticsService


def list_connectors(service: DiagnosticsService) -> str:
    """List all configured data connectors with their status and loaded table counts."""
    return service.list_connectors()
