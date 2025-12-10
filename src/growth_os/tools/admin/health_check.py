"""Server health check."""

from __future__ import annotations


def health_check() -> str:
    """Return server status, connector summary, loaded tables, and current date."""
    from growth_os.app.registry import health_check_tool
    return health_check_tool()
