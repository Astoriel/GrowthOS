"""GrowthOS MCP server factory and entrypoint."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from growth_os.app.lifespan import ensure_sample_data
from growth_os.app.registry import register_tools
from growth_os.config.settings import settings
from growth_os.observability import configure_logging


def create_mcp_server() -> FastMCP:
    """Create and register the GrowthOS MCP server."""
    configure_logging()
    mcp = FastMCP(
        settings.server_name,
        instructions=(
            "GrowthOS is an AI-native growth analytics MCP server. "
            "Use it to inspect schemas, analyze funnels, compute CAC/LTV, "
            "review attribution, detect anomalies, validate data quality, "
            "and build weekly growth summaries."
        ),
    )
    register_tools(mcp)
    return mcp


mcp = create_mcp_server()


def main() -> None:
    """Application entrypoint."""
    ensure_sample_data()
    mcp.run(settings.default_transport)
