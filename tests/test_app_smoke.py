"""Smoke tests for the new app architecture."""

from __future__ import annotations

from growth_os.app.server import create_mcp_server
from growth_os.config import settings
from growth_os.connectors import reset_connector
from growth_os.demo.sample_generator import generate_all_sample_data
from growth_os.server import validate_data, weekly_growth_review


def test_create_mcp_server():
    """The MCP server factory should build a FastMCP instance."""
    mcp = create_mcp_server()
    assert mcp is not None


def test_workflow_tools_smoke(tmp_path):
    """Workflow and validation tools should render output."""
    generate_all_sample_data(str(tmp_path))
    settings.growth_data_dir = str(tmp_path)
    reset_connector()

    validation = validate_data()
    weekly = weekly_growth_review()

    assert "Validation" in validation or "Dataset status" in validation
    assert "Weekly Growth Review" in weekly
    assert "Trust" in weekly

    reset_connector()
    settings.growth_data_dir = ""
