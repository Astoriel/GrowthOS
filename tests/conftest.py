"""Test fixtures for GrowthOS tests."""

from __future__ import annotations

import pytest

from growth_os.core.connector import GrowthConnector, reset_connector
from growth_os.data.mock_generator import (
    generate_marketing_spend,
    generate_user_events,
    generate_campaigns,
)


@pytest.fixture
def connector(tmp_path):
    """Create a fresh DuckDB connector with mock data loaded."""
    reset_connector()

    # Generate CSVs to temp dir
    spend_path = tmp_path / "marketing_spend.csv"
    events_path = tmp_path / "user_events.csv"
    campaigns_path = tmp_path / "campaigns.csv"

    generate_marketing_spend(days=90, output_path=str(spend_path))
    generate_user_events(days=90, total_users=500, output_path=str(events_path))
    generate_campaigns(output_path=str(campaigns_path))

    conn = GrowthConnector(data_dir=str(tmp_path))
    yield conn
    reset_connector()


@pytest.fixture
def empty_connector():
    """Create a connector with no data."""
    reset_connector()
    conn = GrowthConnector()
    yield conn
    reset_connector()
