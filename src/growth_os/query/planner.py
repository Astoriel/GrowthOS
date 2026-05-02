"""Planned query planner scaffold.

The current implementation runs direct DuckDB/Postgres-style queries. This
module names the active planner and reserves space for later planning logic.
"""

from __future__ import annotations


def planner_name() -> str:
    """Return the active planner label."""
    return "direct-duckdb"
