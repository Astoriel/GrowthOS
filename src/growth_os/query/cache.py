"""Planned query cache scaffold.

Caching is not enabled in the current runtime. This module preserves the
extension point while making the feature status explicit.
"""

from __future__ import annotations


def cache_enabled() -> bool:
    """Return whether query caching is enabled."""
    return False
