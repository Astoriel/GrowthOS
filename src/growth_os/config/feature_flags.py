"""Feature flags used by the app."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class FeatureFlags:
    """Simple runtime feature toggles."""

    business_mode: bool = False
    trust_footer: bool = True
    workflow_tools: bool = True
