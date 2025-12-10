"""Domain enums for GrowthOS."""

from __future__ import annotations

from enum import Enum


class FreshnessStatus(str, Enum):
    """Data freshness classification."""

    FRESH = "fresh"
    STALE = "stale"
    OUTDATED = "outdated"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    """Issue severity level."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class ChurnMode(str, Enum):
    """Churn analysis mode."""

    INACTIVITY = "inactivity"
    EVENT_BASED = "event_based"
    SUBSCRIPTION = "subscription"


class AttributionModel(str, Enum):
    """Attribution model type."""

    SOURCE_AND_CAMPAIGN = "source_and_campaign"
    LAST_TOUCH = "last_touch"
    FIRST_TOUCH = "first_touch"
