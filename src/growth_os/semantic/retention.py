"""Retention semantic helpers and thresholds."""

from __future__ import annotations

DEFAULT_RETENTION_PERIOD = "month"

RETENTION_THRESHOLDS: dict[str, dict[str, float]] = {
    "month": {
        "good": 45.0,
        "average": 30.0,
        "poor": 15.0,
    },
    "week": {
        "good": 60.0,
        "average": 40.0,
        "poor": 20.0,
    },
}


def classify_retention(pct: float, period: str = "month") -> str:
    """Classify a retention percentage as Good, Average, or Poor.

    Uses SaaS B2B benchmarks.
    """
    thresholds = RETENTION_THRESHOLDS.get(period, RETENTION_THRESHOLDS["month"])
    if pct >= thresholds["good"]:
        return "Good"
    elif pct >= thresholds["average"]:
        return "Average"
    else:
        return "Poor"


def retention_label(period: str) -> str:
    """Return a human-readable retention period label."""
    labels = {"week": "Weekly", "month": "Monthly"}
    return labels.get(period, period.capitalize())
