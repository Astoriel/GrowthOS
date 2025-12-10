"""Supported schema contracts."""

from __future__ import annotations

from dataclasses import dataclass, field


DATE_COLUMN_CANDIDATES = (
    "date",
    "event_date",
    "created_at",
    "signup_date",
    "purchase_date",
)


@dataclass(frozen=True, slots=True)
class TableContract:
    """Canonical table contract used for validation."""

    name: str
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()
    aliases: dict[str, tuple[str, ...]] = field(default_factory=dict)


CONTRACT_SPECS: dict[str, TableContract] = {
    "marketing_spend": TableContract(
        name="marketing_spend",
        required_columns=("date", "channel", "spend"),
        optional_columns=("campaign", "impressions", "clicks", "conversions"),
        aliases={
            "date": ("day", "spend_date"),
            "channel": ("utm_source", "source"),
            "spend": ("cost", "ad_spend"),
        },
    ),
    "user_events": TableContract(
        name="user_events",
        required_columns=("user_id", "event_type", "event_date"),
        optional_columns=("utm_source", "revenue"),
        aliases={
            "event_date": ("date", "occurred_at"),
            "event_type": ("event", "type"),
            "utm_source": ("channel", "source"),
        },
    ),
    "campaigns": TableContract(
        name="campaigns",
        required_columns=("campaign_id", "name", "channel"),
        optional_columns=("start_date", "monthly_budget", "status"),
        aliases={
            "campaign_id": ("id",),
            "monthly_budget": ("budget",),
        },
    ),
}
