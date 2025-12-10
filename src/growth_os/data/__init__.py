"""Backward-compatible data exports."""

from growth_os.demo.sample_generator import (
    generate_all_sample_data,
    generate_campaigns,
    generate_marketing_spend,
    generate_user_events,
)

__all__ = [
    "generate_all_sample_data",
    "generate_campaigns",
    "generate_marketing_spend",
    "generate_user_events",
]
