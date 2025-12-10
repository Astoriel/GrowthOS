"""Demo scenarios for GrowthOS walkthroughs and testing."""

from __future__ import annotations

DEMO_SCENARIOS: list[dict] = [
    {
        "name": "high_cac_week",
        "title": "High CAC Alert Week",
        "description": (
            "A week where paid CAC spiked above $200 on Google Ads while organic held steady. "
            "LTV:CAC dropped below 1.5 for the first time in the quarter."
        ),
        "prompt_example": "Why did our CAC spike last week and is it sustainable?",
        "tables_required": ["marketing_spend", "user_events"],
        "suggested_tools": ["compute_cac_ltv", "channel_efficiency_review"],
        "tags": ["cac", "paid", "google-ads"],
    },
    {
        "name": "funnel_drop_at_activation",
        "title": "Funnel Drop at Activation Step",
        "description": (
            "Signup→Activation conversion fell from 62% to 38% following a product change. "
            "Activation is defined as completing the first key action within 7 days of signup."
        ),
        "prompt_example": "Where is the funnel breaking and what should we test first?",
        "tables_required": ["user_events"],
        "suggested_tools": ["analyze_funnel", "funnel_diagnosis", "analyze_churn"],
        "tags": ["funnel", "activation", "onboarding"],
    },
    {
        "name": "churn_risk_rising",
        "title": "Rising Churn Risk",
        "description": (
            "Month-1 retention dropped to 28% (below the 45% B2B SaaS median). "
            "At-risk users (inactive 14-30 days) grew 40% MoM."
        ),
        "prompt_example": "How bad is our churn problem and what can we do about it?",
        "tables_required": ["user_events"],
        "suggested_tools": ["cohort_retention", "analyze_churn", "funnel_diagnosis"],
        "tags": ["churn", "retention", "at-risk"],
    },
    {
        "name": "anomaly_spend_spike",
        "title": "Unexplained Spend Spike",
        "description": (
            "Total marketing spend jumped 3× on a single Tuesday with no corresponding revenue lift. "
            "The anomaly appears isolated to Meta Ads."
        ),
        "prompt_example": "We had a weird spend spike on Tuesday — what happened?",
        "tables_required": ["marketing_spend"],
        "suggested_tools": ["detect_anomalies", "anomaly_explanation", "channel_attribution"],
        "tags": ["anomaly", "spend", "meta-ads"],
    },
]

_SCENARIO_INDEX: dict[str, dict] = {s["name"]: s for s in DEMO_SCENARIOS}


def get_scenario(name: str) -> dict | None:
    """Return a demo scenario by name, or None if not found."""
    return _SCENARIO_INDEX.get(name)


def list_scenario_names() -> list[str]:
    """Return all available demo scenario names."""
    return list(_SCENARIO_INDEX.keys())
