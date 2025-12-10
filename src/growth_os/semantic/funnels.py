"""Funnel semantic helpers and step definitions."""

from __future__ import annotations

DEFAULT_FUNNEL_STEPS = ("signup", "activation", "purchase")

SAAS_FUNNEL_STEPS = ("signup", "activation", "trial_start", "purchase")

ECOMMERCE_FUNNEL_STEPS = ("visit", "add_to_cart", "checkout", "purchase")

PLG_FUNNEL_STEPS = ("signup", "activation", "feature_adoption", "expansion")

_STEP_DESCRIPTIONS: dict[str, str] = {
    "signup": "User creates an account",
    "activation": "User completes initial setup or key action",
    "trial_start": "User starts a paid trial",
    "purchase": "User makes a purchase or subscribes",
    "add_to_cart": "User adds a product to cart",
    "checkout": "User begins the checkout process",
    "visit": "User visits the product page",
    "feature_adoption": "User activates a core feature",
    "expansion": "User upgrades or expands usage",
    "onboarding_complete": "User completes the onboarding flow",
    "referral": "User refers another user",
    "renewal": "User renews their subscription",
}


def funnel_step_description(step: str) -> str:
    """Return a human-readable description for a funnel step."""
    return _STEP_DESCRIPTIONS.get(step.lower().strip(), step.replace("_", " ").title())


def parse_funnel_steps(steps_str: str) -> list[str]:
    """Parse a comma-separated funnel steps string into a clean list."""
    return [s.strip() for s in steps_str.split(",") if s.strip()]
