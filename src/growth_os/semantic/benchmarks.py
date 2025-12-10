"""Benchmark data and comparison helpers for SaaS growth metrics."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# SaaS benchmark data
# Source: industry reports (Andreessen Horowitz, OpenView, Bessemer, etc.)
# All values are approximate medians for B2B SaaS
# ---------------------------------------------------------------------------

BENCHMARK_DATA: dict[str, dict] = {
    "retention": {
        "month": {
            "good": 45.0,
            "ok": 30.0,
            "poor": 15.0,
            "source": "SaaS industry median (B2B)",
            "note": "Month-1 retention benchmarks vary significantly by PLG vs sales-led model.",
        },
        "week": {
            "good": 60.0,
            "ok": 40.0,
            "poor": 20.0,
            "source": "SaaS industry median (B2B)",
            "note": "Higher early retention is expected for product-led growth products.",
        },
    },
    "cac": {
        "google_ads": {
            "good": 50.0,
            "ok": 150.0,
            "note": "CAC through paid search depends heavily on ACV.",
        },
        "meta_ads": {
            "good": 40.0,
            "ok": 120.0,
            "note": "Meta Ads typically has lower CAC for consumer-facing SaaS.",
        },
        "linkedin": {
            "good": 100.0,
            "ok": 300.0,
            "note": "LinkedIn CAC is higher but often signals enterprise intent.",
        },
        "organic": {
            "good": 10.0,
            "ok": 50.0,
            "note": "Organic/SEO CAC is often understated due to attribution challenges.",
        },
    },
    "mer": {
        "good": 3.0,
        "ok": 1.5,
        "poor": 0.8,
        "source": "Blended MER benchmark for early-stage SaaS",
        "note": "MER = Total Revenue / Total Ad Spend. Healthy MER depends on LTV:CAC.",
    },
    "ltv_cac_ratio": {
        "good": 3.0,
        "ok": 1.5,
        "poor": 1.0,
        "source": "Standard SaaS benchmark",
        "note": "LTV:CAC > 3 is generally considered healthy for SaaS.",
    },
    "payback_period_months": {
        "good": 12,
        "ok": 24,
        "poor": 36,
        "note": "CAC payback period in months. Shorter is better.",
    },
}


def retention_benchmark(period: str, business_model: str = "saas") -> dict:
    """Return retention benchmark metadata for the given period.

    Returns a dict with: value (float), good, ok, poor, source, note.
    """
    data = BENCHMARK_DATA["retention"].get(period)
    if data is None:
        return {"value": 30.0, "note": f"No specific benchmark for period='{period}'"}
    return {
        "value": data["good"],
        **data,
    }


def cac_benchmark(channel: str) -> dict | None:
    """Return CAC benchmark for a known channel, or None if unknown."""
    channel_clean = channel.lower().replace(" ", "_").replace("-", "_")
    for key in BENCHMARK_DATA["cac"]:
        if key in channel_clean or channel_clean in key:
            return BENCHMARK_DATA["cac"][key]
    return None


def mer_benchmark() -> dict:
    """Return the blended MER benchmark."""
    return BENCHMARK_DATA["mer"]


def ltv_cac_benchmark() -> dict:
    """Return the LTV:CAC ratio benchmark."""
    return BENCHMARK_DATA["ltv_cac_ratio"]


def classify_metric(value: float, good: float, ok: float, higher_is_better: bool = True) -> str:
    """Classify a metric value against good/ok thresholds.

    Returns 'Good', 'Average', or 'Poor'.
    """
    if higher_is_better:
        if value >= good:
            return "Good"
        elif value >= ok:
            return "Average"
        else:
            return "Poor"
    else:
        if value <= good:
            return "Good"
        elif value <= ok:
            return "Average"
        else:
            return "Poor"
