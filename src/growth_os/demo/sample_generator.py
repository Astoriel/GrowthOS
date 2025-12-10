"""Generate demo datasets relative to the current date."""

from __future__ import annotations

import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path
import random


CHANNELS = ["google_ads", "meta_ads", "linkedin", "organic", "email"]
CAMPAIGN_NAMES = {
    "google_ads": ["brand_search", "competitor_kw", "retargeting_display", "shopping_feed"],
    "meta_ads": ["lookalike_broad", "retargeting_web", "video_awareness", "lead_gen_form"],
    "linkedin": ["decision_makers", "saas_founders", "enterprise_it"],
    "organic": ["seo_blog", "referral_program"],
    "email": ["welcome_drip", "reactivation", "weekly_newsletter", "product_update"],
}


def generate_marketing_spend(
    start_date: datetime | None = None,
    days: int = 365,
    output_path: str | None = None,
) -> list[dict]:
    """Generate daily spend data ending on the current day by default."""
    rng = random.Random(42)
    start = _resolve_start_date(start_date, days)
    rows: list[dict] = []

    for day_offset in range(days):
        date = start + timedelta(days=day_offset)
        is_weekend = date.weekday() >= 5
        month_factor = 1.0 + (date.month - 1) * 0.02
        q4_boost = 1.3 if date.month >= 10 else 1.0

        for channel in CHANNELS:
            if channel == "organic":
                rows.append(
                    {
                        "date": date.strftime("%Y-%m-%d"),
                        "channel": channel,
                        "campaign": "organic_traffic",
                        "spend": 0.0,
                        "impressions": rng.randint(500, 3000),
                        "clicks": rng.randint(100, 800),
                        "conversions": rng.randint(5, 30),
                    }
                )
                continue

            campaigns = CAMPAIGN_NAMES.get(channel, ["default"])
            for campaign in campaigns:
                if rng.random() < 0.15:
                    continue

                base_spend = {
                    "google_ads": rng.uniform(80, 250),
                    "meta_ads": rng.uniform(60, 200),
                    "linkedin": rng.uniform(40, 120),
                    "email": rng.uniform(5, 20),
                }.get(channel, 50)

                weekend_factor = 0.4 if is_weekend else 1.0
                spend = round(base_spend * weekend_factor * month_factor * q4_boost, 2)
                cpc = {
                    "google_ads": rng.uniform(0.8, 2.5),
                    "meta_ads": rng.uniform(0.5, 1.8),
                    "linkedin": rng.uniform(3.0, 8.0),
                    "email": rng.uniform(0.1, 0.3),
                }.get(channel, 1.0)

                clicks = max(1, int(spend / cpc))
                impressions = clicks * rng.randint(15, 40)
                conv_rate = {
                    "google_ads": rng.uniform(0.02, 0.05),
                    "meta_ads": rng.uniform(0.01, 0.04),
                    "linkedin": rng.uniform(0.01, 0.03),
                    "email": rng.uniform(0.03, 0.08),
                }.get(channel, 0.02)
                conversions = max(0, int(clicks * conv_rate))

                rows.append(
                    {
                        "date": date.strftime("%Y-%m-%d"),
                        "channel": channel,
                        "campaign": campaign,
                        "spend": spend,
                        "impressions": impressions,
                        "clicks": clicks,
                        "conversions": conversions,
                    }
                )

    if output_path:
        _write_csv(output_path, rows)
    return rows


def generate_user_events(
    start_date: datetime | None = None,
    days: int = 365,
    total_users: int = 2000,
    output_path: str | None = None,
) -> list[dict]:
    """Generate lifecycle events ending on the current day by default."""
    rng = random.Random(42)
    start = _resolve_start_date(start_date, days)
    end = start + timedelta(days=max(days - 1, 0))
    rows: list[dict] = []
    user_id = 1000

    for day_offset in range(days):
        date = start + timedelta(days=day_offset)
        daily_signups = rng.randint(3, 10)

        for _ in range(daily_signups):
            user_id += 1
            uid = f"user_{user_id}"
            channel = rng.choices(CHANNELS, weights=[30, 25, 10, 25, 10])[0]

            rows.append(
                {
                    "user_id": uid,
                    "event_type": "signup",
                    "event_date": date.strftime("%Y-%m-%d"),
                    "utm_source": channel,
                    "revenue": 0.0,
                }
            )

            if rng.random() < 0.70:
                activation_date = date + timedelta(days=rng.randint(0, 3))
                if activation_date <= end:
                    rows.append(
                        {
                            "user_id": uid,
                            "event_type": "activation",
                            "event_date": activation_date.strftime("%Y-%m-%d"),
                            "utm_source": channel,
                            "revenue": 0.0,
                        }
                    )

                if activation_date <= end and rng.random() < 0.40:
                    purchase_date = activation_date + timedelta(days=rng.randint(1, 14))
                    revenue = {
                        "google_ads": rng.uniform(30, 120),
                        "meta_ads": rng.uniform(20, 80),
                        "linkedin": rng.uniform(80, 300),
                        "organic": rng.uniform(25, 100),
                        "email": rng.uniform(15, 60),
                    }.get(channel, 50)

                    if purchase_date <= end:
                        rows.append(
                            {
                                "user_id": uid,
                                "event_type": "purchase",
                                "event_date": purchase_date.strftime("%Y-%m-%d"),
                                "utm_source": channel,
                                "revenue": round(revenue, 2),
                            }
                        )

                    if purchase_date <= end and rng.random() < 0.30:
                        repeat_date = purchase_date + timedelta(days=rng.randint(14, 60))
                        if repeat_date <= end:
                            rows.append(
                                {
                                    "user_id": uid,
                                    "event_type": "purchase",
                                    "event_date": repeat_date.strftime("%Y-%m-%d"),
                                    "utm_source": channel,
                                    "revenue": round(revenue * rng.uniform(0.5, 1.5), 2),
                                }
                            )

            if rng.random() < 0.25:
                churn_date = date + timedelta(days=rng.randint(30, 90))
                if churn_date <= end:
                    rows.append(
                        {
                            "user_id": uid,
                            "event_type": "churn",
                            "event_date": churn_date.strftime("%Y-%m-%d"),
                            "utm_source": channel,
                            "revenue": 0.0,
                        }
                    )

            if user_id - 1000 >= total_users:
                break

        if user_id - 1000 >= total_users:
            break

    if output_path:
        _write_csv(output_path, rows)
    return rows


def generate_campaigns(output_path: str | None = None) -> list[dict]:
    """Generate campaign metadata."""
    rng = random.Random(42)
    rows: list[dict] = []
    campaign_id = 100
    start_date = (datetime.now(UTC) - timedelta(days=365)).strftime("%Y-%m-%d")

    for channel, campaigns in CAMPAIGN_NAMES.items():
        for campaign in campaigns:
            campaign_id += 1
            budget = {
                "google_ads": rng.randint(5000, 15000),
                "meta_ads": rng.randint(3000, 12000),
                "linkedin": rng.randint(2000, 8000),
                "organic": 0,
                "email": rng.randint(500, 2000),
            }.get(channel, 5000)
            rows.append(
                {
                    "campaign_id": f"camp_{campaign_id}",
                    "name": campaign,
                    "channel": channel,
                    "start_date": start_date,
                    "monthly_budget": budget,
                    "status": rng.choice(["active", "active", "active", "paused"]),
                }
            )

    if output_path:
        _write_csv(output_path, rows)
    return rows


def generate_all_sample_data(output_dir: str | None = None) -> None:
    """Generate all bundled demo data."""
    path = Path(output_dir or Path(__file__).resolve().parents[1] / "data" / "sample")
    path.mkdir(parents=True, exist_ok=True)
    generate_marketing_spend(output_path=str(path / "marketing_spend.csv"))
    generate_user_events(output_path=str(path / "user_events.csv"))
    generate_campaigns(output_path=str(path / "campaigns.csv"))


def _resolve_start_date(start_date: datetime | None, days: int) -> datetime:
    """Resolve the start date so generated data reaches today."""
    if start_date is not None:
        return start_date
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    return today - timedelta(days=max(days - 1, 0))


def _write_csv(filepath: str, rows: list[dict]) -> None:
    """Write rows to CSV."""
    if not rows:
        return
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
