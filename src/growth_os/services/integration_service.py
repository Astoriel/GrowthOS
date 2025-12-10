"""External integration services."""

from __future__ import annotations

from growth_os.config import settings
from growth_os.connectors import GoogleAdsConnector, GrowthConnector, MetaAdsConnector, StripeConnector, reset_connector
from growth_os.presentation.markdown import format_actions, format_insight, format_table
from growth_os.query.builder import safe_identifier
from growth_os.services._helpers import build_tool_envelope


class IntegrationService:
    """Services for pulling data from external sources."""

    def __init__(self, connector: GrowthConnector):
        self.connector = connector

    def sync_stripe_billing(self, output_dir: str = "", lookback_days: int = 365):
        """Sync Stripe customers, invoices, subscriptions, and purchase-like user events."""
        target_dir = output_dir or settings.growth_data_dir or settings.sample_data_dir
        stripe = StripeConnector()
        if not stripe.configured:
            body = (
                "Stripe sync is not configured. Set `STRIPE_API_KEY` and run the tool again. "
                f"Target output directory would be `{target_dir}`."
            )
            return build_tool_envelope("Stripe Billing Sync", body, self.connector, [])

        result = stripe.sync_billing_data(target_dir, lookback_days=lookback_days)

        if target_dir == settings.growth_data_dir:
            reset_connector()

        rows = [
            {"Dataset": "stripe_customers", "Rows": result.customers},
            {"Dataset": "stripe_invoices", "Rows": result.invoices},
            {"Dataset": "stripe_subscriptions", "Rows": result.subscriptions},
            {"Dataset": "stripe_user_events", "Rows": result.user_events},
        ]
        body = format_table(rows, "Stripe Sync Output")
        body += format_insight(
            "Stripe paid invoices are also exported as `stripe_user_events.csv` for GrowthOS-compatible purchase analysis."
        )
        body += format_actions(
            [
                "Use `describe_table('stripe_invoices')` to inspect synced billing data.",
                "Use `run_query` to join `stripe_invoices` with other sources.",
                "Use `growth_summary(events_table='stripe_user_events')` for a revenue-only view if needed.",
            ]
        )
        return build_tool_envelope("Stripe Billing Sync", body, self.connector, ["stripe_invoices", "stripe_subscriptions", "stripe_customers"])

    def sync_meta_ads(self, output_dir: str = "", lookback_days: int = 90):
        """Sync Meta Ads campaigns and daily campaign insights."""
        target_dir = output_dir or settings.growth_data_dir or settings.sample_data_dir
        meta = MetaAdsConnector()
        if not meta.configured:
            body = (
                "Meta Ads sync is not configured. Set `META_ACCESS_TOKEN` and `META_AD_ACCOUNT_ID` "
                f"and run the tool again. Target output directory would be `{target_dir}`."
            )
            return build_tool_envelope("Meta Ads Sync", body, self.connector, [])

        result = meta.sync_ads_data(target_dir, lookback_days=lookback_days)
        if target_dir == settings.growth_data_dir:
            reset_connector()

        rows = [
            {"Dataset": "meta_campaigns", "Rows": result.campaigns},
            {"Dataset": "meta_marketing_spend", "Rows": result.spend_rows},
        ]
        body = format_table(rows, "Meta Ads Sync Output")
        body += format_insight(
            "Meta Ads daily campaign insights are exported as `meta_marketing_spend.csv` for GrowthOS spend analysis."
        )
        body += format_actions(
            [
                "Use `describe_table('meta_marketing_spend')` to inspect synced spend data.",
                "Use `channel_attribution` or `compute_cac_ltv` with a matching events table when available.",
                "Use `meta_ads_summary` for a quick acquisition-side overview.",
            ]
        )
        return build_tool_envelope("Meta Ads Sync", body, self.connector, ["meta_campaigns", "meta_marketing_spend"])

    def sync_google_ads(self, output_dir: str = "", lookback_days: int = 90):
        """Sync Google Ads campaigns and daily campaign performance."""
        target_dir = output_dir or settings.growth_data_dir or settings.sample_data_dir
        google_ads = GoogleAdsConnector()
        if not google_ads.configured:
            body = (
                "Google Ads sync is not configured. Set `GOOGLE_ADS_DEVELOPER_TOKEN`, "
                "`GOOGLE_ADS_CUSTOMER_ID`, and OAuth credentials, then run the tool again. "
                f"Target output directory would be `{target_dir}`."
            )
            return build_tool_envelope("Google Ads Sync", body, self.connector, [])

        result = google_ads.sync_ads_data(target_dir, lookback_days=lookback_days)
        if target_dir == settings.growth_data_dir:
            reset_connector()

        rows = [
            {"Dataset": "google_ads_campaigns", "Rows": result.campaigns},
            {"Dataset": "google_marketing_spend", "Rows": result.spend_rows},
        ]
        body = format_table(rows, "Google Ads Sync Output")
        body += format_insight(
            "Google Ads campaign performance is exported as `google_marketing_spend.csv` for GrowthOS spend analysis."
        )
        body += format_actions(
            [
                "Use `describe_table('google_marketing_spend')` to inspect synced spend data.",
                "Use `google_ads_summary` for a quick search and paid media overview.",
                "Use `paid_growth_review` after syncing Stripe to compare spend against revenue.",
            ]
        )
        return build_tool_envelope("Google Ads Sync", body, self.connector, ["google_ads_campaigns", "google_marketing_spend"])

    def meta_ads_summary(self, spend_table: str = "meta_marketing_spend"):
        """Summarize synced Meta Ads campaign spend over the last 30 days."""
        spend_table = safe_identifier(spend_table)
        try:
            rows = self.connector.query(
                f"""
                WITH current_period AS (
                    SELECT
                        SUM(spend) AS spend,
                        SUM(clicks) AS clicks,
                        SUM(impressions) AS impressions
                    FROM {spend_table}
                    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                ),
                previous_period AS (
                    SELECT
                        SUM(spend) AS spend,
                        SUM(clicks) AS clicks,
                        SUM(impressions) AS impressions
                    FROM {spend_table}
                    WHERE date >= CURRENT_DATE - INTERVAL '60 days'
                      AND date < CURRENT_DATE - INTERVAL '30 days'
                )
                SELECT
                    COALESCE(c.spend, 0) AS current_spend,
                    COALESCE(p.spend, 0) AS previous_spend,
                    COALESCE(c.clicks, 0) AS current_clicks,
                    COALESCE(p.clicks, 0) AS previous_clicks,
                    COALESCE(c.impressions, 0) AS current_impressions,
                    COALESCE(p.impressions, 0) AS previous_impressions
                FROM current_period c, previous_period p
                """
            )
        except Exception:
            body = f"Meta Ads spend table `{spend_table}` is not available yet. Run `sync_meta_ads` first."
            return build_tool_envelope("Meta Ads Summary", body, self.connector, [spend_table])

        if not rows:
            body = "No synced Meta Ads spend data found."
            return build_tool_envelope("Meta Ads Summary", body, self.connector, [spend_table])

        row = rows[0]
        summary_rows = [
            {
                "Metric": "Spend (30d)",
                "Current": f"${(row.get('current_spend') or 0):,.2f}",
                "Previous": f"${(row.get('previous_spend') or 0):,.2f}",
            },
            {
                "Metric": "Clicks (30d)",
                "Current": int(row.get("current_clicks") or 0),
                "Previous": int(row.get("previous_clicks") or 0),
            },
            {
                "Metric": "Impressions (30d)",
                "Current": int(row.get("current_impressions") or 0),
                "Previous": int(row.get("previous_impressions") or 0),
            },
        ]
        body = format_table(summary_rows, "Meta Ads Summary")
        return build_tool_envelope("Meta Ads Summary", body, self.connector, [spend_table])

    def google_ads_summary(self, spend_table: str = "google_marketing_spend"):
        """Summarize synced Google Ads campaign performance over the last 30 days."""
        spend_table = safe_identifier(spend_table)
        try:
            rows = self.connector.query(
                f"""
                WITH current_period AS (
                    SELECT
                        SUM(spend) AS spend,
                        SUM(clicks) AS clicks,
                        SUM(impressions) AS impressions,
                        SUM(conversions) AS conversions
                    FROM {spend_table}
                    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                ),
                previous_period AS (
                    SELECT
                        SUM(spend) AS spend,
                        SUM(clicks) AS clicks,
                        SUM(impressions) AS impressions,
                        SUM(conversions) AS conversions
                    FROM {spend_table}
                    WHERE date >= CURRENT_DATE - INTERVAL '60 days'
                      AND date < CURRENT_DATE - INTERVAL '30 days'
                )
                SELECT
                    COALESCE(c.spend, 0) AS current_spend,
                    COALESCE(p.spend, 0) AS previous_spend,
                    COALESCE(c.clicks, 0) AS current_clicks,
                    COALESCE(p.clicks, 0) AS previous_clicks,
                    COALESCE(c.impressions, 0) AS current_impressions,
                    COALESCE(p.impressions, 0) AS previous_impressions,
                    COALESCE(c.conversions, 0) AS current_conversions,
                    COALESCE(p.conversions, 0) AS previous_conversions
                FROM current_period c, previous_period p
                """
            )
        except Exception:
            body = f"Google Ads spend table `{spend_table}` is not available yet. Run `sync_google_ads` first."
            return build_tool_envelope("Google Ads Summary", body, self.connector, [spend_table])

        if not rows:
            body = "No synced Google Ads spend data found."
            return build_tool_envelope("Google Ads Summary", body, self.connector, [spend_table])

        row = rows[0]
        summary_rows = [
            {
                "Metric": "Spend (30d)",
                "Current": f"${(row.get('current_spend') or 0):,.2f}",
                "Previous": f"${(row.get('previous_spend') or 0):,.2f}",
            },
            {
                "Metric": "Clicks (30d)",
                "Current": int(row.get("current_clicks") or 0),
                "Previous": int(row.get("previous_clicks") or 0),
            },
            {
                "Metric": "Impressions (30d)",
                "Current": int(row.get("current_impressions") or 0),
                "Previous": int(row.get("previous_impressions") or 0),
            },
            {
                "Metric": "Conversions (30d)",
                "Current": round(float(row.get("current_conversions") or 0), 2),
                "Previous": round(float(row.get("previous_conversions") or 0), 2),
            },
        ]
        body = format_table(summary_rows, "Google Ads Summary")
        return build_tool_envelope("Google Ads Summary", body, self.connector, [spend_table])

    def stripe_revenue_summary(self, invoices_table: str = "stripe_invoices"):
        """Build a compact revenue summary over synced Stripe invoices."""
        invoices_table = safe_identifier(invoices_table)
        try:
            rows = self.connector.query(
                f"""
                WITH current_period AS (
                    SELECT
                        COUNT(*) AS paid_invoices,
                        SUM(amount_paid) AS revenue
                    FROM {invoices_table}
                    WHERE status = 'paid'
                      AND created >= CURRENT_DATE - INTERVAL '30 days'
                ),
                previous_period AS (
                    SELECT
                        COUNT(*) AS paid_invoices,
                        SUM(amount_paid) AS revenue
                    FROM {invoices_table}
                    WHERE status = 'paid'
                      AND created >= CURRENT_DATE - INTERVAL '60 days'
                      AND created < CURRENT_DATE - INTERVAL '30 days'
                )
                SELECT
                    COALESCE(c.revenue, 0) AS current_revenue,
                    COALESCE(p.revenue, 0) AS previous_revenue,
                    COALESCE(c.paid_invoices, 0) AS current_paid_invoices,
                    COALESCE(p.paid_invoices, 0) AS previous_paid_invoices
                FROM current_period c, previous_period p
                """
            )
        except Exception:
            body = (
                f"Stripe invoice table `{invoices_table}` is not available yet. "
                "Run `sync_stripe_billing` first."
            )
            return build_tool_envelope("Stripe Revenue Summary", body, self.connector, [invoices_table])
        if not rows:
            body = "No synced Stripe invoices found."
            return build_tool_envelope("Stripe Revenue Summary", body, self.connector, [invoices_table])

        row = rows[0]
        current_revenue = row.get("current_revenue", 0) or 0
        previous_revenue = row.get("previous_revenue", 0) or 0
        paid_now = row.get("current_paid_invoices", 0) or 0
        paid_before = row.get("previous_paid_invoices", 0) or 0

        revenue_change = None
        if previous_revenue:
            revenue_change = round((current_revenue - previous_revenue) / previous_revenue * 100, 1)

        invoice_change = None
        if paid_before:
            invoice_change = round((paid_now - paid_before) / paid_before * 100, 1)

        summary_rows = [
            {
                "Metric": "Revenue (30d)",
                "Current": f"${current_revenue:,.2f}",
                "Previous": f"${previous_revenue:,.2f}",
                "Change": f"{revenue_change:+.1f}%" if revenue_change is not None else "—",
            },
            {
                "Metric": "Paid invoices (30d)",
                "Current": paid_now,
                "Previous": paid_before,
                "Change": f"{invoice_change:+.1f}%" if invoice_change is not None else "—",
            },
        ]
        body = format_table(summary_rows, "Stripe Revenue Summary")
        return build_tool_envelope("Stripe Revenue Summary", body, self.connector, [invoices_table])
