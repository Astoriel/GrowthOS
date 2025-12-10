"""Summary and workflow services."""

from __future__ import annotations

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.presentation.markdown import format_actions, format_insight, format_narrative, format_table
from growth_os.query.builder import safe_identifier
from growth_os.semantic.attribution import canonical_sql, load_attribution_rules
from growth_os.semantic import metrics as sql_templates
from growth_os.services.analysis_service import AnalysisService
from growth_os.services._helpers import build_tool_envelope


class ReportingService:
    """Services for weekly and executive reporting."""

    def __init__(self, connector: GrowthConnector):
        self.connector = connector
        self.analysis = AnalysisService(connector)

    def growth_summary(self, spend_table: str, events_table: str):
        """Generate a weekly KPI summary."""
        results = self.connector.query(sql_templates.growth_summary(spend_table, events_table))
        if not results:
            body = "Not enough data for growth summary. Need at least 2 weeks of data."
            return build_tool_envelope("Weekly Growth Summary", body, self.connector, [spend_table, events_table])

        parts = ["## 📊 Weekly Growth Summary\n", "*Last 7 days vs previous 7 days*\n"]
        for row in results:
            metric = row.get("metric", "")
            current = row.get("current_value", 0)
            change = row.get("change_pct")
            arrow = "▲" if change and change > 0 else "▼" if change and change < 0 else "→"
            change_str = f" {arrow} {change:+.1f}%" if change is not None else ""
            if metric in ("Revenue", "Spend"):
                parts.append(f"- **{metric}:** ${current:,.0f}{change_str}")
            else:
                parts.append(f"- **{metric}:** {current:,.0f}{change_str}")
        body = "\n".join(parts)
        return build_tool_envelope("Weekly Growth Summary", body, self.connector, [spend_table, events_table])

    def weekly_growth_review(self, spend_table: str, events_table: str):
        """Build a higher-level weekly review workflow."""
        summary = self.connector.query(sql_templates.growth_summary(spend_table, events_table))
        anomalies = self.connector.query(sql_templates.anomaly_detection(spend_table, "spend", "date", 30))
        cac_ltv = self.connector.query(sql_templates.cac_by_channel(spend_table, events_table))

        parts = ["## Weekly Growth Review\n"]
        if summary:
            positive = [row["metric"] for row in summary if row.get("change_pct") and row["change_pct"] > 0]
            negative = [row["metric"] for row in summary if row.get("change_pct") and row["change_pct"] < 0]
            if positive:
                parts.append(f"- Positive movement: {', '.join(positive)}")
            if negative:
                parts.append(f"- Negative movement: {', '.join(negative)}")
        else:
            parts.append("- Not enough data for period-over-period review.")

        if anomalies:
            parts.append(f"- Anomalies detected: {len(anomalies)}")
        else:
            parts.append("- No recent anomalies detected in spend.")

        if cac_ltv:
            best_channel = next((row["channel"] for row in cac_ltv if row.get("cac") is not None), None)
            if best_channel:
                parts.append(f"- Lowest CAC channel observed: {best_channel}")

        body = "\n".join(parts)
        body += format_insight("Use this as the weekly founder or growth lead check-in.")
        body += format_actions(
            [
                "Inspect the biggest negative metric first.",
                "Review channel efficiency before budget moves.",
                "Check anomalies against campaigns or launches.",
            ]
        )
        return build_tool_envelope("Weekly Growth Review", body, self.connector, [spend_table, events_table])

    def executive_summary(self, spend_table: str, events_table: str):
        """Generate a concise executive summary."""
        summary = self.connector.query(sql_templates.growth_summary(spend_table, events_table))
        if not summary:
            body = "Not enough data for an executive summary."
            return build_tool_envelope("Executive Summary", body, self.connector, [spend_table, events_table])

        wins = []
        risks = []
        for row in summary:
            change = row.get("change_pct")
            metric = row["metric"]
            if change is None:
                continue
            if change > 0:
                wins.append(f"{metric} {change:+.1f}%")
            elif change < 0:
                risks.append(f"{metric} {change:+.1f}%")

        parts = ["## Executive Summary\n"]
        parts.append(f"- Wins: {', '.join(wins) if wins else 'No major wins detected.'}")
        parts.append(f"- Risks: {', '.join(risks) if risks else 'No major risks detected.'}")
        parts.append("- Recommendation: focus on the largest negative swing before scaling spend.")
        body = "\n".join(parts)
        return build_tool_envelope("Executive Summary", body, self.connector, [spend_table, events_table])

    def paid_growth_review(
        self,
        spend_tables: str = "meta_marketing_spend,google_marketing_spend",
        invoices_table: str = "stripe_invoices",
    ):
        """Review paid acquisition spend against Stripe revenue across sources."""
        invoice_table = safe_identifier(invoices_table)
        spend_sources = [safe_identifier(table.strip()) for table in spend_tables.split(",") if table.strip()]
        if not spend_sources:
            body = "No spend tables were provided."
            return build_tool_envelope("Paid Growth Review", body, self.connector, [invoice_table])

        spend_union = self._build_spend_union(spend_sources)

        try:
            overview_rows = self.connector.query(
                f"""
                WITH spend_data AS (
                    {spend_union}
                ),
                current_spend AS (
                    SELECT
                        SUM(spend) AS spend,
                        SUM(clicks) AS clicks,
                        SUM(impressions) AS impressions,
                        SUM(conversions) AS conversions
                    FROM spend_data
                    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                ),
                previous_spend AS (
                    SELECT
                        SUM(spend) AS spend,
                        SUM(clicks) AS clicks,
                        SUM(impressions) AS impressions,
                        SUM(conversions) AS conversions
                    FROM spend_data
                    WHERE date >= CURRENT_DATE - INTERVAL '60 days'
                      AND date < CURRENT_DATE - INTERVAL '30 days'
                ),
                current_revenue AS (
                    SELECT SUM(amount_paid) AS revenue
                    FROM {invoice_table}
                    WHERE status = 'paid'
                      AND created >= CURRENT_DATE - INTERVAL '30 days'
                ),
                previous_revenue AS (
                    SELECT SUM(amount_paid) AS revenue
                    FROM {invoice_table}
                    WHERE status = 'paid'
                      AND created >= CURRENT_DATE - INTERVAL '60 days'
                      AND created < CURRENT_DATE - INTERVAL '30 days'
                )
                SELECT
                    COALESCE(cs.spend, 0) AS current_spend,
                    COALESCE(ps.spend, 0) AS previous_spend,
                    COALESCE(cs.clicks, 0) AS current_clicks,
                    COALESCE(cs.impressions, 0) AS current_impressions,
                    COALESCE(cs.conversions, 0) AS current_conversions,
                    COALESCE(cr.revenue, 0) AS current_revenue,
                    COALESCE(pr.revenue, 0) AS previous_revenue
                FROM current_spend cs, previous_spend ps, current_revenue cr, previous_revenue pr
                """
            )
            mix_rows = self.connector.query(
                f"""
                WITH spend_data AS (
                    {spend_union}
                )
                SELECT
                    channel,
                    ROUND(SUM(spend), 2) AS spend_30d,
                    SUM(clicks) AS clicks_30d,
                    SUM(impressions) AS impressions_30d,
                    SUM(conversions) AS conversions_30d
                FROM spend_data
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY 1
                ORDER BY spend_30d DESC
                """
            )
        except Exception:
            body = (
                "Paid growth review needs synced ad spend and Stripe invoice data. "
                "Run `sync_meta_ads`, `sync_google_ads`, and `sync_stripe_billing` first."
            )
            return build_tool_envelope("Paid Growth Review", body, self.connector, spend_sources + [invoice_table])

        if not overview_rows:
            body = "Not enough cross-source data for a paid growth review."
            return build_tool_envelope("Paid Growth Review", body, self.connector, spend_sources + [invoice_table])

        overview = overview_rows[0]
        current_spend = float(overview.get("current_spend") or 0)
        previous_spend = float(overview.get("previous_spend") or 0)
        current_revenue = float(overview.get("current_revenue") or 0)
        previous_revenue = float(overview.get("previous_revenue") or 0)
        mer_now = round(current_revenue / current_spend, 2) if current_spend else None
        mer_prev = round(previous_revenue / previous_spend, 2) if previous_spend else None

        summary_rows = [
            {
                "Metric": "Paid spend (30d)",
                "Current": f"${current_spend:,.2f}",
                "Previous": f"${previous_spend:,.2f}",
            },
            {
                "Metric": "Stripe revenue (30d)",
                "Current": f"${current_revenue:,.2f}",
                "Previous": f"${previous_revenue:,.2f}",
            },
            {
                "Metric": "Blended MER",
                "Current": f"{mer_now:.2f}" if mer_now is not None else "—",
                "Previous": f"{mer_prev:.2f}" if mer_prev is not None else "—",
            },
            {
                "Metric": "Clicks (30d)",
                "Current": int(overview.get("current_clicks") or 0),
                "Previous": "—",
            },
            {
                "Metric": "Impressions (30d)",
                "Current": int(overview.get("current_impressions") or 0),
                "Previous": "—",
            },
            {
                "Metric": "Platform conversions (30d)",
                "Current": round(float(overview.get("current_conversions") or 0), 2),
                "Previous": "—",
            },
        ]

        body = format_table(summary_rows, "Paid Growth Overview")
        if mix_rows:
            channel_rows = [
                {
                    "Channel": row["channel"],
                    "Spend (30d)": f"${float(row.get('spend_30d') or 0):,.2f}",
                    "Clicks": int(row.get("clicks_30d") or 0),
                    "Impressions": int(row.get("impressions_30d") or 0),
                    "Conversions": round(float(row.get("conversions_30d") or 0), 2),
                }
                for row in mix_rows
            ]
            body += "\n\n" + format_table(channel_rows, "Paid Channel Mix")

        if mer_now is not None and mer_prev is not None:
            if mer_now > mer_prev:
                body += format_insight("Blended paid efficiency improved versus the previous 30-day period.")
            elif mer_now < mer_prev:
                body += format_insight("Blended paid efficiency declined versus the previous 30-day period.")
        body += format_actions(
            [
                "Review the largest paid channel first before reallocating budget.",
                "Compare platform conversions against downstream Stripe revenue before scaling.",
                "Use `run_query` to inspect campaign-level drivers inside Google Ads or Meta Ads tables.",
            ]
        )
        return build_tool_envelope("Paid Growth Review", body, self.connector, spend_sources + [invoice_table])

    def campaign_performance_review(
        self,
        spend_tables: str = "meta_marketing_spend,google_marketing_spend",
        min_spend: float = 50,
        limit: int = 5,
    ):
        """Review campaign performance across synced Meta Ads and Google Ads sources."""
        spend_sources = [safe_identifier(table.strip()) for table in spend_tables.split(",") if table.strip()]
        if not spend_sources:
            body = "No spend tables were provided."
            return build_tool_envelope("Campaign Performance Review", body, self.connector, [])

        spend_union = self._build_spend_union(spend_sources)

        try:
            scorecard_rows = self.connector.query(
                f"""
                WITH spend_data AS (
                    {spend_union}
                ),
                campaign_rollup AS (
                    SELECT
                        channel,
                        campaign_id,
                        campaign,
                        SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN spend ELSE 0 END) AS current_spend,
                        SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN clicks ELSE 0 END) AS current_clicks,
                        SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN impressions ELSE 0 END) AS current_impressions,
                        SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN conversions ELSE 0 END) AS current_conversions,
                        SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '60 days' AND date < CURRENT_DATE - INTERVAL '30 days' THEN spend ELSE 0 END) AS previous_spend,
                        SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '60 days' AND date < CURRENT_DATE - INTERVAL '30 days' THEN conversions ELSE 0 END) AS previous_conversions
                    FROM spend_data
                    GROUP BY 1, 2, 3
                )
                SELECT
                    channel,
                    campaign_id,
                    campaign,
                    ROUND(current_spend, 2) AS current_spend,
                    current_clicks,
                    current_impressions,
                    ROUND(current_conversions, 2) AS current_conversions,
                    ROUND(previous_spend, 2) AS previous_spend,
                    ROUND(previous_conversions, 2) AS previous_conversions,
                    CASE
                        WHEN current_clicks > 0 THEN ROUND(current_spend / current_clicks, 2)
                        ELSE NULL
                    END AS current_cpc,
                    CASE
                        WHEN current_impressions > 0 THEN ROUND(current_clicks * 100.0 / current_impressions, 2)
                        ELSE NULL
                    END AS current_ctr,
                    CASE
                        WHEN current_conversions > 0 THEN ROUND(current_spend / current_conversions, 2)
                        ELSE NULL
                    END AS current_cpa,
                    CASE
                        WHEN previous_spend > 0 THEN ROUND((current_spend - previous_spend) * 100.0 / previous_spend, 1)
                        ELSE NULL
                    END AS spend_change_pct
                FROM campaign_rollup
                WHERE current_spend > 0 OR previous_spend > 0
                ORDER BY current_spend DESC, current_conversions DESC
                """
            )
        except Exception:
            body = (
                "Campaign performance review needs synced ad spend data. "
                "Run `sync_meta_ads` and `sync_google_ads` first."
            )
            return build_tool_envelope("Campaign Performance Review", body, self.connector, spend_sources)

        if not scorecard_rows:
            body = "No campaign performance data found for the selected spend sources."
            return build_tool_envelope("Campaign Performance Review", body, self.connector, spend_sources)

        top_spenders = scorecard_rows[: max(limit, 1)]
        efficiency_rows = [
            row for row in scorecard_rows if float(row.get("current_spend") or 0) >= min_spend and (row.get("current_conversions") or 0) > 0
        ]
        efficiency_rows.sort(
            key=lambda row: (
                float(row.get("current_cpa")) if row.get("current_cpa") is not None else float("inf"),
                -float(row.get("current_conversions") or 0),
            )
        )
        watchlist_rows = [
            row
            for row in scorecard_rows
            if float(row.get("current_spend") or 0) >= min_spend
            and (
                (row.get("current_conversions") or 0) == 0
                or (
                    row.get("spend_change_pct") is not None
                    and float(row.get("spend_change_pct") or 0) > 20
                    and float(row.get("current_conversions") or 0) <= float(row.get("previous_conversions") or 0)
                )
            )
        ]

        top_rows = [
            {
                "Channel": row["channel"],
                "Campaign": row["campaign"],
                "Spend (30d)": f"${float(row.get('current_spend') or 0):,.2f}",
                "Conversions": round(float(row.get("current_conversions") or 0), 2),
                "CPC": f"${float(row.get('current_cpc')):,.2f}" if row.get("current_cpc") is not None else "—",
                "CPA": f"${float(row.get('current_cpa')):,.2f}" if row.get("current_cpa") is not None else "—",
                "Spend Change": f"{float(row.get('spend_change_pct')):+.1f}%" if row.get("spend_change_pct") is not None else "—",
            }
            for row in top_spenders
        ]

        body = format_table(top_rows, "Top Campaigns by Spend")

        if efficiency_rows:
            efficient = [
                {
                    "Channel": row["channel"],
                    "Campaign": row["campaign"],
                    "CPA": f"${float(row.get('current_cpa')):,.2f}" if row.get("current_cpa") is not None else "—",
                    "Conversions": round(float(row.get("current_conversions") or 0), 2),
                    "Spend (30d)": f"${float(row.get('current_spend') or 0):,.2f}",
                    "CTR": f"{float(row.get('current_ctr')):.2f}%" if row.get("current_ctr") is not None else "—",
                }
                for row in efficiency_rows[: max(limit, 1)]
            ]
            body += "\n\n" + format_table(efficient, "Efficiency Leaders")

        if watchlist_rows:
            watchlist = [
                {
                    "Channel": row["channel"],
                    "Campaign": row["campaign"],
                    "Spend (30d)": f"${float(row.get('current_spend') or 0):,.2f}",
                    "Conversions": round(float(row.get("current_conversions") or 0), 2),
                    "Previous Conv.": round(float(row.get("previous_conversions") or 0), 2),
                    "Spend Change": f"{float(row.get('spend_change_pct')):+.1f}%" if row.get("spend_change_pct") is not None else "—",
                }
                for row in watchlist_rows[: max(limit, 1)]
            ]
            body += "\n\n" + format_table(watchlist, "Watchlist")

        best_campaign = efficiency_rows[0]["campaign"] if efficiency_rows else top_spenders[0]["campaign"]
        body += format_insight(f"Start with `{best_campaign}` to understand what is driving the strongest current paid performance.")
        body += format_actions(
            [
                "Double-check campaigns with rising spend but flat conversions before increasing budgets.",
                "Use `run_query` to inspect daily patterns for the top and weakest campaigns.",
                "Compare campaign mix changes against the `paid_growth_review` before rebalancing channels.",
            ]
        )
        return build_tool_envelope("Campaign Performance Review", body, self.connector, spend_sources)

    def attribution_bridge_review(
        self,
        spend_tables: str = "meta_marketing_spend,google_marketing_spend",
        events_table: str = "user_events",
        revenue_event_type: str = "purchase",
        min_spend: float = 50,
        limit: int = 10,
    ):
        """Bridge paid acquisition spend to downstream events and revenue."""
        spend_sources = [safe_identifier(table.strip()) for table in spend_tables.split(",") if table.strip()]
        events_table = safe_identifier(events_table)
        revenue_event_type = revenue_event_type.strip() or "purchase"
        min_spend = max(float(min_spend), 0)
        limit = max(int(limit), 1)
        warnings: list[str] = []

        if not spend_sources:
            body = "No spend tables were provided."
            return build_tool_envelope("Attribution Bridge Review", body, self.connector, [events_table])

        if not self._table_has_column(events_table, "revenue"):
            body = f"Events table `{events_table}` does not include a `revenue` column."
            return build_tool_envelope("Attribution Bridge Review", body, self.connector, spend_sources + [events_table])

        has_campaign_grain = self._table_has_column(events_table, "utm_campaign")
        if not has_campaign_grain:
            warnings.append(
                f"`{events_table}` has no `utm_campaign` column, so attribution falls back to channel-level instead of campaign-level."
            )

        spend_union = self._build_spend_union(spend_sources)
        rules = load_attribution_rules()
        spend_channel_expr = canonical_sql("channel", "channel", rules)
        event_channel_expr = canonical_sql("utm_source", "channel", rules)
        spend_campaign_expr = canonical_sql("campaign", "campaign", rules, channel_sql="channel")
        event_campaign_expr = canonical_sql("utm_campaign", "campaign", rules, channel_sql="utm_source")
        spend_rollup = self._campaign_spend_rollup_sql(has_campaign_grain, spend_channel_expr, spend_campaign_expr)
        event_rollup = self._event_attribution_rollup_sql(
            events_table,
            revenue_event_type,
            has_campaign_grain,
            event_channel_expr,
            event_campaign_expr,
        )
        join_condition = "s.channel_key = e.channel_key AND s.campaign_key = e.campaign_key" if has_campaign_grain else "s.channel_key = e.channel_key"

        try:
            attributed_rows = self.connector.query(
                f"""
                WITH spend_data AS (
                    {spend_union}
                ),
                spend_rollup AS (
                    {spend_rollup}
                ),
                event_rollup AS (
                    {event_rollup}
                )
                SELECT
                    s.channel,
                    s.campaign_id,
                    s.campaign,
                    ROUND(s.spend_30d, 2) AS spend_30d,
                    s.clicks_30d,
                    ROUND(s.platform_conversions_30d, 2) AS platform_conversions_30d,
                    COALESCE(e.signups_30d, 0) AS signups_30d,
                    COALESCE(e.purchasers_30d, 0) AS purchasers_30d,
                    ROUND(COALESCE(e.revenue_30d, 0), 2) AS attributed_revenue_30d,
                    CASE
                        WHEN s.spend_30d > 0 THEN ROUND(COALESCE(e.revenue_30d, 0) / s.spend_30d, 2)
                        ELSE NULL
                    END AS attributed_roas,
                    CASE
                        WHEN COALESCE(e.purchasers_30d, 0) > 0 THEN ROUND(s.spend_30d / e.purchasers_30d, 2)
                        ELSE NULL
                    END AS cost_per_purchaser
                FROM spend_rollup s
                LEFT JOIN event_rollup e ON {join_condition}
                WHERE s.spend_30d >= {min_spend}
                ORDER BY attributed_revenue_30d DESC, spend_30d DESC
                LIMIT {limit}
                """
            )
            coverage_rows = self.connector.query(
                f"""
                WITH spend_data AS (
                    {spend_union}
                ),
                spend_rollup AS (
                    {spend_rollup}
                ),
                event_rollup AS (
                    {event_rollup}
                ),
                joined AS (
                    SELECT
                        COALESCE(e.revenue_30d, 0) AS attributed_revenue_30d
                    FROM spend_rollup s
                    LEFT JOIN event_rollup e ON {join_condition}
                ),
                total_revenue AS (
                    SELECT
                        COALESCE(SUM(revenue), 0) AS total_revenue_30d,
                        COUNT(DISTINCT CASE WHEN event_type = '{revenue_event_type.replace("'", "''")}' THEN user_id END) AS total_purchasers_30d
                    FROM {events_table}
                    WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
                )
                SELECT
                    ROUND(COALESCE(SUM(j.attributed_revenue_30d), 0), 2) AS attributed_revenue_30d,
                    t.total_revenue_30d,
                    t.total_purchasers_30d
                FROM joined j
                CROSS JOIN total_revenue t
                GROUP BY 2, 3
                """
            )
        except Exception:
            body = (
                "Attribution bridge review needs synced paid spend plus an events table with "
                "`utm_source` and `revenue` columns."
            )
            return build_tool_envelope(
                "Attribution Bridge Review",
                body,
                self.connector,
                spend_sources + [events_table],
                warnings=warnings,
            )

        if not attributed_rows:
            body = "No attributed spend rows met the current filters."
            return build_tool_envelope(
                "Attribution Bridge Review",
                body,
                self.connector,
                spend_sources + [events_table],
                warnings=warnings,
            )

        coverage = coverage_rows[0] if coverage_rows else {}
        attributed_revenue = float(coverage.get("attributed_revenue_30d") or 0)
        total_revenue = float(coverage.get("total_revenue_30d") or 0)
        total_purchasers = int(coverage.get("total_purchasers_30d") or 0)
        coverage_pct = round(attributed_revenue / total_revenue * 100, 1) if total_revenue else None

        coverage_table = [
            {
                "Metric": "Attributed revenue (30d)",
                "Value": f"${attributed_revenue:,.2f}",
            },
            {
                "Metric": "Total purchase revenue (30d)",
                "Value": f"${total_revenue:,.2f}",
            },
            {
                "Metric": "Revenue coverage",
                "Value": f"{coverage_pct:.1f}%" if coverage_pct is not None else "—",
            },
            {
                "Metric": "Purchasers (30d)",
                "Value": total_purchasers,
            },
        ]
        body = format_table(coverage_table, "Attribution Coverage")

        if has_campaign_grain:
            performance_rows = [
                {
                    "Channel": row["channel"],
                    "Campaign": row["campaign"],
                    "Spend (30d)": f"${float(row.get('spend_30d') or 0):,.2f}",
                    "Purchasers": int(row.get("purchasers_30d") or 0),
                    "Revenue (30d)": f"${float(row.get('attributed_revenue_30d') or 0):,.2f}",
                    "ROAS": f"{float(row.get('attributed_roas')):.2f}x" if row.get("attributed_roas") is not None else "—",
                    "Cost / Purchaser": f"${float(row.get('cost_per_purchaser')):,.2f}" if row.get("cost_per_purchaser") is not None else "—",
                }
                for row in attributed_rows
            ]
            body += "\n\n" + format_table(performance_rows, "Attributed Campaign Revenue")
        else:
            performance_rows = [
                {
                    "Channel": row["channel"],
                    "Spend (30d)": f"${float(row.get('spend_30d') or 0):,.2f}",
                    "Purchasers": int(row.get("purchasers_30d") or 0),
                    "Revenue (30d)": f"${float(row.get('attributed_revenue_30d') or 0):,.2f}",
                    "ROAS": f"{float(row.get('attributed_roas')):.2f}x" if row.get("attributed_roas") is not None else "—",
                    "Platform Conversions": round(float(row.get("platform_conversions_30d") or 0), 2),
                }
                for row in attributed_rows
            ]
            body += "\n\n" + format_table(performance_rows, "Attributed Revenue by Channel")

        best_row = max(
            attributed_rows,
            key=lambda row: (
                float(row.get("attributed_revenue_30d") or 0),
                float(row.get("attributed_roas") or 0) if row.get("attributed_roas") is not None else 0,
            ),
        )
        best_label = best_row["campaign"] if has_campaign_grain else best_row["channel"]
        body += format_insight(
            f"`{best_label}` currently drives the strongest downstream attributed revenue signal in the selected paid mix."
        )
        body += format_actions(
            [
                "Compare campaigns with high platform conversions but low attributed revenue before scaling them.",
                "Standardize `utm_campaign` naming across Meta and Google Ads to improve attribution quality.",
                "Use this report together with `campaign_performance_review` to separate platform efficiency from real revenue impact.",
            ]
        )
        return build_tool_envelope(
            "Attribution Bridge Review",
            body,
            self.connector,
            spend_sources + [events_table],
            warnings=warnings,
        )

    @staticmethod
    def _build_spend_union(spend_sources: list[str]) -> str:
        """Build a union over normalized spend tables."""
        return "\nUNION ALL\n".join(
            f"SELECT '{table}' AS source_table, channel, campaign_id, campaign, date, spend, clicks, impressions, COALESCE(conversions, 0) AS conversions FROM {table}"
            for table in spend_sources
        )

    @staticmethod
    def _campaign_spend_rollup_sql(has_campaign_grain: bool, channel_key_sql: str, campaign_key_sql: str) -> str:
        """Build spend rollup SQL at campaign or channel grain."""
        if has_campaign_grain:
            return """
            SELECT
                """ + channel_key_sql + """ AS channel_key,
                """ + campaign_key_sql + """ AS campaign_key,
                MIN(channel) AS channel,
                MIN(campaign_id) AS campaign_id,
                MIN(campaign) AS campaign,
                SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN spend ELSE 0 END) AS spend_30d,
                SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN clicks ELSE 0 END) AS clicks_30d,
                SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN conversions ELSE 0 END) AS platform_conversions_30d
            FROM spend_data
            GROUP BY 1, 2
            """
        return """
        SELECT
            """ + channel_key_sql + """ AS channel_key,
            """ + channel_key_sql + """ AS campaign_key,
            MIN(channel) AS channel,
            NULL AS campaign_id,
            MIN(channel) AS campaign,
            SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN spend ELSE 0 END) AS spend_30d,
            SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN clicks ELSE 0 END) AS clicks_30d,
            SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN conversions ELSE 0 END) AS platform_conversions_30d
        FROM spend_data
        GROUP BY 1, 2
        """

    @staticmethod
    def _event_attribution_rollup_sql(
        events_table: str,
        revenue_event_type: str,
        has_campaign_grain: bool,
        channel_key_sql: str,
        campaign_key_sql: str,
    ) -> str:
        """Build downstream event aggregation for attribution."""
        revenue_event_type = revenue_event_type.replace("'", "''")
        if has_campaign_grain:
            return f"""
            SELECT
                {channel_key_sql} AS channel_key,
                {campaign_key_sql} AS campaign_key,
                COUNT(DISTINCT CASE WHEN event_type = 'signup' THEN user_id END) AS signups_30d,
                COUNT(DISTINCT CASE WHEN event_type = '{revenue_event_type}' THEN user_id END) AS purchasers_30d,
                SUM(CASE WHEN event_type = '{revenue_event_type}' THEN revenue ELSE 0 END) AS revenue_30d
            FROM {events_table}
            WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY 1, 2
            """
        return f"""
        SELECT
            {channel_key_sql} AS channel_key,
            COUNT(DISTINCT CASE WHEN event_type = 'signup' THEN user_id END) AS signups_30d,
            COUNT(DISTINCT CASE WHEN event_type = '{revenue_event_type}' THEN user_id END) AS purchasers_30d,
            SUM(CASE WHEN event_type = '{revenue_event_type}' THEN revenue ELSE 0 END) AS revenue_30d
        FROM {events_table}
        WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY 1
        """

    def _table_has_column(self, table_name: str, column_name: str) -> bool:
        """Return True when a column exists in the given table."""
        try:
            columns = self.connector.query(f"DESCRIBE {table_name}")
        except Exception:
            return False
        for column in columns:
            name = column.get("column_name", column.get("Field", ""))
            if name == column_name:
                return True
        return False

    def narrative_growth_review(self, spend_table: str, events_table: str):
        """Generate a prose narrative growth review for executive briefings."""
        results = self.connector.query(sql_templates.growth_summary(spend_table, events_table))
        if not results:
            body = format_narrative(
                headline="Growth Narrative",
                context="This period's growth data is unavailable.",
                findings=["Insufficient data to produce a narrative (need at least 2 weeks of history)"],
                recommendation="Load at least two weeks of spend and events data, then retry.",
            )
            return build_tool_envelope("Growth Narrative", body, self.connector, [spend_table, events_table])

        positive = []
        negative = []
        neutral = []
        for row in results:
            metric = row.get("metric", "")
            change = row.get("change_pct")
            current = row.get("current_value", 0)
            if change is None:
                neutral.append(f"{metric} is {current:,.0f} (no prior period for comparison)")
            elif change > 0:
                positive.append(f"{metric} grew {change:+.1f}% to {current:,.0f}")
            else:
                negative.append(f"{metric} declined {change:+.1f}% to {current:,.0f}")

        findings: list[str] = positive + negative + neutral
        if not findings:
            findings = ["all tracked metrics were flat versus the prior period"]

        if positive and negative:
            context = "Growth this period shows a mixed picture."
            recommendation = (
                "Prioritise reversing the declining metrics before reallocating budget to top performers."
            )
        elif positive:
            context = "Growth this period is broadly positive across tracked metrics."
            recommendation = (
                "Sustain momentum by reviewing the highest-performing channels and doubling down on what is working."
            )
        else:
            context = "Growth this period is under pressure."
            recommendation = (
                "Conduct a channel-level audit immediately and pause under-performing spend until root causes are identified."
            )

        body = format_narrative(
            headline="Growth Narrative",
            context=context,
            findings=findings,
            recommendation=recommendation,
        )
        return build_tool_envelope("Growth Narrative", body, self.connector, [spend_table, events_table])
