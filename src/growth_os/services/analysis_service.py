"""Core analytical services."""

from __future__ import annotations

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.presentation.markdown import format_actions, format_insight, format_table
from growth_os.semantic import metrics as sql_templates
from growth_os.semantic.benchmarks import cac_benchmark, classify_metric, retention_benchmark
from growth_os.services._helpers import build_tool_envelope


class AnalysisService:
    """Services for marketing and growth analysis."""

    def __init__(self, connector: GrowthConnector):
        self.connector = connector

    def analyze_funnel(self, events_table: str, steps: str, date_from: str = "", date_to: str = ""):
        """Analyze funnel conversion rates."""
        step_list = [step.strip() for step in steps.split(",") if step.strip()]
        sql = sql_templates.funnel_conversion(events_table, step_list, date_from or None, date_to or None)
        results = self.connector.query(sql)
        if not results:
            body = "No funnel data found."
            return build_tool_envelope("Conversion Funnel", body, self.connector, [events_table])

        first_users = results[0]["users"] if results else 0
        enhanced = []
        for index, row in enumerate(results):
            step_rate = (row["users"] / first_users * 100) if first_users > 0 else 0
            drop = 0
            if index > 0:
                previous = results[index - 1]["users"]
                drop = ((previous - row["users"]) / previous * 100) if previous > 0 else 0
            enhanced.append(
                {
                    "Step": f"{index + 1}. {row['step']}",
                    "Users": row["users"],
                    "Conversion": f"{step_rate:.1f}%",
                    "Drop-off": f"{drop:.1f}%" if index > 0 else "—",
                }
            )

        body = format_table(enhanced, "🔄 Conversion Funnel")
        if len(enhanced) > 1:
            drops = [(entry["Step"], float(entry["Drop-off"].replace("%", ""))) for entry in enhanced[1:]]
            worst = max(drops, key=lambda item: item[1])
            body += format_insight(
                f"Biggest drop-off at **{worst[0]}** ({worst[1]:.1f}% lost). "
                "Investigate UX friction or messaging at this stage."
            )
            body += format_actions(
                [
                    f"Run an experiment on the {worst[0].split('. ')[1]} step.",
                    "Check technical errors or slow-loading flows.",
                    "Talk to users who dropped before the next step.",
                ]
            )
        return build_tool_envelope("Conversion Funnel", body, self.connector, [events_table])

    def compute_cac_ltv(self, spend_table: str, events_table: str):
        """Calculate CAC and LTV by channel."""
        cac_results = self.connector.query(sql_templates.cac_by_channel(spend_table, events_table))
        ltv_results = self.connector.query(sql_templates.ltv_by_channel(events_table))
        ltv_map = {row["channel"]: row for row in ltv_results}
        combined = []
        for row in cac_results:
            channel = row["channel"]
            ltv_row = ltv_map.get(channel, {})
            ltv = ltv_row.get("avg_ltv", 0)
            cac = row.get("cac", 0)
            roas = round(ltv / cac, 2) if cac and cac > 0 else None
            combined.append(
                {
                    "Channel": channel,
                    "Spend": f"${row.get('total_spend', 0):,.0f}",
                    "Users": row.get("users_acquired", 0),
                    "CAC": f"${cac:.2f}" if cac else "—",
                    "LTV": f"${ltv:.2f}" if ltv else "—",
                    "ROAS": f"{roas}x" if roas else "—",
                }
            )

        body = format_table(combined, "💰 CAC & LTV by Channel")
        if combined:
            best_cac = min(
                (entry for entry in combined if entry["CAC"] != "—"),
                key=lambda entry: float(entry["CAC"].replace("$", "")),
                default=None,
            )
            best_roas = max(
                (entry for entry in combined if entry["ROAS"] != "—"),
                key=lambda entry: float(entry["ROAS"].replace("x", "")),
                default=None,
            )
            insights = []
            if best_cac:
                insights.append(f"Best CAC: **{best_cac['Channel']}** at {best_cac['CAC']}")
            if best_roas:
                insights.append(f"Best ROAS: **{best_roas['Channel']}** at {best_roas['ROAS']}")
            if insights:
                body += format_insight(" | ".join(insights))
                body += format_actions(
                    [
                        f"Consider increasing budget for {best_roas['Channel'] if best_roas else 'the top ROAS channel'}.",
                        "Investigate channels with high CAC.",
                        "Validate attribution assumptions before reallocating major budget.",
                    ]
                )
        return build_tool_envelope("CAC & LTV", body, self.connector, [spend_table, events_table])

    def cohort_retention(self, events_table: str, period: str = "month"):
        """Analyze cohort retention."""
        results = self.connector.query(sql_templates.cohort_retention(events_table, period))
        if not results:
            body = "No cohort data found."
            return build_tool_envelope("Cohort Retention", body, self.connector, [events_table])

        body = format_table(results, f"📊 Cohort Retention ({period}ly)")
        period_one = [row["retention_pct"] for row in results if row["period_number"] == 1]
        if period_one:
            avg_retention = sum(period_one) / len(period_one)
            benchmark = retention_benchmark(period)
            verdict = (
                f"Good, above the {benchmark:.0f}% benchmark."
                if avg_retention > benchmark
                else f"Below the {benchmark:.0f}% benchmark, focus on activation."
            )
            body += format_insight(f"Average {period} 1 retention: **{avg_retention:.1f}%**. {verdict}")
            body += format_actions(
                [
                    "Improve onboarding to lift early retention.",
                    "Build a re-engagement motion for inactive users.",
                    "Look for feature usage patterns in stronger cohorts.",
                ]
            )
        return build_tool_envelope("Cohort Retention", body, self.connector, [events_table])

    def channel_attribution(self, spend_table: str, events_table: str):
        """Analyze channel attribution."""
        results = self.connector.query(sql_templates.channel_attribution(spend_table, events_table))
        if not results:
            body = "No attribution data found."
            return build_tool_envelope("Channel Attribution", body, self.connector, [spend_table, events_table])

        formatted = []
        total_revenue = sum(row.get("revenue", 0) for row in results)
        for row in results:
            revenue = row.get("revenue", 0)
            pct = (revenue / total_revenue * 100) if total_revenue > 0 else 0
            formatted.append(
                {
                    "Channel": row["channel"],
                    "Spend": f"${row.get('spend', 0):,.0f}",
                    "Conversions": row.get("conversions", 0),
                    "Revenue": f"${revenue:,.0f}",
                    "Rev %": f"{pct:.1f}%",
                    "ROAS": f"{row.get('roas', 0)}x" if row.get("roas") else "—",
                }
            )

        body = format_table(formatted, "📈 Channel Attribution")
        if formatted:
            body += format_insight(
                f"**{formatted[0]['Channel']}** is the top revenue driver. Review ROAS before scaling."
            )
        return build_tool_envelope("Channel Attribution", body, self.connector, [spend_table, events_table])

    def analyze_churn(self, events_table: str, inactive_days: int = 30):
        """Analyze churn by inactivity."""
        results = self.connector.query(sql_templates.churn_analysis(events_table, inactive_days))
        if not results:
            body = "No user activity data found."
            return build_tool_envelope("Churn Analysis", body, self.connector, [events_table])

        body = format_table(results, "🚨 User Churn Analysis")
        churned = next((row for row in results if "Churned" in str(row.get("segment", ""))), None)
        at_risk = next((row for row in results if "Risk" in str(row.get("segment", ""))), None)
        insights = []
        if churned:
            insights.append(f"{churned.get('users', 0):,} users churned ({churned.get('pct', 0)}%)")
        if at_risk:
            insights.append(f"{at_risk.get('users', 0):,} users at risk ({at_risk.get('pct', 0)}%)")
        if insights:
            body += format_insight(" | ".join(insights))
            body += format_actions(
                [
                    "Launch a win-back sequence for churned users.",
                    "Target at-risk users with a re-engagement message.",
                    "Check whether inactivity threshold matches your business model.",
                ]
            )
        return build_tool_envelope("Churn Analysis", body, self.connector, [events_table])

    def detect_anomalies(self, table: str, metric_column: str, date_column: str = "date", lookback_days: int = 30):
        """Detect anomalies in one metric."""
        results = self.connector.query(sql_templates.anomaly_detection(table, metric_column, date_column, lookback_days))
        if not results:
            body = f"✅ No anomalies detected in `{metric_column}` over the past {lookback_days} days."
            return build_tool_envelope("Anomaly Detection", body, self.connector, [table])

        body = format_table(results, f"⚠️ Anomalies in `{metric_column}` (Last {lookback_days} Days)")
        spikes = [row for row in results if "Spike" in str(row.get("status", ""))]
        drops = [row for row in results if "Drop" in str(row.get("status", ""))]
        insights = []
        if spikes:
            insights.append(f"{len(spikes)} spike(s) detected")
        if drops:
            insights.append(f"{len(drops)} drop(s) detected")
        if insights:
            body += format_insight(" and ".join(insights) + ". Investigate root causes.")
            body += format_actions(
                [
                    "Check campaign changes or budget shifts on flagged dates.",
                    "Cross-reference with launches, promos, or outages.",
                    "Verify pipeline integrity before reacting.",
                ]
            )
        return build_tool_envelope("Anomaly Detection", body, self.connector, [table])

    # ------------------------------------------------------------------
    # Workflow tools — composite analyses combining multiple signals
    # ------------------------------------------------------------------

    def funnel_diagnosis(self, events_table: str, steps: str, date_from: str = "", date_to: str = ""):
        """Funnel conversion + churn insight + recommended next steps in one response."""
        step_list = [step.strip() for step in steps.split(",") if step.strip()]
        funnel_sql = sql_templates.funnel_conversion(events_table, step_list, date_from or None, date_to or None)
        churn_sql = sql_templates.churn_analysis(events_table)
        funnel_results = self.connector.query(funnel_sql)
        churn_results = self.connector.query(churn_sql)

        parts = []
        if funnel_results:
            first = funnel_results[0]["users"] if funnel_results else 0
            enhanced = []
            for index, row in enumerate(funnel_results):
                step_rate = (row["users"] / first * 100) if first > 0 else 0
                drop = 0
                if index > 0:
                    prev = funnel_results[index - 1]["users"]
                    drop = ((prev - row["users"]) / prev * 100) if prev > 0 else 0
                enhanced.append({
                    "Step": f"{index + 1}. {row['step']}",
                    "Users": row["users"],
                    "Conversion": f"{step_rate:.1f}%",
                    "Drop-off": f"{drop:.1f}%" if index > 0 else "—",
                })
            parts.append(format_table(enhanced, "🔄 Funnel Conversion"))
            if len(enhanced) > 1:
                drops = [(e["Step"], float(e["Drop-off"].replace("%", ""))) for e in enhanced[1:]]
                worst = max(drops, key=lambda x: x[1])
                parts.append(format_insight(
                    f"Biggest drop-off at **{worst[0]}** ({worst[1]:.1f}% lost)."
                ))
        else:
            parts.append("_No funnel data found._")

        if churn_results:
            parts.append(format_table(churn_results, "🚨 Churn Signals"))

        actions = [
            "Fix the highest-drop funnel step first.",
            "Launch a re-engagement sequence for at-risk users.",
            "A/B test onboarding messaging at weak steps.",
        ]
        parts.append(format_actions(actions))
        body = "\n".join(parts)
        return build_tool_envelope("Funnel Diagnosis", body, self.connector, [events_table])

    def channel_efficiency_review(self, spend_table: str, events_table: str):
        """CAC + LTV + ROAS per channel with invest/cut/watch classification."""
        cac_results = self.connector.query(sql_templates.cac_by_channel(spend_table, events_table))
        ltv_results = self.connector.query(sql_templates.ltv_by_channel(events_table))
        ltv_map = {row["channel"]: row for row in ltv_results}

        table_rows = []
        actions = []
        for row in cac_results:
            channel = row["channel"]
            ltv_row = ltv_map.get(channel, {})
            ltv = ltv_row.get("avg_ltv", 0) or 0
            cac = row.get("cac", 0) or 0
            roas = round(ltv / cac, 2) if cac > 0 else None

            bench = cac_benchmark(channel)
            if roas is not None and bench:
                verdict = classify_metric(roas, good=bench.get("good", 3.0), ok=bench.get("ok", 1.5))
                signal = {"Good": "✅ Invest more", "Average": "👀 Watch", "Poor": "🔴 Cut/Review"}[verdict]
            elif roas is not None:
                signal = "✅ Invest more" if roas >= 3.0 else ("👀 Watch" if roas >= 1.5 else "🔴 Cut/Review")
            else:
                signal = "—"

            table_rows.append({
                "Channel": channel,
                "Spend": f"${row.get('total_spend', 0):,.0f}",
                "CAC": f"${cac:.2f}" if cac else "—",
                "LTV": f"${ltv:.2f}" if ltv else "—",
                "ROAS": f"{roas}x" if roas else "—",
                "Signal": signal,
            })

        body = format_table(table_rows, "📊 Channel Efficiency Review")
        invest = [r["Channel"] for r in table_rows if "Invest" in r.get("Signal", "")]
        cut = [r["Channel"] for r in table_rows if "Cut" in r.get("Signal", "")]
        if invest:
            actions.append(f"Scale up budget for: {', '.join(invest)}.")
        if cut:
            actions.append(f"Review or pause: {', '.join(cut)}.")
        actions.append("Validate attribution before major reallocation.")
        body += format_actions(actions)
        return build_tool_envelope("Channel Efficiency Review", body, self.connector, [spend_table, events_table])

    def anomaly_explanation(self, table: str, metric_column: str, date_column: str = "date", lookback_days: int = 30):
        """Anomalies + likely cause hypotheses + recommended actions."""
        results = self.connector.query(
            sql_templates.anomaly_detection(table, metric_column, date_column, lookback_days)
        )
        if not results:
            body = f"✅ No anomalies detected in `{metric_column}` over the past {lookback_days} days."
            return build_tool_envelope("Anomaly Explanation", body, self.connector, [table])

        body = format_table(results, f"⚠️ Anomalies: `{metric_column}` (Last {lookback_days} Days)")
        spikes = [row for row in results if "Spike" in str(row.get("status", ""))]
        drops = [row for row in results if "Drop" in str(row.get("status", ""))]

        hypotheses = []
        if spikes:
            hypotheses.append(f"**{len(spikes)} spike(s):** Check campaign launches, promotions, or bot traffic on these dates.")
        if drops:
            hypotheses.append(f"**{len(drops)} drop(s):** Check for tracking outages, product bugs, or seasonality.")

        if hypotheses:
            body += format_insight("Likely cause hypotheses:\n" + "\n".join(f"- {h}" for h in hypotheses))
            body += format_actions([
                "Cross-reference anomaly dates with deploy logs and campaign changes.",
                "Verify data pipeline integrity before drawing conclusions.",
                "Segment the anomaly by channel or cohort to isolate cause.",
            ])
        return build_tool_envelope("Anomaly Explanation", body, self.connector, [table])

    def detect_data_drift(
        self,
        table: str,
        metric_column: str,
        date_column: str = "date",
        lookback_days: int = 7,
    ):
        """Compare current period vs. previous equal-length period for a metric."""
        results = self.connector.query(
            sql_templates.detect_data_drift(table, metric_column, date_column, lookback_days)
        )
        if not results or len(results) < 2:
            body = (
                f"Not enough data to detect drift in `{metric_column}` "
                f"over the past {lookback_days} days."
            )
            return build_tool_envelope("Data Drift", body, self.connector, [table])

        current = next((r for r in results if r.get("period") == "current"), results[0])
        previous = next((r for r in results if r.get("period") == "previous"), results[1])

        cur_avg = float(current.get("avg_val") or 0)
        prev_avg = float(previous.get("avg_val") or 0)
        pct_avg = ((cur_avg - prev_avg) / prev_avg * 100) if prev_avg else None

        cur_total = float(current.get("total_val") or 0)
        prev_total = float(previous.get("total_val") or 0)
        pct_total = ((cur_total - prev_total) / prev_total * 100) if prev_total else None

        table_rows = [
            {
                "Period": f"Last {lookback_days}d",
                f"Avg {metric_column}": f"{cur_avg:.2f}",
                f"Total {metric_column}": f"{cur_total:.2f}",
                "Rows": current.get("n_rows", 0),
            },
            {
                "Period": f"Prev {lookback_days}d",
                f"Avg {metric_column}": f"{prev_avg:.2f}",
                f"Total {metric_column}": f"{prev_total:.2f}",
                "Rows": previous.get("n_rows", 0),
            },
        ]
        body = format_table(table_rows, f"📉 Data Drift: `{metric_column}` ({lookback_days}d vs prior)")

        if pct_avg is not None:
            direction = "up" if pct_avg > 0 else "down"
            body += format_insight(
                f"Average `{metric_column}` is **{direction} {abs(pct_avg):.1f}%** "
                f"vs. the prior {lookback_days}-day window."
            )
        if pct_total is not None and abs(pct_total) > 20:
            body += format_actions([
                f"{'Increase/spike' if pct_total > 0 else 'Drop'} exceeds 20% — investigate root cause.",
                "Check for campaign changes, seasonality, or data pipeline issues.",
                "Compare against channel-level breakdowns to isolate the driver.",
            ])
        return build_tool_envelope("Data Drift", body, self.connector, [table])

    def funnel_ab_comparison(
        self,
        events_table: str,
        steps: str,
        period_a_label: str = "Period A",
        period_a_start: str = "",
        period_a_end: str = "",
        period_b_label: str = "Period B",
        period_b_start: str = "",
        period_b_end: str = "",
    ):
        """Compare funnel conversion between two time periods side by side."""
        step_list = [s.strip() for s in steps.split(",") if s.strip()]
        results_a = self.connector.query(
            sql_templates.funnel_conversion(
                events_table, step_list, period_a_start or None, period_a_end or None
            )
        )
        results_b = self.connector.query(
            sql_templates.funnel_conversion(
                events_table, step_list, period_b_start or None, period_b_end or None
            )
        )

        if not results_a and not results_b:
            body = "No funnel data found for either period."
            return build_tool_envelope("Funnel A/B Comparison", body, self.connector, [events_table])

        first_a = results_a[0]["users"] if results_a else 0
        first_b = results_b[0]["users"] if results_b else 0
        b_map = {row["step"]: row for row in results_b}

        rows = []
        for row in results_a:
            step = row["step"]
            conv_a = (row["users"] / first_a * 100) if first_a > 0 else 0
            b_row = b_map.get(step)
            conv_b = (b_row["users"] / first_b * 100) if b_row and first_b > 0 else 0
            delta = conv_b - conv_a
            rows.append({
                "Step": step,
                f"{period_a_label}": f"{conv_a:.1f}%",
                f"{period_b_label}": f"{conv_b:.1f}%",
                "Δ (pp)": f"{delta:+.1f}",
            })

        body = format_table(rows, f"🔄 Funnel A/B: {period_a_label} vs {period_b_label}")
        improved = [r for r in rows if float(r["Δ (pp)"]) > 2]
        regressed = [r for r in rows if float(r["Δ (pp)"]) < -2]
        insights = []
        if improved:
            insights.append(f"{len(improved)} step(s) improved in {period_b_label}")
        if regressed:
            insights.append(f"{len(regressed)} step(s) regressed in {period_b_label}")
        if insights:
            body += format_insight(" | ".join(insights) + ".")
            body += format_actions([
                "Investigate what changed between periods (product, copy, targeting).",
                "Run a significance test before shipping the winning variant.",
                "Focus on steps with the largest absolute delta.",
            ])
        return build_tool_envelope("Funnel A/B Comparison", body, self.connector, [events_table])
