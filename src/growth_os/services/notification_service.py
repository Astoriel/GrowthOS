"""Notification service for scheduled alerts and webhook-based reporting."""
from __future__ import annotations

from datetime import datetime

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.connectors.webhook import WebhookPayload, dispatch_webhook
from growth_os.presentation.markdown import format_actions, format_insight, format_table
from growth_os.query.builder import safe_identifier
from growth_os.semantic import metrics as sql_templates
from growth_os.services._helpers import build_tool_envelope


class NotificationService:
    def __init__(self, connector: GrowthConnector):
        self.connector = connector

    def drift_alert(
        self,
        table: str,
        metric_column: str,
        date_column: str = "date",
        lookback_days: int = 7,
        webhook_url: str = "",
        threshold_pct: float = 20.0,
    ):
        results = self.connector.query(
            sql_templates.detect_data_drift(table, metric_column, date_column, lookback_days)
        )
        if not results or len(results) < 2:
            body = (
                f"Not enough data to detect drift in `{metric_column}` "
                f"over the past {lookback_days} days."
            )
            return build_tool_envelope("Drift Alert", body, self.connector, [table])

        current = next((r for r in results if r.get("period") == "current"), results[0])
        previous = next((r for r in results if r.get("period") == "previous"), results[1])

        cur_avg = float(current.get("avg_val") or 0)
        prev_avg = float(previous.get("avg_val") or 0)
        pct_change = ((cur_avg - prev_avg) / prev_avg * 100) if prev_avg else None

        cur_total = float(current.get("total_val") or 0)
        prev_total = float(previous.get("total_val") or 0)

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
        body = format_table(table_rows, f"Drift Alert: `{metric_column}` ({lookback_days}d vs prior)")

        webhook_fired = False
        direction = None

        if pct_change is not None:
            direction = "up" if pct_change > 0 else "down"
            body += format_insight(
                f"`{metric_column}` is **{direction} {abs(pct_change):.1f}%** "
                f"vs. the prior {lookback_days}-day window."
            )
            if abs(pct_change) >= threshold_pct:
                body += format_actions(
                    [
                        f"{'Spike' if pct_change > 0 else 'Drop'} of {abs(pct_change):.1f}% exceeds threshold — investigate root cause.",
                        "Check for campaign changes, seasonality, or data pipeline issues.",
                        "Compare against channel-level breakdowns to isolate the driver.",
                    ]
                )
                if webhook_url:
                    payload = WebhookPayload(
                        event="drift_alert",
                        data={
                            "table": table,
                            "metric_column": metric_column,
                            "pct_change": round(pct_change, 2),
                            "lookback_days": lookback_days,
                            "direction": direction,
                            "timestamp": datetime.utcnow().isoformat() + "Z",
                        },
                    )
                    webhook_fired = dispatch_webhook(webhook_url, payload)

        if webhook_fired:
            body += f"\nWebhook fired to {webhook_url}."
        elif webhook_url and pct_change is not None and abs(pct_change) >= threshold_pct:
            body += f"\nWebhook to {webhook_url} failed."
        elif webhook_url:
            body += f"\nDrift within threshold ({threshold_pct}%) — webhook not fired."
        else:
            body += "\nNo webhook configured."

        return build_tool_envelope("Drift Alert", body, self.connector, [table])

    def scheduled_report_preview(
        self,
        spend_table: str = "marketing_spend",
        events_table: str = "user_events",
        webhook_url: str = "",
    ):
        t_events = safe_identifier(events_table)
        signups_sql = (
            f"SELECT COUNT(DISTINCT user_id) AS signups, "
            f"COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchasers "
            f"FROM {t_events} "
            f"WHERE date >= CURRENT_DATE - INTERVAL '7 days'"
        )

        summary_results = self.connector.query(sql_templates.growth_summary(spend_table, events_table))
        signups_results = self.connector.query(signups_sql)

        parts = []

        if summary_results:
            parts.append(format_table(summary_results, "Weekly Growth Summary (Last 7 Days)"))
        else:
            parts.append("_No summary data found._")

        signups = 0
        purchasers = 0
        if signups_results:
            row = signups_results[0]
            signups = int(row.get("signups") or 0)
            purchasers = int(row.get("purchasers") or 0)
            signup_rows = [
                {"Metric": "Signups (7d)", "Value": signups},
                {"Metric": "Purchasers (7d)", "Value": purchasers},
            ]
            parts.append(format_table(signup_rows, "User Activity"))

        body = "\n\n".join(parts)

        if webhook_url:
            payload = WebhookPayload(
                event="weekly_report",
                data={
                    "spend_table": spend_table,
                    "events_table": events_table,
                    "signups_7d": signups,
                    "purchasers_7d": purchasers,
                    "summary_rows": len(summary_results) if summary_results else 0,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
            )
            fired = dispatch_webhook(webhook_url, payload)
            if fired:
                body += f"\nWebhook fired to {webhook_url}."
            else:
                body += f"\nWebhook to {webhook_url} failed."
        else:
            body += "\nNo webhook configured."

        return build_tool_envelope(
            "Scheduled Report Preview",
            body,
            self.connector,
            [spend_table, events_table],
        )
