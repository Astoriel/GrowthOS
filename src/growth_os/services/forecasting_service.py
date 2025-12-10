"""Time-series forecasting service."""
from __future__ import annotations

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.domain.models import ToolEnvelope
from growth_os.presentation.markdown import format_actions, format_insight, format_table
from growth_os.query.builder import safe_identifier
from growth_os.semantic.forecasting import ForecastPoint, exponential_smoothing, linear_forecast
from growth_os.services._helpers import build_tool_envelope


class ForecastingService:
    """Time-series forecasting for growth metrics."""

    def __init__(self, connector: GrowthConnector):
        self.connector = connector

    def forecast_metric(
        self,
        table: str,
        metric_col: str,
        date_col: str = "date",
        horizon: int = 30,
        method: str = "linear",
    ) -> ToolEnvelope:
        """Forecast a single daily metric over a future horizon.

        Queries the last 90 days of data, fits a trend, and returns a table
        of (Day, Forecast, Lower, Upper) covering the requested horizon.
        """
        t = safe_identifier(table)
        m = safe_identifier(metric_col)
        d = safe_identifier(date_col)

        sql = (
            f"SELECT {d}, SUM({m}) AS val "
            f"FROM {t} "
            f"WHERE {d} >= CURRENT_DATE - INTERVAL '90 days' "
            f"GROUP BY {d} "
            f"ORDER BY {d}"
        )
        rows = self.connector.query(sql)
        values = [float(row["val"] or 0) for row in rows]

        if method == "exponential":
            forecast_points = exponential_smoothing(values, horizon=horizon)
        else:
            forecast_points = linear_forecast(values, horizon=horizon)

        table_data = [
            {
                "Day": p.day_offset,
                "Forecast": p.value,
                "Lower": p.lower,
                "Upper": p.upper,
            }
            for p in forecast_points
        ]

        body = format_table(table_data, f"📈 Forecast: {metric_col} (next {horizon} days)")

        if forecast_points:
            first_val = forecast_points[0].value
            last_val = forecast_points[-1].value
            trend = _trend_label(forecast_points)
            body += format_insight(
                f"Trend is **{trend}** — forecast moves from {first_val} to {last_val} "
                f"over {horizon} days using {method} regression."
            )
            if trend == "Up":
                actions = [
                    f"Scale investment in {table} to capitalize on upward momentum.",
                    "Validate forecast assumptions with recent market data.",
                    "Set alerts if actuals fall below the lower confidence band.",
                ]
            elif trend == "Down":
                actions = [
                    f"Investigate root causes of declining {metric_col} in {table}.",
                    "Consider tactical interventions to reverse the trend.",
                    "Monitor weekly to catch an inflection point early.",
                ]
            else:
                actions = [
                    f"{metric_col} is forecast to remain flat — look for growth levers.",
                    "Segment by channel or cohort to find hidden growth pockets.",
                    "Revisit acquisition and activation strategies.",
                ]
            body += format_actions(actions)

        return build_tool_envelope(f"Forecast: {metric_col}", body, self.connector, [table])

    def forecast_growth_kpis(
        self,
        spend_table: str = "marketing_spend",
        events_table: str = "user_events",
        horizon: int = 30,
    ) -> ToolEnvelope:
        """Forecast key growth KPIs and present a summary table.

        Returns a KPI summary with: current 7-day average, forecast value at
        day {horizon}, and trend direction for spend and daily active users.
        """
        spend_t = safe_identifier(spend_table)
        events_t = safe_identifier(events_table)

        # -- spend: current 7d average --
        spend_avg_sql = (
            f"SELECT COALESCE(SUM(spend), 0) AS val "
            f"FROM {spend_t} "
            f"WHERE date >= CURRENT_DATE - INTERVAL '7 days'"
        )
        spend_avg_rows = self.connector.query(spend_avg_sql)
        current_spend_avg = float((spend_avg_rows[0]["val"] or 0) if spend_avg_rows else 0) / 7

        # -- spend: 90-day history for forecast --
        spend_hist_sql = (
            f"SELECT date, SUM(spend) AS val "
            f"FROM {spend_t} "
            f"WHERE date >= CURRENT_DATE - INTERVAL '90 days' "
            f"GROUP BY date ORDER BY date"
        )
        spend_hist = self.connector.query(spend_hist_sql)
        spend_values = [float(row["val"] or 0) for row in spend_hist]
        spend_forecast = linear_forecast(spend_values, horizon=horizon)
        spend_day_horizon = spend_forecast[-1].value if spend_forecast else round(current_spend_avg, 2)
        spend_trend = _trend_label(spend_forecast)

        # -- daily active users: current 7d average --
        users_avg_sql = (
            f"SELECT COALESCE(COUNT(DISTINCT user_id), 0) AS val "
            f"FROM {events_t} "
            f"WHERE event_date >= CURRENT_DATE - INTERVAL '7 days'"
        )
        users_avg_rows = self.connector.query(users_avg_sql)
        current_users_avg = float((users_avg_rows[0]["val"] or 0) if users_avg_rows else 0) / 7

        # -- daily active users: 90-day history for forecast --
        users_hist_sql = (
            f"SELECT event_date AS date, COUNT(DISTINCT user_id) AS val "
            f"FROM {events_t} "
            f"WHERE event_date >= CURRENT_DATE - INTERVAL '90 days' "
            f"GROUP BY event_date ORDER BY event_date"
        )
        users_hist = self.connector.query(users_hist_sql)
        users_values = [float(row["val"] or 0) for row in users_hist]
        users_forecast = linear_forecast(users_values, horizon=horizon)
        users_day_horizon = users_forecast[-1].value if users_forecast else round(current_users_avg, 2)
        users_trend = _trend_label(users_forecast)

        summary = [
            {
                "KPI": "Daily Spend",
                "Current (7d avg)": round(current_spend_avg, 2),
                f"Forecast (day {horizon})": spend_day_horizon,
                "Trend": spend_trend,
            },
            {
                "KPI": "Daily Active Users",
                "Current (7d avg)": round(current_users_avg, 2),
                f"Forecast (day {horizon})": users_day_horizon,
                "Trend": users_trend,
            },
        ]

        body = format_table(summary, f"🔮 Growth KPI Forecast (next {horizon} days)")
        body += format_insight(
            "Forecasts use linear regression on 90-day history. "
            "Treat as directional guidance, not precise predictions."
        )
        body += format_actions([
            "Cross-check forecasts against your budget plan.",
            "Investigate any KPI trending down with a drill-down analysis.",
            "Revisit forecasts monthly as new actuals come in.",
        ])

        return build_tool_envelope(
            "Growth KPI Forecast", body, self.connector, [spend_table, events_table]
        )


def _trend_label(points: list[ForecastPoint]) -> str:
    """Return 'Up', 'Down', or 'Flat' based on first vs last forecast value."""
    if not points:
        return "Flat"
    first = points[0].value
    last = points[-1].value
    if last > first * 1.02:
        return "Up"
    if last < first * 0.98:
        return "Down"
    return "Flat"
