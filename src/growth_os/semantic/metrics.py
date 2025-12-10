"""Pre-built SQL templates for marketing analytics."""

from __future__ import annotations

from growth_os.query.builder import safe_date, safe_identifier, safe_sql_string


def cac_by_channel(spend_table: str, events_table: str, channel_col: str = "channel") -> str:
    """Calculate CAC per channel."""
    spend_table = safe_identifier(spend_table)
    events_table = safe_identifier(events_table)
    channel_col = safe_identifier(channel_col)
    return f"""
    WITH total_spend AS (
        SELECT
            {channel_col},
            SUM(spend) AS total_spend
        FROM {spend_table}
        GROUP BY 1
    ),
    signups AS (
        SELECT
            utm_source AS {channel_col},
            COUNT(DISTINCT user_id) AS total_users
        FROM {events_table}
        WHERE event_type = 'signup'
        GROUP BY 1
    )
    SELECT
        s.{channel_col} AS channel,
        s.total_spend,
        COALESCE(u.total_users, 0) AS users_acquired,
        CASE
            WHEN COALESCE(u.total_users, 0) > 0
            THEN ROUND(s.total_spend / u.total_users, 2)
            ELSE NULL
        END AS cac
    FROM total_spend s
    LEFT JOIN signups u ON s.{channel_col} = u.{channel_col}
    ORDER BY cac NULLS LAST
    """


def ltv_by_channel(events_table: str) -> str:
    """Calculate observed LTV per acquisition channel."""
    events_table = safe_identifier(events_table)
    return f"""
    WITH user_revenue AS (
        SELECT
            user_id,
            utm_source AS channel,
            SUM(revenue) AS total_revenue,
            MIN(event_date) AS first_event,
            MAX(event_date) AS last_event
        FROM {events_table}
        GROUP BY 1, 2
    )
    SELECT
        channel,
        COUNT(DISTINCT user_id) AS users,
        ROUND(AVG(total_revenue), 2) AS avg_ltv,
        ROUND(SUM(total_revenue), 2) AS total_revenue,
        ROUND(AVG(DATEDIFF('day', first_event, last_event)), 0) AS avg_lifetime_days
    FROM user_revenue
    GROUP BY 1
    ORDER BY avg_ltv DESC
    """


def cohort_retention(events_table: str, period: str = "month") -> str:
    """Build a cohort retention matrix."""
    events_table = safe_identifier(events_table)
    period = _safe_period(period)
    return f"""
    WITH cohorts AS (
        SELECT
            user_id,
            DATE_TRUNC('{period}', MIN(event_date)) AS cohort_{period}
        FROM {events_table}
        GROUP BY 1
    ),
    activity AS (
        SELECT
            c.user_id,
            c.cohort_{period},
            DATEDIFF('{period}', c.cohort_{period}, DATE_TRUNC('{period}', e.event_date)) AS period_number
        FROM {events_table} e
        JOIN cohorts c USING (user_id)
    ),
    cohort_sizes AS (
        SELECT cohort_{period}, COUNT(DISTINCT user_id) AS cohort_size
        FROM cohorts
        GROUP BY 1
    )
    SELECT
        a.cohort_{period},
        cs.cohort_size,
        a.period_number,
        COUNT(DISTINCT a.user_id) AS active_users,
        ROUND(100.0 * COUNT(DISTINCT a.user_id) / cs.cohort_size, 1) AS retention_pct
    FROM activity a
    JOIN cohort_sizes cs ON a.cohort_{period} = cs.cohort_{period}
    GROUP BY 1, 2, 3
    ORDER BY 1, 3
    """


def funnel_conversion(
    events_table: str,
    steps: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
) -> str:
    """Build a funnel query for a sequence of events."""
    events_table = safe_identifier(events_table)
    date_filter = ""
    if date_from:
        date_filter += f" AND event_date >= '{safe_date(date_from)}'"
    if date_to:
        date_filter += f" AND event_date <= '{safe_date(date_to)}'"

    ctes = []
    selects = []
    for index, step in enumerate(steps):
        step_name = safe_sql_string(step.strip())
        cte_name = f"step_{index}"
        if index == 0:
            ctes.append(
                f"""
    {cte_name} AS (
        SELECT DISTINCT user_id
        FROM {events_table}
        WHERE event_type = '{step_name}'{date_filter}
    )"""
            )
        else:
            previous = f"step_{index - 1}"
            ctes.append(
                f"""
    {cte_name} AS (
        SELECT DISTINCT e.user_id
        FROM {events_table} e
        JOIN {previous} p ON e.user_id = p.user_id
        WHERE e.event_type = '{step_name}'{date_filter}
    )"""
            )
        selects.append(f"SELECT '{step_name}' AS step, {index + 1} AS step_order, COUNT(*) AS users FROM {cte_name}")

    return "WITH" + ",".join(ctes) + "\n" + "\nUNION ALL\n".join(selects) + "\nORDER BY step_order"


def channel_attribution(spend_table: str, events_table: str) -> str:
    """Compute revenue attribution by channel."""
    spend_table = safe_identifier(spend_table)
    events_table = safe_identifier(events_table)
    return f"""
    WITH channel_spend AS (
        SELECT channel, SUM(spend) AS total_spend
        FROM {spend_table}
        GROUP BY 1
    ),
    channel_revenue AS (
        SELECT
            utm_source AS channel,
            COUNT(DISTINCT user_id) AS converters,
            SUM(revenue) AS total_revenue
        FROM {events_table}
        WHERE event_type = 'purchase'
        GROUP BY 1
    )
    SELECT
        COALESCE(s.channel, r.channel) AS channel,
        COALESCE(s.total_spend, 0) AS spend,
        COALESCE(r.converters, 0) AS conversions,
        COALESCE(r.total_revenue, 0) AS revenue,
        CASE
            WHEN COALESCE(s.total_spend, 0) > 0
            THEN ROUND(COALESCE(r.total_revenue, 0) / s.total_spend, 2)
            ELSE NULL
        END AS roas
    FROM channel_spend s
    FULL OUTER JOIN channel_revenue r ON s.channel = r.channel
    ORDER BY revenue DESC
    """


def churn_analysis(events_table: str, inactive_days: int = 30) -> str:
    """Segment users based on inactivity."""
    events_table = safe_identifier(events_table)
    inactive_days = max(int(inactive_days), 1)
    at_risk_threshold = max(inactive_days // 3, 1)
    return f"""
    WITH user_activity AS (
        SELECT
            user_id,
            MIN(event_date) AS first_seen,
            MAX(event_date) AS last_seen,
            COUNT(*) AS total_events,
            DATEDIFF('day', MAX(event_date), CURRENT_DATE) AS days_inactive
        FROM {events_table}
        GROUP BY 1
    )
    SELECT
        CASE
            WHEN days_inactive <= {at_risk_threshold} THEN '🟢 Active'
            WHEN days_inactive <= {inactive_days} THEN '🟡 At Risk'
            ELSE '🔴 Churned'
        END AS segment,
        COUNT(*) AS users,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
        ROUND(AVG(total_events), 0) AS avg_events,
        ROUND(AVG(days_inactive), 0) AS avg_days_inactive
    FROM user_activity
    GROUP BY 1
    ORDER BY 1
    """


def anomaly_detection(table: str, metric_col: str, date_col: str = "date", lookback_days: int = 30) -> str:
    """Detect simple z-score anomalies."""
    table = safe_identifier(table)
    metric_col = safe_identifier(metric_col)
    date_col = safe_identifier(date_col)
    lookback_days = max(int(lookback_days), 1)
    return f"""
    WITH daily_metrics AS (
        SELECT
            {date_col},
            SUM({metric_col}) AS daily_value
        FROM {table}
        WHERE {date_col} >= CURRENT_DATE - INTERVAL '{lookback_days} days'
        GROUP BY 1
    ),
    stats AS (
        SELECT
            AVG(daily_value) AS mean_val,
            STDDEV(daily_value) AS std_val
        FROM daily_metrics
    ),
    scored AS (
        SELECT
            dm.{date_col},
            dm.daily_value,
            s.mean_val,
            s.std_val,
            CASE
                WHEN s.std_val > 0
                THEN ROUND((dm.daily_value - s.mean_val) / s.std_val, 2)
                ELSE 0
            END AS z_score
        FROM daily_metrics dm
        CROSS JOIN stats s
    )
    SELECT
        {date_col},
        daily_value,
        ROUND(mean_val, 2) AS avg_value,
        z_score,
        CASE
            WHEN z_score > 2 THEN '🔴 Spike'
            WHEN z_score < -2 THEN '🔴 Drop'
            WHEN z_score > 1.5 THEN '🟡 Elevated'
            WHEN z_score < -1.5 THEN '🟡 Below Normal'
            ELSE '🟢 Normal'
        END AS status
    FROM scored
    WHERE ABS(z_score) > 1.5
    ORDER BY ABS(z_score) DESC
    """


def growth_summary(spend_table: str, events_table: str) -> str:
    """Generate a weekly summary query."""
    spend_table = safe_identifier(spend_table)
    events_table = safe_identifier(events_table)
    return f"""
    WITH current_period AS (
        SELECT
            SUM(spend) AS spend,
            SUM(clicks) AS clicks,
            SUM(conversions) AS conversions
        FROM {spend_table}
        WHERE date >= CURRENT_DATE - INTERVAL '7 days'
    ),
    previous_period AS (
        SELECT
            SUM(spend) AS spend,
            SUM(clicks) AS clicks,
            SUM(conversions) AS conversions
        FROM {spend_table}
        WHERE date >= CURRENT_DATE - INTERVAL '14 days'
          AND date < CURRENT_DATE - INTERVAL '7 days'
    ),
    current_revenue AS (
        SELECT SUM(revenue) AS revenue, COUNT(DISTINCT user_id) AS active_users
        FROM {events_table}
        WHERE event_date >= CURRENT_DATE - INTERVAL '7 days'
    ),
    previous_revenue AS (
        SELECT SUM(revenue) AS revenue, COUNT(DISTINCT user_id) AS active_users
        FROM {events_table}
        WHERE event_date >= CURRENT_DATE - INTERVAL '14 days'
          AND event_date < CURRENT_DATE - INTERVAL '7 days'
    )
    SELECT
        'Revenue' AS metric,
        COALESCE(cr.revenue, 0) AS current_value,
        COALESCE(pr.revenue, 0) AS previous_value,
        CASE WHEN COALESCE(pr.revenue, 0) > 0
            THEN ROUND(100.0 * (cr.revenue - pr.revenue) / pr.revenue, 1)
            ELSE NULL END AS change_pct
    FROM current_revenue cr, previous_revenue pr
    UNION ALL
    SELECT
        'Spend',
        COALESCE(c.spend, 0),
        COALESCE(p.spend, 0),
        CASE WHEN COALESCE(p.spend, 0) > 0
            THEN ROUND(100.0 * (c.spend - p.spend) / p.spend, 1)
            ELSE NULL END
    FROM current_period c, previous_period p
    UNION ALL
    SELECT
        'Conversions',
        COALESCE(c.conversions, 0),
        COALESCE(p.conversions, 0),
        CASE WHEN COALESCE(p.conversions, 0) > 0
            THEN ROUND(100.0 * (c.conversions - p.conversions) / p.conversions, 1)
            ELSE NULL END
    FROM current_period c, previous_period p
    UNION ALL
    SELECT
        'Active Users',
        COALESCE(cr.active_users, 0),
        COALESCE(pr.active_users, 0),
        CASE WHEN COALESCE(pr.active_users, 0) > 0
            THEN ROUND(100.0 * (cr.active_users - pr.active_users) / pr.active_users, 1)
            ELSE NULL END
    FROM current_revenue cr, previous_revenue pr
    """


def _safe_period(period: str) -> str:
    """Validate retention period values."""
    period = period.lower().strip()
    if period not in {"week", "month"}:
        raise ValueError("period must be 'week' or 'month'")
    return period


# Alias for clarity — the original churn_analysis uses inactivity-based logic
churn_analysis_inactivity = churn_analysis


def churn_analysis_subscription(events_table: str, cancelled_event: str = "cancel") -> str:
    """Segment users based on explicit subscription cancellation events.

    Classifies users as Active (no cancel), Recently Cancelled (last 30d), or Churned.
    """
    events_table = safe_identifier(events_table)
    cancelled_event = safe_sql_string(cancelled_event.strip())
    return f"""
    WITH cancellations AS (
        SELECT
            user_id,
            MAX(event_date) AS cancelled_at
        FROM {events_table}
        WHERE event_type = '{cancelled_event}'
        GROUP BY 1
    ),
    all_users AS (
        SELECT DISTINCT user_id FROM {events_table}
    )
    SELECT
        CASE
            WHEN c.cancelled_at IS NULL THEN '🟢 Active'
            WHEN DATEDIFF('day', c.cancelled_at, CURRENT_DATE) <= 30 THEN '🟡 Recently Cancelled'
            ELSE '🔴 Churned'
        END AS segment,
        COUNT(*) AS users,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM all_users u
    LEFT JOIN cancellations c ON u.user_id = c.user_id
    GROUP BY 1
    ORDER BY 1
    """


def churn_analysis_event_based(events_table: str, churn_event: str = "churn") -> str:
    """Segment users based on explicit churn events.

    Assumes there is a dedicated churn event (e.g. 'plan_cancelled', 'account_closed').
    Classifies users as Active (no churn event) or Churned.
    """
    events_table = safe_identifier(events_table)
    churn_event = safe_sql_string(churn_event.strip())
    return f"""
    WITH churned_users AS (
        SELECT DISTINCT user_id
        FROM {events_table}
        WHERE event_type = '{churn_event}'
    ),
    all_users AS (
        SELECT
            user_id,
            MIN(event_date) AS first_seen,
            MAX(event_date) AS last_seen
        FROM {events_table}
        GROUP BY 1
    )
    SELECT
        CASE WHEN c.user_id IS NULL THEN '🟢 Active' ELSE '🔴 Churned' END AS segment,
        COUNT(*) AS users,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
        ROUND(AVG(DATEDIFF('day', a.first_seen, a.last_seen)), 0) AS avg_lifetime_days
    FROM all_users a
    LEFT JOIN churned_users c ON a.user_id = c.user_id
    GROUP BY 1
    ORDER BY 1
    """


def detect_data_drift(
    table: str,
    metric_column: str,
    date_column: str = "date",
    lookback_days: int = 7,
) -> str:
    """Compare current period vs. previous equal-length period for a numeric metric.

    Returns two rows — 'current' and 'previous' — with avg_val, total_val, n_rows.
    """
    table = safe_identifier(table)
    metric_col = safe_identifier(metric_column)
    date_col = safe_identifier(date_column)
    lookback_days = max(int(lookback_days), 1)
    double_days = lookback_days * 2
    return f"""
    WITH current_period AS (
        SELECT
            'current' AS period,
            ROUND(AVG({metric_col}), 4) AS avg_val,
            ROUND(SUM({metric_col}), 4) AS total_val,
            COUNT(*) AS n_rows
        FROM {table}
        WHERE {date_col} >= CURRENT_DATE - INTERVAL '{lookback_days} DAYS'
    ),
    prev_period AS (
        SELECT
            'previous' AS period,
            ROUND(AVG({metric_col}), 4) AS avg_val,
            ROUND(SUM({metric_col}), 4) AS total_val,
            COUNT(*) AS n_rows
        FROM {table}
        WHERE {date_col} >= CURRENT_DATE - INTERVAL '{double_days} DAYS'
          AND {date_col} < CURRENT_DATE - INTERVAL '{lookback_days} DAYS'
    )
    SELECT * FROM current_period
    UNION ALL
    SELECT * FROM prev_period
    ORDER BY period DESC
    """
