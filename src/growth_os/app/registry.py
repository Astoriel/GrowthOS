"""MCP tool registration."""

from __future__ import annotations

import functools
import time
from datetime import date
from typing import Any, Callable

from mcp.server.fastmcp import FastMCP

from growth_os.config.profiles import WorkspaceProfile, apply_profile, list_profiles, load_profile, save_profile
from growth_os.config.settings import settings as _settings
from growth_os.connectors import get_connector
from growth_os.connectors.amplitude import AmplitudeConnector
from growth_os.connectors.hubspot import HubSpotConnector
from growth_os.connectors.mixpanel import MixpanelConnector
from growth_os.observability.audit import AuditEvent, write_audit_event
from growth_os.presentation import wrap_tool_envelope
from growth_os.services import AnalysisService, CatalogService, DiagnosticsService, IntegrationService, ReportingService
from growth_os.services.forecasting_service import ForecastingService
from growth_os.services.notification_service import NotificationService


def _catalog_service() -> CatalogService:
    return CatalogService(get_connector())


def _analysis_service() -> AnalysisService:
    return AnalysisService(get_connector())


def _diagnostics_service() -> DiagnosticsService:
    return DiagnosticsService(get_connector())


def _reporting_service() -> ReportingService:
    return ReportingService(get_connector())


def _integration_service() -> IntegrationService:
    return IntegrationService(get_connector())


def _notification_service() -> NotificationService:
    return NotificationService(get_connector())


def _forecasting_service() -> ForecastingService:
    return ForecastingService(get_connector())


def _with_audit(fn: Callable[..., Any], tool_name: str) -> Callable[..., Any]:
    """Wrap a tool function with audit logging."""
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.monotonic()
        try:
            result = fn(*args, **kwargs)
            write_audit_event(AuditEvent(
                name="tool_call",
                detail=f"tool={tool_name}",
                tool_name=tool_name,
                duration_ms=round((time.monotonic() - start) * 1000, 1),
                status="ok",
            ))
            return result
        except Exception as exc:
            write_audit_event(AuditEvent(
                name="tool_error",
                detail=f"tool={tool_name} error={exc}",
                tool_name=tool_name,
                duration_ms=round((time.monotonic() - start) * 1000, 1),
                status="error",
            ))
            raise
    return wrapper


# ---------------------------------------------------------------------------
# Tool functions
# ---------------------------------------------------------------------------

def health_check_tool() -> str:
    """Return server status, connector summary, and current date."""
    connector = get_connector()
    tables = connector.get_tables()
    return (
        f"## GrowthOS Health Check\n\n"
        f"- **Status:** OK\n"
        f"- **Date:** {date.today()}\n"
        f"- **Loaded tables:** {len(tables)}\n"
        f"- **Tables:** {', '.join(tables) if tables else '_none_'}\n"
    )


def list_tables_tool() -> str:
    return wrap_tool_envelope(_catalog_service().list_tables())


def describe_table_tool(table_name: str) -> str:
    return wrap_tool_envelope(_catalog_service().describe_table(table_name))


def run_query_tool(sql: str, offset: int = 0, limit: int = 50) -> str:
    return wrap_tool_envelope(_catalog_service().run_query(sql, offset=offset, limit=limit))


def analyze_funnel_tool(
    events_table: str = "user_events",
    steps: str = "signup,activation,purchase",
    date_from: str = "",
    date_to: str = "",
) -> str:
    return wrap_tool_envelope(_analysis_service().analyze_funnel(events_table, steps, date_from, date_to))


def compute_cac_ltv_tool(spend_table: str = "marketing_spend", events_table: str = "user_events") -> str:
    return wrap_tool_envelope(_analysis_service().compute_cac_ltv(spend_table, events_table))


def cohort_retention_tool(events_table: str = "user_events", period: str = "month") -> str:
    return wrap_tool_envelope(_analysis_service().cohort_retention(events_table, period))


def channel_attribution_tool(spend_table: str = "marketing_spend", events_table: str = "user_events") -> str:
    return wrap_tool_envelope(_analysis_service().channel_attribution(spend_table, events_table))


def analyze_churn_tool(events_table: str = "user_events", inactive_days: int = 30) -> str:
    return wrap_tool_envelope(_analysis_service().analyze_churn(events_table, inactive_days))


def detect_anomalies_tool(
    table: str = "marketing_spend",
    metric_column: str = "spend",
    date_column: str = "date",
    lookback_days: int = 30,
) -> str:
    return wrap_tool_envelope(_analysis_service().detect_anomalies(table, metric_column, date_column, lookback_days))


def growth_summary_tool(spend_table: str = "marketing_spend", events_table: str = "user_events") -> str:
    return wrap_tool_envelope(_reporting_service().growth_summary(spend_table, events_table))


def weekly_growth_review_tool(spend_table: str = "marketing_spend", events_table: str = "user_events") -> str:
    return wrap_tool_envelope(_reporting_service().weekly_growth_review(spend_table, events_table))


def executive_summary_tool(spend_table: str = "marketing_spend", events_table: str = "user_events") -> str:
    return wrap_tool_envelope(_reporting_service().executive_summary(spend_table, events_table))


def paid_growth_review_tool(
    spend_tables: str = "meta_marketing_spend,google_marketing_spend",
    invoices_table: str = "stripe_invoices",
) -> str:
    return wrap_tool_envelope(_reporting_service().paid_growth_review(spend_tables, invoices_table))


def campaign_performance_review_tool(
    spend_tables: str = "meta_marketing_spend,google_marketing_spend",
    min_spend: float = 50,
    limit: int = 5,
) -> str:
    return wrap_tool_envelope(_reporting_service().campaign_performance_review(spend_tables, min_spend, limit))


def attribution_bridge_review_tool(
    spend_tables: str = "meta_marketing_spend,google_marketing_spend",
    events_table: str = "user_events",
    revenue_event_type: str = "purchase",
    min_spend: float = 50,
    limit: int = 10,
) -> str:
    return wrap_tool_envelope(
        _reporting_service().attribution_bridge_review(
            spend_tables,
            events_table,
            revenue_event_type,
            min_spend,
            limit,
        )
    )


def validate_data_tool() -> str:
    return wrap_tool_envelope(_diagnostics_service().validate_data())


def inspect_freshness_tool() -> str:
    return wrap_tool_envelope(_diagnostics_service().freshness_report())


def list_connectors_tool() -> str:
    return wrap_tool_envelope(_diagnostics_service().list_connectors())


def attribution_mapping_diagnostics_tool(
    spend_tables: str = "meta_marketing_spend,google_marketing_spend",
    events_table: str = "user_events",
    revenue_event_type: str = "purchase",
    limit: int = 10,
) -> str:
    return wrap_tool_envelope(
        _diagnostics_service().attribution_mapping_diagnostics(
            spend_tables,
            events_table,
            revenue_event_type,
            limit,
        )
    )


def suggest_attribution_mappings_tool(
    spend_tables: str = "meta_marketing_spend,google_marketing_spend",
    events_table: str = "user_events",
    revenue_event_type: str = "purchase",
    limit: int = 10,
) -> str:
    return wrap_tool_envelope(
        _diagnostics_service().suggest_attribution_mappings(
            spend_tables,
            events_table,
            revenue_event_type,
            limit,
        )
    )


def apply_suggested_attribution_mappings_tool(
    profile_path: str = "",
    mapping_file: str = "",
    aliases: str = "",
    min_confidence: float = 0.8,
    limit: int = 20,
    force: bool = False,
) -> str:
    return wrap_tool_envelope(
        _diagnostics_service().apply_suggested_attribution_mappings(
            profile_path,
            mapping_file,
            aliases,
            min_confidence,
            limit,
            force,
        )
    )


def attribution_mapping_review_pack_tool(
    profile_path: str = "",
    mapping_file: str = "",
    aliases: str = "",
    min_confidence: float = 0.8,
    limit: int = 20,
) -> str:
    return wrap_tool_envelope(
        _diagnostics_service().attribution_mapping_review_pack(
            profile_path,
            mapping_file,
            aliases,
            min_confidence,
            limit,
        )
    )


def preview_apply_attribution_mappings_tool(
    profile_path: str = "",
    mapping_file: str = "",
    aliases: str = "",
    min_confidence: float = 0.8,
    limit: int = 20,
) -> str:
    return wrap_tool_envelope(
        _diagnostics_service().preview_apply_attribution_mappings(
            profile_path,
            mapping_file,
            aliases,
            min_confidence,
            limit,
        )
    )


def review_attribution_mappings_tool(
    mapping_file: str = "",
    history_limit: int = 20,
    rules_limit: int = 50,
) -> str:
    return wrap_tool_envelope(
        _diagnostics_service().review_attribution_mappings(
            mapping_file,
            history_limit,
            rules_limit,
        )
    )


def rollback_attribution_mappings_tool(
    aliases: str,
    mapping_file: str = "",
    scope: str = "",
    channel: str = "",
) -> str:
    return wrap_tool_envelope(
        _diagnostics_service().rollback_attribution_mappings(
            aliases,
            mapping_file,
            scope,
            channel,
        )
    )


def sync_stripe_billing_tool(output_dir: str = "", lookback_days: int = 365) -> str:
    return wrap_tool_envelope(_integration_service().sync_stripe_billing(output_dir, lookback_days))


def stripe_revenue_summary_tool(invoices_table: str = "stripe_invoices") -> str:
    return wrap_tool_envelope(_integration_service().stripe_revenue_summary(invoices_table))


def sync_meta_ads_tool(output_dir: str = "", lookback_days: int = 90) -> str:
    return wrap_tool_envelope(_integration_service().sync_meta_ads(output_dir, lookback_days))


def meta_ads_summary_tool(spend_table: str = "meta_marketing_spend") -> str:
    return wrap_tool_envelope(_integration_service().meta_ads_summary(spend_table))


def sync_google_ads_tool(output_dir: str = "", lookback_days: int = 90) -> str:
    return wrap_tool_envelope(_integration_service().sync_google_ads(output_dir, lookback_days))


def google_ads_summary_tool(spend_table: str = "google_marketing_spend") -> str:
    return wrap_tool_envelope(_integration_service().google_ads_summary(spend_table))


def funnel_diagnosis_tool(events_table: str, steps: str, date_from: str = "", date_to: str = "") -> str:
    return wrap_tool_envelope(_analysis_service().funnel_diagnosis(events_table, steps, date_from, date_to))


def channel_efficiency_review_tool(spend_table: str, events_table: str) -> str:
    return wrap_tool_envelope(_analysis_service().channel_efficiency_review(spend_table, events_table))


def anomaly_explanation_tool(table: str, metric_column: str, date_column: str = "date", lookback_days: int = 30) -> str:
    return wrap_tool_envelope(_analysis_service().anomaly_explanation(table, metric_column, date_column, lookback_days))


def save_workspace_profile_tool(
    name: str,
    growth_data_dir: str = "",
    postgres_url: str = "",
    business_mode: bool = False,
    notes: str = "",
) -> str:
    """Save a named workspace profile for reuse across sessions."""
    profile = WorkspaceProfile(
        name=name,
        growth_data_dir=growth_data_dir,
        postgres_url=postgres_url,
        business_mode=business_mode,
        notes=notes,
    )
    path = save_profile(profile)
    return f"Profile **{name}** saved to `{path}`."


def load_workspace_profile_tool(name: str) -> str:
    """Load and apply a saved workspace profile."""
    profile = load_profile(name)
    if profile is None:
        return f"No profile found with name **{name}**."
    apply_profile(profile, _settings)
    lines = [f"Profile **{profile.name}** loaded and applied."]
    if profile.growth_data_dir:
        lines.append(f"- Data directory: `{profile.growth_data_dir}`")
    if profile.postgres_url:
        lines.append("- PostgreSQL: configured")
    if profile.notes:
        lines.append(f"- Notes: {profile.notes}")
    lines.append(f"- Business mode: {'on' if profile.business_mode else 'off'}")
    return "\n".join(lines)


def list_workspace_profiles_tool() -> str:
    """List all saved workspace profiles."""
    profiles = list_profiles()
    if not profiles:
        return "No saved workspace profiles found. Use `save_workspace_profile` to create one."
    lines = ["**Saved Workspace Profiles:**\n"]
    for profile in profiles:
        line = f"- **{profile.name}**"
        if profile.growth_data_dir:
            line += f" — `{profile.growth_data_dir}`"
        if profile.notes:
            line += f" — _{profile.notes}_"
        lines.append(line)
    return "\n".join(lines)


def detect_data_drift_tool(
    table: str,
    metric_column: str,
    date_column: str = "date",
    lookback_days: int = 7,
) -> str:
    return wrap_tool_envelope(
        _analysis_service().detect_data_drift(table, metric_column, date_column, lookback_days)
    )


def funnel_ab_comparison_tool(
    events_table: str,
    steps: str,
    period_a_label: str = "Period A",
    period_a_start: str = "",
    period_a_end: str = "",
    period_b_label: str = "Period B",
    period_b_start: str = "",
    period_b_end: str = "",
) -> str:
    return wrap_tool_envelope(
        _analysis_service().funnel_ab_comparison(
            events_table, steps,
            period_a_label, period_a_start, period_a_end,
            period_b_label, period_b_start, period_b_end,
        )
    )


def drift_alert_tool(
    table: str,
    metric_column: str,
    date_column: str = "date",
    lookback_days: int = 7,
    webhook_url: str = "",
    threshold_pct: float = 20.0,
) -> str:
    return wrap_tool_envelope(
        _notification_service().drift_alert(table, metric_column, date_column, lookback_days, webhook_url, threshold_pct)
    )


def scheduled_report_preview_tool(
    spend_table: str = "marketing_spend",
    events_table: str = "user_events",
    webhook_url: str = "",
) -> str:
    return wrap_tool_envelope(
        _notification_service().scheduled_report_preview(spend_table, events_table, webhook_url)
    )


def forecast_metric_tool(
    table: str,
    metric_col: str,
    date_col: str = "date",
    horizon: int = 30,
    method: str = "linear",
) -> str:
    return wrap_tool_envelope(
        _forecasting_service().forecast_metric(table, metric_col, date_col, horizon, method)
    )


def forecast_growth_kpis_tool(
    spend_table: str = "marketing_spend",
    events_table: str = "user_events",
    horizon: int = 30,
) -> str:
    return wrap_tool_envelope(
        _forecasting_service().forecast_growth_kpis(spend_table, events_table, horizon)
    )


def sync_hubspot_tool(output_dir: str = "", lookback_days: int = 90) -> str:
    """Sync HubSpot contacts and deals to CSV files."""
    connector = HubSpotConnector(
        api_key=_settings.hubspot_access_token,
        base_url=_settings.hubspot_base_url,
    )
    result = connector.sync(output_dir or _settings.growth_data_dir or ".", lookback_days)
    lines = [f"## HubSpot Sync ({connector.status} mode)\n", f"- Contacts: {result.contacts}", f"- Deals: {result.deals}"]
    for f in result.files:
        lines.append(f"- File: `{f}`")
    return "\n".join(lines)


def hubspot_contacts_summary_tool() -> str:
    """Summarise HubSpot contacts from the synced table."""
    connector = HubSpotConnector(api_key=_settings.hubspot_access_token)
    return wrap_tool_envelope(connector.contacts_summary())


def sync_mixpanel_tool(output_dir: str = "") -> str:
    """Sync Mixpanel events and funnels to CSV files."""
    connector = MixpanelConnector(
        api_secret=_settings.mixpanel_api_secret,
        project_id=_settings.mixpanel_project_id,
        eu=_settings.mixpanel_eu,
    )
    result = connector.sync(output_dir or _settings.growth_data_dir or ".")
    return connector.events_summary()


def sync_amplitude_tool(output_dir: str = "") -> str:
    """Sync Amplitude events and cohorts to CSV files."""
    connector = AmplitudeConnector(
        api_key=_settings.amplitude_api_key,
        secret_key=_settings.amplitude_secret_key,
        eu=_settings.amplitude_eu,
    )
    result = connector.sync(output_dir or _settings.growth_data_dir or ".")
    return connector.events_summary()


def narrative_growth_review_tool(
    spend_table: str = "marketing_spend",
    events_table: str = "user_events",
) -> str:
    return wrap_tool_envelope(
        _reporting_service().narrative_growth_review(spend_table, events_table)
    )


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------

def register_tools(mcp: FastMCP) -> None:
    """Register all GrowthOS tools on the MCP server."""
    _r = mcp.tool

    _r(name="health_check", description="Check server status, loaded tables, and current date.")(_with_audit(health_check_tool, "health_check"))
    _r(name="list_tables", description="List all available marketing data tables.")(_with_audit(list_tables_tool, "list_tables"))
    _r(name="describe_table", description="Describe one table schema and sample rows.")(_with_audit(describe_table_tool, "describe_table"))
    _r(name="run_query", description="Run a read-only SQL query.")(_with_audit(run_query_tool, "run_query"))
    _r(name="analyze_funnel", description="Analyze conversion funnel performance.")(_with_audit(analyze_funnel_tool, "analyze_funnel"))
    _r(name="compute_cac_ltv", description="Compute CAC, LTV, and ROAS by channel.")(_with_audit(compute_cac_ltv_tool, "compute_cac_ltv"))
    _r(name="cohort_retention", description="Analyze cohort retention.")(_with_audit(cohort_retention_tool, "cohort_retention"))
    _r(name="channel_attribution", description="Analyze attributed revenue and ROAS.")(_with_audit(channel_attribution_tool, "channel_attribution"))
    _r(name="analyze_churn", description="Analyze churn segments by inactivity.")(_with_audit(analyze_churn_tool, "analyze_churn"))
    _r(name="detect_anomalies", description="Detect anomalies in a metric time series.")(_with_audit(detect_anomalies_tool, "detect_anomalies"))
    _r(name="growth_summary", description="Generate a weekly growth summary.")(_with_audit(growth_summary_tool, "growth_summary"))
    _r(name="weekly_growth_review", description="Run the weekly growth review workflow.")(_with_audit(weekly_growth_review_tool, "weekly_growth_review"))
    _r(name="executive_summary", description="Generate a concise executive summary.")(_with_audit(executive_summary_tool, "executive_summary"))
    _r(name="paid_growth_review", description="Compare paid ad spend across channels against Stripe revenue.")(_with_audit(paid_growth_review_tool, "paid_growth_review"))
    _r(name="campaign_performance_review", description="Rank Meta Ads and Google Ads campaigns by spend, efficiency, and watchlist risk.")(_with_audit(campaign_performance_review_tool, "campaign_performance_review"))
    _r(name="attribution_bridge_review", description="Bridge paid campaigns or channels to downstream revenue from user events.")(_with_audit(attribution_bridge_review_tool, "attribution_bridge_review"))
    _r(name="validate_data", description="Validate canonical marketing dataset readiness.")(_with_audit(validate_data_tool, "validate_data"))
    _r(name="inspect_freshness", description="Inspect freshness across known tables.")(_with_audit(inspect_freshness_tool, "inspect_freshness"))
    _r(name="list_connectors", description="List supported connectors and readiness.")(_with_audit(list_connectors_tool, "list_connectors"))
    _r(name="attribution_mapping_diagnostics", description="Inspect attribution coverage, unmapped keys, and alias rule usage.")(_with_audit(attribution_mapping_diagnostics_tool, "attribution_mapping_diagnostics"))
    _r(name="suggest_attribution_mappings", description="Suggest high-confidence attribution mapping rules and persist a semantic profile snapshot.")(_with_audit(suggest_attribution_mappings_tool, "suggest_attribution_mappings"))
    _r(name="attribution_mapping_review_pack", description="Build a read-only review pack with coverage lift and risk flags for suggested mappings.")(_with_audit(attribution_mapping_review_pack_tool, "attribution_mapping_review_pack"))
    _r(name="preview_apply_attribution_mappings", description="Preview coverage changes from suggested mappings without writing to disk.")(_with_audit(preview_apply_attribution_mappings_tool, "preview_apply_attribution_mappings"))
    _r(name="apply_suggested_attribution_mappings", description="Apply approved mapping rules. High-risk rules (collision, low confidence) are blocked unless force=True.")(_with_audit(apply_suggested_attribution_mappings_tool, "apply_suggested_attribution_mappings"))
    _r(name="review_attribution_mappings", description="Review active attribution mappings and recent mapping history.")(_with_audit(review_attribution_mappings_tool, "review_attribution_mappings"))
    _r(name="rollback_attribution_mappings", description="Rollback specific attribution mapping aliases and log the change.")(_with_audit(rollback_attribution_mappings_tool, "rollback_attribution_mappings"))
    _r(name="sync_stripe_billing", description="Sync Stripe customers, invoices, subscriptions, and purchase-like events.")(_with_audit(sync_stripe_billing_tool, "sync_stripe_billing"))
    _r(name="stripe_revenue_summary", description="Summarize revenue from synced Stripe invoices.")(_with_audit(stripe_revenue_summary_tool, "stripe_revenue_summary"))
    _r(name="sync_meta_ads", description="Sync Meta Ads campaigns and daily campaign insights.")(_with_audit(sync_meta_ads_tool, "sync_meta_ads"))
    _r(name="meta_ads_summary", description="Summarize spend, clicks, and impressions from synced Meta Ads data.")(_with_audit(meta_ads_summary_tool, "meta_ads_summary"))
    _r(name="sync_google_ads", description="Sync Google Ads campaigns and daily campaign performance.")(_with_audit(sync_google_ads_tool, "sync_google_ads"))
    _r(name="google_ads_summary", description="Summarize spend, clicks, impressions, and conversions from synced Google Ads data.")(_with_audit(google_ads_summary_tool, "google_ads_summary"))
    _r(name="funnel_diagnosis", description="Combined funnel + churn diagnosis with recommended next steps.")(_with_audit(funnel_diagnosis_tool, "funnel_diagnosis"))
    _r(name="channel_efficiency_review", description="CAC + LTV + ROAS per channel with invest/cut/watch classification.")(_with_audit(channel_efficiency_review_tool, "channel_efficiency_review"))
    _r(name="anomaly_explanation", description="Detect anomalies with likely cause hypotheses and recommended actions.")(_with_audit(anomaly_explanation_tool, "anomaly_explanation"))
    _r(name="save_workspace_profile", description="Save a named workspace profile for reuse across sessions.")(_with_audit(save_workspace_profile_tool, "save_workspace_profile"))
    _r(name="load_workspace_profile", description="Load and apply a saved workspace profile.")(_with_audit(load_workspace_profile_tool, "load_workspace_profile"))
    _r(name="list_workspace_profiles", description="List all saved workspace profiles.")(_with_audit(list_workspace_profiles_tool, "list_workspace_profiles"))
    _r(name="detect_data_drift", description="Compare current vs. previous period for any metric column to surface significant drift.")(_with_audit(detect_data_drift_tool, "detect_data_drift"))
    _r(name="funnel_ab_comparison", description="Compare funnel conversion between two time periods side by side.")(_with_audit(funnel_ab_comparison_tool, "funnel_ab_comparison"))
    _r(name="drift_alert", description="Detect metric drift and optionally fire a webhook alert.")(_with_audit(drift_alert_tool, "drift_alert"))
    _r(name="scheduled_report_preview", description="Preview a scheduled weekly growth report and optionally deliver via webhook.")(_with_audit(scheduled_report_preview_tool, "scheduled_report_preview"))
    _r(name="forecast_metric", description="Forecast a metric time series using linear regression or exponential smoothing.")(_with_audit(forecast_metric_tool, "forecast_metric"))
    _r(name="forecast_growth_kpis", description="Forecast spend and daily active users for the next N days.")(_with_audit(forecast_growth_kpis_tool, "forecast_growth_kpis"))
    _r(name="sync_hubspot", description="Sync HubSpot contacts and deals to local CSV files (demo mode if no API key).")(_with_audit(sync_hubspot_tool, "sync_hubspot"))
    _r(name="hubspot_contacts_summary", description="Summarise HubSpot contacts by lifecycle stage.")(_with_audit(hubspot_contacts_summary_tool, "hubspot_contacts_summary"))
    _r(name="sync_mixpanel", description="Sync Mixpanel events and funnels to local CSV files (demo mode if no credentials).")(_with_audit(sync_mixpanel_tool, "sync_mixpanel"))
    _r(name="sync_amplitude", description="Sync Amplitude events and cohorts to local CSV files (demo mode if no credentials).")(_with_audit(sync_amplitude_tool, "sync_amplitude"))
    _r(name="narrative_growth_review", description="Generate a prose narrative growth review with headline, findings, and recommendation.")(_with_audit(narrative_growth_review_tool, "narrative_growth_review"))
