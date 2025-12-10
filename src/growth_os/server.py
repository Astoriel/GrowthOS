"""Backward-compatible server exports."""

from growth_os.app.registry import (
    analyze_churn_tool as analyze_churn,
)
from growth_os.app.registry import (
    analyze_funnel_tool as analyze_funnel,
)
from growth_os.app.registry import (
    apply_suggested_attribution_mappings_tool as apply_suggested_attribution_mappings,
)
from growth_os.app.registry import (
    attribution_bridge_review_tool as attribution_bridge_review,
)
from growth_os.app.registry import (
    attribution_mapping_diagnostics_tool as attribution_mapping_diagnostics,
)
from growth_os.app.registry import (
    attribution_mapping_review_pack_tool as attribution_mapping_review_pack,
)
from growth_os.app.registry import (
    campaign_performance_review_tool as campaign_performance_review,
)
from growth_os.app.registry import (
    channel_attribution_tool as channel_attribution,
)
from growth_os.app.registry import (
    cohort_retention_tool as cohort_retention,
)
from growth_os.app.registry import (
    compute_cac_ltv_tool as compute_cac_ltv,
)
from growth_os.app.registry import (
    describe_table_tool as describe_table,
)
from growth_os.app.registry import (
    detect_anomalies_tool as detect_anomalies,
)
from growth_os.app.registry import (
    executive_summary_tool as executive_summary,
)
from growth_os.app.registry import (
    google_ads_summary_tool as google_ads_summary,
)
from growth_os.app.registry import (
    growth_summary_tool as growth_summary,
)
from growth_os.app.registry import (
    health_check_tool as health_check,
)
from growth_os.app.registry import (
    inspect_freshness_tool as inspect_freshness,
)
from growth_os.app.registry import (
    list_connectors_tool as list_connectors,
)
from growth_os.app.registry import (
    list_tables_tool as list_tables,
)
from growth_os.app.registry import (
    meta_ads_summary_tool as meta_ads_summary,
)
from growth_os.app.registry import (
    paid_growth_review_tool as paid_growth_review,
)
from growth_os.app.registry import (
    preview_apply_attribution_mappings_tool as preview_apply_attribution_mappings,
)
from growth_os.app.registry import (
    review_attribution_mappings_tool as review_attribution_mappings,
)
from growth_os.app.registry import (
    rollback_attribution_mappings_tool as rollback_attribution_mappings,
)
from growth_os.app.registry import (
    run_query_tool as run_query,
)
from growth_os.app.registry import (
    stripe_revenue_summary_tool as stripe_revenue_summary,
)
from growth_os.app.registry import (
    suggest_attribution_mappings_tool as suggest_attribution_mappings,
)
from growth_os.app.registry import (
    sync_google_ads_tool as sync_google_ads,
)
from growth_os.app.registry import (
    sync_meta_ads_tool as sync_meta_ads,
)
from growth_os.app.registry import (
    sync_stripe_billing_tool as sync_stripe_billing,
)
from growth_os.app.registry import (
    validate_data_tool as validate_data,
)
from growth_os.app.registry import (
    weekly_growth_review_tool as weekly_growth_review,
)
from growth_os.app.server import create_mcp_server, main, mcp

__all__ = [
    "analyze_churn",
    "analyze_funnel",
    "apply_suggested_attribution_mappings",
    "attribution_bridge_review",
    "attribution_mapping_diagnostics",
    "attribution_mapping_review_pack",
    "campaign_performance_review",
    "channel_attribution",
    "cohort_retention",
    "compute_cac_ltv",
    "create_mcp_server",
    "describe_table",
    "detect_anomalies",
    "executive_summary",
    "google_ads_summary",
    "growth_summary",
    "health_check",
    "inspect_freshness",
    "list_connectors",
    "list_tables",
    "main",
    "meta_ads_summary",
    "mcp",
    "paid_growth_review",
    "preview_apply_attribution_mappings",
    "review_attribution_mappings",
    "rollback_attribution_mappings",
    "run_query",
    "stripe_revenue_summary",
    "suggest_attribution_mappings",
    "sync_google_ads",
    "sync_meta_ads",
    "sync_stripe_billing",
    "validate_data",
    "weekly_growth_review",
]
