"""Tests for Google Ads connector and cross-source reporting."""

from __future__ import annotations

import csv
import json

import httpx

from growth_os.connectors import GoogleAdsConnector, GrowthConnector
from growth_os.config import settings
from growth_os.services.diagnostics_service import DiagnosticsService
from growth_os.services.integration_service import IntegrationService
from growth_os.services.reporting_service import ReportingService


def test_google_ads_connector_sync_writes_files(tmp_path):
    """Google Ads sync should write normalized campaign and spend extracts."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["developer-token"] == "dev-token"
        assert request.headers["authorization"] == "Bearer access-token"
        assert request.url.path.endswith("/googleAds:searchStream")

        query = json.loads(request.content.decode("utf-8"))["query"]
        if "metrics.cost_micros" in query:
            payload = [
                {
                    "results": [
                        {
                            "campaign": {"id": "111", "name": "Brand Search"},
                            "segments": {"date": "2026-03-05"},
                            "metrics": {
                                "impressions": "12000",
                                "clicks": "640",
                                "costMicros": "185000000",
                                "conversions": 28.5,
                            },
                        },
                        {
                            "campaign": {"id": "222", "name": "Nonbrand Search"},
                            "segments": {"date": "2026-03-06"},
                            "metrics": {
                                "impressions": "18000",
                                "clicks": "710",
                                "costMicros": "214500000",
                                "conversions": 31,
                            },
                        },
                    ]
                }
            ]
        else:
            payload = [
                {
                    "results": [
                        {
                            "campaign": {
                                "id": "111",
                                "name": "Brand Search",
                                "status": "ENABLED",
                                "advertisingChannelType": "SEARCH",
                                "advertisingChannelSubType": "SEARCH_MOBILE_APP",
                                "startDate": "2026-02-01",
                                "endDate": "2037-12-30",
                            }
                        },
                        {
                            "campaign": {
                                "id": "222",
                                "name": "Nonbrand Search",
                                "status": "PAUSED",
                                "advertisingChannelType": "SEARCH",
                                "advertisingChannelSubType": "SEARCH_EXPANDED_DYNAMIC_SEARCH_ADS",
                                "startDate": "2026-02-10",
                                "endDate": "2037-12-30",
                            }
                        },
                    ]
                }
            ]
        return httpx.Response(200, json=payload)

    client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://googleads.googleapis.com")
    connector = GoogleAdsConnector(
        developer_token="dev-token",
        customer_id="123-456-7890",
        access_token="access-token",
        client=client,
    )

    result = connector.sync_ads_data(str(tmp_path), lookback_days=30)

    assert result.campaigns == 2
    assert result.spend_rows == 2
    assert (tmp_path / "google_ads_campaigns.csv").exists()
    assert (tmp_path / "google_marketing_spend.csv").exists()

    with open(tmp_path / "google_ads_campaigns.csv", encoding="utf-8") as file:
        campaign_rows = list(csv.DictReader(file))
    with open(tmp_path / "google_marketing_spend.csv", encoding="utf-8") as file:
        spend_rows = list(csv.DictReader(file))

    assert campaign_rows[0]["channel"] == "google_ads"
    assert spend_rows[0]["spend"] == "185.0"
    assert spend_rows[1]["conversions"] == "31.0"

    client.close()


def test_integration_service_google_ads_summary(tmp_path):
    """Google Ads summary should query synced spend data."""
    spend_path = tmp_path / "google_marketing_spend.csv"
    spend_path.write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-05,google_ads,111,Brand Search,185.00,12000,640,28.5,google_ads\n"
        "2026-02-05,google_ads,222,Nonbrand Search,214.50,18000,710,31.0,google_ads\n",
        encoding="utf-8",
    )
    connector = GrowthConnector(data_dir=str(tmp_path))
    service = IntegrationService(connector)

    envelope = service.google_ads_summary()

    assert "Google Ads Summary" in envelope.body
    assert "$185.00" in envelope.body or "$399.50" in envelope.body


def test_paid_growth_review_combines_sources(tmp_path):
    """Paid growth review should blend Meta, Google Ads, and Stripe into one report."""
    (tmp_path / "meta_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-05,meta_ads,cmp_1,Prospecting,150.00,10000,320,12,meta_ads\n"
        "2026-02-05,meta_ads,cmp_2,Retargeting,100.00,7000,210,8,meta_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "google_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-06,google_ads,111,Brand Search,185.00,12000,640,28.5,google_ads\n"
        "2026-02-10,google_ads,222,Nonbrand Search,214.50,18000,710,31.0,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "stripe_invoices.csv").write_text(
        "invoice_id,customer_id,customer_email,status,currency,amount_paid,amount_due,amount_remaining,created,period_start,period_end,subscription_id\n"
        "in_1,cus_1,a@example.com,paid,usd,900.0,900.0,0.0,2026-03-02,2026-03-01,2026-03-31,sub_1\n"
        "in_2,cus_2,b@example.com,paid,usd,600.0,600.0,0.0,2026-02-08,2026-02-01,2026-02-28,sub_2\n",
        encoding="utf-8",
    )
    connector = GrowthConnector(data_dir=str(tmp_path))
    service = ReportingService(connector)

    envelope = service.paid_growth_review()

    assert "Paid Growth Overview" in envelope.body
    assert "Paid Channel Mix" in envelope.body
    assert "$549.50" in envelope.body
    assert "$900.00" in envelope.body


def test_campaign_performance_review_ranks_cross_source_campaigns(tmp_path):
    """Campaign review should rank Meta and Google Ads campaigns in one report."""
    (tmp_path / "meta_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-05,meta_ads,cmp_1,Prospecting,150.00,10000,320,12,meta_ads\n"
        "2026-03-04,meta_ads,cmp_2,Retargeting,90.00,5000,120,0,meta_ads\n"
        "2026-02-05,meta_ads,cmp_2,Retargeting,50.00,3000,95,4,meta_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "google_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-06,google_ads,111,Brand Search,185.00,12000,640,28.5,google_ads\n"
        "2026-03-03,google_ads,222,Nonbrand Search,110.00,9500,310,6.0,google_ads\n"
        "2026-02-10,google_ads,222,Nonbrand Search,70.00,5000,200,7.0,google_ads\n",
        encoding="utf-8",
    )
    connector = GrowthConnector(data_dir=str(tmp_path))
    service = ReportingService(connector)

    envelope = service.campaign_performance_review(limit=3)

    assert "Top Campaigns by Spend" in envelope.body
    assert "Efficiency Leaders" in envelope.body
    assert "Watchlist" in envelope.body
    assert "Brand Search" in envelope.body
    assert "Retargeting" in envelope.body
    assert "Trust" not in envelope.body


def test_attribution_bridge_review_links_campaigns_to_revenue(tmp_path):
    """Attribution bridge should join paid campaigns to downstream revenue when utm_campaign exists."""
    (tmp_path / "meta_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-05,meta_ads,cmp_1,Prospecting,150.00,10000,320,12,meta_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "google_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-06,google_ads,111,Brand Search,185.00,12000,640,28.5,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "user_events.csv").write_text(
        "user_id,event_type,event_date,utm_source,utm_campaign,revenue\n"
        "u1,signup,2026-03-02,meta_ads,Prospecting,0.0\n"
        "u1,purchase,2026-03-05,meta_ads,Prospecting,240.0\n"
        "u2,signup,2026-03-03,google_ads,Brand Search,0.0\n"
        "u2,purchase,2026-03-06,google_ads,Brand Search,420.0\n",
        encoding="utf-8",
    )
    connector = GrowthConnector(data_dir=str(tmp_path))
    service = ReportingService(connector)

    envelope = service.attribution_bridge_review(limit=5)

    assert "Attribution Coverage" in envelope.body
    assert "Attributed Campaign Revenue" in envelope.body
    assert "Brand Search" in envelope.body
    assert "$420.00" in envelope.body
    assert "Revenue coverage" in envelope.body


def test_attribution_bridge_review_falls_back_to_channel_level(tmp_path):
    """Attribution bridge should warn and fall back when utm_campaign is missing."""
    (tmp_path / "meta_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-05,meta_ads,cmp_1,Prospecting,150.00,10000,320,12,meta_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "user_events.csv").write_text(
        "user_id,event_type,event_date,utm_source,revenue\n"
        "u1,signup,2026-03-02,meta_ads,0.0\n"
        "u1,purchase,2026-03-05,meta_ads,240.0\n",
        encoding="utf-8",
    )
    connector = GrowthConnector(data_dir=str(tmp_path))
    service = ReportingService(connector)

    envelope = service.attribution_bridge_review(spend_tables="meta_marketing_spend", limit=5)

    assert "Attributed Revenue by Channel" in envelope.body
    assert envelope.warnings
    assert any("utm_campaign" in warning for warning in envelope.warnings)


def test_attribution_bridge_review_uses_mapping_rules_for_naming_mismatches(tmp_path):
    """Attribution bridge should use normalized keys and explicit alias mappings."""
    mapping_path = tmp_path / "attribution_mappings.csv"
    mapping_path.write_text(
        "scope,canonical_value,alias,channel\n"
        "campaign,brandsearch,Brand Search - US,google_ads\n"
        "campaign,brandsearch,brand_search,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "google_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-06,google_ads,111,Brand Search - US,185.00,12000,640,28.5,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "meta_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-05,meta_ads,cmp_1,Prospecting,150.00,10000,320,12,meta_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "user_events.csv").write_text(
        "user_id,event_type,event_date,utm_source,utm_campaign,revenue\n"
        "u1,purchase,2026-03-06,google ads,brand_search,510.0\n"
        "u2,purchase,2026-03-05,facebook ads,Prospecting,250.0\n",
        encoding="utf-8",
    )
    previous_mapping = settings.attribution_mapping_file
    settings.attribution_mapping_file = str(mapping_path)
    try:
        connector = GrowthConnector(data_dir=str(tmp_path))
        service = ReportingService(connector)

        envelope = service.attribution_bridge_review(limit=10)

        assert "Brand Search - US" in envelope.body
        assert "$510.00" in envelope.body
        assert "$250.00" in envelope.body
    finally:
        settings.attribution_mapping_file = previous_mapping


def test_attribution_mapping_diagnostics_reports_gaps_and_alias_hits(tmp_path):
    """Diagnostics should show coverage, unmatched keys, and applied alias rules."""
    mapping_path = tmp_path / "attribution_mappings.csv"
    mapping_path.write_text(
        "scope,canonical_value,alias,channel\n"
        "campaign,brandsearch,Brand Search - US,google_ads\n"
        "campaign,brandsearch,brand_search,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "google_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-06,google_ads,111,Brand Search - US,185.00,12000,640,28.5,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "meta_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-05,meta_ads,cmp_1,Prospecting,150.00,10000,320,12,meta_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "user_events.csv").write_text(
        "user_id,event_type,event_date,utm_source,utm_campaign,revenue\n"
        "u1,purchase,2026-03-06,google ads,brand_search,510.0\n"
        "u2,purchase,2026-03-05,facebook ads,Prospecting,250.0\n"
        "u3,purchase,2026-03-04,linkedin,enterprise_push,300.0\n",
        encoding="utf-8",
    )
    previous_mapping = settings.attribution_mapping_file
    settings.attribution_mapping_file = str(mapping_path)
    try:
        connector = GrowthConnector(data_dir=str(tmp_path))
        service = DiagnosticsService(connector)

        envelope = service.attribution_mapping_diagnostics(limit=10)

        assert "Mapping Coverage" in envelope.body
        assert "Unmatched Event Keys" in envelope.body
        assert "Unmatched Spend Keys" in envelope.body
        assert "Applied Alias Rules" in envelope.body
        assert "linkedin" in envelope.body
        assert "Brand Search - US" in envelope.body
        assert "facebook ads" in envelope.body
    finally:
        settings.attribution_mapping_file = previous_mapping


def test_suggest_attribution_mappings_persists_semantic_profile(tmp_path):
    """Suggestion tool should infer a campaign mapping and persist the semantic profile."""
    profile_path = tmp_path / "semantic_profile.json"
    (tmp_path / "google_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-06,google_ads,111,Brand Search - US,185.00,12000,640,28.5,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "user_events.csv").write_text(
        "user_id,event_type,event_date,utm_source,utm_campaign,revenue\n"
        "u1,purchase,2026-03-06,google_ads,brand_search,510.0\n",
        encoding="utf-8",
    )
    previous_mapping = settings.attribution_mapping_file
    previous_profile = settings.semantic_profile_path
    settings.attribution_mapping_file = ""
    settings.semantic_profile_path = str(profile_path)
    try:
        connector = GrowthConnector(data_dir=str(tmp_path))
        service = DiagnosticsService(connector)

        envelope = service.suggest_attribution_mappings(spend_tables="google_marketing_spend", limit=10)

        assert "Suggested Mapping Rules" in envelope.body
        assert "brand_search" in envelope.body
        assert "Brand Search - US" in envelope.body
        assert profile_path.exists()

        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        assert profile["events_table"] == "user_events"
        assert profile["suggestions"]
        assert profile["suggestions"][0]["alias"] == "brand_search"
    finally:
        settings.attribution_mapping_file = previous_mapping
        settings.semantic_profile_path = previous_profile


def test_apply_suggested_attribution_mappings_writes_mapping_file(tmp_path):
    """Apply flow should persist approved suggestions and skip duplicates on rerun."""
    mapping_path = tmp_path / "applied_mappings.csv"
    profile_path = tmp_path / "semantic_profile.json"
    (tmp_path / "google_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-06,google_ads,111,Brand Search - US,185.00,12000,640,28.5,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "user_events.csv").write_text(
        "user_id,event_type,event_date,utm_source,utm_campaign,revenue\n"
        "u1,purchase,2026-03-06,google_ads,brand_search,510.0\n",
        encoding="utf-8",
    )
    previous_mapping = settings.attribution_mapping_file
    previous_profile = settings.semantic_profile_path
    settings.attribution_mapping_file = ""
    settings.semantic_profile_path = str(profile_path)
    try:
        connector = GrowthConnector(data_dir=str(tmp_path))
        service = DiagnosticsService(connector)

        suggestion_envelope = service.suggest_attribution_mappings(spend_tables="google_marketing_spend", limit=10)
        assert "Suggested Mapping Rules" in suggestion_envelope.body

        apply_envelope = service.apply_suggested_attribution_mappings(mapping_file=str(mapping_path), min_confidence=0.7)
        assert "Applied Mapping Rules" in apply_envelope.body
        assert "applied" in apply_envelope.body
        assert mapping_path.exists()

        rows = list(csv.DictReader(mapping_path.open(encoding="utf-8")))
        assert len(rows) == 1
        assert rows[0]["alias"] == "brand_search"
        assert rows[0]["canonical_value"] == "Brand Search - US"

        second_apply = service.apply_suggested_attribution_mappings(mapping_file=str(mapping_path), min_confidence=0.7)
        assert "skipped" in second_apply.body
        rows_after = list(csv.DictReader(mapping_path.open(encoding="utf-8")))
        assert len(rows_after) == 1
    finally:
        settings.attribution_mapping_file = previous_mapping
        settings.semantic_profile_path = previous_profile


def test_preview_apply_attribution_mappings_is_read_only(tmp_path):
    """Preview flow should show expected coverage lift without writing the mapping file."""
    mapping_path = tmp_path / "preview_mappings.csv"
    profile_path = tmp_path / "semantic_profile.json"
    (tmp_path / "google_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-06,google_ads,111,Brand Search - US,185.00,12000,640,28.5,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "user_events.csv").write_text(
        "user_id,event_type,event_date,utm_source,utm_campaign,revenue\n"
        "u1,purchase,2026-03-06,google_ads,brand_search,510.0\n",
        encoding="utf-8",
    )
    previous_mapping = settings.attribution_mapping_file
    previous_profile = settings.semantic_profile_path
    settings.attribution_mapping_file = ""
    settings.semantic_profile_path = str(profile_path)
    try:
        connector = GrowthConnector(data_dir=str(tmp_path))
        service = DiagnosticsService(connector)

        service.suggest_attribution_mappings(spend_tables="google_marketing_spend", limit=10)
        preview = service.preview_apply_attribution_mappings(mapping_file=str(mapping_path), min_confidence=0.7)

        assert "Coverage Preview" in preview.body
        assert "Rule Risk Review" in preview.body
        assert "Selected Suggestions" in preview.body
        assert "Brand Search - US" in preview.body
        assert "high_revenue_impact" in preview.body
        assert "$0.00" in preview.body
        assert "$185.00" in preview.body
        assert "$510.00" in preview.body
        assert "nothing was written to disk" in preview.body
        assert not mapping_path.exists()
    finally:
        settings.attribution_mapping_file = previous_mapping
        settings.semantic_profile_path = previous_profile


def test_attribution_mapping_review_pack_surfaces_rule_risks(tmp_path):
    """Review pack should combine coverage preview with deterministic rule risk flags."""
    mapping_path = tmp_path / "review_pack_mappings.csv"
    profile_path = tmp_path / "semantic_profile.json"
    (tmp_path / "google_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-06,google_ads,111,Brand Search - US,185.00,12000,640,28.5,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "user_events.csv").write_text(
        "user_id,event_type,event_date,utm_source,utm_campaign,revenue\n"
        "u1,purchase,2026-03-06,google_ads,brand_search,510.0\n",
        encoding="utf-8",
    )
    previous_mapping = settings.attribution_mapping_file
    previous_profile = settings.semantic_profile_path
    settings.attribution_mapping_file = ""
    settings.semantic_profile_path = str(profile_path)
    try:
        connector = GrowthConnector(data_dir=str(tmp_path))
        service = DiagnosticsService(connector)

        service.suggest_attribution_mappings(spend_tables="google_marketing_spend", limit=10)
        review_pack = service.attribution_mapping_review_pack(mapping_file=str(mapping_path), min_confidence=0.7)

        assert "Coverage Preview" in review_pack.body
        assert "Rule Risk Review" in review_pack.body
        assert "Risk mix:" in review_pack.body
        assert "high_revenue_impact" in review_pack.body
        assert "brand_search" in review_pack.body
        assert not mapping_path.exists()
    finally:
        settings.attribution_mapping_file = previous_mapping
        settings.semantic_profile_path = previous_profile


def test_review_and_rollback_attribution_mappings_track_history(tmp_path):
    """Review and rollback flow should show active rules and persist audit history."""
    mapping_path = tmp_path / "applied_mappings.csv"
    profile_path = tmp_path / "semantic_profile.json"
    history_path = tmp_path / "attribution_mapping_history.jsonl"
    (tmp_path / "google_marketing_spend.csv").write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,source_system\n"
        "2026-03-06,google_ads,111,Brand Search - US,185.00,12000,640,28.5,google_ads\n",
        encoding="utf-8",
    )
    (tmp_path / "user_events.csv").write_text(
        "user_id,event_type,event_date,utm_source,utm_campaign,revenue\n"
        "u1,purchase,2026-03-06,google_ads,brand_search,510.0\n",
        encoding="utf-8",
    )
    previous_mapping = settings.attribution_mapping_file
    previous_profile = settings.semantic_profile_path
    previous_history = settings.attribution_mapping_history_path
    settings.attribution_mapping_file = ""
    settings.semantic_profile_path = str(profile_path)
    settings.attribution_mapping_history_path = str(history_path)
    try:
        connector = GrowthConnector(data_dir=str(tmp_path))
        service = DiagnosticsService(connector)

        service.suggest_attribution_mappings(spend_tables="google_marketing_spend", limit=10)
        service.apply_suggested_attribution_mappings(mapping_file=str(mapping_path), min_confidence=0.7)

        review_before = service.review_attribution_mappings(mapping_file=str(mapping_path))
        assert "Active Attribution Mappings" in review_before.body
        assert "Recent Mapping History" in review_before.body
        assert "brand_search" in review_before.body
        assert "apply" in review_before.body

        rollback = service.rollback_attribution_mappings("brand_search", mapping_file=str(mapping_path))
        assert "Rollback Results" in rollback.body
        assert "removed" in rollback.body

        review_after = service.review_attribution_mappings(mapping_file=str(mapping_path))
        assert "rollback" in review_after.body
        rows_after = list(csv.DictReader(mapping_path.open(encoding="utf-8")))
        assert rows_after == []
        history_lines = history_path.read_text(encoding="utf-8").strip().splitlines()
        assert len(history_lines) >= 2
    finally:
        settings.attribution_mapping_file = previous_mapping
        settings.semantic_profile_path = previous_profile
        settings.attribution_mapping_history_path = previous_history


def test_google_ads_and_paid_growth_tools_exported_from_server():
    """The public server export should expose Google Ads and blended review tools."""
    server = __import__(
        "growth_os.server",
        fromlist=[
            "sync_google_ads",
            "google_ads_summary",
            "paid_growth_review",
            "campaign_performance_review",
            "attribution_bridge_review",
            "attribution_mapping_diagnostics",
            "suggest_attribution_mappings",
            "attribution_mapping_review_pack",
            "preview_apply_attribution_mappings",
            "apply_suggested_attribution_mappings",
            "review_attribution_mappings",
            "rollback_attribution_mappings",
        ],
    )

    assert hasattr(server, "sync_google_ads")
    assert hasattr(server, "google_ads_summary")
    assert hasattr(server, "paid_growth_review")
    assert hasattr(server, "campaign_performance_review")
    assert hasattr(server, "attribution_bridge_review")
    assert hasattr(server, "attribution_mapping_diagnostics")
    assert hasattr(server, "suggest_attribution_mappings")
    assert hasattr(server, "attribution_mapping_review_pack")
    assert hasattr(server, "preview_apply_attribution_mappings")
    assert hasattr(server, "apply_suggested_attribution_mappings")
    assert hasattr(server, "review_attribution_mappings")
    assert hasattr(server, "rollback_attribution_mappings")
