"""Tests for Meta Ads connector and integration service."""

from __future__ import annotations

import csv

import httpx

from growth_os.connectors import GrowthConnector, MetaAdsConnector
from growth_os.services.integration_service import IntegrationService


def test_meta_ads_connector_sync_paginates_and_writes_files(tmp_path):
    """Meta Ads sync should paginate and write normalized extracts."""
    calls = {"campaigns": 0, "insights": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        resource = request.url.path.rsplit("/", 1)[-1]
        params = dict(request.url.params)

        if resource == "campaigns":
            calls["campaigns"] += 1
            if "after" in params:
                payload = {
                    "data": [
                        {
                            "id": "cmp_2",
                            "name": "Retargeting",
                            "objective": "OUTCOME_SALES",
                            "status": "ACTIVE",
                            "effective_status": ["ACTIVE"],
                            "created_time": "2026-02-05T12:00:00+0000",
                            "start_time": "2026-02-06T00:00:00+0000",
                            "daily_budget": "350000",
                        }
                    ],
                    "paging": {},
                }
            else:
                payload = {
                    "data": [
                        {
                            "id": "cmp_1",
                            "name": "Prospecting",
                            "objective": "OUTCOME_TRAFFIC",
                            "status": "ACTIVE",
                            "effective_status": ["ACTIVE"],
                            "created_time": "2026-02-01T10:00:00+0000",
                            "start_time": "2026-02-02T00:00:00+0000",
                            "daily_budget": "500000",
                        }
                    ],
                    "paging": {
                        "next": "https://graph.facebook.com/v21.0/act_123/campaigns?after=cursor_1"
                    },
                }
        elif resource == "insights":
            calls["insights"] += 1
            payload = {
                "data": [
                    {
                        "campaign_id": "cmp_1",
                        "campaign_name": "Prospecting",
                        "date_start": "2026-03-05",
                        "date_stop": "2026-03-05",
                        "impressions": "15000",
                        "clicks": "340",
                        "spend": "128.45",
                        "reach": "9800",
                        "cpc": "0.38",
                        "ctr": "2.27",
                    },
                    {
                        "campaign_id": "cmp_2",
                        "campaign_name": "Retargeting",
                        "date_start": "2026-03-06",
                        "date_stop": "2026-03-06",
                        "impressions": "9000",
                        "clicks": "275",
                        "spend": "94.10",
                        "reach": "6100",
                        "cpc": "0.34",
                        "ctr": "3.06",
                    },
                ],
                "paging": {},
            }
        else:
            raise AssertionError(f"Unexpected resource: {resource}")

        return httpx.Response(200, json=payload)

    client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://graph.facebook.com/v21.0")
    connector = MetaAdsConnector(access_token="meta_test_token", ad_account_id="123", client=client)

    result = connector.sync_ads_data(str(tmp_path), lookback_days=30)

    assert calls["campaigns"] == 2
    assert calls["insights"] == 1
    assert result.campaigns == 2
    assert result.spend_rows == 2
    assert (tmp_path / "meta_campaigns.csv").exists()
    assert (tmp_path / "meta_marketing_spend.csv").exists()

    with open(tmp_path / "meta_campaigns.csv", encoding="utf-8") as file:
        campaign_rows = list(csv.DictReader(file))
    with open(tmp_path / "meta_marketing_spend.csv", encoding="utf-8") as file:
        spend_rows = list(csv.DictReader(file))

    assert campaign_rows[0]["channel"] == "meta_ads"
    assert campaign_rows[0]["daily_budget"] == "5000.0"
    assert spend_rows[0]["channel"] == "meta_ads"
    assert spend_rows[0]["spend"] == "128.45"

    client.close()


def test_integration_service_meta_ads_summary(tmp_path):
    """Meta Ads summary should query synced spend data."""
    spend_path = tmp_path / "meta_marketing_spend.csv"
    spend_path.write_text(
        "date,channel,campaign_id,campaign,spend,impressions,clicks,conversions,reach,cpc,ctr,source_system\n"
        "2026-03-05,meta_ads,cmp_1,Prospecting,128.45,15000,340,0,9800,0.38,2.27,meta_ads\n"
        "2026-02-10,meta_ads,cmp_2,Retargeting,94.10,9000,275,0,6100,0.34,3.06,meta_ads\n",
        encoding="utf-8",
    )
    connector = GrowthConnector(data_dir=str(tmp_path))
    service = IntegrationService(connector)

    envelope = service.meta_ads_summary()

    assert "Meta Ads Summary" in envelope.body
    assert "$128.45" in envelope.body or "$222.55" in envelope.body


def test_sync_meta_tools_exported_from_server():
    """The public server export should expose Meta Ads tools."""
    server = __import__("growth_os.server", fromlist=["sync_meta_ads", "meta_ads_summary"])

    assert hasattr(server, "sync_meta_ads")
    assert hasattr(server, "meta_ads_summary")
