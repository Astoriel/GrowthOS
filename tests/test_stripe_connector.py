"""Tests for Stripe connector and integration service."""

from __future__ import annotations

import csv
from pathlib import Path

import httpx

from growth_os.connectors import GrowthConnector, StripeConnector
from growth_os.config import settings
from growth_os.connectors.duckdb import reset_connector
from growth_os.services.integration_service import IntegrationService


def test_stripe_connector_sync_paginates_and_writes_files(tmp_path):
    """Stripe sync should paginate and write normalized extracts."""
    calls = {"customers": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        resource = request.url.path.rsplit("/", 1)[-1]
        params = dict(request.url.params)

        if resource == "customers":
            calls["customers"] += 1
            if "starting_after" in params:
                payload = {
                    "data": [
                        {"id": "cus_2", "email": "b@example.com", "name": "Beta", "created": 1730419200, "currency": "usd", "delinquent": False}
                    ],
                    "has_more": False,
                }
            else:
                payload = {
                    "data": [
                        {"id": "cus_1", "email": "a@example.com", "name": "Acme", "created": 1730332800, "currency": "usd", "delinquent": False}
                    ],
                    "has_more": True,
                }
        elif resource == "invoices":
            payload = {
                "data": [
                    {
                        "id": "in_1",
                        "customer": "cus_1",
                        "status": "paid",
                        "currency": "usd",
                        "amount_paid": 12000,
                        "amount_due": 12000,
                        "amount_remaining": 0,
                        "created": 1741651200,
                        "subscription": "sub_1",
                        "customer_details": {"email": "a@example.com"},
                        "status_transitions": {"paid_at": 1741737600},
                        "lines": {"data": [{"period": {"start": 1741651200, "end": 1744243200}}]},
                    }
                ],
                "has_more": False,
            }
        elif resource == "subscriptions":
            payload = {
                "data": [
                    {
                        "id": "sub_1",
                        "customer": "cus_1",
                        "status": "active",
                        "created": 1741651200,
                        "current_period_start": 1741651200,
                        "current_period_end": 1744243200,
                        "cancel_at_period_end": False,
                        "items": {"data": [{"price": {"id": "price_1", "currency": "usd", "unit_amount": 12000, "recurring": {"interval": "month"}}}]},
                    }
                ],
                "has_more": False,
            }
        else:
            raise AssertionError(f"Unexpected resource: {resource}")

        return httpx.Response(200, json=payload)

    client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.stripe.com/v1")
    connector = StripeConnector(api_key="sk_test_123", client=client)

    result = connector.sync_billing_data(str(tmp_path), lookback_days=30)

    assert calls["customers"] == 2
    assert result.customers == 2
    assert result.invoices == 1
    assert result.subscriptions == 1
    assert result.user_events == 1
    assert (tmp_path / "stripe_invoices.csv").exists()
    assert (tmp_path / "stripe_user_events.csv").exists()

    with open(tmp_path / "stripe_user_events.csv", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    assert rows[0]["event_type"] == "purchase"
    assert rows[0]["utm_source"] == "stripe"

    client.close()


def test_integration_service_stripe_revenue_summary(tmp_path):
    """Stripe revenue summary should query synced invoice data."""
    invoice_path = tmp_path / "stripe_invoices.csv"
    invoice_path.write_text(
        "invoice_id,customer_id,customer_email,status,currency,amount_paid,amount_due,amount_remaining,created,period_start,period_end,subscription_id\n"
        "in_1,cus_1,a@example.com,paid,usd,120.0,120.0,0.0,2026-03-01,2026-03-01,2026-03-31,sub_1\n"
        "in_2,cus_2,b@example.com,paid,usd,80.0,80.0,0.0,2026-02-10,2026-02-10,2026-03-10,sub_2\n",
        encoding="utf-8",
    )
    connector = GrowthConnector(data_dir=str(tmp_path))
    service = IntegrationService(connector)

    envelope = service.stripe_revenue_summary()

    assert "Stripe Revenue Summary" in envelope.body
    assert "$120.00" in envelope.body or "$200.00" in envelope.body


def test_sync_stripe_tool_exports_from_server(tmp_path):
    """The public server export should expose Stripe sync and summary tools."""
    assert hasattr(__import__("growth_os.server", fromlist=["sync_stripe_billing"]), "sync_stripe_billing")
    settings.growth_data_dir = str(tmp_path)
    reset_connector()
