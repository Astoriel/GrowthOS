"""Stripe REST connector."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from growth_os.config.settings import settings


@dataclass(slots=True)
class StripeSyncResult:
    """Output metadata for a Stripe sync run."""

    output_dir: str
    customers: int
    invoices: int
    subscriptions: int
    user_events: int
    files: list[str]


class StripeConnector:
    """Minimal Stripe connector backed by the official REST API."""

    name = "stripe"

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        client: httpx.Client | None = None,
    ):
        self.api_key = api_key or settings.stripe_api_key
        self.base_url = (base_url or settings.stripe_base_url).rstrip("/")
        self._client = client

    @property
    def configured(self) -> bool:
        """Return True when the connector has credentials."""
        return bool(self.api_key)

    @property
    def status(self) -> str:
        """Return connector readiness status."""
        return "active" if self.configured else "available"

    def sync_billing_data(self, output_dir: str, lookback_days: int = 365) -> StripeSyncResult:
        """Fetch Stripe billing data and persist normalized CSV extracts."""
        if not self.configured:
            raise ValueError("STRIPE_API_KEY is required to sync Stripe data.")

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        created_gte = int((datetime.now(UTC) - timedelta(days=max(lookback_days, 1))).timestamp())

        customers = self._collect("customers", params={"limit": 100, "created[gte]": created_gte})
        invoices = self._collect("invoices", params={"limit": 100, "created[gte]": created_gte})
        subscriptions = self._collect("subscriptions", params={"limit": 100, "created[gte]": created_gte})

        customer_rows = [self._normalize_customer(customer) for customer in customers]
        invoice_rows = [self._normalize_invoice(invoice) for invoice in invoices]
        subscription_rows = [self._normalize_subscription(subscription) for subscription in subscriptions]
        user_event_rows = [self._invoice_to_user_event(invoice) for invoice in invoices if invoice.get("status") == "paid"]

        files = [
            self._write_csv(output_path / "stripe_customers.csv", customer_rows),
            self._write_csv(output_path / "stripe_invoices.csv", invoice_rows),
            self._write_csv(output_path / "stripe_subscriptions.csv", subscription_rows),
            self._write_csv(output_path / "stripe_user_events.csv", user_event_rows),
        ]

        return StripeSyncResult(
            output_dir=str(output_path),
            customers=len(customer_rows),
            invoices=len(invoice_rows),
            subscriptions=len(subscription_rows),
            user_events=len(user_event_rows),
            files=[file for file in files if file],
        )

    def _collect(self, resource: str, params: dict[str, int | str]) -> list[dict]:
        """Collect a paginated Stripe list endpoint."""
        results: list[dict] = []
        request_params = dict(params)

        while True:
            payload = self._request("GET", resource, params=request_params)
            data = payload.get("data", [])
            results.extend(data)
            if not payload.get("has_more") or not data:
                break
            request_params["starting_after"] = data[-1]["id"]

        return results

    def _request(self, method: str, resource: str, params: dict[str, int | str] | None = None) -> dict:
        """Perform one Stripe API request."""
        client = self._client or httpx.Client(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=30.0,
        )
        close_client = self._client is None
        try:
            response = client.request(method, f"/{resource}", params=params)
            response.raise_for_status()
            return response.json()
        finally:
            if close_client:
                client.close()

    def _normalize_customer(self, customer: dict) -> dict:
        """Normalize Stripe customer payload."""
        return {
            "customer_id": customer.get("id", ""),
            "email": customer.get("email", "") or "",
            "name": customer.get("name", "") or "",
            "created": _stripe_ts(customer.get("created")),
            "currency": customer.get("currency", "") or "",
            "delinquent": bool(customer.get("delinquent", False)),
        }

    def _normalize_invoice(self, invoice: dict) -> dict:
        """Normalize Stripe invoice payload."""
        customer_email = ""
        customer_details = invoice.get("customer_email") or invoice.get("customer_details", {})
        if isinstance(customer_details, dict):
            customer_email = customer_details.get("email", "") or ""
        elif isinstance(customer_details, str):
            customer_email = customer_details

        period_end = ""
        period_start = ""
        lines = invoice.get("lines", {}).get("data", [])
        if lines:
            first_line = lines[0]
            period = first_line.get("period", {})
            period_start = _stripe_ts(period.get("start"))
            period_end = _stripe_ts(period.get("end"))

        return {
            "invoice_id": invoice.get("id", ""),
            "customer_id": invoice.get("customer", "") or "",
            "customer_email": customer_email,
            "status": invoice.get("status", "") or "",
            "currency": invoice.get("currency", "") or "",
            "amount_paid": _amount_to_decimal(invoice.get("amount_paid")),
            "amount_due": _amount_to_decimal(invoice.get("amount_due")),
            "amount_remaining": _amount_to_decimal(invoice.get("amount_remaining")),
            "created": _stripe_ts(invoice.get("created")),
            "period_start": period_start,
            "period_end": period_end,
            "subscription_id": invoice.get("subscription", "") or "",
        }

    def _normalize_subscription(self, subscription: dict) -> dict:
        """Normalize Stripe subscription payload."""
        item = next(iter(subscription.get("items", {}).get("data", [])), {})
        price = item.get("price", {})
        return {
            "subscription_id": subscription.get("id", ""),
            "customer_id": subscription.get("customer", "") or "",
            "status": subscription.get("status", "") or "",
            "created": _stripe_ts(subscription.get("created")),
            "current_period_start": _stripe_ts(subscription.get("current_period_start")),
            "current_period_end": _stripe_ts(subscription.get("current_period_end")),
            "cancel_at_period_end": bool(subscription.get("cancel_at_period_end", False)),
            "currency": price.get("currency", "") or "",
            "price_id": price.get("id", "") or "",
            "unit_amount": _amount_to_decimal(price.get("unit_amount")),
            "interval": price.get("recurring", {}).get("interval", "") if isinstance(price.get("recurring"), dict) else "",
        }

    def _invoice_to_user_event(self, invoice: dict) -> dict:
        """Convert a paid invoice into canonical user_event-like purchase rows."""
        customer_id = invoice.get("customer", "") or invoice.get("customer_email", "") or invoice.get("id", "")
        return {
            "user_id": customer_id,
            "event_type": "purchase",
            "event_date": _stripe_ts(invoice.get("status_transitions", {}).get("paid_at") or invoice.get("created")),
            "utm_source": "stripe",
            "revenue": _amount_to_decimal(invoice.get("amount_paid")),
            "source_system": "stripe",
        }

    def _write_csv(self, path: Path, rows: list[dict]) -> str:
        """Write a normalized extract to disk."""
        if not rows:
            return ""
        with open(path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        return str(path)


def _stripe_ts(value: int | None) -> str:
    """Convert a Stripe unix timestamp to YYYY-MM-DD."""
    if not value:
        return ""
    return datetime.fromtimestamp(int(value), UTC).strftime("%Y-%m-%d")


def _amount_to_decimal(value: int | None) -> float:
    """Convert Stripe minor units to decimal major units."""
    if value is None:
        return 0.0
    return round(int(value) / 100, 2)
