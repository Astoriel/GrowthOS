"""Google Ads REST connector."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from growth_os.config.settings import settings


@dataclass(slots=True)
class GoogleAdsSyncResult:
    """Output metadata for a Google Ads sync."""

    output_dir: str
    campaigns: int
    spend_rows: int
    files: list[str]


class GoogleAdsConnector:
    """Minimal Google Ads connector backed by the REST searchStream API."""

    name = "google_ads"

    def __init__(
        self,
        developer_token: str | None = None,
        customer_id: str | None = None,
        login_customer_id: str | None = None,
        access_token: str | None = None,
        refresh_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        api_version: str | None = None,
        base_url: str | None = None,
        oauth_token_url: str | None = None,
        client: httpx.Client | None = None,
    ):
        self.developer_token = developer_token or settings.google_ads_developer_token
        self.customer_id = _digits_only(customer_id or settings.google_ads_customer_id)
        self.login_customer_id = _digits_only(login_customer_id or settings.google_ads_login_customer_id)
        self.access_token = access_token or settings.google_ads_access_token
        self.refresh_token = refresh_token or settings.google_ads_refresh_token
        self.client_id = client_id or settings.google_ads_client_id
        self.client_secret = client_secret or settings.google_ads_client_secret
        self.api_version = api_version or settings.google_ads_api_version
        self.base_url = (base_url or settings.google_ads_base_url).rstrip("/")
        self.oauth_token_url = oauth_token_url or settings.google_ads_oauth_token_url
        self._client = client
        self._cached_access_token: str | None = None

    @property
    def configured(self) -> bool:
        """Return True when Google Ads credentials are present."""
        has_token = bool(self.access_token) or bool(self.refresh_token and self.client_id and self.client_secret)
        return bool(self.developer_token and self.customer_id and has_token)

    @property
    def status(self) -> str:
        """Return connector readiness status."""
        return "active" if self.configured else "available"

    def sync_ads_data(self, output_dir: str, lookback_days: int = 90) -> GoogleAdsSyncResult:
        """Fetch Google Ads campaign metadata and daily performance rows."""
        if not self.configured:
            raise ValueError(
                "GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CUSTOMER_ID, and OAuth credentials are required."
            )

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        since = (datetime.now(UTC).date() - timedelta(days=max(lookback_days - 1, 0))).isoformat()
        until = datetime.now(UTC).date().isoformat()

        campaigns = self._search_stream(
            """
            SELECT
              campaign.id,
              campaign.name,
              campaign.status,
              campaign.advertising_channel_type,
              campaign.advertising_channel_sub_type,
              campaign.start_date,
              campaign.end_date
            FROM campaign
            WHERE campaign.status != 'REMOVED'
            """
        )
        performance = self._search_stream(
            f"""
            SELECT
              campaign.id,
              campaign.name,
              segments.date,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions
            FROM campaign
            WHERE campaign.status != 'REMOVED'
              AND segments.date BETWEEN '{since}' AND '{until}'
            """
        )

        campaign_rows = [self._normalize_campaign(row) for row in campaigns]
        spend_rows = [self._normalize_performance(row) for row in performance]
        files = [
            self._write_csv(output_path / "google_ads_campaigns.csv", campaign_rows),
            self._write_csv(output_path / "google_marketing_spend.csv", spend_rows),
        ]
        return GoogleAdsSyncResult(
            output_dir=str(output_path),
            campaigns=len(campaign_rows),
            spend_rows=len(spend_rows),
            files=[file for file in files if file],
        )

    def _search_stream(self, query: str) -> list[dict]:
        """Run a GAQL query via searchStream and flatten the results."""
        url = f"/{self.api_version}/customers/{self.customer_id}/googleAds:searchStream"
        payload = self._request("POST", url, json={"query": _clean_query(query)})
        batches = payload if isinstance(payload, list) else [payload]
        rows: list[dict] = []
        for batch in batches:
            rows.extend(batch.get("results", []))
        return rows

    def _request(self, method: str, path: str, json: dict | None = None) -> dict | list[dict]:
        """Perform one Google Ads REST request."""
        client = self._client or httpx.Client(base_url=self.base_url, timeout=30.0)
        close_client = self._client is None
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "developer-token": self.developer_token,
            "Content-Type": "application/json",
        }
        if self.login_customer_id:
            headers["login-customer-id"] = self.login_customer_id

        try:
            response = client.request(method, path, headers=headers, json=json)
            response.raise_for_status()
            return response.json()
        finally:
            if close_client:
                client.close()

    def _get_access_token(self) -> str:
        """Resolve an access token, using refresh flow when needed."""
        if self.access_token:
            return self.access_token
        if self._cached_access_token:
            return self._cached_access_token
        if not (self.refresh_token and self.client_id and self.client_secret):
            raise ValueError("Google Ads OAuth credentials are incomplete.")

        response = httpx.post(
            self.oauth_token_url,
            data={
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
            },
            timeout=30.0,
        )
        response.raise_for_status()
        self._cached_access_token = response.json()["access_token"]
        return self._cached_access_token

    def _normalize_campaign(self, row: dict) -> dict:
        """Normalize a campaign row from the REST JSON response."""
        campaign = row.get("campaign", {})
        return {
            "campaign_id": campaign.get("id", "") or "",
            "name": campaign.get("name", "") or "",
            "channel": "google_ads",
            "campaign_type": campaign.get("advertisingChannelType", "") or "",
            "campaign_sub_type": campaign.get("advertisingChannelSubType", "") or "",
            "status": campaign.get("status", "") or "",
            "start_date": campaign.get("startDate", "") or "",
            "end_date": campaign.get("endDate", "") or "",
            "source_system": "google_ads",
        }

    def _normalize_performance(self, row: dict) -> dict:
        """Normalize campaign performance into marketing_spend-like rows."""
        campaign = row.get("campaign", {})
        metrics = row.get("metrics", {})
        segments = row.get("segments", {})
        return {
            "date": segments.get("date", "") or "",
            "channel": "google_ads",
            "campaign_id": campaign.get("id", "") or "",
            "campaign": campaign.get("name", "") or "",
            "spend": round(int(metrics.get("costMicros", 0) or 0) / 1_000_000, 2),
            "impressions": int(metrics.get("impressions", 0) or 0),
            "clicks": int(metrics.get("clicks", 0) or 0),
            "conversions": round(float(metrics.get("conversions", 0) or 0), 2),
            "source_system": "google_ads",
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


def _digits_only(value: str | None) -> str:
    """Strip separators from Google Ads customer IDs."""
    if not value:
        return ""
    return "".join(char for char in value if char.isdigit())


def _clean_query(query: str) -> str:
    """Collapse multiline GAQL into a compact request string."""
    return " ".join(line.strip() for line in query.strip().splitlines() if line.strip())
