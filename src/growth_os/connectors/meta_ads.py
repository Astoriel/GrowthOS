"""Meta Ads REST connector."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx

from growth_os.config.settings import settings


@dataclass(slots=True)
class MetaAdsSyncResult:
    """Output metadata for a Meta Ads sync."""

    output_dir: str
    campaigns: int
    spend_rows: int
    files: list[str]


class MetaAdsConnector:
    """Minimal Meta Ads connector backed by the Graph API."""

    name = "meta_ads"

    def __init__(
        self,
        access_token: str | None = None,
        ad_account_id: str | None = None,
        api_version: str | None = None,
        base_url: str | None = None,
        client: httpx.Client | None = None,
    ):
        self.access_token = access_token or settings.meta_access_token
        self.ad_account_id = ad_account_id or settings.meta_ad_account_id
        self.api_version = api_version or settings.meta_api_version
        self.base_url = (base_url or settings.meta_base_url).rstrip("/")
        self._client = client

    @property
    def configured(self) -> bool:
        """Return True when connector credentials are present."""
        return bool(self.access_token and self.ad_account_id)

    @property
    def status(self) -> str:
        """Return connector readiness status."""
        return "active" if self.configured else "available"

    def sync_ads_data(self, output_dir: str, lookback_days: int = 90) -> MetaAdsSyncResult:
        """Fetch campaigns and campaign-level daily insights from Meta Ads."""
        if not self.configured:
            raise ValueError("META_ACCESS_TOKEN and META_AD_ACCOUNT_ID are required.")

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        since = (datetime.now(UTC).date() - timedelta(days=max(lookback_days - 1, 0))).isoformat()
        until = datetime.now(UTC).date().isoformat()

        campaigns = self._collect(
            f"/{self._account_path()}/campaigns",
            params={
                "fields": "id,name,objective,status,effective_status,created_time,start_time,stop_time,daily_budget,lifetime_budget",
                "limit": 100,
            },
        )
        insights = self._collect(
            f"/{self._account_path()}/insights",
            params={
                "fields": "campaign_id,campaign_name,date_start,date_stop,impressions,clicks,spend,reach,cpc,ctr",
                "level": "campaign",
                "time_increment": 1,
                "time_range": f'{{"since":"{since}","until":"{until}"}}',
                "limit": 100,
            },
        )

        campaign_rows = [self._normalize_campaign(campaign) for campaign in campaigns]
        spend_rows = [self._normalize_insight(insight) for insight in insights]

        files = [
            self._write_csv(output_path / "meta_campaigns.csv", campaign_rows),
            self._write_csv(output_path / "meta_marketing_spend.csv", spend_rows),
        ]
        return MetaAdsSyncResult(
            output_dir=str(output_path),
            campaigns=len(campaign_rows),
            spend_rows=len(spend_rows),
            files=[file for file in files if file],
        )

    def _collect(self, path: str, params: dict[str, str | int]) -> list[dict]:
        """Collect a paginated Graph API list."""
        results: list[dict] = []
        next_url: str | None = None
        request_params = dict(params)

        while True:
            payload = self._request("GET", next_url or path, params=request_params if next_url is None else None)
            data = payload.get("data", [])
            results.extend(data)
            paging = payload.get("paging", {})
            next_url = paging.get("next")
            if not next_url or not data:
                break
            request_params = {}

        return results

    def _request(self, method: str, path_or_url: str, params: dict[str, str | int] | None = None) -> dict:
        """Perform one Graph API request."""
        client = self._client or httpx.Client(base_url=f"{self.base_url}/{self.api_version}", timeout=30.0)
        close_client = self._client is None

        url = path_or_url
        request_params = dict(params or {})
        if path_or_url.startswith("https://"):
            if params:
                request_params.setdefault("access_token", self.access_token)
            else:
                request_params = None
        else:
            request_params.setdefault("access_token", self.access_token)

        try:
            response = client.request(method, url, params=request_params)
            response.raise_for_status()
            return response.json()
        finally:
            if close_client:
                client.close()

    def _account_path(self) -> str:
        """Return the Graph API path for the ad account."""
        account_id = self.ad_account_id
        return account_id if account_id.startswith("act_") else f"act_{account_id}"

    def _normalize_campaign(self, campaign: dict) -> dict:
        """Normalize Meta campaign payload."""
        return {
            "campaign_id": campaign.get("id", ""),
            "name": campaign.get("name", "") or "",
            "channel": "meta_ads",
            "objective": campaign.get("objective", "") or "",
            "status": campaign.get("status", "") or "",
            "effective_status": ",".join(campaign.get("effective_status", []))
            if isinstance(campaign.get("effective_status"), list)
            else campaign.get("effective_status", "") or "",
            "created_time": _meta_datetime(campaign.get("created_time")),
            "start_date": _meta_datetime(campaign.get("start_time")),
            "stop_date": _meta_datetime(campaign.get("stop_time")),
            "daily_budget": _minor_units(campaign.get("daily_budget")),
            "lifetime_budget": _minor_units(campaign.get("lifetime_budget")),
            "source_system": "meta_ads",
        }

    def _normalize_insight(self, insight: dict) -> dict:
        """Normalize campaign-level daily insights into marketing_spend-like rows."""
        return {
            "date": insight.get("date_start", ""),
            "channel": "meta_ads",
            "campaign_id": insight.get("campaign_id", "") or "",
            "campaign": insight.get("campaign_name", "") or "",
            "spend": _float_or_zero(insight.get("spend")),
            "impressions": _int_or_zero(insight.get("impressions")),
            "clicks": _int_or_zero(insight.get("clicks")),
            "conversions": 0,
            "reach": _int_or_zero(insight.get("reach")),
            "cpc": _float_or_zero(insight.get("cpc")),
            "ctr": _float_or_zero(insight.get("ctr")),
            "source_system": "meta_ads",
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


def _meta_datetime(value: str | None) -> str:
    """Normalize Meta datetime strings to YYYY-MM-DD when possible."""
    if not value:
        return ""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return value[:10]


def _minor_units(value: str | int | None) -> float:
    """Convert Meta budget minor units to decimal major units."""
    if value in (None, ""):
        return 0.0
    return round(int(value) / 100, 2)


def _float_or_zero(value: str | float | int | None) -> float:
    """Convert API values to float."""
    if value in (None, ""):
        return 0.0
    return round(float(value), 2)


def _int_or_zero(value: str | int | None) -> int:
    """Convert API values to int."""
    if value in (None, ""):
        return 0
    return int(float(value))
