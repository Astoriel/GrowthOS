"""HubSpot CRM connector."""

from __future__ import annotations

import csv
import logging
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from growth_os.domain.exceptions import ConnectorError
from growth_os.domain.models import ToolEnvelope

logger = logging.getLogger(__name__)

_LIFECYCLE_STAGES = [
    "subscriber",
    "lead",
    "marketingqualifiedlead",
    "salesqualifiedlead",
    "opportunity",
    "customer",
    "evangelist",
    "other",
]

_LEAD_SOURCES = [
    "organic_search",
    "direct_traffic",
    "paid_search",
    "paid_social",
    "referral",
    "email_marketing",
]

_DEAL_STAGES = [
    "appointmentscheduled",
    "qualifiedtobuy",
    "presentationscheduled",
    "decisionmakerboughtin",
    "contractsent",
    "closedwon",
    "closedlost",
]

_PIPELINES = ["default", "enterprise", "smb"]

_UTM_SOURCES = ["google", "meta", "linkedin", "organic", "direct", "email"]

_FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Emma",
    "Frank", "Grace", "Henry", "Iris", "Jack",
    "Karen", "Liam", "Maria", "Noah", "Olivia",
]

_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones",
    "Garcia", "Miller", "Davis", "Wilson", "Taylor",
    "Anderson", "Thomas", "Jackson", "White", "Harris",
]

_DEFAULT_BASE_URL = "https://api.hubapi.com"


@dataclass(slots=True)
class HubSpotSyncResult:
    """Output metadata for a HubSpot sync run."""

    output_dir: str
    contacts: int
    deals: int
    files: list[str]


class HubSpotConnector:
    """HubSpot CRM connector.

    Operates in demo mode (generates mock CSV data) when ``api_key`` is empty.
    When real credentials are supplied, makes live API calls to HubSpot CRM v3.
    """

    name = "hubspot"

    def __init__(
        self, api_key: str = "", base_url: str = _DEFAULT_BASE_URL
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    @property
    def configured(self) -> bool:
        """Return True when API credentials are present."""
        return bool(self.api_key)

    @property
    def status(self) -> str:
        """Return connector readiness status."""
        return "active" if self.configured else "demo"

    # ------------------------------------------------------------------
    # Private API helpers
    # ------------------------------------------------------------------

    def _fetch_all(
        self,
        client: httpx.Client,
        endpoint: str,
        properties: list[str],
    ) -> list[dict]:
        """Paginate through a HubSpot CRM v3 list endpoint.

        Returns a list of raw result dicts (each containing ``id`` and
        ``properties``).
        """
        results: list[dict] = []
        params: dict[str, str] = {
            "limit": "100",
            "properties": ",".join(properties),
        }

        while True:
            try:
                response = client.get(endpoint, params=params)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                try:
                    detail = exc.response.json().get("message", exc.response.text)
                except Exception:
                    detail = exc.response.text
                logger.error(
                    "HubSpot API error %d on %s: %s", status, endpoint, detail
                )
                raise ConnectorError(
                    f"HubSpot API returned HTTP {status}: {detail}"
                ) from exc
            except httpx.HTTPError as exc:
                logger.error("HubSpot request failed for %s: %s", endpoint, exc)
                raise ConnectorError(
                    f"HubSpot request failed: {exc}"
                ) from exc

            data = response.json()
            results.extend(data.get("results", []))

            # Cursor-based pagination
            paging = data.get("paging")
            if paging and "next" in paging:
                after = paging["next"].get("after")
                if after:
                    params["after"] = after
                    continue
            break

        return results

    # ------------------------------------------------------------------
    # Public sync methods
    # ------------------------------------------------------------------

    def sync_contacts(self, output_dir: str, lookback_days: int = 90) -> list[dict]:
        """Sync HubSpot contacts into *output_dir*.

        In demo mode (no ``api_key``) generates realistic mock rows and writes
        ``hubspot_contacts.csv``.  When credentials are supplied, fetches
        real contacts from the HubSpot CRM v3 API.

        Returns the list of row dicts written to disk.
        """
        if self.configured:
            rows = self._fetch_real_contacts()
        else:
            rows = self._generate_mock_contacts(lookback_days)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        self._write_csv(output_path / "hubspot_contacts.csv", rows)
        return rows

    def sync_deals(self, output_dir: str, lookback_days: int = 90) -> list[dict]:
        """Sync HubSpot deals into *output_dir*.

        In demo mode (no ``api_key``) generates realistic mock rows and writes
        ``hubspot_deals.csv``.  When credentials are supplied, fetches
        real deals from the HubSpot CRM v3 API.

        Returns the list of row dicts written to disk.
        """
        if self.configured:
            rows = self._fetch_real_deals()
        else:
            rows = self._generate_mock_deals(lookback_days)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        self._write_csv(output_path / "hubspot_deals.csv", rows)
        return rows

    def sync(self, output_dir: str, lookback_days: int = 90) -> HubSpotSyncResult:
        """Sync both contacts and deals in a single call.

        Returns a :class:`HubSpotSyncResult` summarising row counts and file
        paths written.
        """
        contact_rows = self.sync_contacts(output_dir, lookback_days)
        deal_rows = self.sync_deals(output_dir, lookback_days)
        output_path = Path(output_dir)
        files = [
            str(output_path / "hubspot_contacts.csv") if contact_rows else "",
            str(output_path / "hubspot_deals.csv") if deal_rows else "",
        ]
        return HubSpotSyncResult(
            output_dir=str(output_path),
            contacts=len(contact_rows),
            deals=len(deal_rows),
            files=[f for f in files if f],
        )

    def contacts_summary(self, contacts_table: str) -> ToolEnvelope:
        """Return a :class:`ToolEnvelope` summarising contacts by lifecycle stage.

        Uses fixed-seed mock counts so the output is deterministic in demo mode.
        Pass ``contacts_table`` as the logical table name to embed in the output.
        """
        rng = random.Random(42)
        stage_counts: dict[str, int] = {
            stage: rng.randint(5, 120) for stage in _LIFECYCLE_STAGES
        }
        total = sum(stage_counts.values())
        header = "| Lifecycle Stage | Contacts | Share |\n| --- | --- | --- |"
        rows_md = "\n".join(
            f"| {stage} | {count} | {100 * count / total:.1f}% |"
            for stage, count in stage_counts.items()
        )
        body = (
            f"**Table:** `{contacts_table}`\n\n"
            f"{header}\n"
            f"{rows_md}\n\n"
            f"**Total contacts:** {total}"
        )
        return ToolEnvelope(
            title="HubSpot Contacts by Lifecycle Stage",
            body=body,
            sources=[contacts_table],
        )

    # ------------------------------------------------------------------
    # Private: real API fetchers
    # ------------------------------------------------------------------

    def _fetch_real_contacts(self) -> list[dict]:
        """Fetch contacts from HubSpot CRM v3 and normalize to CSV columns."""
        endpoint = f"{self.base_url}/crm/v3/objects/contacts"
        properties = [
            "email",
            "firstname",
            "lastname",
            "createdate",
            "lifecyclestage",
            "hs_lead_status",
            "hs_analytics_source",
        ]

        with httpx.Client(
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=30.0,
        ) as client:
            raw_results = self._fetch_all(client, endpoint, properties)

        rows: list[dict] = []
        for result in raw_results:
            props = result.get("properties", {})
            createdate_raw = props.get("createdate") or ""
            created_date = createdate_raw[:10] if createdate_raw else ""
            rows.append(
                {
                    "id": result.get("id", ""),
                    "email": props.get("email") or "",
                    "firstname": props.get("firstname") or "",
                    "lastname": props.get("lastname") or "",
                    "created_date": created_date,
                    "lifecycle_stage": props.get("lifecyclestage") or "unknown",
                    "lead_source": props.get("hs_analytics_source") or "unknown",
                    "num_associated_deals": 0,
                }
            )

        logger.info("Fetched %d contacts from HubSpot", len(rows))
        return rows

    def _fetch_real_deals(self) -> list[dict]:
        """Fetch deals from HubSpot CRM v3 and normalize to CSV columns."""
        endpoint = f"{self.base_url}/crm/v3/objects/deals"
        properties = [
            "dealname",
            "amount",
            "dealstage",
            "createdate",
            "closedate",
            "pipeline",
            "hs_analytics_source",
        ]

        with httpx.Client(
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=30.0,
        ) as client:
            raw_results = self._fetch_all(client, endpoint, properties)

        rows: list[dict] = []
        for result in raw_results:
            props = result.get("properties", {})
            createdate_raw = props.get("createdate") or ""
            created_date = createdate_raw[:10] if createdate_raw else ""
            closedate_raw = props.get("closedate") or ""
            close_date = closedate_raw[:10] if closedate_raw else ""
            amount_raw = props.get("amount")
            try:
                amount = float(amount_raw) if amount_raw else 0.0
            except (TypeError, ValueError):
                amount = 0.0
            rows.append(
                {
                    "id": result.get("id", ""),
                    "dealname": props.get("dealname") or "",
                    "amount": amount,
                    "stage": props.get("dealstage") or "",
                    "created_date": created_date,
                    "close_date": close_date,
                    "pipeline": props.get("pipeline") or "",
                    "utm_source": props.get("hs_analytics_source") or "direct",
                }
            )

        logger.info("Fetched %d deals from HubSpot", len(rows))
        return rows

    # ------------------------------------------------------------------
    # Private mock-data generators
    # ------------------------------------------------------------------

    def _generate_mock_contacts(self, lookback_days: int) -> list[dict]:
        """Generate realistic mock HubSpot contact rows."""
        rng = random.Random(42)
        now = datetime.now(UTC)
        count = min(max(lookback_days * 2, 20), 200)
        rows: list[dict] = []
        for i in range(1, count + 1):
            days_ago = rng.randint(0, lookback_days)
            created = now - timedelta(days=days_ago)
            rows.append(
                {
                    "id": f"hs_contact_{i}",
                    "email": f"user{i}@example.com",
                    "firstname": rng.choice(_FIRST_NAMES),
                    "lastname": rng.choice(_LAST_NAMES),
                    "created_date": created.strftime("%Y-%m-%d"),
                    "lifecycle_stage": rng.choice(_LIFECYCLE_STAGES),
                    "lead_source": rng.choice(_LEAD_SOURCES),
                    "num_associated_deals": rng.randint(0, 5),
                }
            )
        return rows

    def _generate_mock_deals(self, lookback_days: int) -> list[dict]:
        """Generate realistic mock HubSpot deal rows."""
        rng = random.Random(42)
        now = datetime.now(UTC)
        count = min(max(lookback_days, 10), 100)
        rows: list[dict] = []
        for i in range(1, count + 1):
            days_ago = rng.randint(0, lookback_days)
            created = now - timedelta(days=days_ago)
            close_offset = rng.randint(7, 90)
            close = created + timedelta(days=close_offset)
            rows.append(
                {
                    "id": f"hs_deal_{i}",
                    "dealname": f"Deal with Company {i}",
                    "amount": round(rng.uniform(1_000.0, 50_000.0), 2),
                    "stage": rng.choice(_DEAL_STAGES),
                    "created_date": created.strftime("%Y-%m-%d"),
                    "close_date": close.strftime("%Y-%m-%d"),
                    "pipeline": rng.choice(_PIPELINES),
                    "utm_source": rng.choice(_UTM_SOURCES),
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Shared CSV writer (mirrors pattern from StripeConnector, MetaAdsConnector)
    # ------------------------------------------------------------------

    def _write_csv(self, path: Path, rows: list[dict]) -> str:
        """Write a normalized extract to disk and return the file path."""
        if not rows:
            return ""
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        return str(path)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _iso_date(dt: datetime) -> str:
    """Return an ISO-8601 date string for *dt*."""
    return dt.strftime("%Y-%m-%d")


__all__ = [
    "HubSpotConnector",
    "HubSpotSyncResult",
    "ConnectorError",
]
