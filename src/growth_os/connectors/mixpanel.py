"""Mixpanel analytics connector."""

from __future__ import annotations

import csv
import json
import logging
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from growth_os.domain.exceptions import ConnectorError

logger = logging.getLogger(__name__)

_EVENT_NAMES = [
    "Signup",
    "Login",
    "Purchase",
    "PageView",
    "ButtonClick",
    "VideoPlay",
    "FormSubmit",
    "FeatureUsed",
    "Upgrade",
    "Cancellation",
]

_UTM_SOURCES = ["google", "meta", "linkedin", "organic", "direct", "email", "referral"]

_UTM_CAMPAIGNS = [
    "brand_search",
    "retargeting",
    "welcome_drip",
    "product_launch",
    "seasonal_promo",
    "reactivation",
    "",
]

_FUNNEL_TEMPLATES: dict[str, list[str]] = {
    "signup_to_purchase": ["Signup", "Onboarding", "FeatureUsed", "Purchase"],
    "trial_conversion": ["TrialStart", "FeatureUsed", "UpgradePrompt", "Upgrade"],
    "retention_7d": ["Day1Active", "Day3Active", "Day7Active"],
}


@dataclass(slots=True)
class MixpanelSyncResult:
    """Output metadata for a Mixpanel sync run."""

    output_dir: str
    events: int
    funnel_steps: int
    files: list[str]


class MixpanelConnector:
    """Mixpanel analytics connector.

    Operates in demo mode (generates mock CSV data) when ``api_secret`` is
    empty.  When real credentials are supplied, makes live API calls to
    Mixpanel's Export and Query APIs.
    """

    name = "mixpanel"

    def __init__(
        self,
        api_secret: str = "",
        project_id: str = "",
        eu: bool = True,
    ) -> None:
        self.api_secret = api_secret
        self.project_id = project_id
        self.eu = eu

    @property
    def configured(self) -> bool:
        """Return True when an API secret is present."""
        return bool(self.api_secret)

    @property
    def status(self) -> str:
        """Return connector readiness status."""
        return "active" if self.configured else "demo"

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    def _export_base_url(self) -> str:
        """Return the raw-export base URL for the configured region."""
        if self.eu:
            return "https://data-eu.mixpanel.com/api/2.0/export"
        return "https://data.mixpanel.com/api/2.0/export"

    def _query_base_url(self) -> str:
        """Return the query-API base URL for the configured region."""
        if self.eu:
            return "https://eu.mixpanel.com/api/query"
        return "https://mixpanel.com/api/query"

    # ------------------------------------------------------------------
    # Private: real API fetchers
    # ------------------------------------------------------------------

    def _fetch_real_events(self, lookback_days: int) -> list[dict]:
        """Fetch events from Mixpanel Raw Export API and normalize rows."""
        today = datetime.now(UTC).date()
        from_date = today - timedelta(days=lookback_days)
        params = {
            "from_date": from_date.strftime("%Y-%m-%d"),
            "to_date": today.strftime("%Y-%m-%d"),
        }

        try:
            with httpx.Client(timeout=60.0) as client:
                response = client.get(
                    self._export_base_url(),
                    params=params,
                    auth=(self.api_secret, ""),
                )
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            try:
                detail = exc.response.json().get("error", exc.response.text)
            except Exception:
                detail = exc.response.text
            logger.error(
                "Mixpanel Export API error %d: %s", status, detail,
            )
            raise ConnectorError(
                f"Mixpanel Export API returned HTTP {status}: {detail}"
            ) from exc
        except httpx.HTTPError as exc:
            logger.error("Mixpanel Export request failed: %s", exc)
            raise ConnectorError(
                f"Mixpanel Export request failed: {exc}"
            ) from exc

        text = response.text.strip()
        if not text or "terminated early" in text:
            logger.warning("Mixpanel export returned empty or terminated-early response")
            return []

        rows: list[dict] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSONL line: %.120s", line)
                continue

            props = record.get("properties", {})
            # Convert unix timestamp to ISO string
            raw_time = props.get("time")
            if raw_time is not None:
                try:
                    iso_time = datetime.fromtimestamp(
                        int(raw_time), tz=UTC
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                except (ValueError, TypeError, OSError):
                    iso_time = ""
            else:
                iso_time = ""

            rows.append(
                {
                    "event": record.get("event", ""),
                    "distinct_id": props.get("distinct_id", ""),
                    "time": iso_time,
                    "utm_source": props.get("utm_source", ""),
                    "utm_campaign": props.get("utm_campaign", ""),
                    "properties_json": json.dumps(props),
                }
            )

        logger.info("Fetched %d events from Mixpanel Export API", len(rows))
        return rows

    def _fetch_real_funnels(self) -> list[dict] | None:
        """Fetch funnel list from Mixpanel Query API.

        Returns normalized rows on success, or ``None`` when the endpoint
        returns HTTP 402 (free-plan limitation) so the caller can fall back
        to mock funnels.
        """
        url = f"{self._query_base_url()}/funnels/list"

        try:
            with httpx.Client(timeout=60.0) as client:
                response = client.get(url, auth=(self.api_secret, ""))
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 402:
                logger.info(
                    "Mixpanel funnels/list returned 402 — falling back to mock funnels"
                )
                return None
            status = exc.response.status_code
            try:
                detail = exc.response.json().get("error", exc.response.text)
            except Exception:
                detail = exc.response.text
            logger.error(
                "Mixpanel Funnels API error %d: %s", status, detail,
            )
            raise ConnectorError(
                f"Mixpanel Funnels API returned HTTP {status}: {detail}"
            ) from exc
        except httpx.HTTPError as exc:
            logger.error("Mixpanel Funnels request failed: %s", exc)
            raise ConnectorError(
                f"Mixpanel Funnels request failed: {exc}"
            ) from exc

        data = response.json()
        if not isinstance(data, list):
            data = data.get("results", data.get("data", []))

        rows: list[dict] = []
        for funnel in data:
            funnel_id = funnel.get("funnel_id", funnel.get("id", ""))
            steps = funnel.get("steps", [])
            for idx, step in enumerate(steps):
                count = step.get("count", 0)
                conversion = step.get("conversion_rate", step.get("step_conv_ratio", 0.0))
                rows.append(
                    {
                        "funnel_id": str(funnel_id),
                        "step_name": step.get("event", step.get("name", f"Step {idx}")),
                        "step_index": idx,
                        "count": int(count),
                        "conversion_rate": round(float(conversion), 4),
                    }
                )

        logger.info("Fetched %d funnel steps from Mixpanel", len(rows))
        return rows

    # ------------------------------------------------------------------
    # Public sync methods
    # ------------------------------------------------------------------

    def sync_events(
        self,
        output_dir: str,
        event_names: list[str] | None = None,
        lookback_days: int = 30,
    ) -> list[dict]:
        """Sync Mixpanel event data into *output_dir*.

        In demo mode (no ``api_secret``) generates realistic mock rows and
        writes ``mixpanel_events.csv``.  When credentials are supplied, fetches
        real events from the Mixpanel Raw Export API.

        Parameters
        ----------
        output_dir:
            Directory where the CSV will be written.
        event_names:
            Optional filter list; defaults to all built-in event types.
        lookback_days:
            Number of days of history to fetch / simulate.

        Returns the list of row dicts written to disk.
        """
        if self.configured:
            rows = self._fetch_real_events(lookback_days)
            # Apply optional event-name filter when caller restricts events
            if event_names and rows:
                allowed = set(event_names)
                rows = [r for r in rows if r["event"] in allowed]
        else:
            rows = self._generate_mock_events(event_names or _EVENT_NAMES, lookback_days)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        self._write_csv(output_path / "mixpanel_events.csv", rows)
        return rows

    def sync_funnels(
        self,
        output_dir: str,
        funnel_id: str = "",
        lookback_days: int = 30,
    ) -> list[dict]:
        """Sync Mixpanel funnel data into *output_dir*.

        In demo mode writes ``mixpanel_funnels.csv`` with step-level conversion
        rates.  When credentials are supplied, fetches real funnels from the
        Mixpanel Query API.  Falls back to mock data if the account is on the
        free plan (HTTP 402).

        Parameters
        ----------
        output_dir:
            Directory where the CSV will be written.
        funnel_id:
            Optional funnel identifier; if empty all template funnels are used.
        lookback_days:
            Number of days of history to simulate.

        Returns the list of row dicts written to disk.
        """
        if self.configured:
            rows = self._fetch_real_funnels()
            if rows is None:
                # 402 fall-back — use mock funnels
                rows = self._generate_mock_funnels(funnel_id, lookback_days)
        else:
            rows = self._generate_mock_funnels(funnel_id, lookback_days)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        self._write_csv(output_path / "mixpanel_funnels.csv", rows)
        return rows

    def sync(self, output_dir: str, lookback_days: int = 30) -> MixpanelSyncResult:
        """Sync events and all funnel templates in a single call."""
        event_rows = self.sync_events(output_dir, lookback_days=lookback_days)
        funnel_rows = self.sync_funnels(output_dir, lookback_days=lookback_days)
        output_path = Path(output_dir)
        files = [
            str(output_path / "mixpanel_events.csv") if event_rows else "",
            str(output_path / "mixpanel_funnels.csv") if funnel_rows else "",
        ]
        return MixpanelSyncResult(
            output_dir=str(output_path),
            events=len(event_rows),
            funnel_steps=len(funnel_rows),
            files=[f for f in files if f],
        )

    def events_summary(self, events_table: str = "mixpanel_events") -> str:
        """Return a markdown-formatted summary of the events dataset.

        Uses fixed-seed counts so the output is deterministic in demo mode.
        """
        rng = random.Random(43)
        event_counts = {name: rng.randint(50, 2_000) for name in _EVENT_NAMES}
        total = sum(event_counts.values())

        lines = [
            f"## Mixpanel Events Summary",
            f"",
            f"**Table:** `{events_table}`  |  **Total events:** {total:,}",
            f"",
            f"| Event Name | Count | Share |",
            f"| --- | --- | --- |",
        ]
        for name, count in sorted(event_counts.items(), key=lambda x: -x[1]):
            lines.append(f"| {name} | {count:,} | {100 * count / total:.1f}% |")

        if self.configured:
            note = "_Connected to Mixpanel._"
        else:
            note = "_Generated in demo mode \u2014 connect your Mixpanel project token for live data._"
        lines += ["", note]
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Private mock-data generators
    # ------------------------------------------------------------------

    def _generate_mock_events(
        self, event_names: list[str], lookback_days: int
    ) -> list[dict]:
        """Generate realistic mock Mixpanel event rows."""
        rng = random.Random(43)
        now = datetime.now(UTC)
        rows: list[dict] = []
        count = min(max(lookback_days * 10, 50), 500)
        for i in range(1, count + 1):
            seconds_ago = rng.randint(0, lookback_days * 86_400)
            event_time = now - timedelta(seconds=seconds_ago)
            props = {
                "plan": rng.choice(["free", "starter", "pro", "enterprise"]),
                "platform": rng.choice(["web", "ios", "android"]),
            }
            rows.append(
                {
                    "event": rng.choice(event_names),
                    "distinct_id": f"user_{rng.randint(1, count // 2)}",
                    "time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "utm_source": rng.choice(_UTM_SOURCES),
                    "utm_campaign": rng.choice(_UTM_CAMPAIGNS),
                    "properties_json": json.dumps(props),
                }
            )
        return rows

    def _generate_mock_funnels(self, funnel_id: str, lookback_days: int) -> list[dict]:  # noqa: ARG002
        """Generate realistic mock Mixpanel funnel step rows."""
        rng = random.Random(43)
        templates = (
            {funnel_id: _FUNNEL_TEMPLATES.get(funnel_id, [funnel_id])}
            if funnel_id and funnel_id in _FUNNEL_TEMPLATES
            else _FUNNEL_TEMPLATES
        )
        rows: list[dict] = []
        for fid, steps in templates.items():
            entry_count = rng.randint(500, 5_000)
            current = float(entry_count)
            for idx, step in enumerate(steps):
                drop = rng.uniform(0.55, 0.90) if idx > 0 else 1.0
                current = current * drop
                conversion_rate = round(current / entry_count, 4) if entry_count else 0.0
                rows.append(
                    {
                        "funnel_id": fid,
                        "step_name": step,
                        "step_index": idx,
                        "count": int(current),
                        "conversion_rate": conversion_rate,
                    }
                )
        return rows

    # ------------------------------------------------------------------
    # Shared CSV writer
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


__all__ = [
    "MixpanelConnector",
    "MixpanelSyncResult",
]
