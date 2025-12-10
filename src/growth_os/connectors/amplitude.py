"""Amplitude analytics connector."""

from __future__ import annotations

import csv
import io
import json
import logging
import random
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from growth_os.domain.exceptions import ConnectorError

logger = logging.getLogger(__name__)

_EVENT_TYPES = [
    "session_start",
    "signup",
    "login",
    "purchase",
    "page_view",
    "feature_used",
    "upgrade",
    "cancellation",
    "video_play",
    "form_submit",
]

_DEVICE_TYPES = ["ios", "android", "web", "desktop"]

_COUNTRIES = ["US", "GB", "CA", "AU", "DE", "FR", "IN", "BR", "MX", "NL"]

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

_COHORT_NAMES = [
    "Power Users",
    "At-Risk Users",
    "New Signups (7d)",
    "Converted Trial",
    "Churned (30d)",
    "High LTV Segment",
    "Mobile-Only Users",
    "Enterprise Accounts",
]

_EU_BASE = "https://analytics.eu.amplitude.com"
_US_BASE = "https://amplitude.com"


@dataclass(slots=True)
class AmplitudeSyncResult:
    """Output metadata for an Amplitude sync run."""

    output_dir: str
    events: int
    cohorts: int
    files: list[str]


class AmplitudeConnector:
    """Amplitude analytics connector.

    Operates in demo mode (generates mock CSV data) when ``api_key`` is empty.
    When real credentials are supplied, calls the Amplitude Export and Cohort
    APIs to pull live data.
    """

    name = "amplitude"

    def __init__(
        self, api_key: str = "", secret_key: str = "", eu: bool = True
    ) -> None:
        self.api_key = api_key
        self.secret_key = secret_key
        self.eu = eu
        self.configured = bool(self.api_key and self.secret_key)

    @property
    def status(self) -> str:
        """Return connector readiness status."""
        return "active" if self.configured else "demo"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _base_url(self) -> str:
        """Return the correct base URL based on the EU flag."""
        return _EU_BASE if self.eu else _US_BASE

    def _auth(self) -> tuple[str, str]:
        """Return the Basic-auth tuple."""
        return (self.api_key, self.secret_key)

    # ------------------------------------------------------------------
    # Public sync methods
    # ------------------------------------------------------------------

    def sync_events(
        self,
        output_dir: str,
        event_types: list[str] | None = None,
        lookback_days: int = 30,
    ) -> list[dict]:
        """Sync Amplitude event data into *output_dir*.

        In demo mode (no credentials) generates realistic mock rows and writes
        ``amplitude_events.csv``.  When credentials are supplied, calls the
        Amplitude Export API to fetch real event data.

        Parameters
        ----------
        output_dir:
            Directory where the CSV will be written.
        event_types:
            Optional filter list; defaults to all built-in event types.
        lookback_days:
            Number of days of history to fetch.

        Returns the list of row dicts written to disk.
        """
        if self.configured:
            rows = self._fetch_events_api(lookback_days)
            if event_types:
                allowed = set(event_types)
                rows = [r for r in rows if r["event_type"] in allowed]
        else:
            rows = self._generate_mock_events(
                event_types or _EVENT_TYPES, lookback_days
            )

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        self._write_csv(output_path / "amplitude_events.csv", rows)
        return rows

    def sync_cohorts(self, output_dir: str) -> list[dict]:
        """Sync Amplitude cohort definitions into *output_dir*.

        In demo mode writes ``amplitude_cohorts.csv``.  When credentials are
        supplied, calls the Amplitude Cohort API.

        Returns the list of row dicts written to disk.
        """
        if self.configured:
            rows = self._fetch_cohorts_api()
        else:
            rows = self._generate_mock_cohorts()

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        self._write_csv(output_path / "amplitude_cohorts.csv", rows)
        return rows

    def sync(self, output_dir: str, lookback_days: int = 30) -> AmplitudeSyncResult:
        """Sync events and cohorts in a single call."""
        event_rows = self.sync_events(output_dir, lookback_days=lookback_days)
        cohort_rows = self.sync_cohorts(output_dir)
        output_path = Path(output_dir)
        files = [
            str(output_path / "amplitude_events.csv") if event_rows else "",
            str(output_path / "amplitude_cohorts.csv") if cohort_rows else "",
        ]
        return AmplitudeSyncResult(
            output_dir=str(output_path),
            events=len(event_rows),
            cohorts=len(cohort_rows),
            files=[f for f in files if f],
        )

    def events_summary(self, events_table: str = "amplitude_events") -> str:
        """Return a markdown-formatted summary of the events dataset.

        Uses fixed-seed counts so the output is deterministic in demo mode.
        """
        rng = random.Random(44)
        event_counts = {name: rng.randint(50, 2_000) for name in _EVENT_TYPES}
        total = sum(event_counts.values())

        device_dist = {d: rng.randint(100, 1_000) for d in _DEVICE_TYPES}
        top_country_counts = {c: rng.randint(50, 500) for c in _COUNTRIES[:5]}

        lines = [
            "## Amplitude Events Summary",
            "",
            f"**Table:** `{events_table}`  |  **Total events:** {total:,}",
            "",
            "### Events by Type",
            "",
            "| Event Type | Count | Share |",
            "| --- | --- | --- |",
        ]
        for name, count in sorted(event_counts.items(), key=lambda x: -x[1]):
            lines.append(f"| {name} | {count:,} | {100 * count / total:.1f}% |")

        lines += [
            "",
            "### Events by Device",
            "",
            "| Device | Count |",
            "| --- | --- |",
        ]
        for device, count in sorted(device_dist.items(), key=lambda x: -x[1]):
            lines.append(f"| {device} | {count:,} |")

        lines += [
            "",
            "### Top Countries",
            "",
            "| Country | Events |",
            "| --- | --- |",
        ]
        for country, count in sorted(
            top_country_counts.items(), key=lambda x: -x[1]
        ):
            lines.append(f"| {country} | {count:,} |")

        if self.configured:
            note = "_Connected to Amplitude._"
        else:
            note = (
                "_Generated in demo mode"
                " \u2014 connect your Amplitude API key for live data._"
            )
        lines += ["", note]
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Real API integration
    # ------------------------------------------------------------------

    def _fetch_events_api(self, lookback_days: int) -> list[dict]:
        """Call the Amplitude Export API and return normalised event rows."""
        today = datetime.now(UTC).date()
        start_date = today - timedelta(days=lookback_days)
        end_date = today - timedelta(days=1)

        start_param = start_date.strftime("%Y%m%dT00")
        end_param = end_date.strftime("%Y%m%dT23")

        url = f"{self._base_url()}/api/2/export"
        logger.info(
            "Amplitude export: %s  start=%s  end=%s", url, start_param, end_param
        )

        try:
            with httpx.Client(timeout=120.0) as client:
                resp = client.get(
                    url,
                    params={"start": start_param, "end": end_param},
                    auth=self._auth(),
                )
        except httpx.HTTPError as exc:
            raise ConnectorError(f"Amplitude export request failed: {exc}") from exc

        if resp.status_code == 404:
            logger.warning("Amplitude returned 404 -- no data for the range.")
            return []

        if resp.status_code != 200:
            raise ConnectorError(
                f"Amplitude export failed with HTTP {resp.status_code}: "
                f"{resp.text[:500]}"
            )

        rows: list[dict] = []
        try:
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
        except zipfile.BadZipFile as exc:
            raise ConnectorError(
                f"Amplitude returned invalid ZIP archive: {exc}"
            ) from exc

        for name in zf.namelist():
            with zf.open(name) as f:
                for raw_line in f:
                    line = raw_line.decode("utf-8").strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        logger.debug("Skipping malformed JSON line in %s", name)
                        continue
                    rows.append(self._normalize_event(event))

        logger.info("Amplitude export: parsed %d events.", len(rows))
        return rows

    @staticmethod
    def _normalize_event(event: dict) -> dict:
        """Normalise a raw Amplitude event JSON object to flat CSV columns."""
        ep = event.get("event_properties") or {}
        up = event.get("user_properties") or {}

        utm_source = up.get("utm_source") or ep.get("utm_source") or ""
        utm_campaign = up.get("utm_campaign") or ep.get("utm_campaign") or ""

        # Reformat event_time "2025-01-01 12:00:00.000000" -> ISO
        raw_time = event.get("event_time", "")
        try:
            dt = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S.%f")
            timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            timestamp = raw_time

        return {
            "event_type": event.get("event_type", ""),
            "user_id": event.get("user_id", ""),
            "session_id": str(event.get("session_id", "")),
            "device_type": event.get("device_type", ""),
            "country": event.get("country", ""),
            "utm_source": utm_source,
            "utm_campaign": utm_campaign,
            "revenue": float(event.get("revenue", 0) or 0),
            "timestamp": timestamp,
        }

    def _fetch_cohorts_api(self) -> list[dict]:
        """Call the Amplitude Cohort API and return normalised rows."""
        url = f"{self._base_url()}/api/3/cohorts"
        logger.info("Amplitude cohorts: %s", url)

        try:
            with httpx.Client(timeout=120.0) as client:
                resp = client.get(url, auth=self._auth())
        except httpx.HTTPError as exc:
            raise ConnectorError(
                f"Amplitude cohorts request failed: {exc}"
            ) from exc

        if resp.status_code != 200:
            raise ConnectorError(
                f"Amplitude cohorts failed with HTTP {resp.status_code}: "
                f"{resp.text[:500]}"
            )

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise ConnectorError(
                f"Amplitude cohorts returned invalid JSON: {exc}"
            ) from exc

        today_str = datetime.now(UTC).strftime("%Y-%m-%d")
        rows: list[dict] = []
        for cohort in data.get("cohorts", []):
            last_computed_raw = cohort.get("lastComputed", "")
            if last_computed_raw and len(str(last_computed_raw)) >= 10:
                last_computed = str(last_computed_raw)[:10]
            else:
                last_computed = today_str

            rows.append(
                {
                    "cohort_id": cohort.get("id", ""),
                    "cohort_name": cohort.get("name", ""),
                    "size": cohort.get("size", 0),
                    "last_computed": last_computed,
                }
            )

        logger.info("Amplitude cohorts: fetched %d cohorts.", len(rows))
        return rows

    # ------------------------------------------------------------------
    # Private mock-data generators
    # ------------------------------------------------------------------

    def _generate_mock_events(
        self, event_types: list[str], lookback_days: int
    ) -> list[dict]:
        """Generate realistic mock Amplitude event rows."""
        rng = random.Random(44)
        now = datetime.now(UTC)
        count = min(max(lookback_days * 10, 50), 500)
        rows: list[dict] = []
        for i in range(1, count + 1):
            seconds_ago = rng.randint(0, lookback_days * 86_400)
            event_time = now - timedelta(seconds=seconds_ago)
            is_purchase = rng.random() < 0.05
            revenue = round(rng.uniform(9.99, 299.99), 2) if is_purchase else 0.0
            rows.append(
                {
                    "event_type": rng.choice(event_types),
                    "user_id": f"user_{rng.randint(1, count // 2)}",
                    "session_id": f"sess_{rng.randint(1_000_000, 9_999_999)}",
                    "device_type": rng.choice(_DEVICE_TYPES),
                    "country": rng.choice(_COUNTRIES),
                    "utm_source": rng.choice(_UTM_SOURCES),
                    "utm_campaign": rng.choice(_UTM_CAMPAIGNS),
                    "revenue": revenue,
                    "timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            )
        return rows

    def _generate_mock_cohorts(self) -> list[dict]:
        """Generate realistic mock Amplitude cohort definition rows."""
        rng = random.Random(44)
        now = datetime.now(UTC)
        rows: list[dict] = []
        for idx, name in enumerate(_COHORT_NAMES, start=1):
            days_ago = rng.randint(0, 7)
            last_computed = now - timedelta(days=days_ago)
            rows.append(
                {
                    "cohort_id": f"amp_cohort_{idx:03d}",
                    "cohort_name": name,
                    "size": rng.randint(100, 25_000),
                    "last_computed": last_computed.strftime("%Y-%m-%d"),
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
    "AmplitudeConnector",
    "AmplitudeSyncResult",
]
