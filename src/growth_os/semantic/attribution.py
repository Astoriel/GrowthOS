"""Attribution semantic helpers."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path

from growth_os.config.profiles import AttributionMappingAuditEntry
from growth_os.config.settings import settings


DEFAULT_ATTRIBUTION_MODEL = "source-and-campaign observed revenue"

BUILTIN_CHANNEL_ALIASES: dict[str, tuple[str, ...]] = {
    "meta_ads": ("meta", "facebook", "facebookads", "facebook_ads", "instagram", "instagramads", "fb"),
    "google_ads": ("google", "googleads", "google_ads", "adwords", "googlecpc"),
    "stripe": ("stripebilling",),
}


@dataclass(frozen=True, slots=True)
class AttributionRule:
    """Explicit alias mapping used by attribution joins."""

    scope: str
    canonical_value: str
    alias: str
    channel: str = ""


def resolve_attribution_mapping_path(mapping_file: str | None = None) -> Path:
    """Resolve the target path for persisted attribution mappings."""
    if mapping_file:
        return Path(mapping_file)
    if settings.attribution_mapping_file:
        return Path(settings.attribution_mapping_file)

    base_dir = Path(settings.growth_data_dir) if settings.growth_data_dir else Path.cwd()
    return base_dir / ".growth_os" / "attribution_mappings.csv"


def resolve_attribution_mapping_history_path(
    history_path: str | None = None,
    mapping_file: str | None = None,
) -> Path:
    """Resolve the audit history path for persisted mapping changes."""
    if history_path:
        return Path(history_path)
    if settings.attribution_mapping_history_path:
        return Path(settings.attribution_mapping_history_path)

    mapping_target = resolve_attribution_mapping_path(mapping_file)
    return mapping_target.parent / "attribution_mapping_history.jsonl"


def load_attribution_rules(mapping_file: str | None = None) -> list[AttributionRule]:
    """Load built-in and optional CSV attribution rules."""
    rules = []
    for canonical, aliases in BUILTIN_CHANNEL_ALIASES.items():
        rules.append(AttributionRule(scope="channel", canonical_value=canonical, alias=canonical))
        rules.extend(
            AttributionRule(scope="channel", canonical_value=canonical, alias=alias)
            for alias in aliases
        )

    path = resolve_attribution_mapping_path(mapping_file)
    if not path.exists():
        return rules

    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            scope = (row.get("scope") or "").strip().lower()
            canonical_value = (row.get("canonical_value") or "").strip()
            alias = (row.get("alias") or "").strip()
            channel = (row.get("channel") or "").strip()
            if scope not in {"channel", "campaign"} or not canonical_value or not alias:
                continue
            rules.append(
                AttributionRule(
                    scope=scope,
                    canonical_value=canonical_value,
                    alias=alias,
                    channel=channel,
                )
            )
            rules.append(
                AttributionRule(
                    scope=scope,
                    canonical_value=canonical_value,
                    alias=canonical_value,
                    channel=channel,
                )
            )
    return rules


def load_persisted_attribution_rules(mapping_file: str | None = None) -> list[AttributionRule]:
    """Load only user-defined attribution rules from disk."""
    path = resolve_attribution_mapping_path(mapping_file)
    if not path.exists():
        return []

    rules: list[AttributionRule] = []
    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            scope = (row.get("scope") or "").strip().lower()
            canonical_value = (row.get("canonical_value") or "").strip()
            alias = (row.get("alias") or "").strip()
            channel = (row.get("channel") or "").strip()
            if scope not in {"channel", "campaign"} or not canonical_value or not alias:
                continue
            rules.append(
                AttributionRule(
                    scope=scope,
                    canonical_value=canonical_value,
                    alias=alias,
                    channel=channel,
                )
            )
    return rules


def append_attribution_rules(
    rules: list[AttributionRule],
    mapping_file: str | None = None,
    history_path: str | None = None,
) -> tuple[str, list[AttributionRule], list[AttributionRule]]:
    """Append user-approved attribution rules to disk without duplicates."""
    target = resolve_attribution_mapping_path(mapping_file)
    target.parent.mkdir(parents=True, exist_ok=True)

    existing_rules = load_persisted_attribution_rules(str(target))
    existing_keys = {_rule_identity(rule) for rule in existing_rules}
    applied: list[AttributionRule] = []
    skipped: list[AttributionRule] = []

    for rule in rules:
        key = _rule_identity(rule)
        if key in existing_keys:
            skipped.append(rule)
            continue
        applied.append(rule)
        existing_keys.add(key)

    if not target.exists():
        with open(target, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=["scope", "canonical_value", "alias", "channel"])
            writer.writeheader()
            if applied:
                writer.writerows(
                    {
                        "scope": rule.scope,
                        "canonical_value": rule.canonical_value,
                        "alias": rule.alias,
                        "channel": rule.channel,
                    }
                    for rule in applied
                )
        if applied:
            _append_mapping_history("apply", applied, history_path, str(target))
        return str(target), applied, skipped

    if applied:
        with open(target, "a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=["scope", "canonical_value", "alias", "channel"])
            writer.writerows(
                {
                    "scope": rule.scope,
                    "canonical_value": rule.canonical_value,
                    "alias": rule.alias,
                    "channel": rule.channel,
                }
                for rule in applied
            )
        _append_mapping_history("apply", applied, history_path, str(target))
    return str(target), applied, skipped


def remove_attribution_rules(
    aliases: list[str],
    mapping_file: str | None = None,
    history_path: str | None = None,
    scope: str = "",
    channel: str = "",
) -> tuple[str, list[AttributionRule], list[AttributionRule]]:
    """Remove persisted attribution rules by alias, with optional scope/channel filtering."""
    target = resolve_attribution_mapping_path(mapping_file)
    existing_rules = load_persisted_attribution_rules(str(target))
    alias_keys = {_normalize_value(alias) for alias in aliases if alias.strip()}
    scope = scope.strip().lower()
    channel_norm = _normalize_value(channel)

    kept: list[AttributionRule] = []
    removed: list[AttributionRule] = []
    missing_aliases = set(alias_keys)
    for rule in existing_rules:
        alias_match = _normalize_value(rule.alias) in alias_keys if alias_keys else False
        scope_match = not scope or rule.scope == scope
        channel_match = not channel_norm or _normalize_value(rule.channel) == channel_norm
        if alias_match and scope_match and channel_match:
            removed.append(rule)
            missing_aliases.discard(_normalize_value(rule.alias))
        else:
            kept.append(rule)

    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["scope", "canonical_value", "alias", "channel"])
        writer.writeheader()
        writer.writerows(
            {
                "scope": rule.scope,
                "canonical_value": rule.canonical_value,
                "alias": rule.alias,
                "channel": rule.channel,
            }
            for rule in kept
        )

    if removed:
        _append_mapping_history("rollback", removed, history_path, str(target))

    missing = [
        AttributionRule(scope=scope or "", canonical_value="", alias=alias, channel=channel)
        for alias in aliases
        if _normalize_value(alias) in missing_aliases
    ]
    return str(target), removed, missing


def load_attribution_mapping_history(
    history_path: str | None = None,
    mapping_file: str | None = None,
) -> list[AttributionMappingAuditEntry]:
    """Load persisted mapping audit history."""
    target = resolve_attribution_mapping_history_path(history_path, mapping_file)
    if not target.exists():
        return []

    entries: list[AttributionMappingAuditEntry] = []
    with open(target, encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            entries.append(AttributionMappingAuditEntry.model_validate_json(line))
    return entries


def normalized_sql(column_sql: str) -> str:
    """Normalize text for SQL joins across common naming variations."""
    return (
        "LOWER("
        "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(COALESCE("
        f"{column_sql}, ''"
        ")), ' ', ''), '-', ''), '_', ''), '/', ''), '.', '')"
        ")"
    )


def canonical_sql(
    column_sql: str,
    scope: str,
    rules: list[AttributionRule],
    channel_sql: str | None = None,
) -> str:
    """Build a CASE SQL expression that applies explicit alias mappings."""
    normalized_expr = normalized_sql(column_sql)
    cases: list[str] = []

    for rule in rules:
        if rule.scope != scope:
            continue
        rule_alias = _escape_sql(_normalize_value(rule.alias))
        canonical_value = _escape_sql(rule.canonical_value)
        if scope == "campaign" and rule.channel and channel_sql:
            channel_alias = _escape_sql(_normalize_value(rule.channel))
            channel_expr = normalized_sql(channel_sql)
            cases.append(
                f"WHEN {channel_expr} = '{channel_alias}' AND {normalized_expr} = '{rule_alias}' THEN '{canonical_value}'"
            )
        else:
            cases.append(f"WHEN {normalized_expr} = '{rule_alias}' THEN '{canonical_value}'")

    if not cases:
        return normalized_expr
    return "CASE " + " ".join(cases) + f" ELSE {normalized_expr} END"


def _normalize_value(value: str) -> str:
    """Normalize Python-side values the same way as SQL expressions."""
    return (
        value.strip()
        .lower()
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
        .replace("/", "")
        .replace(".", "")
    )


def _escape_sql(value: str) -> str:
    """Escape SQL string literals."""
    return value.replace("'", "''")


def _rule_identity(rule: AttributionRule) -> tuple[str, str, str, str]:
    """Build a normalized identity key for deduplication."""
    return (
        rule.scope,
        _normalize_value(rule.canonical_value),
        _normalize_value(rule.alias),
        _normalize_value(rule.channel),
    )


def _append_mapping_history(
    action: str,
    rules: list[AttributionRule],
    history_path: str | None,
    mapping_file: str | None,
) -> None:
    """Append audit entries for mapping changes."""
    target = resolve_attribution_mapping_history_path(history_path, mapping_file)
    target.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).isoformat()
    with open(target, "a", encoding="utf-8") as file:
        for rule in rules:
            entry = AttributionMappingAuditEntry(
                timestamp=timestamp,
                action=action,
                scope=rule.scope,
                canonical_value=rule.canonical_value,
                alias=rule.alias,
                channel=rule.channel,
            )
            file.write(json.dumps(entry.model_dump(), ensure_ascii=True) + "\n")


# ---------------------------------------------------------------------------
# Shapley value attribution
# ---------------------------------------------------------------------------

def compute_shapley_values(
    touchpoints: list[str],
    revenue: float,
    all_channel_revenues: dict[str, float] | None = None,
) -> dict[str, float]:
    """Compute Shapley values for multi-touch attribution.

    Uses the exact Shapley formula: for each channel, average its marginal
    contribution across all possible orderings/coalitions.

    Args:
        touchpoints: List of channel names in the customer journey (may repeat).
        revenue: Total revenue to attribute (distributed across touchpoints).
        all_channel_revenues: Optional dict of {channel: total_revenue} for
            cross-journey aggregation. If None, uses uniform base values.

    Returns:
        Dict of {channel: attributed_value} summing to revenue.
    """
    import itertools
    from math import factorial

    if not touchpoints:
        return {}

    unique_channels = list(dict.fromkeys(touchpoints))  # preserve order, deduplicate
    n = len(unique_channels)

    if n == 1:
        return {unique_channels[0]: revenue}

    # Value function: naive additive (proportional to frequency in journey)
    freq = {ch: touchpoints.count(ch) for ch in unique_channels}
    total_freq = sum(freq.values())

    def coalition_value(coalition: frozenset) -> float:
        """Value of a coalition = proportional share of revenue."""
        if not coalition:
            return 0.0
        coalition_freq = sum(freq.get(ch, 0) for ch in coalition)
        return revenue * coalition_freq / total_freq if total_freq > 0 else 0.0

    shapley = {ch: 0.0 for ch in unique_channels}

    for channel in unique_channels:
        others = [ch for ch in unique_channels if ch != channel]
        # Sum over all subsets of others (all coalitions not containing channel)
        for r in range(len(others) + 1):
            for subset in itertools.combinations(others, r):
                s = len(subset)
                coalition_without = frozenset(subset)
                coalition_with = frozenset(subset) | {channel}
                marginal = coalition_value(coalition_with) - coalition_value(coalition_without)
                weight = factorial(s) * factorial(n - s - 1) / factorial(n)
                shapley[channel] += weight * marginal

    return {ch: round(v, 4) for ch, v in shapley.items()}


def shapley_attribution_table(
    channel_journeys: dict[str, list[str]],
    channel_revenues: dict[str, float],
) -> list[dict]:
    """Compute Shapley attribution for multiple channels/journeys.

    Args:
        channel_journeys: {user_id: [channel1, channel2, ...]} mapping
        channel_revenues: {user_id: revenue} mapping

    Returns:
        List of dicts with keys: channel, attributed_revenue, pct
    """
    aggregate: dict[str, float] = {}

    for user_id, journey in channel_journeys.items():
        revenue = channel_revenues.get(user_id, 0.0)
        if revenue <= 0 or not journey:
            continue
        values = compute_shapley_values(journey, revenue)
        for ch, val in values.items():
            aggregate[ch] = aggregate.get(ch, 0.0) + val

    total = sum(aggregate.values())
    result = []
    for ch, val in sorted(aggregate.items(), key=lambda x: -x[1]):
        pct = (val / total * 100) if total > 0 else 0.0
        result.append({
            "channel": ch,
            "attributed_revenue": round(val, 2),
            "pct": round(pct, 1),
        })
    return result
