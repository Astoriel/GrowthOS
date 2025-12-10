"""Workspace profile models."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class CustomMetricDefinition(BaseModel):
    """User-defined metric formula stored in a workspace profile."""

    name: str
    label: str
    sql_expression: str
    table: str
    benchmark_good: float = 0.0
    benchmark_ok: float = 0.0
    unit: str = ""
    description: str = ""


class WorkspaceProfile(BaseModel):
    """Named configuration profile for local or team use."""

    name: str
    growth_data_dir: str = ""
    postgres_url: str = ""
    business_mode: bool = False
    notes: str = ""
    preferred_tables: list[str] = Field(default_factory=list)
    custom_metrics: list[CustomMetricDefinition] = Field(default_factory=list)
    workspace_id: str = ""


class AttributionAliasSuggestion(BaseModel):
    """One suggested alias mapping inferred from diagnostics."""

    scope: str
    canonical_value: str
    alias: str
    channel: str = ""
    confidence: float = 0.0
    reason: str = ""
    estimated_revenue_30d: float = 0.0
    estimated_spend_30d: float = 0.0


class SemanticProfile(BaseModel):
    """Persisted semantic profile for attribution and data interpretation."""

    generated_at: str
    spend_tables: list[str] = Field(default_factory=list)
    events_table: str = ""
    revenue_event_type: str = "purchase"
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    unmatched_event_keys: list[dict[str, Any]] = Field(default_factory=list)
    unmatched_spend_keys: list[dict[str, Any]] = Field(default_factory=list)
    applied_alias_rules: list[dict[str, Any]] = Field(default_factory=list)
    suggestions: list[AttributionAliasSuggestion] = Field(default_factory=list)


class AttributionMappingAuditEntry(BaseModel):
    """Audit entry for persisted mapping changes."""

    timestamp: str
    action: str
    scope: str
    canonical_value: str
    alias: str
    channel: str = ""


# ---------------------------------------------------------------------------
# Profile persistence helpers
# ---------------------------------------------------------------------------

def _profiles_path() -> Path:
    """Return the path to the profiles JSON store."""
    import os
    env_path = os.environ.get("GROWTH_PROFILES_PATH", "")
    if env_path:
        return Path(env_path)
    return Path.home() / ".growth_os" / "profiles.json"


def save_profile(profile: WorkspaceProfile) -> Path:
    """Persist a WorkspaceProfile to the profiles store.

    Creates the directory if it doesn't exist. If a profile with the same
    name already exists it is overwritten.
    """
    path = _profiles_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    existing: list[dict] = []
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            existing = []
    existing = [p for p in existing if p.get("name") != profile.name]
    existing.append(profile.model_dump())
    path.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def load_profile(name: str) -> WorkspaceProfile | None:
    """Load a WorkspaceProfile by name, or None if not found."""
    path = _profiles_path()
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    for entry in data:
        if entry.get("name") == name:
            return WorkspaceProfile(**entry)
    return None


def list_profiles() -> list[WorkspaceProfile]:
    """Return all saved WorkspaceProfiles."""
    path = _profiles_path()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    profiles = []
    for entry in data:
        try:
            profiles.append(WorkspaceProfile(**entry))
        except Exception:
            pass
    return profiles


def delete_profile(name: str) -> bool:
    """Delete a profile by name. Returns True if deleted, False if not found."""
    path = _profiles_path()
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return False
    new_data = [p for p in data if p.get("name") != name]
    if len(new_data) == len(data):
        return False
    path.write_text(json.dumps(new_data, indent=2, ensure_ascii=False), encoding="utf-8")
    return True


def apply_profile(profile: WorkspaceProfile, settings: Any) -> None:
    """Apply a WorkspaceProfile onto a Settings object in place.

    Only non-empty string fields and explicit boolean choices are applied,
    to avoid overwriting settings that are intentionally set via env vars.
    """
    if profile.growth_data_dir:
        settings.growth_data_dir = profile.growth_data_dir
    if profile.postgres_url:
        settings.postgres_url = profile.postgres_url
    settings.business_mode = profile.business_mode


# ---------------------------------------------------------------------------
# Custom metric helpers
# ---------------------------------------------------------------------------

def add_custom_metric(profile_name: str, metric: CustomMetricDefinition) -> bool:
    """Add a custom metric definition to a saved profile. Returns True on success."""
    profile = load_profile(profile_name)
    if profile is None:
        return False
    updated = [m for m in profile.custom_metrics if m.name != metric.name]
    updated.append(metric)
    profile = profile.model_copy(update={"custom_metrics": updated})
    save_profile(profile)
    return True


def remove_custom_metric(profile_name: str, metric_name: str) -> bool:
    """Remove a custom metric by name from a saved profile. Returns True if removed."""
    profile = load_profile(profile_name)
    if profile is None:
        return False
    original_count = len(profile.custom_metrics)
    updated = [m for m in profile.custom_metrics if m.name != metric_name]
    if len(updated) == original_count:
        return False
    profile = profile.model_copy(update={"custom_metrics": updated})
    save_profile(profile)
    return True
