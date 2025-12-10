"""Semantic profile persistence helpers."""

from __future__ import annotations

from pathlib import Path

from growth_os.config.profiles import SemanticProfile
from growth_os.config.settings import settings


def resolve_semantic_profile_path(path: str | None = None) -> Path:
    """Resolve the semantic profile target path."""
    if path:
        return Path(path)
    if settings.semantic_profile_path:
        return Path(settings.semantic_profile_path)

    base_dir = Path(settings.growth_data_dir) if settings.growth_data_dir else Path.cwd()
    return base_dir / ".growth_os" / "semantic_profile.json"


def save_semantic_profile(profile: SemanticProfile, path: str | None = None) -> str:
    """Persist a semantic profile to disk."""
    target = resolve_semantic_profile_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(profile.model_dump_json(indent=2), encoding="utf-8")
    return str(target)


def load_semantic_profile(path: str | None = None) -> SemanticProfile | None:
    """Load a semantic profile from disk when present."""
    target = resolve_semantic_profile_path(path)
    if not target.exists():
        return None
    return SemanticProfile.model_validate_json(target.read_text(encoding="utf-8"))
