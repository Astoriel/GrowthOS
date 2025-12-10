"""Tests for workspace profile persistence (save, load, list, delete, apply)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from growth_os.config.profiles import (
    WorkspaceProfile,
    apply_profile,
    delete_profile,
    list_profiles,
    load_profile,
    save_profile,
)


@pytest.fixture(autouse=True)
def isolated_profiles(tmp_path, monkeypatch):
    """Redirect all profile I/O to a temporary file for test isolation."""
    profiles_file = tmp_path / "profiles.json"
    monkeypatch.setenv("GROWTH_PROFILES_PATH", str(profiles_file))
    return profiles_file


def _make_profile(**kwargs) -> WorkspaceProfile:
    defaults: dict = {"name": "test_profile"}
    defaults.update(kwargs)
    return WorkspaceProfile(**defaults)


class TestSaveProfile:
    def test_returns_path_object(self, tmp_path):
        result = save_profile(_make_profile(name="alpha"))
        assert isinstance(result, Path)

    def test_creates_profiles_file_on_disk(self, tmp_path):
        path = save_profile(_make_profile(name="alpha"))
        assert path.exists()

    def test_file_contains_saved_profile_data(self, tmp_path):
        save_profile(_make_profile(name="alpha", growth_data_dir="/data/alpha"))
        path = tmp_path / "profiles.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        assert any(p["name"] == "alpha" for p in data)

    def test_saved_data_dir_is_preserved(self, tmp_path):
        save_profile(_make_profile(name="alpha", growth_data_dir="/data/alpha"))
        path = tmp_path / "profiles.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        match = next(p for p in data if p["name"] == "alpha")
        assert match["growth_data_dir"] == "/data/alpha"

    def test_saves_multiple_profiles_without_loss(self, tmp_path):
        save_profile(_make_profile(name="one"))
        save_profile(_make_profile(name="two"))
        profiles = list_profiles()
        names = {p.name for p in profiles}
        assert "one" in names
        assert "two" in names

    def test_overwrites_existing_profile_with_same_name(self, tmp_path):
        save_profile(_make_profile(name="alpha", growth_data_dir="/old"))
        save_profile(_make_profile(name="alpha", growth_data_dir="/new"))
        loaded = load_profile("alpha")
        assert loaded is not None
        assert loaded.growth_data_dir == "/new"

    def test_overwrite_does_not_duplicate_entry(self, tmp_path):
        save_profile(_make_profile(name="alpha"))
        save_profile(_make_profile(name="alpha"))
        profiles = [p for p in list_profiles() if p.name == "alpha"]
        assert len(profiles) == 1

    def test_creates_nested_parent_directory_if_missing(self, tmp_path, monkeypatch):
        deep_path = tmp_path / "nested" / "dir" / "profiles.json"
        monkeypatch.setenv("GROWTH_PROFILES_PATH", str(deep_path))
        save_profile(_make_profile(name="deep"))
        assert deep_path.exists()


class TestLoadProfile:
    def test_returns_workspace_profile_instance(self, tmp_path):
        save_profile(_make_profile(name="gamma"))
        result = load_profile("gamma")
        assert isinstance(result, WorkspaceProfile)

    def test_loaded_name_matches_saved_name(self, tmp_path):
        save_profile(_make_profile(name="gamma"))
        result = load_profile("gamma")
        assert result is not None
        assert result.name == "gamma"

    def test_returns_none_for_unknown_profile_name(self, tmp_path):
        save_profile(_make_profile(name="exists"))
        result = load_profile("does_not_exist")
        assert result is None

    def test_returns_none_when_profiles_file_does_not_exist(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GROWTH_PROFILES_PATH", str(tmp_path / "nonexistent.json"))
        result = load_profile("anything")
        assert result is None

    def test_loads_all_scalar_fields_correctly(self, tmp_path):
        profile = WorkspaceProfile(
            name="full",
            growth_data_dir="/some/dir",
            postgres_url="postgresql://localhost/db",
            business_mode=True,
            notes="My notes",
            preferred_tables=["orders", "users"],
        )
        save_profile(profile)
        loaded = load_profile("full")
        assert loaded is not None
        assert loaded.growth_data_dir == "/some/dir"
        assert loaded.postgres_url == "postgresql://localhost/db"
        assert loaded.business_mode is True
        assert loaded.notes == "My notes"

    def test_loads_preferred_tables_list_correctly(self, tmp_path):
        profile = _make_profile(name="tables", preferred_tables=["orders", "users"])
        save_profile(profile)
        loaded = load_profile("tables")
        assert loaded is not None
        assert loaded.preferred_tables == ["orders", "users"]

    def test_correct_profile_returned_among_multiple(self, tmp_path):
        save_profile(_make_profile(name="p1", growth_data_dir="/p1"))
        save_profile(_make_profile(name="p2", growth_data_dir="/p2"))
        save_profile(_make_profile(name="p3", growth_data_dir="/p3"))
        result = load_profile("p2")
        assert result is not None
        assert result.growth_data_dir == "/p2"


class TestListProfiles:
    def test_returns_empty_list_when_file_does_not_exist(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GROWTH_PROFILES_PATH", str(tmp_path / "nonexistent.json"))
        result = list_profiles()
        assert result == []

    def test_returns_list_type(self, tmp_path):
        save_profile(_make_profile(name="x"))
        result = list_profiles()
        assert isinstance(result, list)

    def test_each_entry_is_workspace_profile_instance(self, tmp_path):
        save_profile(_make_profile(name="x"))
        save_profile(_make_profile(name="y"))
        result = list_profiles()
        assert all(isinstance(p, WorkspaceProfile) for p in result)

    def test_returns_correct_count_of_profiles(self, tmp_path):
        save_profile(_make_profile(name="p1"))
        save_profile(_make_profile(name="p2"))
        save_profile(_make_profile(name="p3"))
        result = list_profiles()
        assert len(result) == 3

    def test_returns_empty_list_for_empty_json_array(self, tmp_path, monkeypatch):
        path = tmp_path / "profiles.json"
        path.write_text("[]", encoding="utf-8")
        monkeypatch.setenv("GROWTH_PROFILES_PATH", str(path))
        result = list_profiles()
        assert result == []

    def test_all_saved_names_present_in_list(self, tmp_path):
        save_profile(_make_profile(name="a"))
        save_profile(_make_profile(name="b"))
        names = {p.name for p in list_profiles()}
        assert "a" in names
        assert "b" in names


class TestDeleteProfile:
    def test_returns_true_when_profile_deleted(self, tmp_path):
        save_profile(_make_profile(name="to_delete"))
        result = delete_profile("to_delete")
        assert result is True

    def test_returns_false_for_nonexistent_name(self, tmp_path):
        save_profile(_make_profile(name="exists"))
        result = delete_profile("nonexistent")
        assert result is False

    def test_returns_false_when_file_does_not_exist(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GROWTH_PROFILES_PATH", str(tmp_path / "nonexistent.json"))
        result = delete_profile("anything")
        assert result is False

    def test_deleted_profile_no_longer_loadable(self, tmp_path):
        save_profile(_make_profile(name="gone"))
        delete_profile("gone")
        assert load_profile("gone") is None

    def test_deleted_profile_absent_from_list(self, tmp_path):
        save_profile(_make_profile(name="gone"))
        delete_profile("gone")
        names = {p.name for p in list_profiles()}
        assert "gone" not in names

    def test_other_profiles_unaffected_by_delete(self, tmp_path):
        save_profile(_make_profile(name="keep"))
        save_profile(_make_profile(name="remove"))
        delete_profile("remove")
        assert load_profile("keep") is not None
        assert load_profile("remove") is None

    def test_list_count_decreases_after_delete(self, tmp_path):
        save_profile(_make_profile(name="a"))
        save_profile(_make_profile(name="b"))
        delete_profile("a")
        assert len(list_profiles()) == 1


class TestApplyProfile:
    class _FakeSettings:
        growth_data_dir: str = ""
        postgres_url: str = ""
        business_mode: bool = False

    def _settings(self, **kwargs):
        obj = self._FakeSettings()
        for key, value in kwargs.items():
            setattr(obj, key, value)
        return obj

    def test_applies_growth_data_dir_when_non_empty(self):
        profile = _make_profile(name="p", growth_data_dir="/data/path")
        settings = self._settings()
        apply_profile(profile, settings)
        assert settings.growth_data_dir == "/data/path"

    def test_applies_postgres_url_when_non_empty(self):
        profile = _make_profile(name="p", postgres_url="postgresql://localhost/test")
        settings = self._settings()
        apply_profile(profile, settings)
        assert settings.postgres_url == "postgresql://localhost/test"

    def test_sets_business_mode_true(self):
        profile = _make_profile(name="p", business_mode=True)
        settings = self._settings(business_mode=False)
        apply_profile(profile, settings)
        assert settings.business_mode is True

    def test_sets_business_mode_false(self):
        profile = _make_profile(name="p", business_mode=False)
        settings = self._settings(business_mode=True)
        apply_profile(profile, settings)
        assert settings.business_mode is False

    def test_empty_growth_data_dir_does_not_overwrite_existing(self):
        profile = _make_profile(name="p", growth_data_dir="")
        settings = self._settings(growth_data_dir="/existing/path")
        apply_profile(profile, settings)
        assert settings.growth_data_dir == "/existing/path"

    def test_empty_postgres_url_does_not_overwrite_existing(self):
        profile = _make_profile(name="p", postgres_url="")
        settings = self._settings(postgres_url="postgresql://old/db")
        apply_profile(profile, settings)
        assert settings.postgres_url == "postgresql://old/db"

    def test_applies_all_non_empty_fields_at_once(self):
        profile = _make_profile(
            name="full",
            growth_data_dir="/data",
            postgres_url="postgresql://host/db",
            business_mode=True,
        )
        settings = self._settings()
        apply_profile(profile, settings)
        assert settings.growth_data_dir == "/data"
        assert settings.postgres_url == "postgresql://host/db"
        assert settings.business_mode is True


class TestCorruptionResilience:
    def test_load_profile_with_malformed_json_returns_none(self, tmp_path, monkeypatch):
        path = tmp_path / "broken.json"
        path.write_text("{bad json[[[", encoding="utf-8")
        monkeypatch.setenv("GROWTH_PROFILES_PATH", str(path))
        result = load_profile("anything")
        assert result is None

    def test_list_profiles_with_malformed_json_returns_empty_list(self, tmp_path, monkeypatch):
        path = tmp_path / "broken.json"
        path.write_text("not valid json at all", encoding="utf-8")
        monkeypatch.setenv("GROWTH_PROFILES_PATH", str(path))
        result = list_profiles()
        assert result == []

    def test_delete_profile_with_malformed_json_returns_false(self, tmp_path, monkeypatch):
        path = tmp_path / "broken.json"
        path.write_text("<<<broken>>>", encoding="utf-8")
        monkeypatch.setenv("GROWTH_PROFILES_PATH", str(path))
        result = delete_profile("anything")
        assert result is False

    def test_save_profile_recovers_from_malformed_existing_file(self, tmp_path):
        path = tmp_path / "profiles.json"
        path.write_text("garbage content", encoding="utf-8")
        save_profile(_make_profile(name="recovery"))
        loaded = load_profile("recovery")
        assert loaded is not None
        assert loaded.name == "recovery"

    def test_list_profiles_skips_malformed_entries_silently(self, tmp_path, monkeypatch):
        path = tmp_path / "partial.json"
        mixed = [
            {"name": "valid", "growth_data_dir": "/ok"},
            {"completely_invalid_key": True},
        ]
        path.write_text(json.dumps(mixed), encoding="utf-8")
        monkeypatch.setenv("GROWTH_PROFILES_PATH", str(path))
        result = list_profiles()
        assert any(p.name == "valid" for p in result)

    def test_save_then_load_roundtrip_preserves_all_fields(self, tmp_path):
        original = WorkspaceProfile(
            name="roundtrip",
            growth_data_dir="/roundtrip/data",
            postgres_url="postgresql://rt/db",
            business_mode=True,
            notes="roundtrip note",
            preferred_tables=["alpha", "beta"],
        )
        save_profile(original)
        loaded = load_profile("roundtrip")
        assert loaded is not None
        assert loaded.name == original.name
        assert loaded.growth_data_dir == original.growth_data_dir
        assert loaded.postgres_url == original.postgres_url
        assert loaded.business_mode == original.business_mode
        assert loaded.notes == original.notes
        assert loaded.preferred_tables == original.preferred_tables
