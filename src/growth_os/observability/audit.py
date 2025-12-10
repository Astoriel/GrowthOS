"""File-based audit log for GrowthOS tool executions."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _default_audit_path() -> Path:
    custom = os.environ.get("GROWTH_AUDIT_LOG", "").strip()
    if custom:
        return Path(custom)
    base = Path.home() / ".growth_os"
    base.mkdir(parents=True, exist_ok=True)
    return base / "audit.jsonl"


@dataclass
class AuditEvent:
    """Represents a single auditable action in GrowthOS."""

    name: str
    detail: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tool_name: str = ""
    duration_ms: float | None = None
    status: str = "ok"
    extra: dict[str, Any] | None = None


def write_audit_event(event: AuditEvent, log_path: Path | None = None) -> None:
    """Append an audit event as a JSON line to the audit log."""
    target = log_path or _default_audit_path()
    entry = {k: v for k, v in asdict(event).items() if v is not None}
    try:
        with open(target, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, default=str) + "\n")
    except OSError:
        pass  # Never let audit logging crash the tool


def read_audit_log(log_path: Path | None = None, limit: int = 100) -> list[dict[str, Any]]:
    """Read the last `limit` audit events from the log."""
    target = log_path or _default_audit_path()
    if not target.exists():
        return []
    try:
        lines = target.read_text(encoding="utf-8").splitlines()
        tail = lines[-limit:] if len(lines) > limit else lines
        return [json.loads(line) for line in tail if line.strip()]
    except (OSError, json.JSONDecodeError):
        return []
