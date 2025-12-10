"""Structured logging for GrowthOS."""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class _JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        # Try to parse the message as already-JSON (from StructuredLogger)
        try:
            payload = json.loads(record.getMessage())
        except (json.JSONDecodeError, TypeError):
            payload = {"event": record.getMessage()}

        payload.setdefault("ts", _utc_now())
        payload.setdefault("level", record.levelname)
        payload.setdefault("logger", record.name)
        return json.dumps(payload, default=str)


class StructuredLogger:
    """JSON-line structured logger."""

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)

    def _emit(self, level: int, event: str, **kwargs: Any) -> None:
        payload = {"ts": _utc_now(), "event": event, **kwargs}
        self._logger.log(level, json.dumps(payload, default=str))

    def debug(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.DEBUG, event, **kwargs)

    def info(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.INFO, event, **kwargs)

    def warning(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.WARNING, event, **kwargs)

    def error(self, event: str, **kwargs: Any) -> None:
        self._emit(logging.ERROR, event, **kwargs)

    @contextmanager
    def span(self, name: str, **kwargs: Any) -> Generator[None, None, None]:
        """Context manager that logs elapsed time on exit."""
        start = time.monotonic()
        try:
            yield
        finally:
            elapsed_ms = round((time.monotonic() - start) * 1000, 1)
            self._emit(logging.DEBUG, "span_end", span=name, elapsed_ms=elapsed_ms, **kwargs)


_loggers: dict[str, StructuredLogger] = {}


def get_logger(name: str) -> StructuredLogger:
    """Return a named StructuredLogger (cached)."""
    if name not in _loggers:
        _loggers[name] = StructuredLogger(name)
    return _loggers[name]


def configure_logging(level: int = logging.INFO) -> None:
    """Configure application logging with JSON formatting."""
    root = logging.getLogger()
    if root.handlers:
        return  # already configured

    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level)
