"""Simple trace instrumentation for GrowthOS."""

from __future__ import annotations

import functools
import os
import time
from typing import Any, Callable, TypeVar

from growth_os.observability.logging import get_logger

F = TypeVar("F", bound=Callable[..., Any])


def tracing_enabled() -> bool:
    """Return whether tracing is active (set GROWTH_TRACING=1 to enable)."""
    return os.environ.get("GROWTH_TRACING", "").strip() == "1"


def trace(name: str | None = None) -> Callable[[F], F]:
    """Decorator that logs function start, end, and elapsed time."""

    def decorator(fn: F) -> F:
        span_name = name or fn.__qualname__

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not tracing_enabled():
                return fn(*args, **kwargs)

            logger = get_logger(fn.__module__)
            logger.debug("span_start", span=span_name)
            start = time.monotonic()
            try:
                result = fn(*args, **kwargs)
                elapsed_ms = round((time.monotonic() - start) * 1000, 1)
                logger.debug("span_end", span=span_name, elapsed_ms=elapsed_ms, status="ok")
                return result
            except Exception as exc:
                elapsed_ms = round((time.monotonic() - start) * 1000, 1)
                logger.error("span_error", span=span_name, elapsed_ms=elapsed_ms, error=str(exc))
                raise

        return wrapper  # type: ignore[return-value]

    return decorator
