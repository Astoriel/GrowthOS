"""Webhook dispatcher for outbound notifications."""
from __future__ import annotations
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class WebhookPayload:
    event: str
    data: dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    source: str = "growth-os"


def dispatch_webhook(url: str, payload: WebhookPayload, timeout: int = 10) -> bool:
    """Send a JSON POST to url. Returns True on 2xx, False on error.
    Uses httpx if available, else falls back to urllib.
    """
    import json
    body = json.dumps({"event": payload.event, "data": payload.data,
                       "timestamp": payload.timestamp, "source": payload.source})
    headers = {"Content-Type": "application/json", "User-Agent": "growth-os/1.0"}
    try:
        try:
            import httpx
            r = httpx.post(url, content=body, headers=headers, timeout=timeout)
            r.raise_for_status()
            logger.info("Webhook dispatched to %s: %s %s", url, r.status_code, payload.event)
            return True
        except ImportError:
            import urllib.request
            req = urllib.request.Request(url, data=body.encode(), headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                logger.info("Webhook dispatched to %s: %s %s", url, resp.status, payload.event)
                return True
    except Exception as exc:
        logger.error("Webhook dispatch failed to %s: %s", url, exc)
        return False
