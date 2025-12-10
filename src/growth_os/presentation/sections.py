"""Section helpers for markdown output."""

from __future__ import annotations


def format_section(title: str, content: str) -> str:
    """Wrap content in a titled section."""
    return f"### {title}\n\n{content}"
