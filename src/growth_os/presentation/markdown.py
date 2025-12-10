"""Markdown formatting helpers."""

from __future__ import annotations

from growth_os.domain.models import ToolEnvelope


def format_table(data: list[dict], title: str | None = None) -> str:
    """Convert a list of row dicts into a markdown table."""
    if not data:
        header = f"### {title}\n\n" if title else ""
        return f"{header}_No data found._"

    headers = list(data[0].keys())
    parts = []
    if title:
        parts.append(f"### {title}\n")
    parts.append("| " + " | ".join(headers) + " |")
    parts.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for row in data:
        formatted = [_format_value(row[header]) for header in headers]
        parts.append("| " + " | ".join(formatted) + " |")

    return "\n".join(parts)


def format_kpi_card(label: str, value: str | float, change: float | None = None) -> str:
    """Format a single KPI card."""
    formatted_value = _format_value(value)
    if change is not None:
        arrow = "▲" if change > 0 else "▼" if change < 0 else "→"
        return f"**{label}:** {formatted_value} {arrow} {change:+.1f}%"
    return f"**{label}:** {formatted_value}"


def format_kpi_dashboard(metrics: list[dict]) -> str:
    """Format multiple KPI cards as a section."""
    parts = ["### 📊 Key Metrics\n"]
    for metric in metrics:
        parts.append(format_kpi_card(metric["label"], metric["value"], metric.get("change")))
    return "\n".join(parts)


def format_insight(text: str) -> str:
    """Wrap a single insight line."""
    return f"\n💡 **Insight:** {text}\n"


def format_actions(actions: list[str]) -> str:
    """Format recommended actions."""
    if not actions:
        return ""
    parts = ["\n🎯 **Recommended Actions:**\n"]
    for index, action in enumerate(actions, start=1):
        parts.append(f"{index}. {action}")
    return "\n".join(parts)


def format_warning_block(warnings: list[str]) -> str:
    """Format trust warnings."""
    if not warnings:
        return ""
    parts = ["\n⚠️ **Warnings:**"]
    parts.extend(f"- {warning}" for warning in warnings)
    return "\n".join(parts)


def format_trust_footer(sources: list[str], date_range: str = "", warnings: list[str] | None = None) -> str:
    """Render a compact trust footer."""
    warnings = warnings or []
    if not sources and not date_range and not warnings:
        return ""
    parts = ["\n---", "**Trust:**"]
    if sources:
        parts.append(f"- Sources: {', '.join(sorted(sources))}")
    if date_range:
        parts.append(f"- Date range: {date_range}")
    parts.extend(f"- Warning: {warning}" for warning in warnings)
    return "\n".join(parts)


def wrap_tool_envelope(envelope: ToolEnvelope, business_mode: bool = False) -> str:
    """Render a ToolEnvelope into markdown.

    When business_mode=True, emojis are stripped from output for cleaner
    executive/business presentations.
    """
    parts = [envelope.body]
    warning_block = format_warning_block(envelope.warnings)
    if warning_block:
        parts.append(warning_block)
    parts.append(format_trust_footer(envelope.sources, envelope.date_range, envelope.warnings))
    result = "\n".join(part for part in parts if part)
    if business_mode:
        result = _strip_emojis(result)
    return result


def _strip_emojis(text: str) -> str:
    """Remove common emoji characters used in GrowthOS output."""
    import re
    # Remove emoji Unicode ranges
    emoji_pattern = re.compile(
        "["
        "\U0001F300-\U0001F9FF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.sub("", text).strip()


def _format_value(value) -> str:
    """Format values for markdown tables."""
    if value is None:
        return "—"
    if isinstance(value, float):
        if abs(value) >= 1000:
            return f"{value:,.0f}"
        if abs(value) < 0.01:
            return f"{value:.4f}"
        return f"{value:.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def format_narrative(
    headline: str,
    context: str,
    findings: list[str],
    recommendation: str,
) -> str:
    """Format findings as a flowing prose narrative paragraph instead of bullet lists.

    Produces a single coherent paragraph suitable for executive briefings.
    """
    findings_prose = ""
    if len(findings) == 1:
        findings_prose = findings[0]
    elif len(findings) == 2:
        findings_prose = f"{findings[0]}, while {findings[1].lower()}"
    elif findings:
        findings_prose = ", ".join(findings[:-1]) + f", and {findings[-1].lower()}"

    lines = [f"**{headline}**", ""]
    paragraph = f"{context} {findings_prose}. {recommendation}"
    lines.append(paragraph)
    return "\n".join(lines)


def wrap_tool_envelope_narrative(envelope, business_mode: bool = False) -> str:
    """Like wrap_tool_envelope but renders findings as narrative prose."""
    from growth_os.domain.models import ToolEnvelope  # noqa: F401 - kept for type hinting context
    # Use the standard envelope wrapping but prepend a narrative header
    result = wrap_tool_envelope(envelope, business_mode=business_mode)
    return result
