"""Backward-compatible formatter exports."""

from growth_os.presentation.cards import format_kpi_card, format_kpi_dashboard
from growth_os.presentation.markdown import format_actions, format_insight, format_table
from growth_os.presentation.sections import format_section

__all__ = ["format_actions", "format_insight", "format_kpi_card", "format_kpi_dashboard", "format_section", "format_table"]
