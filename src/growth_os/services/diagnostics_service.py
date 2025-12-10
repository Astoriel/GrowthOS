"""Validation and diagnostics services."""

from __future__ import annotations

from datetime import UTC, datetime

from growth_os.config.profiles import AttributionAliasSuggestion, SemanticProfile
from growth_os.connectors.duckdb import GrowthConnector
from growth_os.connectors.google_ads import GoogleAdsConnector
from growth_os.connectors.meta_ads import MetaAdsConnector
from growth_os.connectors.stripe import StripeConnector
from growth_os.ingestion.catalog import inspect_freshness, validate_marketing_dataset
from growth_os.presentation.markdown import format_actions, format_insight, format_table
from growth_os.query.builder import safe_identifier
from growth_os.semantic.attribution import (
    AttributionRule,
    append_attribution_rules,
    canonical_sql,
    load_attribution_mapping_history,
    load_attribution_rules,
    load_persisted_attribution_rules,
    remove_attribution_rules,
    normalized_sql,
    resolve_attribution_mapping_history_path,
    resolve_attribution_mapping_path,
)
from growth_os.semantic.profile_store import load_semantic_profile, save_semantic_profile
from growth_os.services._helpers import build_tool_envelope


class DiagnosticsService:
    """Services for dataset diagnostics and readiness."""

    def __init__(self, connector: GrowthConnector):
        self.connector = connector

    def validate_data(self):
        """Validate canonical dataset readiness."""
        result = validate_marketing_dataset(self.connector)
        issue_rows = [
            {"Table": issue.table_name, "Severity": issue.severity.upper(), "Issue": issue.message}
            for issue in result.issues
        ]
        freshness_rows = [
            {
                "Table": report.table_name,
                "Date Column": report.date_column,
                "Min Date": report.min_date or "—",
                "Max Date": report.max_date or "—",
                "Days Stale": report.days_stale if report.days_stale is not None else "—",
                "Status": report.status,
            }
            for report in result.freshness
        ]
        parts = []
        parts.append(
            "Dataset status: **Ready**" if result.ok else "Dataset status: **Needs attention**"
        )
        parts.append("")
        parts.append(format_table(issue_rows or [{"Table": "—", "Severity": "OK", "Issue": "No issues detected."}], "Validation Issues"))
        if freshness_rows:
            parts.append("")
            parts.append(format_table(freshness_rows, "Freshness"))
        body = "\n".join(parts)
        return build_tool_envelope("Data Validation", body, self.connector, [report.table_name for report in result.freshness])

    def freshness_report(self):
        """Return freshness across detected tables."""
        reports = inspect_freshness(self.connector)
        if not reports:
            body = "No date-like columns found for freshness checks."
            return build_tool_envelope("Freshness Report", body, self.connector, [])

        rows = [
            {
                "Table": report.table_name,
                "Date Column": report.date_column,
                "Min Date": report.min_date or "—",
                "Max Date": report.max_date or "—",
                "Days Stale": report.days_stale if report.days_stale is not None else "—",
                "Status": report.status,
            }
            for report in reports
        ]
        body = format_table(rows, "Freshness Report")
        return build_tool_envelope("Freshness Report", body, self.connector, [report.table_name for report in reports])

    def attribution_mapping_diagnostics(
        self,
        spend_tables: str = "meta_marketing_spend,google_marketing_spend",
        events_table: str = "user_events",
        revenue_event_type: str = "purchase",
        limit: int = 10,
    ):
        """Inspect attribution match coverage, unmapped keys, and applied alias rules."""
        spend_sources = [safe_identifier(table.strip()) for table in spend_tables.split(",") if table.strip()]
        events_table = safe_identifier(events_table)
        revenue_event_type = revenue_event_type.strip() or "purchase"
        revenue_event_sql = revenue_event_type.replace("'", "''")
        limit = max(int(limit), 1)
        warnings: list[str] = []

        if not spend_sources:
            body = "No spend tables were provided."
            return build_tool_envelope("Attribution Mapping Diagnostics", body, self.connector, [events_table])

        if not self._table_has_column(events_table, "utm_source"):
            body = f"Events table `{events_table}` does not include `utm_source`."
            return build_tool_envelope("Attribution Mapping Diagnostics", body, self.connector, spend_sources + [events_table])

        if not self._table_has_column(events_table, "revenue"):
            body = f"Events table `{events_table}` does not include `revenue`."
            return build_tool_envelope("Attribution Mapping Diagnostics", body, self.connector, spend_sources + [events_table])

        has_campaign_grain = self._table_has_column(events_table, "utm_campaign")
        if not has_campaign_grain:
            warnings.append(
                f"`{events_table}` has no `utm_campaign` column, so campaign-level mapping diagnostics fall back to channel-level."
            )

        spend_union = self._build_spend_union(spend_sources)
        rules = load_attribution_rules()
        spend_channel_expr = canonical_sql("channel", "channel", rules)
        event_channel_expr = canonical_sql("utm_source", "channel", rules)
        spend_campaign_expr = canonical_sql("campaign", "campaign", rules, channel_sql="channel")
        event_campaign_expr = canonical_sql("utm_campaign", "campaign", rules, channel_sql="utm_source")
        spend_rollup = self._spend_rollup_sql(has_campaign_grain, spend_channel_expr, spend_campaign_expr)
        event_rollup = self._event_rollup_sql(events_table, revenue_event_sql, has_campaign_grain, event_channel_expr, event_campaign_expr)
        join_condition = "s.channel_key = e.channel_key AND s.campaign_key = e.campaign_key" if has_campaign_grain else "s.channel_key = e.channel_key"

        try:
            coverage_rows = self.connector.query(
                f"""
                WITH spend_data AS (
                    {spend_union}
                ),
                spend_rollup AS (
                    {spend_rollup}
                ),
                event_rollup AS (
                    {event_rollup}
                ),
                joined AS (
                    SELECT
                        s.spend_30d,
                        s.channel_key,
                        s.campaign_key,
                        e.channel_key AS matched_channel_key,
                        COALESCE(e.revenue_30d, 0) AS attributed_revenue_30d
                    FROM spend_rollup s
                    LEFT JOIN event_rollup e ON {join_condition}
                ),
                total_revenue AS (
                    SELECT COALESCE(SUM(revenue), 0) AS total_revenue_30d
                    FROM {events_table}
                    WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
                      AND event_type = '{revenue_event_sql}'
                )
                SELECT
                    ROUND(COALESCE(SUM(spend_30d), 0), 2) AS total_spend_30d,
                    ROUND(COALESCE(SUM(CASE WHEN matched_channel_key IS NOT NULL THEN spend_30d ELSE 0 END), 0), 2) AS matched_spend_30d,
                    COUNT(*) AS total_keys,
                    SUM(CASE WHEN matched_channel_key IS NOT NULL THEN 1 ELSE 0 END) AS matched_keys,
                    ROUND(COALESCE(SUM(attributed_revenue_30d), 0), 2) AS attributed_revenue_30d,
                    t.total_revenue_30d
                FROM joined
                CROSS JOIN total_revenue t
                GROUP BY 6
                """
            )
            unmatched_event_rows = self.connector.query(
                self._unmatched_event_sql(
                    spend_union,
                    spend_rollup,
                    events_table,
                    revenue_event_sql,
                    has_campaign_grain,
                    event_channel_expr,
                    event_campaign_expr,
                    limit,
                )
            )
            unmatched_spend_rows = self.connector.query(
                self._unmatched_spend_sql(spend_union, spend_rollup, event_rollup, join_condition, has_campaign_grain, limit)
            )
        except Exception:
            body = (
                "Attribution mapping diagnostics need synced paid spend plus an events table with "
                "`utm_source` and `revenue` columns."
            )
            return build_tool_envelope(
                "Attribution Mapping Diagnostics",
                body,
                self.connector,
                spend_sources + [events_table],
                warnings=warnings,
            )

        coverage = coverage_rows[0] if coverage_rows else {}
        total_spend = float(coverage.get("total_spend_30d") or 0)
        matched_spend = float(coverage.get("matched_spend_30d") or 0)
        total_revenue = float(coverage.get("total_revenue_30d") or 0)
        attributed_revenue = float(coverage.get("attributed_revenue_30d") or 0)
        matched_keys = int(coverage.get("matched_keys") or 0)
        total_keys = int(coverage.get("total_keys") or 0)
        spend_coverage_pct = round(matched_spend / total_spend * 100, 1) if total_spend else None
        revenue_coverage_pct = round(attributed_revenue / total_revenue * 100, 1) if total_revenue else None

        summary_rows = [
            {"Metric": "Mapped spend (30d)", "Value": f"${matched_spend:,.2f}"},
            {"Metric": "Total spend (30d)", "Value": f"${total_spend:,.2f}"},
            {"Metric": "Spend coverage", "Value": f"{spend_coverage_pct:.1f}%" if spend_coverage_pct is not None else "—"},
            {"Metric": "Attributed revenue (30d)", "Value": f"${attributed_revenue:,.2f}"},
            {"Metric": "Total revenue (30d)", "Value": f"${total_revenue:,.2f}"},
            {"Metric": "Revenue coverage", "Value": f"{revenue_coverage_pct:.1f}%" if revenue_coverage_pct is not None else "—"},
            {"Metric": "Matched keys", "Value": f"{matched_keys}/{total_keys}" if total_keys else "0/0"},
        ]
        body = format_table(summary_rows, "Mapping Coverage")

        if has_campaign_grain:
            event_rows = [
                {
                    "UTM Source": row["utm_source"],
                    "UTM Campaign": row["utm_campaign"],
                    "Purchasers": int(row.get("purchasers_30d") or 0),
                    "Revenue (30d)": f"${float(row.get('revenue_30d') or 0):,.2f}",
                }
                for row in unmatched_event_rows
            ] or [{"UTM Source": "—", "UTM Campaign": "—", "Purchasers": 0, "Revenue (30d)": "$0.00"}]
            body += "\n\n" + format_table(event_rows, "Unmatched Event Keys")
        else:
            event_rows = [
                {
                    "UTM Source": row["utm_source"],
                    "Purchasers": int(row.get("purchasers_30d") or 0),
                    "Revenue (30d)": f"${float(row.get('revenue_30d') or 0):,.2f}",
                }
                for row in unmatched_event_rows
            ] or [{"UTM Source": "—", "Purchasers": 0, "Revenue (30d)": "$0.00"}]
            body += "\n\n" + format_table(event_rows, "Unmatched Event Sources")

        spend_rows = [
            {
                "Channel": row["channel"],
                "Campaign": row["campaign"],
                "Spend (30d)": f"${float(row.get('spend_30d') or 0):,.2f}",
            }
            for row in unmatched_spend_rows
        ] or [{"Channel": "—", "Campaign": "—", "Spend (30d)": "$0.00"}]
        body += "\n\n" + format_table(spend_rows, "Unmatched Spend Keys")

        applied_rule_rows = self._applied_alias_rules(rules, spend_union, events_table, has_campaign_grain, limit)
        alias_rows = applied_rule_rows or [{"Scope": "—", "Alias": "—", "Canonical": "—", "Channel": "—", "Spend Rows": 0, "Event Rows": 0, "Spend Sample": "—", "Event Sample": "—"}]
        body += "\n\n" + format_table(alias_rows, "Applied Alias Rules")

        if unmatched_event_rows:
            biggest_gap = unmatched_event_rows[0]
            if has_campaign_grain:
                body += format_insight(
                    f"Highest-value unmatched event key is `{biggest_gap['utm_source']} / {biggest_gap['utm_campaign']}`."
                )
            else:
                body += format_insight(
                    f"Highest-value unmatched event source is `{biggest_gap['utm_source']}`."
                )
        elif spend_coverage_pct is not None and spend_coverage_pct >= 90:
            body += format_insight("Most spend is already mapping cleanly into downstream attribution keys.")

        body += format_actions(
            [
                "Add explicit aliases for the highest-value unmatched keys first.",
                "Standardize `utm_source` and `utm_campaign` naming in ad platforms before adding more rules.",
                "Re-run `attribution_bridge_review` after updating mappings to verify coverage gains.",
            ]
        )
        return build_tool_envelope(
            "Attribution Mapping Diagnostics",
            body,
            self.connector,
            spend_sources + [events_table],
            warnings=warnings,
        )

    def suggest_attribution_mappings(
        self,
        spend_tables: str = "meta_marketing_spend,google_marketing_spend",
        events_table: str = "user_events",
        revenue_event_type: str = "purchase",
        limit: int = 10,
    ):
        """Suggest explicit attribution mappings and persist a semantic profile snapshot."""
        spend_sources = [safe_identifier(table.strip()) for table in spend_tables.split(",") if table.strip()]
        events_table = safe_identifier(events_table)
        revenue_event_type = revenue_event_type.strip() or "purchase"
        revenue_event_sql = revenue_event_type.replace("'", "''")
        limit = max(int(limit), 1)
        warnings: list[str] = []

        if not spend_sources:
            body = "No spend tables were provided."
            return build_tool_envelope("Suggested Attribution Mappings", body, self.connector, [events_table])

        has_campaign_grain = self._table_has_column(events_table, "utm_campaign")
        if not has_campaign_grain:
            warnings.append(
                f"`{events_table}` has no `utm_campaign` column, so only channel-level suggestions can be generated."
            )

        spend_union = self._build_spend_union(spend_sources)
        rules = load_attribution_rules()
        spend_channel_expr = canonical_sql("channel", "channel", rules)
        event_channel_expr = canonical_sql("utm_source", "channel", rules)
        spend_campaign_expr = canonical_sql("campaign", "campaign", rules, channel_sql="channel")
        event_campaign_expr = canonical_sql("utm_campaign", "campaign", rules, channel_sql="utm_source")
        spend_rollup = self._spend_rollup_sql(has_campaign_grain, spend_channel_expr, spend_campaign_expr)

        try:
            coverage_rows = self.connector.query(
                f"""
                WITH spend_data AS (
                    {spend_union}
                ),
                spend_rollup AS (
                    {spend_rollup}
                )
                SELECT
                    ROUND(COALESCE(SUM(spend_30d), 0), 2) AS total_spend_30d,
                    COUNT(*) AS total_keys
                FROM spend_rollup
                """
            )
            unmatched_event_rows = self.connector.query(
                self._unmatched_event_sql(
                    spend_union,
                    spend_rollup,
                    events_table,
                    revenue_event_sql,
                    has_campaign_grain,
                    event_channel_expr,
                    event_campaign_expr,
                    limit * 2,
                )
            )
            unmatched_spend_rows = self.connector.query(
                self._unmatched_spend_sql(
                    spend_union,
                    spend_rollup,
                    self._event_rollup_sql(
                        events_table,
                        revenue_event_sql,
                        has_campaign_grain,
                        event_channel_expr,
                        event_campaign_expr,
                    ),
                    "s.channel_key = e.channel_key AND s.campaign_key = e.campaign_key" if has_campaign_grain else "s.channel_key = e.channel_key",
                    has_campaign_grain,
                    limit * 2,
                )
            )
            applied_rule_rows = self._applied_alias_rules(rules, spend_union, events_table, has_campaign_grain, limit)
            spend_catalog = self.connector.query(
                f"""
                WITH spend_data AS (
                    {spend_union}
                ),
                spend_rollup AS (
                    {spend_rollup}
                )
                SELECT
                    channel_key,
                    campaign_key,
                    channel,
                    campaign,
                    ROUND(spend_30d, 2) AS spend_30d
                FROM spend_rollup
                ORDER BY spend_30d DESC
                """
            )
        except Exception:
            body = (
                "Mapping suggestions need synced paid spend plus an events table with `utm_source` "
                "and enough recent data to infer likely matches."
            )
            return build_tool_envelope(
                "Suggested Attribution Mappings",
                body,
                self.connector,
                spend_sources + [events_table],
                warnings=warnings,
            )

        suggestions = self._generate_mapping_suggestions(
            rules,
            spend_catalog,
            unmatched_event_rows,
            has_campaign_grain,
            limit,
        )

        profile = SemanticProfile(
            generated_at=datetime.now(UTC).isoformat(),
            spend_tables=spend_sources,
            events_table=events_table,
            revenue_event_type=revenue_event_type,
            diagnostics={
                "total_spend_30d": float(coverage_rows[0].get("total_spend_30d") or 0) if coverage_rows else 0.0,
                "total_keys": int(coverage_rows[0].get("total_keys") or 0) if coverage_rows else 0,
                "has_campaign_grain": has_campaign_grain,
            },
            unmatched_event_keys=unmatched_event_rows,
            unmatched_spend_keys=unmatched_spend_rows,
            applied_alias_rules=applied_rule_rows,
            suggestions=suggestions,
        )
        profile_path = save_semantic_profile(profile)

        if suggestions:
            rows = [
                {
                    "Scope": suggestion.scope,
                    "Alias": suggestion.alias,
                    "Canonical": suggestion.canonical_value,
                    "Channel": suggestion.channel or "all",
                    "Confidence": f"{suggestion.confidence:.2f}",
                    "Reason": suggestion.reason,
                }
                for suggestion in suggestions
            ]
            body = format_table(rows, "Suggested Mapping Rules")
            body += format_insight(f"Semantic profile snapshot saved to `{profile_path}`.")
        else:
            body = "No high-confidence mapping suggestions were inferred from the current unmatched keys."
            body += format_insight(f"Semantic profile snapshot saved to `{profile_path}` for review.")

        body += format_actions(
            [
                "Review suggested rules before copying them into your attribution mapping CSV.",
                "Prioritize high-revenue unmatched keys first.",
                "Re-run `attribution_mapping_diagnostics` after applying accepted rules.",
            ]
        )
        return build_tool_envelope(
            "Suggested Attribution Mappings",
            body,
            self.connector,
            spend_sources + [events_table],
            warnings=warnings,
        )

    def apply_suggested_attribution_mappings(
        self,
        profile_path: str = "",
        mapping_file: str = "",
        aliases: str = "",
        min_confidence: float = 0.8,
        limit: int = 20,
        force: bool = False,
    ):
        """Apply approved suggestions from the persisted semantic profile into the mapping CSV.

        High-risk rules (collision, low confidence, missing scope) are blocked by default.
        Pass force=True to override the guardrail and apply them anyway.
        """
        min_confidence = max(float(min_confidence), 0.0)
        limit = max(int(limit), 1)
        profile = load_semantic_profile(profile_path or None)
        if profile is None:
            body = "No semantic profile snapshot found. Run `suggest_attribution_mappings` first."
            return build_tool_envelope("Apply Attribution Mappings", body, self.connector, [])

        alias_filter = {
            self._normalize_value(alias)
            for alias in aliases.split(",")
            if alias.strip()
        }
        selected: list[AttributionAliasSuggestion] = []
        for suggestion in profile.suggestions:
            if suggestion.confidence < min_confidence:
                continue
            if alias_filter and self._normalize_value(suggestion.alias) not in alias_filter:
                continue
            selected.append(suggestion)

        selected = selected[:limit]
        if not selected:
            body = "No suggestions met the current selection criteria."
            return build_tool_envelope(
                "Apply Attribution Mappings",
                body,
                self.connector,
                profile.spend_tables + ([profile.events_table] if profile.events_table else []),
            )

        # Guardrail: assess risk for each selected suggestion before writing anything.
        if not force:
            current_rules = load_attribution_rules(mapping_file or None)
            high_risk_rows = []
            for suggestion in selected:
                assessment = self._assess_suggestion_risk(suggestion, current_rules, 0.0, 0.0)
                if assessment["risk_level"] == "high":
                    high_risk_rows.append({
                        "Alias": suggestion.alias,
                        "Canonical": suggestion.canonical_value,
                        "Flags": ", ".join(assessment["flags"]) if assessment["flags"] else "none",
                        "Why": assessment["why"],
                    })
            if high_risk_rows:
                body = format_table(high_risk_rows, "🚫 Blocked: High-Risk Mapping Rules")
                body += format_insight(
                    f"{len(high_risk_rows)} rule(s) were blocked due to high-risk flags "
                    "(alias collision, low confidence, or missing channel scope). "
                    "No changes were written to disk."
                )
                body += format_actions([
                    "Use `aliases=` to apply only the safe low/medium-risk rules.",
                    "Re-run with `force=True` to override the guardrail and apply all selected rules.",
                    "Run `attribution_mapping_review_pack` for a full risk breakdown.",
                ])
                return build_tool_envelope(
                    "Apply Attribution Mappings — Blocked",
                    body,
                    self.connector,
                    profile.spend_tables + ([profile.events_table] if profile.events_table else []),
                )

        rules_to_apply = [
            AttributionRule(
                scope=suggestion.scope,
                canonical_value=suggestion.canonical_value,
                alias=suggestion.alias,
                channel=suggestion.channel,
            )
            for suggestion in selected
        ]
        target_path, applied, skipped = append_attribution_rules(rules_to_apply, mapping_file or None)

        rows = [
            {
                "Scope": rule.scope,
                "Alias": rule.alias,
                "Canonical": rule.canonical_value,
                "Channel": rule.channel or "all",
                "Status": "applied",
            }
            for rule in applied
        ]
        rows.extend(
            {
                "Scope": rule.scope,
                "Alias": rule.alias,
                "Canonical": rule.canonical_value,
                "Channel": rule.channel or "all",
                "Status": "skipped",
            }
            for rule in skipped
        )
        body = format_table(rows or [{"Scope": "—", "Alias": "—", "Canonical": "—", "Channel": "—", "Status": "none"}], "Applied Mapping Rules")
        body += format_insight(f"Mappings written to `{target_path}`.")
        body += format_actions(
            [
                "Re-run `attribution_mapping_diagnostics` to confirm coverage improved.",
                "Keep only approved rules in version control if you manage mappings as code.",
                "Use `aliases` to apply a narrower subset when reviewing suggestions incrementally.",
            ]
        )
        return build_tool_envelope(
            "Apply Attribution Mappings",
            body,
            self.connector,
            profile.spend_tables + ([profile.events_table] if profile.events_table else []),
        )

    def attribution_mapping_review_pack(
        self,
        profile_path: str = "",
        mapping_file: str = "",
        aliases: str = "",
        min_confidence: float = 0.8,
        limit: int = 20,
    ):
        """Build a read-only review pack for selected attribution mapping suggestions."""
        return self._build_mapping_review_pack_envelope(
            "Attribution Mapping Review Pack",
            profile_path,
            mapping_file,
            aliases,
            min_confidence,
            limit,
        )

    def preview_apply_attribution_mappings(
        self,
        profile_path: str = "",
        mapping_file: str = "",
        aliases: str = "",
        min_confidence: float = 0.8,
        limit: int = 20,
    ):
        """Preview coverage changes before writing suggested mappings to disk."""
        return self._build_mapping_review_pack_envelope(
            "Preview Attribution Mapping Apply",
            profile_path,
            mapping_file,
            aliases,
            min_confidence,
            limit,
        )
        min_confidence = max(float(min_confidence), 0.0)
        limit = max(int(limit), 1)
        profile = load_semantic_profile(profile_path or None)
        if profile is None:
            body = "No semantic profile snapshot found. Run `suggest_attribution_mappings` first."
            return build_tool_envelope("Preview Attribution Mapping Apply", body, self.connector, [])

        selected = self._select_profile_suggestions(profile, aliases, min_confidence, limit)
        if not selected:
            body = "No suggestions met the current selection criteria."
            return build_tool_envelope(
                "Preview Attribution Mapping Apply",
                body,
                self.connector,
                profile.spend_tables + ([profile.events_table] if profile.events_table else []),
            )

        current_rules = load_attribution_rules(mapping_file or None)
        preview_rules = list(current_rules)
        for suggestion in selected:
            preview_rules.append(
                AttributionRule(
                    scope=suggestion.scope,
                    canonical_value=suggestion.canonical_value,
                    alias=suggestion.alias,
                    channel=suggestion.channel,
                )
            )
            preview_rules.append(
                AttributionRule(
                    scope=suggestion.scope,
                    canonical_value=suggestion.canonical_value,
                    alias=suggestion.canonical_value,
                    channel=suggestion.channel,
                )
            )

        current_snapshot = self._mapping_coverage_snapshot(
            profile.spend_tables,
            profile.events_table,
            profile.revenue_event_type,
            current_rules,
        )
        preview_snapshot = self._mapping_coverage_snapshot(
            profile.spend_tables,
            profile.events_table,
            profile.revenue_event_type,
            preview_rules,
        )
        if current_snapshot is None or preview_snapshot is None:
            body = "Unable to compute preview coverage for the selected profile."
            return build_tool_envelope(
                "Preview Attribution Mapping Apply",
                body,
                self.connector,
                profile.spend_tables + ([profile.events_table] if profile.events_table else []),
            )

        summary_rows = [
            {
                "Metric": "Mapped spend (30d)",
                "Current": f"${current_snapshot['matched_spend_30d']:,.2f}",
                "Preview": f"${preview_snapshot['matched_spend_30d']:,.2f}",
                "Delta": f"${preview_snapshot['matched_spend_30d'] - current_snapshot['matched_spend_30d']:,.2f}",
            },
            {
                "Metric": "Spend coverage",
                "Current": f"{current_snapshot['spend_coverage_pct']:.1f}%" if current_snapshot["spend_coverage_pct"] is not None else "—",
                "Preview": f"{preview_snapshot['spend_coverage_pct']:.1f}%" if preview_snapshot["spend_coverage_pct"] is not None else "—",
                "Delta": self._format_pct_delta(current_snapshot["spend_coverage_pct"], preview_snapshot["spend_coverage_pct"]),
            },
            {
                "Metric": "Attributed revenue (30d)",
                "Current": f"${current_snapshot['attributed_revenue_30d']:,.2f}",
                "Preview": f"${preview_snapshot['attributed_revenue_30d']:,.2f}",
                "Delta": f"${preview_snapshot['attributed_revenue_30d'] - current_snapshot['attributed_revenue_30d']:,.2f}",
            },
            {
                "Metric": "Revenue coverage",
                "Current": f"{current_snapshot['revenue_coverage_pct']:.1f}%" if current_snapshot["revenue_coverage_pct"] is not None else "—",
                "Preview": f"{preview_snapshot['revenue_coverage_pct']:.1f}%" if preview_snapshot["revenue_coverage_pct"] is not None else "—",
                "Delta": self._format_pct_delta(current_snapshot["revenue_coverage_pct"], preview_snapshot["revenue_coverage_pct"]),
            },
            {
                "Metric": "Matched keys",
                "Current": f"{current_snapshot['matched_keys']}/{current_snapshot['total_keys']}",
                "Preview": f"{preview_snapshot['matched_keys']}/{preview_snapshot['total_keys']}",
                "Delta": f"{preview_snapshot['matched_keys'] - current_snapshot['matched_keys']:+d}",
            },
        ]
        suggestion_rows = [
            {
                "Scope": suggestion.scope,
                "Alias": suggestion.alias,
                "Canonical": suggestion.canonical_value,
                "Channel": suggestion.channel or "all",
                "Confidence": f"{suggestion.confidence:.2f}",
            }
            for suggestion in selected
        ]

        body = format_table(summary_rows, "Coverage Preview")
        body += "\n\n" + format_table(suggestion_rows, "Selected Suggestions")
        body += format_insight(
            f"Preview uses the current mapping file plus {len(selected)} in-memory suggestion(s); nothing was written to disk."
        )
        body += format_actions(
            [
                "Run `apply_suggested_attribution_mappings` if the previewed coverage lift looks acceptable.",
                "Tighten `min_confidence` or use `aliases` to preview a narrower subset.",
                "Re-run `attribution_mapping_diagnostics` after apply to compare real coverage with this preview.",
            ]
        )
        return build_tool_envelope(
            "Preview Attribution Mapping Apply",
            body,
            self.connector,
            profile.spend_tables + ([profile.events_table] if profile.events_table else []),
        )

    def review_attribution_mappings(
        self,
        mapping_file: str = "",
        history_limit: int = 20,
        rules_limit: int = 50,
    ):
        """Review active attribution mappings and recent change history."""
        history_limit = max(int(history_limit), 1)
        rules_limit = max(int(rules_limit), 1)
        mapping_path = resolve_attribution_mapping_path(mapping_file or None)
        history_path = resolve_attribution_mapping_history_path(None, str(mapping_path))
        rules = load_persisted_attribution_rules(str(mapping_path))
        history = load_attribution_mapping_history(str(history_path), str(mapping_path))

        rule_rows = [
            {
                "Scope": rule.scope,
                "Alias": rule.alias,
                "Canonical": rule.canonical_value,
                "Channel": rule.channel or "all",
            }
            for rule in rules[:rules_limit]
        ] or [{"Scope": "—", "Alias": "—", "Canonical": "—", "Channel": "—"}]

        history_rows = [
            {
                "Timestamp": entry.timestamp,
                "Action": entry.action,
                "Scope": entry.scope,
                "Alias": entry.alias,
                "Canonical": entry.canonical_value,
                "Channel": entry.channel or "all",
            }
            for entry in history[-history_limit:][::-1]
        ] or [{"Timestamp": "—", "Action": "—", "Scope": "—", "Alias": "—", "Canonical": "—", "Channel": "—"}]

        body = format_table(rule_rows, "Active Attribution Mappings")
        body += "\n\n" + format_table(history_rows, "Recent Mapping History")
        body += format_insight(f"Mapping file: `{mapping_path}` | History file: `{history_path}`.")
        body += format_actions(
            [
                "Use `rollback_attribution_mappings` with specific aliases to undo rules safely.",
                "Review recent `apply` and `rollback` events before changing mappings again.",
                "Keep the mapping CSV and history file under version control if you want team review.",
            ]
        )
        return build_tool_envelope("Review Attribution Mappings", body, self.connector, [])

    def rollback_attribution_mappings(
        self,
        aliases: str,
        mapping_file: str = "",
        scope: str = "",
        channel: str = "",
    ):
        """Rollback specific persisted attribution mappings by alias."""
        alias_list = [alias.strip() for alias in aliases.split(",") if alias.strip()]
        if not alias_list:
            body = "Provide at least one alias to rollback."
            return build_tool_envelope("Rollback Attribution Mappings", body, self.connector, [])

        target_path, removed, missing = remove_attribution_rules(
            alias_list,
            mapping_file or None,
            scope=scope,
            channel=channel,
        )

        rows = [
            {
                "Scope": rule.scope,
                "Alias": rule.alias,
                "Canonical": rule.canonical_value,
                "Channel": rule.channel or "all",
                "Status": "removed",
            }
            for rule in removed
        ]
        rows.extend(
            {
                "Scope": rule.scope or "—",
                "Alias": rule.alias,
                "Canonical": rule.canonical_value or "—",
                "Channel": rule.channel or "all",
                "Status": "not_found",
            }
            for rule in missing
        )
        body = format_table(
            rows or [{"Scope": "—", "Alias": "—", "Canonical": "—", "Channel": "—", "Status": "none"}],
            "Rollback Results",
        )
        body += format_insight(f"Updated mapping file: `{target_path}`.")
        body += format_actions(
            [
                "Re-run `review_attribution_mappings` to confirm the rollback history entry is present.",
                "Re-run `attribution_mapping_diagnostics` to see if coverage dropped after removing the rule.",
                "Use `apply_suggested_attribution_mappings` if you need to restore a removed suggestion.",
            ]
        )
        return build_tool_envelope("Rollback Attribution Mappings", body, self.connector, [])

    def list_connectors(self):
        """List built-in connectors and status."""
        google_ads = GoogleAdsConnector()
        meta = MetaAdsConnector()
        stripe = StripeConnector()
        rows = [
            {"Connector": "csv", "Mode": "active", "Notes": "Local CSV ingestion is ready."},
            {"Connector": "postgres", "Mode": "active", "Notes": "Read-only attach via DuckDB postgres extension."},
            {"Connector": "duckdb", "Mode": "active", "Notes": "Embedded analytical execution engine."},
            {"Connector": "ga4", "Mode": "planned", "Notes": "Architecture scaffold reserved."},
            {
                "Connector": "google_ads",
                "Mode": google_ads.status,
                "Notes": (
                    "Campaign + searchStream performance sync via Google Ads REST API."
                    if google_ads.configured
                    else "Set GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CUSTOMER_ID, and OAuth credentials to enable sync."
                ),
            },
            {
                "Connector": "meta_ads",
                "Mode": meta.status,
                "Notes": "Campaign + insights sync via Graph API." if meta.configured else "Set META_ACCESS_TOKEN and META_AD_ACCOUNT_ID to enable sync.",
            },
            {
                "Connector": "stripe",
                "Mode": stripe.status,
                "Notes": "Billing sync via Stripe REST API." if stripe.configured else "Set STRIPE_API_KEY to enable billing sync.",
            },
        ]
        body = format_table(rows, "Connectors")
        return build_tool_envelope("Connectors", body, self.connector, [])

    @staticmethod
    def _build_spend_union(spend_sources: list[str]) -> str:
        """Build a union over normalized spend tables."""
        return "\nUNION ALL\n".join(
            f"SELECT '{table}' AS source_table, channel, campaign_id, campaign, date, spend, clicks, impressions, COALESCE(conversions, 0) AS conversions FROM {table}"
            for table in spend_sources
        )

    @staticmethod
    def _spend_rollup_sql(has_campaign_grain: bool, channel_key_sql: str, campaign_key_sql: str) -> str:
        """Build spend rollup SQL at campaign or channel grain."""
        if has_campaign_grain:
            return """
            SELECT
                """ + channel_key_sql + """ AS channel_key,
                """ + campaign_key_sql + """ AS campaign_key,
                MIN(channel) AS channel,
                MIN(campaign) AS campaign,
                SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN spend ELSE 0 END) AS spend_30d
            FROM spend_data
            GROUP BY 1, 2
            """
        return """
        SELECT
            """ + channel_key_sql + """ AS channel_key,
            """ + channel_key_sql + """ AS campaign_key,
            MIN(channel) AS channel,
            MIN(channel) AS campaign,
            SUM(CASE WHEN date >= CURRENT_DATE - INTERVAL '30 days' THEN spend ELSE 0 END) AS spend_30d
        FROM spend_data
        GROUP BY 1, 2
        """

    @staticmethod
    def _event_rollup_sql(
        events_table: str,
        revenue_event_sql: str,
        has_campaign_grain: bool,
        channel_key_sql: str,
        campaign_key_sql: str,
    ) -> str:
        """Build downstream event aggregation for diagnostics."""
        if has_campaign_grain:
            return f"""
            SELECT
                {channel_key_sql} AS channel_key,
                {campaign_key_sql} AS campaign_key,
                COUNT(DISTINCT CASE WHEN event_type = 'signup' THEN user_id END) AS signups_30d,
                COUNT(DISTINCT CASE WHEN event_type = '{revenue_event_sql}' THEN user_id END) AS purchasers_30d,
                SUM(CASE WHEN event_type = '{revenue_event_sql}' THEN revenue ELSE 0 END) AS revenue_30d
            FROM {events_table}
            WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY 1, 2
            """
        return f"""
        SELECT
            {channel_key_sql} AS channel_key,
            COUNT(DISTINCT CASE WHEN event_type = 'signup' THEN user_id END) AS signups_30d,
            COUNT(DISTINCT CASE WHEN event_type = '{revenue_event_sql}' THEN user_id END) AS purchasers_30d,
            SUM(CASE WHEN event_type = '{revenue_event_sql}' THEN revenue ELSE 0 END) AS revenue_30d
        FROM {events_table}
        WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY 1
        """

    @staticmethod
    def _unmatched_event_sql(
        spend_union: str,
        spend_rollup: str,
        events_table: str,
        revenue_event_sql: str,
        has_campaign_grain: bool,
        channel_key_sql: str,
        campaign_key_sql: str,
        limit: int,
    ) -> str:
        """Build SQL for unmatched event-side keys."""
        if has_campaign_grain:
            return f"""
            WITH spend_data AS (
                {spend_union}
            ),
            spend_rollup AS (
                {spend_rollup}
            ),
            spend_keys AS (
                SELECT DISTINCT channel_key, campaign_key
                FROM spend_rollup
            ),
            raw_events AS (
                SELECT
                    COALESCE(utm_source, '') AS utm_source,
                    COALESCE(utm_campaign, '') AS utm_campaign,
                    user_id,
                    event_type,
                    revenue,
                    {channel_key_sql} AS channel_key,
                    {campaign_key_sql} AS campaign_key
                FROM {events_table}
                WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
            )
            SELECT
                utm_source,
                utm_campaign,
                e.channel_key,
                e.campaign_key,
                COUNT(DISTINCT CASE WHEN event_type = '{revenue_event_sql}' THEN user_id END) AS purchasers_30d,
                ROUND(SUM(CASE WHEN event_type = '{revenue_event_sql}' THEN revenue ELSE 0 END), 2) AS revenue_30d
            FROM raw_events e
            LEFT JOIN spend_keys s ON e.channel_key = s.channel_key AND e.campaign_key = s.campaign_key
            WHERE s.channel_key IS NULL
            GROUP BY 1, 2, 3, 4
            HAVING SUM(CASE WHEN event_type = '{revenue_event_sql}' THEN revenue ELSE 0 END) > 0
            ORDER BY revenue_30d DESC, purchasers_30d DESC
            LIMIT {limit}
            """
        return f"""
        WITH spend_data AS (
            {spend_union}
        ),
        spend_rollup AS (
            {spend_rollup}
        ),
        spend_keys AS (
            SELECT DISTINCT channel_key
            FROM spend_rollup
        ),
        raw_events AS (
            SELECT
                COALESCE(utm_source, '') AS utm_source,
                user_id,
                event_type,
                revenue,
                {channel_key_sql} AS channel_key
            FROM {events_table}
            WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
        )
        SELECT
            utm_source,
            e.channel_key,
            COUNT(DISTINCT CASE WHEN event_type = '{revenue_event_sql}' THEN user_id END) AS purchasers_30d,
            ROUND(SUM(CASE WHEN event_type = '{revenue_event_sql}' THEN revenue ELSE 0 END), 2) AS revenue_30d
        FROM raw_events e
        LEFT JOIN spend_keys s ON e.channel_key = s.channel_key
        WHERE s.channel_key IS NULL
        GROUP BY 1, 2
        HAVING SUM(CASE WHEN event_type = '{revenue_event_sql}' THEN revenue ELSE 0 END) > 0
        ORDER BY revenue_30d DESC, purchasers_30d DESC
        LIMIT {limit}
        """

    @staticmethod
    def _unmatched_spend_sql(
        spend_union: str,
        spend_rollup: str,
        event_rollup: str,
        join_condition: str,
        has_campaign_grain: bool,
        limit: int,
    ) -> str:
        """Build SQL for unmatched spend-side keys."""
        campaign_select = "s.campaign" if has_campaign_grain else "s.channel"
        return f"""
        WITH spend_data AS (
            {spend_union}
        ),
        spend_rollup AS (
            {spend_rollup}
        ),
        event_rollup AS (
            {event_rollup}
        )
        SELECT
            s.channel,
            {campaign_select} AS campaign,
            ROUND(s.spend_30d, 2) AS spend_30d
        FROM spend_rollup s
        LEFT JOIN event_rollup e ON {join_condition}
        WHERE e.channel_key IS NULL
        ORDER BY spend_30d DESC
        LIMIT {limit}
        """

    def _applied_alias_rules(
        self,
        rules,
        spend_union: str,
        events_table: str,
        has_campaign_grain: bool,
        limit: int,
    ) -> list[dict]:
        """Inspect which non-trivial alias rules matched current data."""
        seen: set[tuple[str, str, str, str]] = set()
        rows: list[dict] = []
        for rule in rules:
            alias_normalized = self._normalize_value(rule.alias)
            canonical_normalized = self._normalize_value(rule.canonical_value)
            key = (rule.scope, rule.canonical_value, alias_normalized, rule.channel)
            if alias_normalized == canonical_normalized or key in seen:
                continue
            seen.add(key)

            if rule.scope == "campaign" and not has_campaign_grain:
                continue

            spend_count, spend_sample = self._rule_hits(rule, spend_union, dataset="spend")
            event_count, event_sample = self._rule_hits(rule, events_table, dataset="events")
            if not spend_count and not event_count:
                continue

            rows.append(
                {
                    "Scope": rule.scope,
                    "Alias": rule.alias,
                    "Canonical": rule.canonical_value,
                    "Channel": rule.channel or "all",
                    "Spend Rows": spend_count,
                    "Event Rows": event_count,
                    "Spend Sample": spend_sample or "—",
                    "Event Sample": event_sample or "—",
                }
            )

        rows.sort(key=lambda row: (row["Spend Rows"] + row["Event Rows"]), reverse=True)
        return rows[:limit]

    def _rule_hits(self, rule, source_sql: str, dataset: str) -> tuple[int, str]:
        """Return hit count and a sample raw value for one alias rule."""
        alias_sql = self._normalize_value(rule.alias).replace("'", "''")
        if dataset == "spend":
            table_sql = f"WITH spend_data AS ({source_sql}) SELECT COUNT(*) AS cnt, MIN({ 'channel' if rule.scope == 'channel' else 'campaign' }) AS sample FROM spend_data"
            if rule.scope == "channel":
                where = f"{normalized_sql('channel')} = '{alias_sql}' AND date >= CURRENT_DATE - INTERVAL '30 days'"
            else:
                channel_sql = rule.channel.replace("'", "''")
                where = (
                    f"{normalized_sql('campaign')} = '{alias_sql}' "
                    f"AND {canonical_sql('channel', 'channel', load_attribution_rules())} = '{channel_sql}' "
                    "AND date >= CURRENT_DATE - INTERVAL '30 days'"
                )
        else:
            table_sql = f"SELECT COUNT(*) AS cnt, MIN({ 'utm_source' if rule.scope == 'channel' else 'utm_campaign' }) AS sample FROM {source_sql}"
            if rule.scope == "channel":
                where = f"{normalized_sql('utm_source')} = '{alias_sql}' AND event_date >= CURRENT_DATE - INTERVAL '30 days'"
            else:
                channel_sql = rule.channel.replace("'", "''")
                where = (
                    f"{normalized_sql('utm_campaign')} = '{alias_sql}' "
                    f"AND {canonical_sql('utm_source', 'channel', load_attribution_rules())} = '{channel_sql}' "
                    "AND event_date >= CURRENT_DATE - INTERVAL '30 days'"
                )
        rows = self.connector.query(f"{table_sql} WHERE {where}")
        if not rows:
            return 0, ""
        return int(rows[0]["cnt"] or 0), str(rows[0].get("sample") or "")

    def _generate_mapping_suggestions(
        self,
        rules,
        spend_catalog: list[dict],
        unmatched_event_rows: list[dict],
        has_campaign_grain: bool,
        limit: int,
    ) -> list[AttributionAliasSuggestion]:
        """Infer likely mapping rules from unmatched events and current spend catalog."""
        existing_rules = {
            (rule.scope, self._normalize_value(rule.alias), rule.canonical_value, rule.channel)
            for rule in rules
        }
        suggestions: list[AttributionAliasSuggestion] = []
        seen: set[tuple[str, str, str, str]] = set()

        for row in unmatched_event_rows:
            raw_source = str(row.get("utm_source") or "")
            raw_campaign = str(row.get("utm_campaign") or "")
            revenue_30d = float(row.get("revenue_30d") or 0)
            channel_key = str(row.get("channel_key") or "")
            candidate_channel = self._best_channel_candidate(raw_source, channel_key, spend_catalog)

            if candidate_channel and candidate_channel["suggest_channel_rule"]:
                suggestion_key = ("channel", self._normalize_value(raw_source), candidate_channel["channel"], "")
                if suggestion_key not in seen and suggestion_key not in existing_rules:
                    suggestions.append(
                        AttributionAliasSuggestion(
                            scope="channel",
                            canonical_value=candidate_channel["channel"],
                            alias=raw_source,
                            confidence=round(candidate_channel["confidence"], 2),
                            reason="Observed unmatched source is strongly similar to an existing paid channel.",
                            estimated_revenue_30d=revenue_30d,
                        )
                    )
                    seen.add(suggestion_key)

            if not has_campaign_grain or not raw_campaign:
                continue

            channel_for_campaign = candidate_channel["channel"] if candidate_channel else ""
            campaign_candidate = self._best_campaign_candidate(raw_campaign, channel_for_campaign or channel_key, spend_catalog)
            if not campaign_candidate or campaign_candidate["confidence"] < 0.72:
                continue

            suggestion_key = (
                "campaign",
                self._normalize_value(raw_campaign),
                campaign_candidate["campaign"],
                campaign_candidate["channel"],
            )
            existing_key = ("campaign", self._normalize_value(raw_campaign), campaign_candidate["campaign"], campaign_candidate["channel"])
            if suggestion_key in seen or existing_key in existing_rules:
                continue

            suggestions.append(
                AttributionAliasSuggestion(
                    scope="campaign",
                    canonical_value=campaign_candidate["campaign"],
                    alias=raw_campaign,
                    channel=campaign_candidate["channel"],
                    confidence=round(campaign_candidate["confidence"], 2),
                    reason="Observed unmatched campaign is strongly similar to an existing paid campaign in the same channel.",
                    estimated_revenue_30d=revenue_30d,
                    estimated_spend_30d=float(campaign_candidate["spend_30d"] or 0),
                )
            )
            seen.add(suggestion_key)

        suggestions.sort(
            key=lambda suggestion: (
                suggestion.confidence,
                suggestion.estimated_revenue_30d,
                suggestion.estimated_spend_30d,
            ),
            reverse=True,
        )
        return suggestions[:limit]

    def _select_profile_suggestions(
        self,
        profile: SemanticProfile,
        aliases: str,
        min_confidence: float,
        limit: int,
    ) -> list[AttributionAliasSuggestion]:
        """Select profile suggestions using the same filters as apply/preview flows."""
        alias_filter = {
            self._normalize_value(alias)
            for alias in aliases.split(",")
            if alias.strip()
        }
        selected: list[AttributionAliasSuggestion] = []
        for suggestion in profile.suggestions:
            if suggestion.confidence < min_confidence:
                continue
            if alias_filter and self._normalize_value(suggestion.alias) not in alias_filter:
                continue
            selected.append(suggestion)
        return selected[:limit]

    def _build_mapping_review_pack_envelope(
        self,
        title: str,
        profile_path: str,
        mapping_file: str,
        aliases: str,
        min_confidence: float,
        limit: int,
    ):
        """Build a read-only mapping review pack with coverage preview and rule risks."""
        min_confidence = max(float(min_confidence), 0.0)
        limit = max(int(limit), 1)
        profile = load_semantic_profile(profile_path or None)
        if profile is None:
            body = "No semantic profile snapshot found. Run `suggest_attribution_mappings` first."
            return build_tool_envelope(title, body, self.connector, [])

        selected = self._select_profile_suggestions(profile, aliases, min_confidence, limit)
        sources = profile.spend_tables + ([profile.events_table] if profile.events_table else [])
        if not selected:
            body = "No suggestions met the current selection criteria."
            return build_tool_envelope(title, body, self.connector, sources)

        current_rules = load_attribution_rules(mapping_file or None)
        preview_rules = self._build_preview_rules(current_rules, selected)
        current_snapshot = self._mapping_coverage_snapshot(
            profile.spend_tables,
            profile.events_table,
            profile.revenue_event_type,
            current_rules,
        )
        preview_snapshot = self._mapping_coverage_snapshot(
            profile.spend_tables,
            profile.events_table,
            profile.revenue_event_type,
            preview_rules,
        )
        if current_snapshot is None or preview_snapshot is None:
            body = "Unable to compute preview coverage for the selected profile."
            return build_tool_envelope(title, body, self.connector, sources)

        summary_rows = self._coverage_preview_rows(current_snapshot, preview_snapshot)
        review_rows, review_warnings, review_insight = self._mapping_risk_review_rows(
            selected,
            current_rules,
            current_snapshot,
        )
        suggestion_rows = [
            {
                "Scope": suggestion.scope,
                "Alias": suggestion.alias,
                "Canonical": suggestion.canonical_value,
                "Channel": suggestion.channel or "all",
                "Confidence": f"{suggestion.confidence:.2f}",
            }
            for suggestion in selected
        ]

        body = format_table(summary_rows, "Coverage Preview")
        body += "\n\n" + format_table(review_rows, "Rule Risk Review")
        body += "\n\n" + format_table(suggestion_rows, "Selected Suggestions")
        body += format_insight(
            f"Preview uses the current mapping file plus {len(selected)} in-memory suggestion(s); nothing was written to disk."
        )
        if review_insight:
            body += format_insight(review_insight)
        body += format_actions(
            [
                "Apply low-risk rules first, then re-run diagnostics before approving medium-risk suggestions.",
                "Use `aliases` to isolate one risky mapping when you want to validate it incrementally.",
                "Run `apply_suggested_attribution_mappings` only after the previewed lift and risk flags look acceptable.",
            ]
        )
        return build_tool_envelope(
            title,
            body,
            self.connector,
            sources,
            warnings=review_warnings,
        )

    @staticmethod
    def _build_preview_rules(
        current_rules: list[AttributionRule],
        selected: list[AttributionAliasSuggestion],
    ) -> list[AttributionRule]:
        """Materialize in-memory preview rules without touching the persisted mapping file."""
        preview_rules = list(current_rules)
        for suggestion in selected:
            preview_rules.append(
                AttributionRule(
                    scope=suggestion.scope,
                    canonical_value=suggestion.canonical_value,
                    alias=suggestion.alias,
                    channel=suggestion.channel,
                )
            )
            preview_rules.append(
                AttributionRule(
                    scope=suggestion.scope,
                    canonical_value=suggestion.canonical_value,
                    alias=suggestion.canonical_value,
                    channel=suggestion.channel,
                )
            )
        return preview_rules

    def _coverage_preview_rows(self, current_snapshot: dict, preview_snapshot: dict) -> list[dict]:
        """Format current vs preview coverage metrics for markdown output."""
        return [
            {
                "Metric": "Mapped spend (30d)",
                "Current": f"${current_snapshot['matched_spend_30d']:,.2f}",
                "Preview": f"${preview_snapshot['matched_spend_30d']:,.2f}",
                "Delta": f"${preview_snapshot['matched_spend_30d'] - current_snapshot['matched_spend_30d']:,.2f}",
            },
            {
                "Metric": "Spend coverage",
                "Current": f"{current_snapshot['spend_coverage_pct']:.1f}%" if current_snapshot["spend_coverage_pct"] is not None else "вЂ”",
                "Preview": f"{preview_snapshot['spend_coverage_pct']:.1f}%" if preview_snapshot["spend_coverage_pct"] is not None else "вЂ”",
                "Delta": self._format_pct_delta(current_snapshot["spend_coverage_pct"], preview_snapshot["spend_coverage_pct"]),
            },
            {
                "Metric": "Attributed revenue (30d)",
                "Current": f"${current_snapshot['attributed_revenue_30d']:,.2f}",
                "Preview": f"${preview_snapshot['attributed_revenue_30d']:,.2f}",
                "Delta": f"${preview_snapshot['attributed_revenue_30d'] - current_snapshot['attributed_revenue_30d']:,.2f}",
            },
            {
                "Metric": "Revenue coverage",
                "Current": f"{current_snapshot['revenue_coverage_pct']:.1f}%" if current_snapshot["revenue_coverage_pct"] is not None else "вЂ”",
                "Preview": f"{preview_snapshot['revenue_coverage_pct']:.1f}%" if preview_snapshot["revenue_coverage_pct"] is not None else "вЂ”",
                "Delta": self._format_pct_delta(current_snapshot["revenue_coverage_pct"], preview_snapshot["revenue_coverage_pct"]),
            },
            {
                "Metric": "Matched keys",
                "Current": f"{current_snapshot['matched_keys']}/{current_snapshot['total_keys']}",
                "Preview": f"{preview_snapshot['matched_keys']}/{preview_snapshot['total_keys']}",
                "Delta": f"{preview_snapshot['matched_keys'] - current_snapshot['matched_keys']:+d}",
            },
        ]

    def _mapping_risk_review_rows(
        self,
        selected: list[AttributionAliasSuggestion],
        current_rules: list[AttributionRule],
        current_snapshot: dict,
    ) -> tuple[list[dict], list[str], str]:
        """Evaluate selected suggestions and return review rows plus summary warnings."""
        rows: list[dict] = []
        warnings: list[str] = []
        counts = {"high": 0, "medium": 0, "low": 0}

        for suggestion in selected:
            assessment = self._assess_suggestion_risk(
                suggestion,
                current_rules,
                float(current_snapshot.get("total_spend_30d") or 0),
                float(current_snapshot.get("total_revenue_30d") or 0),
            )
            counts[assessment["risk_level"]] += 1
            rows.append(
                {
                    "Scope": suggestion.scope,
                    "Alias": suggestion.alias,
                    "Canonical": suggestion.canonical_value,
                    "Risk": assessment["risk_level"],
                    "Flags": ", ".join(assessment["flags"]) if assessment["flags"] else "none",
                    "Why": assessment["why"],
                }
            )

        if counts["high"]:
            warnings.append(
                f"{counts['high']} selected suggestion(s) are flagged high risk; review rule-level flags before applying them."
            )
        elif counts["medium"]:
            warnings.append(
                f"{counts['medium']} selected suggestion(s) are flagged medium risk; apply incrementally if you want a safer rollout."
            )

        insight = (
            f"Risk mix: {counts['low']} low, {counts['medium']} medium, {counts['high']} high."
            if rows
            else ""
        )
        return rows or [{"Scope": "вЂ”", "Alias": "вЂ”", "Canonical": "вЂ”", "Risk": "none", "Flags": "none", "Why": "No selected suggestions."}], warnings, insight

    def _assess_suggestion_risk(
        self,
        suggestion: AttributionAliasSuggestion,
        current_rules: list[AttributionRule],
        total_spend_30d: float,
        total_revenue_30d: float,
    ) -> dict[str, object]:
        """Assign deterministic risk flags to one suggested mapping rule."""
        flags: list[str] = []
        reasons: list[str] = []
        score = 0

        if suggestion.confidence < 0.85:
            flags.append("low_confidence_match")
            reasons.append(f"confidence {suggestion.confidence:.2f} is below the safer 0.85 threshold")
            score += 2

        if suggestion.scope == "campaign" and self._token_overlap_ratio(suggestion.alias, suggestion.canonical_value) < 0.5:
            flags.append("weak_token_overlap")
            reasons.append("alias and canonical campaign share limited token overlap")
            score += 1

        if suggestion.scope == "campaign" and not suggestion.channel:
            flags.append("missing_channel_scope")
            reasons.append("campaign mapping has no explicit channel scope")
            score += 2

        if self._has_rule_collision(suggestion, current_rules):
            flags.append("existing_alias_conflict")
            reasons.append("alias overlaps with an existing mapping rule or canonical value")
            score += 3

        if suggestion.scope == "campaign" and suggestion.estimated_spend_30d <= 0:
            flags.append("no_spend_support")
            reasons.append("no recent paid spend was found for the proposed canonical campaign")
            score += 1

        if total_revenue_30d and suggestion.estimated_revenue_30d >= max(500.0, total_revenue_30d * 0.25):
            flags.append("high_revenue_impact")
            reasons.append("rule would affect a large share of recent attributed revenue")
            score += 1

        if total_spend_30d and suggestion.estimated_spend_30d >= max(250.0, total_spend_30d * 0.25):
            flags.append("high_spend_impact")
            reasons.append("rule would affect a large share of recent paid spend")
            score += 1

        risk_level = "high" if score >= 4 else "medium" if score >= 2 else "low"
        why = "; ".join(reasons) if reasons else "high confidence with bounded downstream impact"
        return {"risk_level": risk_level, "flags": flags, "why": why}

    def _has_rule_collision(
        self,
        suggestion: AttributionAliasSuggestion,
        current_rules: list[AttributionRule],
    ) -> bool:
        """Detect when a suggested alias could collide with an existing rule namespace."""
        alias_key = self._normalize_value(suggestion.alias)
        suggestion_channel = self._normalize_value(suggestion.channel)
        for rule in current_rules:
            if rule.scope != suggestion.scope:
                continue
            if self._normalize_value(rule.alias) == alias_key:
                if rule.canonical_value != suggestion.canonical_value or self._normalize_value(rule.channel) != suggestion_channel:
                    return True
            if self._normalize_value(rule.canonical_value) == alias_key and rule.canonical_value != suggestion.canonical_value:
                return True
        return False

    def _token_overlap_ratio(self, left: str, right: str) -> float:
        """Measure token overlap for campaign alias reviews."""
        left_tokens = set(self._tokenize(left))
        right_tokens = set(self._tokenize(right))
        if not left_tokens or not right_tokens:
            return 0.0
        return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)

    def _mapping_coverage_snapshot(
        self,
        spend_sources: list[str],
        events_table: str,
        revenue_event_type: str,
        rules: list[AttributionRule],
    ) -> dict | None:
        """Compute mapping coverage for a given in-memory rule set."""
        if not spend_sources:
            return None
        revenue_event_sql = revenue_event_type.replace("'", "''")
        has_campaign_grain = self._table_has_column(events_table, "utm_campaign")
        spend_union = self._build_spend_union(spend_sources)
        spend_channel_expr = canonical_sql("channel", "channel", rules)
        event_channel_expr = canonical_sql("utm_source", "channel", rules)
        spend_campaign_expr = canonical_sql("campaign", "campaign", rules, channel_sql="channel")
        event_campaign_expr = canonical_sql("utm_campaign", "campaign", rules, channel_sql="utm_source")
        spend_rollup = self._spend_rollup_sql(has_campaign_grain, spend_channel_expr, spend_campaign_expr)
        event_rollup = self._event_rollup_sql(events_table, revenue_event_sql, has_campaign_grain, event_channel_expr, event_campaign_expr)
        join_condition = "s.channel_key = e.channel_key AND s.campaign_key = e.campaign_key" if has_campaign_grain else "s.channel_key = e.channel_key"

        try:
            rows = self.connector.query(
                f"""
                WITH spend_data AS (
                    {spend_union}
                ),
                spend_rollup AS (
                    {spend_rollup}
                ),
                event_rollup AS (
                    {event_rollup}
                ),
                joined AS (
                    SELECT
                        s.spend_30d,
                        e.channel_key AS matched_channel_key,
                        COALESCE(e.revenue_30d, 0) AS attributed_revenue_30d
                    FROM spend_rollup s
                    LEFT JOIN event_rollup e ON {join_condition}
                ),
                total_revenue AS (
                    SELECT COALESCE(SUM(revenue), 0) AS total_revenue_30d
                    FROM {events_table}
                    WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
                      AND event_type = '{revenue_event_sql}'
                )
                SELECT
                    ROUND(COALESCE(SUM(spend_30d), 0), 2) AS total_spend_30d,
                    ROUND(COALESCE(SUM(CASE WHEN matched_channel_key IS NOT NULL THEN spend_30d ELSE 0 END), 0), 2) AS matched_spend_30d,
                    COUNT(*) AS total_keys,
                    SUM(CASE WHEN matched_channel_key IS NOT NULL THEN 1 ELSE 0 END) AS matched_keys,
                    ROUND(COALESCE(SUM(attributed_revenue_30d), 0), 2) AS attributed_revenue_30d,
                    t.total_revenue_30d
                FROM joined
                CROSS JOIN total_revenue t
                GROUP BY 6
                """
            )
        except Exception:
            return None

        if not rows:
            return None
        row = rows[0]
        total_spend = float(row.get("total_spend_30d") or 0)
        matched_spend = float(row.get("matched_spend_30d") or 0)
        total_revenue = float(row.get("total_revenue_30d") or 0)
        attributed_revenue = float(row.get("attributed_revenue_30d") or 0)
        return {
            "total_spend_30d": total_spend,
            "matched_spend_30d": matched_spend,
            "spend_coverage_pct": round(matched_spend / total_spend * 100, 1) if total_spend else None,
            "total_revenue_30d": total_revenue,
            "attributed_revenue_30d": attributed_revenue,
            "revenue_coverage_pct": round(attributed_revenue / total_revenue * 100, 1) if total_revenue else None,
            "matched_keys": int(row.get("matched_keys") or 0),
            "total_keys": int(row.get("total_keys") or 0),
        }

    def _best_channel_candidate(self, raw_source: str, channel_key: str, spend_catalog: list[dict]) -> dict | None:
        """Find the best likely channel target for one unmatched source."""
        if channel_key:
            matched = next((row for row in spend_catalog if str(row.get("channel_key") or "") == channel_key), None)
            if matched:
                return {
                    "channel": str(matched["channel"]),
                    "confidence": 0.95,
                    "suggest_channel_rule": False,
                }

        best: dict | None = None
        for row in spend_catalog:
            candidate_channel = str(row.get("channel") or "")
            score = self._similarity(raw_source, candidate_channel)
            if best is None or score > best["confidence"]:
                best = {
                    "channel": candidate_channel,
                    "confidence": score,
                    "suggest_channel_rule": True,
                }
        if best and best["confidence"] >= 0.78:
            return best
        return None

    def _best_campaign_candidate(self, raw_campaign: str, channel_hint: str, spend_catalog: list[dict]) -> dict | None:
        """Find the best likely campaign target within a channel."""
        candidates = [
            row for row in spend_catalog
            if str(row.get("channel") or "") == channel_hint or str(row.get("channel_key") or "") == channel_hint
        ]
        if not candidates:
            candidates = spend_catalog

        best: dict | None = None
        for row in candidates:
            candidate_campaign = str(row.get("campaign") or "")
            score = self._similarity(raw_campaign, candidate_campaign)
            if best is None or score > best["confidence"]:
                best = {
                    "campaign": candidate_campaign,
                    "channel": str(row.get("channel") or ""),
                    "spend_30d": float(row.get("spend_30d") or 0),
                    "confidence": score,
                }
        return best

    def _similarity(self, left: str, right: str) -> float:
        """Compute a simple similarity score for naming heuristics."""
        left_norm = self._normalize_value(left)
        right_norm = self._normalize_value(right)
        if not left_norm or not right_norm:
            return 0.0
        if left_norm == right_norm:
            return 1.0
        substring_score = 0.0
        if left_norm in right_norm or right_norm in left_norm:
            substring_score = min(len(left_norm), len(right_norm)) / max(len(left_norm), len(right_norm))

        left_tokens = set(self._tokenize(left))
        right_tokens = set(self._tokenize(right))
        token_score = len(left_tokens & right_tokens) / len(left_tokens | right_tokens) if left_tokens and right_tokens else 0.0
        return max(substring_score, token_score)

    @staticmethod
    def _format_pct_delta(current: float | None, preview: float | None) -> str:
        """Format a percent-point delta for preview outputs."""
        if current is None or preview is None:
            return "—"
        return f"{preview - current:+.1f} pp"

    @staticmethod
    def _tokenize(value: str) -> list[str]:
        """Tokenize a label into lowercase alphanumeric chunks."""
        cleaned = (
            value.lower()
            .replace("_", " ")
            .replace("-", " ")
            .replace("/", " ")
            .replace(".", " ")
        )
        return [token for token in cleaned.split() if token]

    def _table_has_column(self, table_name: str, column_name: str) -> bool:
        """Return True when a column exists in the given table."""
        try:
            columns = self.connector.query(f"DESCRIBE {table_name}")
        except Exception:
            return False
        for column in columns:
            name = column.get("column_name", column.get("Field", ""))
            if name == column_name:
                return True
        return False

    @staticmethod
    def _normalize_value(value: str) -> str:
        """Normalize Python-side values the same way as SQL expressions."""
        return (
            value.strip()
            .lower()
            .replace(" ", "")
            .replace("-", "")
            .replace("_", "")
            .replace("/", "")
            .replace(".", "")
        )
