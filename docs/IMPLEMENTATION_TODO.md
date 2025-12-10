# GrowthOS Implementation TODO

## Phase 0 - Stabilize The Existing MVP

- [ ] Fix `FastMCP` initialization against the currently supported Python SDK API.
- [ ] Pin supported `mcp` versions and document the compatibility matrix.
- [ ] Add a server startup smoke test that imports and runs the app bootstrap path.
- [ ] Add CI tasks for `pytest`, lint, and startup smoke test.
- [ ] Make the sample data generator relative to the current date instead of fixed 2025 dates.
- [ ] Add tests proving that summary, churn, and anomaly tools produce believable demo outputs today.
- [ ] Split runtime errors into user-facing errors vs internal exceptions.
- [ ] Add a simple health/self-check command for local debugging.

## Phase 1 - Define The Product Contract

- [ ] Write a supported-schema spec for `marketing_spend`, `user_events`, and `campaigns`.
- [ ] Define required columns, optional columns, aliases, and fallback logic.
- [ ] Introduce typed domain models for table metadata, freshness, warnings, and tool results.
- [ ] Add dataset validation output: missing columns, inferred types, freshness, row counts.
- [ ] Add clear warnings when metrics are computed with assumptions or partial data.
- [ ] Add date-range reporting to every tool response.

## Phase 2 - Restructure The Codebase

- [ ] Create the new package layout: `app`, `config`, `domain`, `connectors`, `ingestion`, `semantic`, `query`, `services`, `presentation`, `observability`, `demo`.
- [ ] Move `server.py` logic into `app/server.py` and `app/registry.py`.
- [ ] Move settings into `config/settings.py`.
- [ ] Move formatting into `presentation/`.
- [ ] Move sample generation into `demo/`.
- [ ] Extract direct SQL templates into semantic/query modules.
- [ ] Keep backward-compatible imports during the migration to avoid breaking users.

## Phase 3 - Replace Naive SQL Safety

- [ ] Add AST-based SQL parsing and validation.
- [ ] Only allow a safe subset for custom queries.
- [ ] Enforce single-statement read-only rules structurally, not via string matching.
- [ ] Add limits for rows, scan size, and timeout behavior.
- [ ] Add regression tests for dangerous, malformed, and edge-case queries.

## Phase 4 - Build A Real Ingestion Layer

- [ ] Add source registry for CSV and PostgreSQL connectors.
- [ ] Implement column alias mapping into canonical semantic names.
- [ ] Add ingestion validation with schema and type checks.
- [ ] Add freshness status per table.
- [ ] Add source metadata inspection and validation tools.
- [ ] Add persisted DuckDB option for local caching and repeated usage.

## Phase 5 - Build The Semantic Layer

- [ ] Define canonical metrics and dimensions.
- [ ] Define funnel logic with explicit step semantics.
- [ ] Define churn modes: event-based, inactivity-based, subscription-based.
- [ ] Define attribution modes and their assumptions.
- [ ] Define benchmark metadata and confidence notes.
- [ ] Add high-quality tests for every metric definition.

## Phase 6 - Upgrade The Tool Experience

- [ ] Make all tool wrappers thin and service-driven.
- [ ] Standardize tool outputs around five sections: summary, table/cards, warnings, likely causes, recommended actions.
- [ ] Add a trust footer with sources and date range.
- [ ] Add an executive mode with reduced emoji and cleaner presentation.
- [ ] Add the `weekly growth review` workflow tool.
- [ ] Add the `funnel diagnosis` workflow tool.
- [ ] Add the `channel efficiency review` workflow tool.
- [ ] Add the `anomaly explanation` workflow tool.

## Phase 7 - Add The First Real Connectors

- [ ] Choose the first billing/revenue source.
- [ ] Recommended first billing connector: Stripe.
- [ ] Choose the first paid acquisition source.
- [ ] Recommended first acquisition connector: Meta Ads or Google Ads.
- [ ] Build connector interfaces and auth/config contracts.
- [ ] Normalize connector outputs into canonical tables.
- [ ] Add connector-specific tests and sample fixtures.

## Phase 8 - Add Trust And Observability

- [ ] Add structured logs for tool calls and query execution.
- [ ] Add traces around ingestion, semantic planning, query execution, and formatting.
- [ ] Add audit events for source usage and tool execution.
- [ ] Add debug diagnostics for failed metric runs.
- [ ] Add latency and error metrics.

## Phase 9 - Add Recurring Workflows

- [ ] Add saved workspace profiles.
- [ ] Add scheduled weekly brief generation.
- [ ] Add anomaly alert jobs.
- [ ] Add output persistence for latest reports.
- [ ] Add report templates for founders and growth leads.

## Phase 10 - Improve Docs And Positioning

- [ ] Rewrite the README around user outcome, not tool inventory.
- [ ] Add sample prompts and sample outputs.
- [ ] Add architecture and roadmap docs links.
- [ ] Add setup guides for demo, CSV, and PostgreSQL modes.
- [ ] Add a "who this is for / not for" section.
- [ ] Add limitations and trust disclaimers.

## Suggested Delivery Sequence

### Week 1

- [ ] Runtime fix
- [ ] version pinning
- [ ] sample data date fix
- [ ] smoke tests
- [ ] README rewrite

### Week 2

- [ ] schema contracts
- [ ] validation tool
- [ ] trust warnings
- [ ] first response-format standardization

### Week 3-4

- [ ] codebase restructuring
- [ ] service layer extraction
- [ ] AST-based query safety

### Week 5-6

- [ ] semantic layer cleanup
- [ ] workflow tools
- [ ] better outputs and weekly brief

### Week 7-10

- [ ] first real connectors
- [ ] caching
- [ ] observability
- [ ] scheduled workflows

## Acceptance Criteria For "Needed And Complete"

- [ ] New user reaches first useful answer in less than 5 minutes.
- [ ] Demo mode works correctly on the current date.
- [ ] Server startup is covered by automated tests.
- [ ] Supported schemas are explicit and validated.
- [ ] Core workflows require no manual SQL.
- [ ] Every important response contains trust metadata.
- [ ] At least one revenue source and one acquisition source work end to end.
