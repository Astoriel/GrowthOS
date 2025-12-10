# GrowthOS Target Architecture

## Architecture Goal

The current codebase is a compact single-server MVP. The target architecture should preserve that simplicity while separating concerns:

- app lifecycle
- connectors and ingestion
- semantic metric definitions
- tool orchestration
- output formatting
- trust/observability

This keeps the project usable as OSS while making it extensible enough for business use.

## Target Shape

```text
src/growth_os/
  app/
    server.py
    lifespan.py
    registry.py
  config/
    settings.py
    profiles.py
    feature_flags.py
  domain/
    models.py
    enums.py
    contracts.py
    exceptions.py
  connectors/
    base.py
    duckdb.py
    csv.py
    postgres.py
    ga4.py
    meta_ads.py
    stripe.py
  ingestion/
    catalog.py
    loaders.py
    mapping.py
    validators.py
    freshness.py
  semantic/
    metrics.py
    funnels.py
    retention.py
    attribution.py
    benchmarks.py
  query/
    builder.py
    safety.py
    planner.py
    cache.py
  services/
    catalog_service.py
    analysis_service.py
    diagnostics_service.py
    reporting_service.py
  tools/
    discovery/
      list_tables.py
      describe_table.py
      inspect_freshness.py
    analysis/
      analyze_funnel.py
      compute_cac_ltv.py
      cohort_retention.py
      channel_attribution.py
      analyze_churn.py
      detect_anomalies.py
    reports/
      growth_summary.py
      weekly_brief.py
      executive_summary.py
    admin/
      validate_data.py
      list_connectors.py
  presentation/
    markdown.py
    cards.py
    warnings.py
    sections.py
  demo/
    sample_generator.py
    scenarios.py
  observability/
    logging.py
    tracing.py
    audit.py
  prompts/
    recipes.py
    personas.py
  testing/
    fixtures.py
    factories.py
    smoke.py
```

## Layer Responsibilities

### `app/`

Owns MCP server startup, tool registration, startup/shutdown lifecycle, and dependency wiring.

### `config/`

Owns settings, workspace profiles, and feature flags. Keep this Pydantic-based and typed.

### `domain/`

Owns shared business models such as:

- table metadata
- freshness status
- metric definitions
- tool result envelopes
- warning and trust metadata

### `connectors/`

Owns raw source access.

Examples:

- CSV source adapter
- PostgreSQL source adapter
- DuckDB session management
- future GA4 / ad platform connectors

These modules should not contain business analytics logic.

### `ingestion/`

Owns turning raw sources into a usable internal catalog.

Responsibilities:

- register source tables
- map source columns into expected semantic columns
- validate contracts
- compute freshness and completeness metadata

### `semantic/`

Owns the metric truth.

Responsibilities:

- funnel definitions
- CAC/LTV formulas
- retention rules
- attribution assumptions
- churn logic
- benchmark ranges

This layer should be test-heavy and independent from MCP.

### `query/`

Owns safe query construction and execution planning.

Responsibilities:

- build SQL from approved metric definitions
- validate custom SQL
- enforce read-only policies
- add row/time limits
- add query caching

### `services/`

Owns business workflows that tools call.

Examples:

- `AnalysisService.run_funnel_review(...)`
- `ReportingService.build_weekly_brief(...)`
- `DiagnosticsService.validate_dataset(...)`

This is where multi-step orchestration belongs.

### `tools/`

Very thin MCP wrappers. They should:

- validate inputs
- call service methods
- return formatted results

Tool modules should not embed large SQL strings directly.

### `presentation/`

Owns response formatting:

- markdown tables
- KPI cards
- warnings
- executive summaries
- action items

This should support at least two tones:

- demo/community mode
- business/executive mode

### `demo/`

Owns sample generation, current-date fixtures, and canned scenarios for onboarding.

### `observability/`

Owns structured logging, traces, audit events, and tool execution telemetry.

## Request Flow

```text
MCP Client
  -> app/server
  -> tool module
  -> service layer
  -> query/semantic/ingestion layers
  -> connectors + DuckDB
  -> presentation layer
  -> MCP response
```

## Recommended Technology Choices

### Keep

- `mcp` for server and transport layer
- `duckdb` as the local analytical engine
- `pydantic` + `pydantic-settings` for typed settings and contracts
- `pytest` + `ruff`

### Add

- `sqlglot` for AST-based SQL validation, parsing, and safer query checks
- `pandera` for dataframe-like schema validation in ingestion and demo checks
- `httpx` for future API-based connectors
- `tenacity` for retries on remote sources
- `opentelemetry` for traces and instrumentation

## Why These Choices

- `duckdb` remains the right center because it is easy to embed and can use in-memory or persisted connections
- `sqlglot` is better than string-based SQL blocking because it can parse SQL structure
- `pandera` gives an explicit validation layer for incoming tables
- `opentelemetry` gives a standard observability path if GrowthOS becomes a long-running service

## Runtime Modes

GrowthOS should support three modes:

### Mode 1: Local Demo

- bundled sample data
- zero setup
- current-date fixtures

### Mode 2: Local Team Use

- CSV/Postgres sources
- profile-based configuration
- weekly brief generation

### Mode 3: Managed/Remote

- remote MCP server
- shared connectors
- tracing/audit
- optional UI

## Data Trust Envelope

Every analytical response should include machine-readable trust metadata:

- source tables used
- date range covered
- freshness state
- missing columns or fallbacks
- assumptions used
- warnings and confidence

This should become part of the internal result model, not hand-written strings.

## Migration From Current Structure

Current to target mapping:

- `server.py` -> `app/server.py` + `app/registry.py`
- `config.py` -> `config/settings.py`
- `core/connector.py` -> `connectors/duckdb.py` + `query/safety.py`
- `core/metrics.py` -> `semantic/*` + `query/builder.py`
- `core/schema.py` -> `ingestion/catalog.py` + `domain/contracts.py`
- `core/formatters.py` -> `presentation/*`
- `data/mock_generator.py` -> `demo/sample_generator.py`

## Non-Goals For Now

Do not build these before P1 is stable:

- full BI dashboard suite
- custom visualization system
- multi-tenant auth platform
- broad no-code builder
- complex attribution science

First build a narrow, trusted analytics copilot.
