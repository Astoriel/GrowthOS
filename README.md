# GrowthOS

**Answer growth questions in plain English. No SQL required.**

GrowthOS is an open-source MCP analytics server that connects your AI assistant (Claude, Cursor, or any MCP-compatible client) directly to your marketing and product data. Ask questions about funnels, retention, CAC, churn, anomalies, channel performance, forecasts, Shapley attribution, and narrative summaries — and get structured, benchmark-referenced answers in seconds.

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Tests: 219 passing](https://img.shields.io/badge/tests-219%20passing-brightgreen)](#test-suite)
[![MCP tools: 49](https://img.shields.io/badge/MCP%20tools-49-blue)](#tool-reference)
[![DuckDB](https://img.shields.io/badge/powered%20by-DuckDB-yellow)](https://duckdb.org)

---

## Screenshots

| CAC & LTV by Channel | Funnel Analysis |
|---|---|
| ![CAC/LTV](docs/screenshots/demo_cac_ltv.png) | ![Funnel](docs/screenshots/demo_funnel.png) |

| Churn Segmentation | Weekly Growth Summary |
|---|---|
| ![Churn](docs/screenshots/demo_churn.png) | ![Growth Summary](docs/screenshots/demo_growth.png) |

**REST API (Swagger UI)**
![Swagger](docs/screenshots/swagger_with_data.png)

---

## What GrowthOS Does

GrowthOS sits between your AI assistant and your data. It exposes **49 MCP tools** that your AI can call to run real analysis — funnel breakdowns, CAC/LTV calculations, cohort retention matrices, anomaly detection, SQL queries, and more — then formats results with benchmark comparisons so you get context, not just numbers.

```
Claude / Cursor / any MCP client
        │
        ▼
┌──────────────────────────────┐
│        GrowthOS MCP Server   │
│  49 tools · DuckDB · SQL     │
│  Semantic layer · Benchmarks │
└────────────┬─────────────────┘
             │
   ┌──────────────────────┐
   │   Your data sources  │
   │  CSV · PostgreSQL     │
   │  Stripe · Meta Ads   │
   │  Google Ads · HubSpot│
   │  Mixpanel · Amplitude│
   └──────────────────────┘
```

---

## 5 Questions GrowthOS Answers Reliably

1. **"Why did CAC increase last week and is it sustainable?"**
   Channel-level CAC, LTV, and ROAS with automated invest/cut/watch classification.

2. **"Where is our funnel breaking and what should we test first?"**
   Step-by-step conversion rates with the biggest drop-off identified automatically.

3. **"How bad is our retention and how does it compare to benchmarks?"**
   Cohort retention matrix vs. B2B SaaS medians with Good / Average / Poor rating.

4. **"Which users are at risk of churning and how do we bring them back?"**
   Active / At-Risk / Churned segmentation using configurable inactivity thresholds.

5. **"What will our spend and signups look like in the next 30 days?"**
   Linear regression and exponential smoothing forecasts with confidence bands.

---

## Who It's For

- **Growth teams** who want fast, repeatable answers without waiting for a data analyst
- **Founders and PMs** reviewing weekly metrics in Claude, Cursor, or any AI
- **Marketing ops** managing multi-channel attribution across Google, Meta, Stripe, HubSpot, Mixpanel, Amplitude, and CSV exports

## Who It's Not For

- Teams that need dashboards or BI charts (use Metabase, Looker, etc.)
- Real-time streaming data (GrowthOS reads CSV/DuckDB snapshots)
- Enterprise RBAC or multi-tenant data isolation

---

## Quick Start

### Install

```bash
pip install growth-os
```

### Claude Desktop

Add to `~/.claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "growth-os": {
      "command": "uvx",
      "args": ["growth-os"],
      "env": {
        "GROWTH_DATA_DIR": "/path/to/your/csv/folder"
      }
    }
  }
}
```

### Cursor / Windsurf / any MCP client

```json
{
  "mcpServers": {
    "growth-os": {
      "command": "uvx",
      "args": ["growth-os"],
      "env": {
        "GROWTH_DATA_DIR": "./data"
      }
    }
  }
}
```

### From source

```bash
git clone https://github.com/Astoriel/GrowthOS.git
cd GrowthOS
pip install -e ".[dev]"
python -m growth_os.server
```

---

## Demo Mode — No Data Required

Skip `GROWTH_DATA_DIR` entirely. GrowthOS auto-generates realistic sample marketing data on startup — useful for exploring all 49 tools before connecting real data. Every connector (HubSpot, Mixpanel, Amplitude, Stripe, Meta, Google Ads) also runs in **demo mode** with synthetic mock data when credentials are absent.

Try it immediately:

```bash
pip install growth-os
# In Claude: "Give me a weekly growth summary"
# GrowthOS responds with real analysis on auto-generated sample data
```

---

## Data Sources and Connectors

### CSV Mode

Point `GROWTH_DATA_DIR` at a folder containing CSV files. GrowthOS loads all CSVs as queryable DuckDB tables.

```bash
export GROWTH_DATA_DIR=/path/to/csv/folder
```

Recommended file names (auto-detected schema):

| File | Key Columns |
|---|---|
| `marketing_spend.csv` | `date`, `channel`, `spend`, `impressions`, `clicks` |
| `user_events.csv` | `user_id`, `event_type`, `event_date`, `utm_source` |
| `campaigns.csv` | `campaign_id`, `channel`, `name`, `start_date`, `spend` |

Column names are flexible — use `suggest_attribution_mappings` to auto-map aliases.

### PostgreSQL Mode

```bash
export POSTGRES_URL=postgresql://user:pass@host:5432/dbname
```

GrowthOS attaches PostgreSQL as a **read-only DuckDB extension**. All queries are read-only; write/DDL operations are blocked at the AST level.

### Stripe

```bash
export STRIPE_API_KEY=sk_live_...
```

Syncs invoices, subscriptions, charges, and events. Call `sync_stripe_billing` from your AI assistant.

### Meta Ads

```bash
export META_ACCESS_TOKEN=...
export META_AD_ACCOUNT_ID=act_...
export META_API_VERSION=v21.0   # optional, default v21.0
```

Syncs campaigns and daily ad insights. Call `sync_meta_ads`.

### Google Ads

```bash
export GOOGLE_ADS_DEVELOPER_TOKEN=...
export GOOGLE_ADS_CUSTOMER_ID=...
export GOOGLE_ADS_LOGIN_CUSTOMER_ID=...   # manager account ID, if applicable
export GOOGLE_ADS_ACCESS_TOKEN=...
export GOOGLE_ADS_REFRESH_TOKEN=...
export GOOGLE_ADS_CLIENT_ID=...
export GOOGLE_ADS_CLIENT_SECRET=...
export GOOGLE_ADS_API_VERSION=v19         # optional, default v19
```

Syncs campaigns and click performance. Call `sync_google_ads`.

### HubSpot

Uses a **Private App access token** (not an API key). Create a Private App in HubSpot Settings → Integrations → Private Apps, and grant these scopes:

- `crm.objects.contacts.read`
- `crm.objects.deals.read`

```bash
export HUBSPOT_ACCESS_TOKEN=pat-eu1-...   # Private App token
export HUBSPOT_BASE_URL=https://api.hubapi.com  # optional
```

Call `sync_hubspot` to pull contacts and deals. Falls back to **demo mode** (100–200 mock rows) when `HUBSPOT_ACCESS_TOKEN` is unset.

### Mixpanel

Uses the **API Secret** from Project Settings (not the project token).

```bash
export MIXPANEL_API_SECRET=...          # Project API Secret
export MIXPANEL_PROJECT_ID=...          # Project numeric ID (optional)
export MIXPANEL_EU=true                 # true = EU data residency (default)
```

Call `sync_mixpanel` to pull event streams and funnel data. Funnel data falls back to mock if the account is on the free plan (HTTP 402). Falls back to **demo mode** (up to 500 mock events + 3 funnel templates) when `MIXPANEL_API_SECRET` is unset.

### Amplitude

Requires both the **API Key** and **Secret Key** from the Amplitude project settings (under General → API Keys). Note: the JavaScript SDK snippet key is a different key and will not work here.

```bash
export AMPLITUDE_API_KEY=...            # From Project Settings → API Keys
export AMPLITUDE_SECRET_KEY=...         # Secret key (required alongside API key)
export AMPLITUDE_EU=true                # true = EU data residency (default)
```

Call `sync_amplitude` to pull events and user cohorts. Falls back to **demo mode** (500 mock events + 8 mock cohorts) when either key is missing.

---

## REST API Server

Run GrowthOS as a standalone HTTP API alongside the MCP server:

```bash
pip install "growth-os[api]"
growth-os-api
```

The API server starts at `http://localhost:8000`. Swagger docs at `http://localhost:8000/docs`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Server health, loaded tables, timestamp |
| `POST` | `/query` | Execute a read-only SQL query |
| `GET` | `/tables` | List all loaded tables |
| `GET` | `/tables/{name}` | Schema and stats for a specific table |
| `GET` | `/analysis/funnel` | Funnel conversion rates |
| `GET` | `/analysis/cac-ltv` | CAC / LTV / ROAS by channel |
| `GET` | `/analysis/churn` | User churn segmentation |
| `GET` | `/analysis/anomalies` | Anomaly detection in any metric |
| `GET` | `/reports/growth-summary` | Weekly growth KPI summary |

---

## Sample Prompts

> All prompts work in demo mode or with real data.

**Weekly review:**
> "Give me a weekly growth summary comparing this week to last week."

**Funnel diagnosis:**
> "Analyze the signup → activation → purchase funnel and tell me where we're losing users."

**CAC/LTV:**
> "What's our CAC and LTV by channel? Which channels should we scale up?"

**Retention check:**
> "How is our month-1 retention trending and how does it compare to SaaS benchmarks?"

**Churn investigation:**
> "How many users churned last month and how many are at risk right now?"

**Anomaly investigation:**
> "Something looks off in our spend last Tuesday. Detect any anomalies in marketing_spend."

**Shapley attribution:**
> "Compute Shapley-value attribution across all channels for the last 90 days."

**Forecasting:**
> "Forecast our daily signups for the next 30 days using linear trend and exponential smoothing."

**Narrative review:**
> "Write a plain-English growth narrative with headline findings and a recommendation."

**Data drift alert:**
> "Alert me via webhook if spend drifts more than 15% compared to last week."

**HubSpot contacts:**
> "Sync our HubSpot contacts and show a breakdown by lifecycle stage."

**Profile setup:**
> "Save my current setup as a profile called 'production' for next session."

---

## Tool Reference

All 49 registered MCP tools:

### Discovery

| Tool | Description |
|---|---|
| `health_check` | Server status, loaded tables, DuckDB version, timestamp |
| `list_tables` | List all available data tables |
| `describe_table` | Schema, sample rows, and column statistics for any table |
| `run_query` | Execute a read-only SQL query (DuckDB dialect); supports `offset`/`limit` pagination |

### Analysis

| Tool | Description |
|---|---|
| `analyze_funnel` | Step-by-step funnel conversion rates |
| `compute_cac_ltv` | CAC, LTV, and ROAS by channel with benchmark comparison |
| `cohort_retention` | Cohort retention matrix (weekly or monthly) vs. B2B SaaS medians |
| `channel_attribution` | Attributed revenue and ROAS per channel |
| `analyze_churn` | User segmentation: Active / At-Risk / Churned |
| `detect_anomalies` | Spike/drop detection in any metric time series |

### Workflow Tools (Combined Multi-Step Analysis)

| Tool | Description |
|---|---|
| `funnel_diagnosis` | Funnel + churn in one response with prioritized next-step recommendations |
| `channel_efficiency_review` | CAC + LTV + ROAS with automated invest / cut / watch classification |
| `anomaly_explanation` | Anomalies + likely cause hypotheses + investigation actions |
| `detect_data_drift` | Compare current vs. previous period; flags shifts >20% |
| `funnel_ab_comparison` | Side-by-side funnel conversion for two time periods with Δ (pp) column |
| `drift_alert` | Detect metric drift and optionally fire a webhook notification |
| `scheduled_report_preview` | Preview weekly growth report and optionally deliver it via webhook |

### Forecasting

| Tool | Description |
|---|---|
| `forecast_metric` | Forecast any metric column using linear regression or exponential smoothing, with confidence bands |
| `forecast_growth_kpis` | Forecast spend and daily active users for the next N days |

### Reports

| Tool | Description |
|---|---|
| `growth_summary` | Weekly growth KPI summary with period-over-period comparison |
| `weekly_growth_review` | Full weekly growth review workflow |
| `executive_summary` | Board-level executive summary |
| `paid_growth_review` | Paid spend vs. Stripe revenue comparison |
| `campaign_performance_review` | Campaign ranking by spend, efficiency, and risk |
| `attribution_bridge_review` | Campaign-to-revenue attribution bridge |
| `narrative_growth_review` | Plain-English prose narrative with headline, findings, and recommendation |

### Data Quality

| Tool | Description |
|---|---|
| `validate_data` | Validate tables against GrowthOS schema contracts |
| `inspect_freshness` | Check data freshness across all loaded tables |
| `list_connectors` | List configured connectors and their readiness status |

### Attribution Mapping

| Tool | Description |
|---|---|
| `attribution_mapping_diagnostics` | Coverage gaps, unmapped keys, alias rule usage |
| `suggest_attribution_mappings` | Auto-suggest high-confidence alias rules from your data |
| `attribution_mapping_review_pack` | Read-only review: coverage deltas, risk flags, audit trail |
| `preview_apply_attribution_mappings` | Preview coverage lift before applying any changes |
| `apply_suggested_attribution_mappings` | Apply approved aliases (high-risk rules blocked unless `force=true`) |
| `review_attribution_mappings` | Inspect active rules and full audit history |
| `rollback_attribution_mappings` | Undo specific alias changes by ID |

### Integrations

| Tool | Description |
|---|---|
| `sync_stripe_billing` | Sync Stripe invoices, subscriptions, and events to local CSV |
| `stripe_revenue_summary` | Revenue summary from synced Stripe data |
| `sync_meta_ads` | Sync Meta Ads campaigns and daily insights |
| `meta_ads_summary` | Spend, clicks, impressions from synced Meta data |
| `sync_google_ads` | Sync Google Ads campaigns and click performance |
| `google_ads_summary` | Spend, clicks, conversions from synced Google Ads data |
| `sync_hubspot` | Sync HubSpot contacts and deals (demo mode when no token) |
| `hubspot_contacts_summary` | Contacts breakdown by lifecycle stage |
| `sync_mixpanel` | Sync Mixpanel events and funnels (demo mode when no secret) |
| `sync_amplitude` | Sync Amplitude events and user cohorts (demo mode when no keys) |

### Workspace Profiles

| Tool | Description |
|---|---|
| `save_workspace_profile` | Save named configuration (data dir, Postgres URL, custom metrics) |
| `load_workspace_profile` | Load and apply a saved profile |
| `list_workspace_profiles` | List all saved profiles |

---

## Workspace Profiles and Custom Metrics

Save any combination of data directory, Postgres URL, and business mode as a named profile:

```
> "Save my current setup as a profile called 'production'."
> "Load the 'production' profile."
```

Profiles support **custom metric definitions** — define SQL-based KPIs with benchmark thresholds and units that GrowthOS evaluates alongside built-in metrics:

```
> "Add a custom metric: 'avg_order_value' = revenue / orders, benchmark 45, unit USD."
```

---

## Architecture

```
src/growth_os/
├── app/
│   ├── server.py         # FastMCP server factory
│   ├── registry.py       # 49 MCP tool registrations
│   └── lifespan.py       # Startup sample data provisioning
├── api/
│   └── server.py         # FastAPI REST API (optional)
├── config/
│   ├── settings.py       # Pydantic-settings env config
│   └── profiles.py       # Workspace profiles + custom metrics
├── connectors/
│   ├── duckdb.py         # DuckDB engine + query cache
│   ├── csv.py            # CSV ingestion
│   ├── postgres.py       # PostgreSQL attach (read-only)
│   ├── stripe.py         # Stripe Billing API
│   ├── meta_ads.py       # Meta Marketing API
│   ├── google_ads.py     # Google Ads API
│   ├── hubspot.py        # HubSpot CRM v3 (contacts + deals)
│   ├── mixpanel.py       # Mixpanel Export + Funnels API
│   ├── amplitude.py      # Amplitude Export + Cohorts API
│   └── webhook.py        # Outbound webhook dispatcher
├── domain/
│   ├── models.py         # ToolEnvelope, shared models
│   ├── exceptions.py     # ConnectorError, QueryError
│   ├── enums.py          # FreshnessStatus, Severity, ChurnMode
│   └── contracts.py      # Schema contracts for validation
├── ingestion/
│   ├── catalog.py        # Table catalog
│   ├── freshness.py      # Freshness detection
│   ├── validators.py     # Data quality validators
│   ├── mapping.py        # Attribution mapping engine
│   └── loaders.py        # Data loading orchestration
├── observability/
│   ├── logging.py        # Structured JSON logging
│   ├── tracing.py        # @trace decorator
│   └── audit.py          # Append-only audit log
├── presentation/
│   └── markdown.py       # ToolEnvelope formatter + narrative builder
├── query/
│   ├── safety.py         # AST-based SQL validator (sqlglot)
│   └── builder.py        # Query builder
├── semantic/
│   ├── metrics.py        # CAC, LTV, churn (3 modes), ROAS
│   ├── benchmarks.py     # B2B SaaS benchmark data
│   ├── funnels.py        # Funnel step definitions
│   ├── retention.py      # Retention cohort logic
│   ├── attribution.py    # UTM attribution + Shapley values
│   └── forecasting.py    # Linear OLS + exponential smoothing
├── services/
│   ├── analysis_service.py
│   ├── catalog_service.py
│   ├── diagnostics_service.py
│   ├── reporting_service.py
│   ├── integration_service.py
│   ├── notification_service.py
│   └── forecasting_service.py
├── demo/
│   ├── sample_generator.py  # Synthetic marketing data
│   └── scenarios.py         # 4 curated demo scenarios
└── tools/
    ├── discovery/        # health_check, list_tables, describe_table, run_query
    ├── analysis/         # funnel, cac_ltv, retention, churn, anomalies
    ├── reports/          # growth_summary, executive, narrative, forecasting
    └── admin/            # profiles, connectors, attribution mapping
```

**Key design decisions:**

- **Read-only enforcement** — All SQL is validated by an AST parser (sqlglot) before execution. Any write, DDL, multi-statement, or PRAGMA operation is rejected.
- **Demo-first** — Every connector and every analysis runs on synthetic data with no configuration. Real credentials are optional.
- **Benchmark context** — Metrics are always compared to B2B SaaS median benchmarks (A16Z, OpenView) so raw numbers come with meaning.
- **Structured output** — All tools return `ToolEnvelope` objects rendered as clean markdown tables, not raw JSON blobs.

---

## Test Suite

219 tests across 14 test files, covering unit tests, integration tests, and smoke tests.

```bash
pytest tests/ -q --tb=short
# 219 passed in ~8s
```

### Test files

| File | What it covers |
|---|---|
| `test_app_smoke.py` | Server creation, tool registration, FastMCP integration |
| `test_connector.py` | HubSpot, Mixpanel, Amplitude — demo mode, real API mock, error paths |
| `test_demo_runtime.py` | Full demo scenario execution end-to-end |
| `test_formatters.py` | ToolEnvelope markdown rendering, narrative formatting, business mode |
| `test_google_ads_connector.py` | Google Ads mock API, pagination, error handling |
| `test_guardrails.py` | Attribution guardrails: high-risk rule blocking, force override |
| `test_ingestion.py` | CSV loading, freshness detection, schema validation, mapping engine |
| `test_meta_ads_connector.py` | Meta Ads API mock, rate limits, data normalization |
| `test_metrics.py` | CAC/LTV/ROAS computation, churn modes, Shapley attribution |
| `test_profiles.py` | Save/load/list/delete profiles, custom metric definitions |
| `test_schema.py` | Schema contracts, data validation, column type inference |
| `test_sql_safety.py` | SQL AST validator — 40+ blocked patterns, allowed queries |
| `test_stripe_connector.py` | Stripe invoice/subscription sync, pagination, normalization |
| `test_workflow_tools.py` | funnel_diagnosis, channel_efficiency_review, anomaly_explanation |

### Real API connectivity tests

Connectors were tested against live APIs in development. Results:

| Connector | Auth | Data | Notes |
|---|---|---|---|
| **HubSpot** (`pat-eu1-...`) | ✅ Token accepted | Requires CRM scopes | Add `crm.objects.contacts.read` + `crm.objects.deals.read` to Private App |
| **Mixpanel** | ✅ Auth confirmed | 0 events (empty project) | Funnels fallback to mock on free plan (HTTP 402) |
| **Amplitude** | ✅ Auth confirmed | 6 real cohorts fetched | Export 404 = no events tracked yet |
| **Stripe** | ✅ (mock) | Full invoice/subscription sync | Tested against Stripe test mode |
| **Meta Ads** | ✅ (mock) | Campaign + insights sync | Tested against Graph API mock |
| **Google Ads** | ✅ (mock) | Campaign + click sync | Full OAuth flow tested |

---

## Configuration Reference

All settings are environment variables (case-insensitive):

### Core

| Variable | Default | Description |
|---|---|---|
| `GROWTH_DATA_DIR` | _(none)_ | Path to folder with CSV files |
| `POSTGRES_URL` | _(none)_ | PostgreSQL connection URL (read-only) |
| `DB_PATH` | _(in-memory)_ | DuckDB file path (set to persist between runs) |
| `SERVER_NAME` | `growth-os` | MCP server display name |
| `MAX_QUERY_ROWS` | `10000` | Maximum rows returned per query |
| `QUERY_TIMEOUT_SECONDS` | `30` | Query execution timeout |
| `DEFAULT_TRANSPORT` | `stdio` | MCP transport: `stdio` or `sse` |
| `BUSINESS_MODE` | `false` | Enable business-friendly output mode |
| `SEMANTIC_PROFILE_PATH` | _(none)_ | Path to semantic profile JSON |

### Stripe

| Variable | Default | Description |
|---|---|---|
| `STRIPE_API_KEY` | _(none)_ | Stripe secret key (`sk_live_...` or `sk_test_...`) |
| `STRIPE_BASE_URL` | `https://api.stripe.com/v1` | Override for testing |

### Meta Ads

| Variable | Default | Description |
|---|---|---|
| `META_ACCESS_TOKEN` | _(none)_ | User access token |
| `META_AD_ACCOUNT_ID` | _(none)_ | Ad account ID (`act_...`) |
| `META_API_VERSION` | `v21.0` | Graph API version |
| `META_BASE_URL` | `https://graph.facebook.com` | Override for testing |

### Google Ads

| Variable | Default | Description |
|---|---|---|
| `GOOGLE_ADS_DEVELOPER_TOKEN` | _(none)_ | Developer token |
| `GOOGLE_ADS_CUSTOMER_ID` | _(none)_ | Customer (CID) without dashes |
| `GOOGLE_ADS_LOGIN_CUSTOMER_ID` | _(none)_ | Manager account CID (if applicable) |
| `GOOGLE_ADS_ACCESS_TOKEN` | _(none)_ | OAuth access token |
| `GOOGLE_ADS_REFRESH_TOKEN` | _(none)_ | OAuth refresh token |
| `GOOGLE_ADS_CLIENT_ID` | _(none)_ | OAuth client ID |
| `GOOGLE_ADS_CLIENT_SECRET` | _(none)_ | OAuth client secret |
| `GOOGLE_ADS_API_VERSION` | `v19` | Ads API version |

### HubSpot

| Variable | Default | Description |
|---|---|---|
| `HUBSPOT_ACCESS_TOKEN` | _(none)_ | Private App token (`pat-eu1-...`). Omit = demo mode |
| `HUBSPOT_BASE_URL` | `https://api.hubapi.com` | Override for testing |

### Mixpanel

| Variable | Default | Description |
|---|---|---|
| `MIXPANEL_API_SECRET` | _(none)_ | Project API Secret. Omit = demo mode |
| `MIXPANEL_PROJECT_ID` | _(none)_ | Project numeric ID (optional) |
| `MIXPANEL_EU` | `true` | Use EU data residency endpoints |

### Amplitude

| Variable | Default | Description |
|---|---|---|
| `AMPLITUDE_API_KEY` | _(none)_ | Project API key. **Both keys required** for live mode |
| `AMPLITUDE_SECRET_KEY` | _(none)_ | Project Secret key |
| `AMPLITUDE_EU` | `true` | Use EU data residency endpoints |

---

## Docker

```bash
# Build
docker build -t growth-os .

# Demo mode (no credentials)
docker run -p 8000:8000 growth-os

# With CSV data
docker run -e GROWTH_DATA_DIR=/data -v /local/csv:/data growth-os

# With PostgreSQL
docker run -e POSTGRES_URL=postgresql://user:pass@host:5432/db growth-os

# With Stripe and HubSpot
docker run \
  -e STRIPE_API_KEY=sk_live_... \
  -e HUBSPOT_ACCESS_TOKEN=pat-eu1-... \
  growth-os
```

---

## Limitations

- **Read-only enforcement.** GrowthOS enforces an AST-based SQL sandbox (sqlglot) that blocks any write, DDL, or multi-statement operation. You cannot accidentally modify data.
- **Attribution is approximate.** UTM-based attribution has known gaps: direct traffic, cross-device, and offline conversions are not captured.
- **Forecasts are statistical, not causal.** Linear and exponential smoothing forecasts extrapolate historical trends — they do not account for seasonality, planned campaigns, or market shocks. Use for directional guidance only.
- **Benchmarks are medians.** Industry benchmarks (B2B SaaS, A16Z, OpenView) reflect median-performing companies. Valid reasons to be above or below exist for every metric.
- **Data freshness matters.** Stale CSVs produce stale answers. Always run `inspect_freshness` before drawing conclusions from time-sensitive analyses.
- **Connector demo mode produces synthetic data.** Mock rows use fixed random seeds so output is deterministic and will not match your actual business metrics.
- **HubSpot scope requirement.** The Private App must have `crm.objects.contacts.read` and `crm.objects.deals.read` scopes. Without these scopes the connector returns HTTP 403 even with a valid token.
- **Mixpanel free plan.** The Funnels API returns HTTP 402 on free-tier projects. GrowthOS automatically falls back to synthetic funnel data in this case.
- **Amplitude key types.** Only the server-side API Key + Secret Key pair works for the Export API. The JavaScript SDK snippet key will not authenticate.

---

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -q --tb=short

# Lint
python -m ruff check src/ tests/

# Verify server starts cleanly
python -c "from growth_os.app.server import create_mcp_server; create_mcp_server()"
```

### Running the REST API in development

```bash
pip install -e ".[api]"
growth-os-api
# Swagger: http://localhost:8000/docs
```

---

## Docs

- [Product Roadmap](docs/PRODUCT_ROADMAP.md)
- [Target Architecture](docs/TARGET_ARCHITECTURE.md)
- [Implementation TODO](docs/IMPLEMENTATION_TODO.md)

---

## License

MIT — see [LICENSE](LICENSE).
