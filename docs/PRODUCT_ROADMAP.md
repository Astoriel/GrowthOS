# GrowthOS Product Roadmap

## Product Goal

GrowthOS should evolve from a "marketing analytics MCP demo" into an AI-native growth analytics copilot for founders, growth leads, and performance marketers.

The product should help a user answer five questions reliably:

1. What changed?
2. Why did it change?
3. Which channel or segment caused it?
4. What should we do next?
5. Can I trust this answer?

## Ideal User Profile

### Primary ICP

- B2B SaaS founder or growth lead
- Has CSV exports, PostgreSQL, or warehouse tables
- Wants fast answers in chat without writing SQL manually
- Needs funnel, CAC/LTV, retention, anomalies, and weekly summaries

### Secondary ICP

- Agencies and analytics consultants
- AI/MCP enthusiasts who want a practical analytics server
- Product and lifecycle teams with event data in a clean schema

## What "Complete" Means

GrowthOS is only "complete" when it has all of these layers:

- Reliable runtime: the server starts, tools work, versions are pinned/tested
- Data contracts: supported schemas are explicit and validated
- Real connectors: users do not have to hand-clean everything before use
- Semantic layer: business metrics are defined centrally and consistently
- Trust layer: freshness, assumptions, warnings, lineage, and auditability
- Action layer: outputs explain not only metrics, but recommended next steps
- Team readiness: alerts, scheduled briefs, configuration profiles, observability

## Product Packaging

### OSS Core

- Local MCP server
- CSV + PostgreSQL ingestion
- DuckDB execution engine
- Semantic metrics for funnel, CAC/LTV, retention, attribution, churn
- Sample data that always matches current dates
- Safe query execution and schema discovery
- Prompt recipes and example workflows
- Tests, smoke tests, and MCP inspector support

### Business/Team Add-ons

- GA4, Meta Ads, Google Ads, Stripe, HubSpot connectors
- Saved profiles per workspace/client
- Scheduled weekly brief and anomaly alerts
- Data freshness checks and warning banners
- Audit trail for executed tools and queries
- Benchmarks by business model
- Optional thin web UI for reports and configuration

## Priority Plan

## P0 - Make It Real And Trustworthy

Target: 1-2 weeks

Outcome:

- GrowthOS starts reliably
- Demo mode shows real value today
- README and docs clearly explain who the product is for

Deliverables:

- Fix MCP runtime compatibility and pin/test supported versions
- Add server startup smoke tests
- Regenerate sample data relative to current date
- Add schema contract docs for required columns
- Add warnings when data is stale, incomplete, or assumptions are weak
- Rewrite README hero, use cases, sample prompts, and limitations

Definition of done:

- `python -m growth_os.server` works in a clean environment
- Demo tools return believable, non-zero results today
- First-time user can get value in under 5 minutes

## P1 - Make It Needed

Target: 3-6 weeks

Outcome:

- The product solves a repeated workflow for a specific user type
- Results are more trustworthy and more actionable

Deliverables:

- Introduce semantic metric definitions
- Add source adapters and schema mapping
- Add data validation and freshness checks
- Add saved prompt recipes: funnel diagnosis, weekly growth review, channel efficiency review
- Add better narratives: "what changed / likely causes / recommended actions"
- Add two high-value connectors

Recommended first connectors:

- Stripe or billing source
- One acquisition source: Meta Ads or Google Ads

Definition of done:

- A founder or growth lead can connect data and answer weekly growth questions without editing SQL
- Output includes metric context, assumptions, and next actions

## P2 - Make It Team-Ready

Target: 6-12 weeks

Outcome:

- GrowthOS becomes usable for ongoing work, not only ad hoc analysis

Deliverables:

- Scheduled briefs and anomaly alerts
- Workspace profiles and per-source config
- Query/result caching
- Observability, audit trail, structured logs, traces
- Optional remote server mode
- Optional lightweight web UI for setup, health, and report history

Definition of done:

- Teams can run GrowthOS continuously and trust it for recurring reporting

## P3 - Make It Differentiated

Target: after P2

Outcome:

- GrowthOS feels like a product, not only a toolkit

Deliverables:

- Benchmark packs by business type
- Industry-specific recipes
- Root-cause analysis workflows
- Suggested experiments and prioritization
- Multi-source attribution models
- Segment templates for SaaS, ecommerce, and lifecycle growth

## What To Build First

Do not try to build "all analytics."

Build the narrowest useful wedge:

- Weekly growth review
- Funnel diagnosis
- Channel efficiency review

If these three workflows are excellent, the product becomes useful.
If these three workflows are weak, more tools will not save it.

## Success Metrics

### Product metrics

- Time to first useful answer under 5 minutes
- Time to setup under 15 minutes for CSV/Postgres users
- At least 3 repeat workflows per user per week
- Demo outputs are valid without manual fixing

### Quality metrics

- Startup smoke tests always pass
- No broken supported-version installs
- Every tool either returns a useful answer or a clear trust warning
- Every analytical output cites source tables and date ranges

## Recommended Build Order

1. Runtime and demo reliability
2. Schema contracts and data trust
3. ICP-focused workflows
4. Connectors
5. Scheduling, observability, and team features
