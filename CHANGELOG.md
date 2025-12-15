# Changelog

All notable changes to `GrowthOS` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-12-10

### Added
- Initial release of GrowthOS v0.1.0.
- **MCP Server Base**: 
  - Standardized request/response models.
  - Resource and Prompt definitions for MCP.
- **Analytics Metrics**: built-in SQL logic for:
  - Customer Acquisition Cost (CAC)
  - Lifetime Value (LTV)
  - Cohort Retention
  - Funnel Conversion Drop-off
  - Multi-touch Attribution (Shapley approximation)
  - Root Cause Analysis
- **Data Guardrails**:
  - AST-based SQL validation using `sqlglot`.
  - Blocks unsafe operations (`DROP`, `GRANT`, `COPY`).
  - Strict type enforcement returning pure Python primitives.
- **Connectors**:
  - `DuckDBConnector`: In-memory fast analytics.
  - `GrowthConnector` interface.
- **REST API**: FastAPI wrapper for direct querying outside MCP.
- Dockerfile and CI setup.

### Fixed
- Fixed Python type normalization edge cases (UUID, DateTime, Boolean) for valid JSON responses.
