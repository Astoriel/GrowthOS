"""Ingestion exports."""

from growth_os.ingestion.catalog import (
    discover_table,
    discover_tables,
    format_schema_for_prompt,
    inspect_freshness,
    validate_marketing_dataset,
)
from growth_os.ingestion.freshness import compute_freshness
from growth_os.ingestion.loaders import SourceRegistry
from growth_os.ingestion.mapping import apply_contract_aliases
from growth_os.ingestion.validators import validate_all_contracts, validate_contract

__all__ = [
    "SourceRegistry",
    "apply_contract_aliases",
    "compute_freshness",
    "discover_table",
    "discover_tables",
    "format_schema_for_prompt",
    "inspect_freshness",
    "validate_all_contracts",
    "validate_contract",
    "validate_marketing_dataset",
]
