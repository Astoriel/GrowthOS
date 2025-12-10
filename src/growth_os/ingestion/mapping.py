"""Runtime column alias mapping for ingested tables."""

from __future__ import annotations

import logging

from growth_os.connectors.duckdb import GrowthConnector
from growth_os.domain.contracts import CONTRACT_SPECS, TableContract
from growth_os.ingestion.catalog import discover_tables

logger = logging.getLogger(__name__)


def apply_contract_aliases(
    connector: GrowthConnector,
    contract_specs: dict | None = None,
) -> list[str]:
    """For each loaded table that matches a contract, rename alias columns to
    their canonical names if the canonical column is missing but an alias exists.

    Returns a list of human-readable strings describing each remapping applied.
    """
    specs = contract_specs if contract_specs is not None else CONTRACT_SPECS
    remappings: list[str] = []
    tables = {table.name: table for table in discover_tables(connector)}

    for contract_name, contract in specs.items():
        table = tables.get(contract_name)
        if table is None:
            continue

        present_columns = {col.name for col in table.columns}
        remappings.extend(
            _apply_aliases_for_table(connector, contract, contract_name, present_columns)
        )

    return remappings


def _apply_aliases_for_table(
    connector: GrowthConnector,
    contract: TableContract,
    table_name: str,
    present_columns: set[str],
) -> list[str]:
    """Apply alias renaming for a single table. Returns descriptions of applied remappings."""
    applied: list[str] = []

    for required, aliases in contract.aliases.items():
        if required in present_columns:
            continue  # canonical column already present — nothing to do

        for alias in aliases:
            if alias not in present_columns:
                continue

            # Rename the alias column to the canonical name
            try:
                connector.db.execute(
                    f'ALTER TABLE {table_name} RENAME COLUMN "{alias}" TO "{required}"'
                )
                msg = f"{table_name}: renamed `{alias}` → `{required}` (alias mapping)"
                logger.info("Alias mapping: %s", msg)
                applied.append(msg)
                present_columns.discard(alias)
                present_columns.add(required)
                break  # applied first matching alias — stop checking others
            except Exception as exc:
                logger.warning("Alias mapping failed for %s.%s: %s", table_name, alias, exc)

    return applied
